package it.pagopa.ecommerce.eventdispatcher.queues.v2.helpers

import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationRequestedData
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.Transaction
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.TransactionsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.UserStatsServiceClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.ecommerce.eventdispatcher.queues.v2.*
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.AuthorizationStateRetrieverRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import java.time.Instant
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.switchIfEmpty

/**
 * This helper implements the business logic related to handling calling `getState` from NPG. In
 * particular, the [getAuthorizationState] method does the following:
 * - checks for the transaction current status
 * - determines whether the transaction was requesting authorization via NPG
 * - calls NPG's `getSTate`
 * - calls transactions-service PATCH auth-request
 * - enqueues a retry event in case of error
 */
@Component
class AuthorizationRequestedHelper(
  @Autowired private val transactionsServiceClient: TransactionsServiceClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val authorizationStateRetrieverRetryService: AuthorizationStateRetrieverRetryService,
  @Autowired private val authorizationStateRetrieverService: AuthorizationStateRetrieverService,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider,
  @Autowired private val userStatsServiceClient: UserStatsServiceClient
) {

  var logger: Logger = LoggerFactory.getLogger(AuthorizationRequestedHelper::class.java)

  private fun isWalletPayment(
    baseTransactionWithRequestedAuthorization: BaseTransactionWithRequestedAuthorization
  ) =
    (baseTransactionWithRequestedAuthorization.transactionAuthorizationRequestData
      .transactionGatewayAuthorizationRequestedData) is
      NpgTransactionGatewayAuthorizationRequestedData &&
      (baseTransactionWithRequestedAuthorization.transactionAuthorizationRequestData
          .transactionGatewayAuthorizationRequestedData
          as NpgTransactionGatewayAuthorizationRequestedData)
        .walletInfo != null

  private fun getWalletIdPayment(
    baseTransactionWithRequestedAuthorization: BaseTransactionWithRequestedAuthorization
  ) =
    (baseTransactionWithRequestedAuthorization.transactionAuthorizationRequestData
        .transactionGatewayAuthorizationRequestedData
        as NpgTransactionGatewayAuthorizationRequestedData)
      .walletInfo
      ?.walletId

  private fun getPaymentMethodId(
    baseTransactionWithRequestedAuthorization: BaseTransactionWithRequestedAuthorization
  ) =
    (baseTransactionWithRequestedAuthorization.transactionAuthorizationRequestData)
      .paymentInstrumentId

  private fun getLastUsageMethodId(
    baseTransactionWithRequestedAuthorization: BaseTransactionWithRequestedAuthorization
  ) =
    when (isWalletPayment(baseTransactionWithRequestedAuthorization)) {
      true -> getWalletIdPayment(baseTransactionWithRequestedAuthorization)
      false -> getPaymentMethodId(baseTransactionWithRequestedAuthorization)
    }

  fun authorizationStateRetrieve(
    parsedEvent:
      Either<
        QueueEvent<TransactionAuthorizationRequestedEvent>,
        QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>,
    checkPointer: Checkpointer
  ): Mono<Unit> {
    val tracingInfo = getTracingInfo(parsedEvent)
    val transactionId = getTransactionId(parsedEvent)
    val retryCount = getRetryCount(parsedEvent)
    val creationDate = getCreationDate(parsedEvent)
    val saveLastUsage = isAuthRequestEvent(parsedEvent)
    val baseTransaction =
      Mono.defer {
        transactionsEventStoreRepository
          .findByTransactionIdOrderByCreationDateAsc(transactionId)
          .reduce(EmptyTransaction(), Transaction::applyEvent)
          .cast(BaseTransaction::class.java)
      }

    val authorizationRequestedPipeline =
      baseTransaction
        .filter { it.status == TransactionStatusDto.AUTHORIZATION_REQUESTED }
        .switchIfEmpty {
          baseTransaction.flatMap {
            logger.info("Transaction [$transactionId] status [${it.status}]. No action needed")
            Mono.empty()
          }
        }
        .cast(BaseTransactionWithRequestedAuthorization::class.java)
        .doOnNext { t ->
          if (saveLastUsage && isAuthenticatedTransaction(t)) {
            // TODO here call userStatsService
            userStatsServiceClient.saveLastUsage(
              t.transactionActivatedData.userId!!,
              UUID.fromString(getLastUsageMethodId(t)),
              Instant.parse(creationDate),
              isWalletPayment(t))
          }
        }
        .filter {
          it.transactionAuthorizationRequestData.paymentGateway ==
            TransactionAuthorizationRequestData.PaymentGateway.NPG
        }
        .switchIfEmpty {
          baseTransaction.flatMap {
            logger.info(
              "Transaction [$transactionId] has not been authorized via NPG gateway. No action needed")
            Mono.empty()
          }
        }
        .doOnNext {
          logger.info(
            "Handling get state request for transaction with id ${it.transactionId.value()}")
        }
        .flatMap { tx ->
          handleGetStateByPatchTransactionService(
            tx = tx,
            authorizationStateRetrieverRetryService = authorizationStateRetrieverRetryService,
            authorizationStateRetrieverService = authorizationStateRetrieverService,
            transactionsServiceClient = transactionsServiceClient,
            tracingInfo = tracingInfo,
            retryCount = retryCount)
        }
    return runTracedPipelineWithDeadLetterQueue(
      checkPointer,
      authorizationRequestedPipeline,
      QueueEvent(parsedEvent.fold({ it }, { it }).event, tracingInfo),
      deadLetterTracedQueueAsyncClient,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }

  private fun getTracingInfo(
    event:
      Either<
        QueueEvent<TransactionAuthorizationRequestedEvent>,
        QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>
  ): TracingInfo {
    return event.fold({ it.tracingInfo }, { it.tracingInfo })
  }

  private fun isAuthRequestEvent(
    event:
      Either<
        QueueEvent<TransactionAuthorizationRequestedEvent>,
        QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>
  ): Boolean {
    return event.fold({ true }, { false })
  }

  private fun isAuthenticatedTransaction(
    baseTransactionWithRequestedAuthorization: BaseTransactionWithRequestedAuthorization
  ) = baseTransactionWithRequestedAuthorization.transactionActivatedData.userId != null

  private fun getCreationDate(
    event:
      Either<
        QueueEvent<TransactionAuthorizationRequestedEvent>,
        QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>
  ): String {
    return event.fold({ it.event.creationDate }, { it.event.creationDate })
  }

  private fun getTransactionId(
    event:
      Either<
        QueueEvent<TransactionAuthorizationRequestedEvent>,
        QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>
  ): String {
    return event.fold({ it.event.transactionId }, { it.event.transactionId })
  }

  private fun getRetryCount(
    event:
      Either<
        QueueEvent<TransactionAuthorizationRequestedEvent>,
        QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>
  ): Int {
    return event.fold({ 0 }, { it.event.data.retryCount })
  }
}
