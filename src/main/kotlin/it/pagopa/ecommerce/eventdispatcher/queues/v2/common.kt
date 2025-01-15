package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.client.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.activation.NpgTransactionGatewayActivationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.*
import it.pagopa.ecommerce.commons.documents.v2.refund.EmptyGatewayRefundData
import it.pagopa.ecommerce.commons.documents.v2.refund.GatewayRefundData
import it.pagopa.ecommerce.commons.documents.v2.refund.NpgGatewayRefundData
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v2.TransactionWithClosureError
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.exceptions.NpgResponseException
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.*
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.commons.utils.NpgClientUtils
import it.pagopa.ecommerce.eventdispatcher.client.TransactionsServiceClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.ecommerce.eventdispatcher.queues.v2.QueueCommonsLogger.logger
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.RefundService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.AuthorizationStateRetrieverRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.generated.ecommerce.redirect.v1.dto.RefundOutcomeDto
import it.pagopa.generated.transactionauthrequests.v1.dto.OutcomeNpgGatewayDto
import it.pagopa.generated.transactionauthrequests.v1.dto.TransactionInfoDto
import it.pagopa.generated.transactionauthrequests.v1.dto.UpdateAuthorizationRequestDto
import java.math.BigDecimal
import java.time.*
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono

object QueueCommonsLogger {
  val logger: Logger = LoggerFactory.getLogger(QueueCommonsLogger::class.java)
}

fun updateTransactionToExpired(
  transaction: BaseTransaction,
  transactionsExpiredEventStoreRepository: TransactionsEventStoreRepository<TransactionExpiredData>,
  transactionsViewRepository: TransactionsViewRepository
): Mono<BaseTransaction> {

  return transactionsExpiredEventStoreRepository
    .save(
      TransactionExpiredEvent(
        transaction.transactionId.value(), TransactionExpiredData(transaction.status)))
    .map {
      (transaction as it.pagopa.ecommerce.commons.domain.v2.Transaction).applyEvent(it)
        as BaseTransaction
    }
    .flatMap {
      transactionsViewRepository
        .findByTransactionId(transaction.transactionId.value())
        .cast(Transaction::class.java)
        .flatMap { tx ->
          tx.status = getExpiredTransactionStatus(transaction)
          transactionsViewRepository.save(tx)
        }
        .thenReturn(it)
    }
    .doOnSuccess {
      logger.info("Transaction expired for transaction ${transaction.transactionId.value()}")
    }
    .doOnError {
      logger.error(
        "Transaction expired error for transaction ${transaction.transactionId.value()} : ${it.message}")
    }
}

fun getExpiredTransactionStatus(transaction: BaseTransaction): TransactionStatusDto =
  when (transaction) {
    is BaseTransactionWithRequestedAuthorization -> TransactionStatusDto.EXPIRED
    is BaseTransactionWithCancellationRequested -> TransactionStatusDto.CANCELLATION_EXPIRED
    is TransactionWithClosureError ->
      transaction
        .transactionAtPreviousState()
        .map {
          it.fold({ TransactionStatusDto.CANCELLATION_EXPIRED }, { TransactionStatusDto.EXPIRED })
        }
        .orElse(TransactionStatusDto.EXPIRED)
    else -> TransactionStatusDto.EXPIRED_NOT_AUTHORIZED
  }

fun getTransactionAuthorizationRequestData(
  tx: BaseTransaction
): TransactionAuthorizationRequestData? {
  return when (tx) {
    is BaseTransactionWithRequestedAuthorization -> tx.transactionAuthorizationRequestData
    is TransactionWithClosureError ->
      getTransactionAuthorizationRequestData(tx.transactionAtPreviousState)
    else -> null
  }
}

fun updateTransactionToRefundRequested(
  transaction: BaseTransaction,
  transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
  transactionGatewayAuthorizationData: TransactionGatewayAuthorizationData? = null
): Mono<Pair<BaseTransactionWithRefundRequested, TransactionRefundRequestedEvent>> {
  val event =
    TransactionRefundRequestedEvent(
      transaction.transactionId.value(),
      TransactionRefundRequestedData(transactionGatewayAuthorizationData, transaction.status))

  return updateTransactionWithRefundEvent(
      transaction,
      transactionsRefundedEventStoreRepository,
      transactionsViewRepository,
      event as TransactionEvent<BaseTransactionRefundedData>,
      TransactionStatusDto.REFUND_REQUESTED)
    .thenReturn(
      Pair(
        (transaction as it.pagopa.ecommerce.commons.domain.v2.Transaction).applyEvent(event)
          as BaseTransactionWithRefundRequested,
        event))
}

fun updateTransactionToRefundError(
  transaction: BaseTransaction,
  transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository
): Mono<BaseTransaction> {
  val event =
    TransactionRefundErrorEvent(
      transaction.transactionId.value(), TransactionRefundErrorData(transaction.status))

  return updateTransactionWithRefundEvent(
    transaction,
    transactionsRefundedEventStoreRepository,
    transactionsViewRepository,
    event as TransactionEvent<BaseTransactionRefundedData>,
    TransactionStatusDto.REFUND_ERROR)
}

fun updateTransactionToRefunded(
  transaction: BaseTransaction,
  transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
  gatewayRefundData: GatewayRefundData = EmptyGatewayRefundData()
): Mono<BaseTransaction> {
  val event =
    TransactionRefundedEvent(
      transaction.transactionId.value(),
      TransactionRefundedData(gatewayRefundData, transaction.status))

  return updateTransactionWithRefundEvent(
      transaction,
      transactionsRefundedEventStoreRepository,
      transactionsViewRepository,
      event as TransactionEvent<BaseTransactionRefundedData>,
      TransactionStatusDto.REFUNDED)
    .thenReturn(
      (transaction as it.pagopa.ecommerce.commons.domain.v2.Transaction).applyEvent(event)
        as BaseTransactionRefunded)
}

fun updateTransactionWithRefundEvent(
  transaction: BaseTransaction,
  transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
  event: TransactionEvent<BaseTransactionRefundedData>,
  status: TransactionStatusDto
): Mono<BaseTransaction> {
  return transactionsRefundedEventStoreRepository
    .save(event)
    .then(
      transactionsViewRepository
        .findByTransactionId(transaction.transactionId.value())
        .cast(Transaction::class.java)
        .flatMap { tx ->
          tx.status = status
          transactionsViewRepository.save(tx)
        })
    .doOnSuccess {
      logger.info(
        "Updated event for transaction with id ${transaction.transactionId.value()} to status $status")
    }
    .thenReturn(transaction)
}

fun retrieveAuthorizationState(
  trx: BaseTransaction,
  authorizationStateRetrieverService: AuthorizationStateRetrieverService
): Mono<StateResponseDto> {
  return Mono.just(trx)
    .cast(BaseTransactionWithRequestedAuthorization::class.java)
    .filter { transaction ->
      transaction.transactionAuthorizationRequestData.paymentGateway ==
        TransactionAuthorizationRequestData.PaymentGateway.NPG
    }
    .switchIfEmpty(Mono.error(InvalidNPGPaymentGatewayException(trx.transactionId)))
    .flatMap { transaction ->
      authorizationStateRetrieverService.getStateNpg(
        transactionId = transaction.transactionId,
        sessionId =
          retrieveGetStateSessionId(
            transaction.transactionAuthorizationRequestData
              .transactionGatewayAuthorizationRequestedData
              as NpgTransactionGatewayAuthorizationRequestedData),
        pspId = transaction.transactionAuthorizationRequestData.pspId,
        correlationId =
          (transaction.transactionActivatedData.transactionGatewayActivationData
              as NpgTransactionGatewayActivationData)
            .correlationId,
        paymentMethod =
          NpgClient.PaymentMethod.valueOf(
            transaction.transactionAuthorizationRequestData.paymentMethodName))
    }
}

fun handleGetStateByPatchTransactionService(
  tx: BaseTransaction,
  authorizationStateRetrieverRetryService: AuthorizationStateRetrieverRetryService,
  authorizationStateRetrieverService: AuthorizationStateRetrieverService,
  transactionsServiceClient: TransactionsServiceClient,
  tracingInfo: TracingInfo,
  retryCount: Int = 0
): Mono<BaseTransaction> {
  return retrieveAuthorizationState(tx, authorizationStateRetrieverService)
    .flatMap { stateResponseDto ->
      patchAuthRequestByState(
        stateResponseDto = stateResponseDto,
        tx = tx,
        transactionsServiceClient = transactionsServiceClient)
    }
    .thenReturn(tx)
    .onErrorResume { exception ->
      logger.error(
        "Transaction handleGetState error for transaction ${tx.transactionId.value()}", exception)
      Mono.just(tx)
        .flatMap {
          when (exception) {
            // Enqueue retry event only if getState is 5xx or 2xx with no PAYMENT_COMPLETE or
            // patchRequest is 5xx
            is NpgBadRequestException, // 400 from NPG
            is TransactionNotFound, // 404 from transactions-service
            is UnauthorizedPatchAuthorizationRequestException, // 401 from transactions-service
            is PatchAuthRequestErrorResponseException, // 400 from transactions-service
            is InvalidNPGPaymentGatewayException, // 400 from NPG
            -> Mono.empty() //
            else ->
              authorizationStateRetrieverRetryService
                .enqueueRetryEvent(tx, retryCount, tracingInfo)
                .onErrorResume { enqueueException ->
                  logger.error(
                    "Transaction enqueue retry event error for transaction ${tx.transactionId.value()}",
                    enqueueException)
                  Mono.just(tx).flatMap {
                    when (enqueueException) {
                      is TooLateRetryAttemptException,
                      is NoRetryAttemptsLeftException, -> Mono.empty()
                      else -> Mono.error(enqueueException)
                    }
                  }
                }
          }
        }
        .thenReturn(tx)
    }
}

fun retrieveGetStateSessionId(
  authRequestedGatewayData: NpgTransactionGatewayAuthorizationRequestedData
): String {
  val sessionId = authRequestedGatewayData.sessionId
  val confirmPaymentSessionId = authRequestedGatewayData.confirmPaymentSessionId
  val sessionIdToUse = Optional.ofNullable(confirmPaymentSessionId).orElse(sessionId)
  logger.info(
    "NPG authorization request sessionId: [{}], confirm payment session id: [{}] -> session id to use for retrieve state: [{}]",
    sessionId,
    confirmPaymentSessionId,
    sessionIdToUse)
  return sessionIdToUse
}

fun patchAuthRequestByState(
  stateResponseDto: StateResponseDto,
  tx: BaseTransaction,
  transactionsServiceClient: TransactionsServiceClient,
): Mono<TransactionInfoDto> {
  logger.info(
    "NPG Get State for transaction with id: [{}] processed successfully with state result [{}]",
    tx.transactionId.value(),
    stateResponseDto.state?.value ?: "N/A")
  // invoke transaction service patch
  return Mono.just(stateResponseDto)
    .filter { s ->
      s.operation != null &&
        s.operation!!.operationTime != null &&
        s.operation!!.operationResult != null
    }
    .switchIfEmpty(Mono.error(InvalidNPGResponseException()))
    .filter { s -> s.state == WorkflowStateDto.PAYMENT_COMPLETE }
    .switchIfEmpty(
      Mono.error(
        NpgPaymentGatewayStateException(
          transactionID = tx.transactionId, stateResponseDto.state?.value)))
    .map { tx }
    .flatMap { t ->
      transactionsServiceClient
        .patchAuthRequest(
          t.transactionId,
          UpdateAuthorizationRequestDto().apply {
            outcomeGateway =
              OutcomeNpgGatewayDto().apply {
                paymentGatewayType = "NPG"
                operationResult =
                  OutcomeNpgGatewayDto.OperationResultEnum.valueOf(
                    stateResponseDto.operation!!.operationResult!!.value)
                orderId = stateResponseDto.operation!!.orderId
                operationId = stateResponseDto.operation!!.operationId
                if (stateResponseDto.operation!!.additionalData != null) {
                  authorizationCode =
                    stateResponseDto.operation!!.additionalData!!["authorizationCode"] as String?
                  rrn = stateResponseDto.operation!!.additionalData!!["rrn"] as String?
                  validationServiceId =
                    stateResponseDto.operation!!.additionalData!!["validationServiceId"] as String?
                  errorCode = stateResponseDto.operation!!.additionalData!!["errorCode"] as String?
                }
                paymentEndToEndId = NpgClientUtils.getPaymentEndToEndId(stateResponseDto.operation)
              }
            timestampOperation = getTimeStampOperation(stateResponseDto.operation!!.operationTime!!)
          })
        .doOnNext { patchResponse ->
          logger.info(
            "Transactions service PATCH authRequest for transaction with id: [{}] processed successfully. New state for transaction is [{}]",
            tx.transactionId.value(),
            patchResponse.status)
        }
    }
}

fun getTimeStampOperation(operationTime: String): OffsetDateTime {
  val operationTimeT: String = operationTime.replace(" ", "T")
  val localDateTime = LocalDateTime.parse(operationTimeT)
  val zonedDateTime = localDateTime.atZone(ZoneId.of("CET"))
  return zonedDateTime.toOffsetDateTime()
}

fun appendRefundRequestedEventIfNeeded(
  transaction: BaseTransaction,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
  authorizationData: TransactionGatewayAuthorizationData? = null
): Mono<Pair<BaseTransactionWithRefundRequested, TransactionRefundRequestedEvent?>> {
  return if (transaction !is BaseTransactionWithRefundRequested) {
    Mono.just(transaction).flatMap { tx ->
      updateTransactionToRefundRequested(
        tx, transactionsEventStoreRepository, transactionsViewRepository, authorizationData)
    }
  } else {
    Mono.just(transaction to null)
  }
}

fun requestRefundTransaction(
  transaction: BaseTransaction,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
  npgService: NpgService,
  tracingInfo: TracingInfo?,
  refundRequestedAsyncClient: QueueAsyncClient,
  transientQueueTTLSeconds: Duration
): Mono<BaseTransactionWithRefundRequested> {
  val transactionAuthorizationRequestData = getTransactionAuthorizationRequestData(transaction)

  if (transactionAuthorizationRequestData == null) {
    logger.warn(
      "Tried to call `requestRefundTransaction` on transaction with null authorization request data in status {}!",
      transaction.status.value)
    return Mono.error(
      IllegalArgumentException(
        "Tried to call `refundRequested` on transaction with null authorization request data in status ${transaction.status.value}!"))
  }

  return when (transactionAuthorizationRequestData.paymentGateway) {
    TransactionAuthorizationRequestData.PaymentGateway.REDIRECT ->
      appendRefundRequestedEventIfNeeded(
        transaction, transactionsEventStoreRepository, transactionsViewRepository)
    TransactionAuthorizationRequestData.PaymentGateway.NPG ->
      appendNpgRefundRequestedEventIfNeeded(
        transaction, transactionsEventStoreRepository, transactionsViewRepository, npgService)
    else ->
      Mono.error(
        RuntimeException(
          "Refund request error for transaction ${transaction.transactionId.value()} - unhandled payment-gateway"))
  }.flatMap { (tx, refundRequestedEvent) ->
    if (refundRequestedEvent == null) {
      logger.warn(
        "Called `requestRefundTransaction` on transaction with id ${tx.transactionId.value()} which seems with a refund already requested. Current transaction status: ${tx.status}")
      Mono.empty()
    } else {
      refundRequestedAsyncClient
        .sendMessageWithResponse(
          QueueEvent(refundRequestedEvent, tracingInfo), Duration.ZERO, transientQueueTTLSeconds)
        .thenReturn(tx)
    }
  }
}

/*
 * @formatter:off
 *
 * Warning kotlin:S107 - Functions should not have too many parameters
 * Suppressed because the inner business logic is complex
 * TODO: will refactor into separate functions down the line
 *
 * @formatter:on
 */
@SuppressWarnings("kotlin:S107")
fun refundTransaction(
  tx: BaseTransactionWithRefundRequested,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
  refundService: RefundService,
  refundRetryService: RefundRetryService,
  npgService: NpgService,
  tracingInfo: TracingInfo?,
  retryCount: Int = 0
): Mono<BaseTransaction> {
  val transactionAuthorizationRequestData = getTransactionAuthorizationRequestData(tx)

  if (transactionAuthorizationRequestData == null) {
    logger.warn(
      "Tried to call `refundRequested` on transaction with null authorization request data in status {}!",
      tx.status.value)
    return Mono.error(
      IllegalArgumentException(
        "Tried to call `refundRequested` on transaction with null authorization request data in status ${tx.status.value}!"))
  }

  return Mono.just(tx)
    .flatMap { transaction ->
      when (transactionAuthorizationRequestData.paymentGateway) {
        TransactionAuthorizationRequestData.PaymentGateway.NPG ->
          refundTransactionNPG(
            transaction = tx, refundService = refundService, npgService = npgService)
        TransactionAuthorizationRequestData.PaymentGateway.REDIRECT ->
          refundService
            .requestRedirectRefund(
              transactionId = transaction.transactionId,
              touchpoint =
                transaction.clientId.effectiveClient.let {
                  when (it) {
                    Transaction.ClientId.CHECKOUT_CART -> Transaction.ClientId.CHECKOUT.name
                    Transaction.ClientId.CHECKOUT,
                    Transaction.ClientId.IO -> it.name
                    else ->
                      throw IllegalArgumentException(
                        "Cannot determine touch point: [$it] for redirect transaction refund")
                  }
                },
              pspTransactionId = transactionAuthorizationRequestData.authorizationRequestId,
              paymentTypeCode = transactionAuthorizationRequestData.paymentTypeCode,
              pspId = transactionAuthorizationRequestData.pspId)
            .map { refundResponse -> Pair(refundResponse, transaction) }
        else ->
          Mono.error(
            RuntimeException(
              "Refund error for transaction ${transaction.transactionId} - unhandled payment-gateway"))
      }
    }
    .flatMap {
      val (refundResponse, transaction) = it

      when (refundResponse) {
        is RefundResponseDto ->
          handleNpgRefundResponse(
            transaction,
            refundResponse,
            transactionsEventStoreRepository,
            transactionsViewRepository)
        is it.pagopa.generated.ecommerce.redirect.v1.dto.RefundResponseDto ->
          handleRedirectRefundResponse(
            transaction,
            transactionAuthorizationRequestData,
            refundResponse,
            transactionsEventStoreRepository,
            transactionsViewRepository)
        else ->
          Mono.error(
            RuntimeException(
              "Refund error for transaction ${transaction.transactionId}, unhandled refund response: ${refundResponse.javaClass}"))
      }
    }
    .cast(BaseTransaction::class.java)
    .onErrorResume { exception ->
      logger.error(
        "Transaction requestRefund error for transaction ${tx.transactionId.value()}", exception)
      if (retryCount == 0) {
          // refund error event written only the first time
          updateTransactionToRefundError(
            tx, transactionsEventStoreRepository, transactionsViewRepository)
        } else {
          Mono.just(tx)
        }
        .flatMap {
          when (exception) {
            // Enqueue retry event only if refund is allowed
            is RefundNotAllowedException -> Mono.error(exception)
            is RefundError.UnexpectedPaymentGatewayResponse -> Mono.error(exception)
            is RefundError.RefundFailed ->
              refundRetryService.enqueueRetryEvent(
                it, retryCount, tracingInfo, exception.authorizationData)
            else -> refundRetryService.enqueueRetryEvent(it, retryCount, tracingInfo)
          }
        }
        .thenReturn(tx)
    }
}

private fun appendNpgRefundRequestedEventIfNeeded(
  transaction: BaseTransaction,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
  npgService: NpgService,
): Mono<Pair<BaseTransactionWithRefundRequested, TransactionRefundRequestedEvent?>> {
  return getAuthorizationCompletedData(transaction, npgService)
    .map { Optional.of(it) }
    .onErrorResume { error ->
      logger.error(
        "Error performing GET orders with NPG for transaction: [%s]".format(
          transaction.transactionId.value()),
        error)
      when (error) {
        // in case of 4xx NPG errors or invalid response (missing mandatory data such as
        // operationId) write event to dead letter
        is NpgBadRequestException,
        is InvalidNPGResponseException -> {
          Mono.error(
            RefundError.UnexpectedPaymentGatewayResponse(
              transactionId = transaction.transactionId,
              cause = error,
              message = "Unrecoverable error retrieving authorization status through GET orders"))
        }
        // in this case operation result already refunded by NPG but no attempt have already being
        // done by eCommerce -> write event to dead letter
        is InvalidNpgOrderStateException.OrderAlreadyRefunded -> {
          logger.error(
            "Unexpected error, transaction : [{}] already refunded by NPG!",
            transaction.transactionId.value())
          // write refund requested event and refund error event into events and return error in
          // order to make transaction being written into dead letter for further investigations
          appendRefundRequestedEventIfNeeded(
              transaction,
              transactionsEventStoreRepository,
              transactionsViewRepository,
              error.authorizationData)
            .flatMap { (tx, _) ->
              updateTransactionToRefundError(
                transaction = tx,
                transactionsRefundedEventStoreRepository = transactionsEventStoreRepository,
                transactionsViewRepository = transactionsViewRepository)
            }
            .then(
              Mono.error(
                RefundError.UnexpectedPaymentGatewayResponse(
                  transactionId = transaction.transactionId,
                  cause = error,
                  message = "Unexpected operation in refunded status!")))
        }
        // error retrieving auth data from NPG using GET orders, another attempt will be performed
        // during refund retry
        else -> {
          Mono.just(Optional.empty())
        }
      }
    }
    .flatMap { authorizationData ->
      appendRefundRequestedEventIfNeeded(
        transaction,
        transactionsEventStoreRepository,
        transactionsViewRepository,
        authorizationData.orElse(null))
    }
}

private fun refundTransactionNPG(
  transaction: BaseTransactionWithRefundRequested,
  refundService: RefundService,
  npgService: NpgService
): Mono<Pair<RefundResponseDto, BaseTransactionWithRefundRequested>> {
  if (transaction.transactionAuthorizationRequestData.paymentGateway !=
    TransactionAuthorizationRequestData.PaymentGateway.NPG) {
    return Mono.error(
      IllegalStateException(
        "Tried to call refund for NPG transaction on wrong gateway ${transaction.transactionAuthorizationRequestData.paymentGateway}"))
  }

  val authorizationDataPipeline =
    Mono.just(transaction)
      .flatMap { tx ->
        tx.transactionAuthorizationGatewayData
          .map { Mono.just(it) }
          .orElse(getAuthorizationCompletedData(tx = tx, npgService = npgService))
      }
      .cast(NpgTransactionGatewayAuthorizationData::class.java)
      .onErrorResume { error ->
        logger.error(
          "Error performing GET orders with NPG for transaction: [%s]".format(
            transaction.transactionId.value()),
          error)
        when (error) {
          // in case of 4xx NPG errors or invalid response (missing mandatory data such as
          // operationId) write event to dead letter
          is NpgBadRequestException,
          is InvalidNPGResponseException -> {
            Mono.error(
              RefundNotAllowedException(
                transaction.transactionId, "Unrecoverable error performing GET orders", error))
          }
          // in this case operation result already refunded by NPG but no attempt have already being
          // done by eCommerce -> write event to dead letter
          is InvalidNpgOrderStateException.OrderAlreadyRefunded -> {
            logger.error(
              "Unexpected error, transaction : [{}] already refunded by NPG!",
              transaction.transactionId.value())
            Mono.error(
              RefundNotAllowedException(
                transaction.transactionId, "Order already refunded!", error))
          }
          // error retrieving auth data from NPG using GET orders, another attempt will be performed
          // during refund retry
          else -> {
            Mono.error(
              RefundError.RefundFailed(
                transactionId = transaction.transactionId,
                authorizationData = null,
                message = "Error retrieving NPG auth status with GET orders",
                cause = error))
          }
        }
      }

  return authorizationDataPipeline.flatMap { authData ->
    refundService
      .requestNpgRefund(
        operationId = authData.operationId,
        idempotenceKey = transaction.transactionId.uuid,
        amount =
          BigDecimal(transaction.transactionAuthorizationRequestData.amount)
            .add(BigDecimal(transaction.transactionAuthorizationRequestData.fee)),
        pspId = transaction.transactionAuthorizationRequestData.pspId,
        correlationId =
          (transaction.transactionActivatedData.transactionGatewayActivationData
              as NpgTransactionGatewayActivationData)
            .correlationId,
        paymentMethod =
          NpgClient.PaymentMethod.valueOf(
            transaction.transactionAuthorizationRequestData.paymentMethodName))
      .map { refundResponse -> Pair(refundResponse, transaction) }
      .onErrorMap({ e -> e is BadGatewayException || e is NpgResponseException }) { e ->
        logger.error(
          "Error during refund NPG for transaction [{}]", transaction.transactionId.value(), e)
        RefundError.RefundFailed(
          transactionId = transaction.transactionId,
          authorizationData = authData,
          message = "Error during refund NPG",
          cause = e)
      }
  }
}

fun handleNpgRefundResponse(
  transaction: BaseTransaction,
  refundResponse: RefundResponseDto,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository
): Mono<BaseTransaction> {
  logger.info(
    "Refund for transaction with id: [{}] and NPG operationId [{}] processed successfully",
    transaction.transactionId.value(),
    refundResponse.operationId ?: "N/A")
  return updateTransactionToRefunded(
    transaction,
    transactionsEventStoreRepository,
    transactionsViewRepository,
    NpgGatewayRefundData(refundResponse.operationId))
}

fun handleRedirectRefundResponse(
  transaction: BaseTransaction,
  transactionAuthorizationRequestData: TransactionAuthorizationRequestData,
  refundResponse: it.pagopa.generated.ecommerce.redirect.v1.dto.RefundResponseDto,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository
): Mono<BaseTransaction> {
  val refundOutcome = refundResponse.outcome
  logger.info(
    "Refund for redirect transaction for psp: [{}] with id: [{}] processed successfully. Received outcome: [{}]",
    transactionAuthorizationRequestData.pspId,
    transaction.transactionId.value(),
    refundOutcome)
  return when (refundOutcome) {
    RefundOutcomeDto.OK,
    RefundOutcomeDto.CANCELED ->
      updateTransactionToRefunded(
        transaction, transactionsEventStoreRepository, transactionsViewRepository)
    RefundOutcomeDto.KO ->
      updateTransactionToRefundError(
        transaction, transactionsEventStoreRepository, transactionsViewRepository)
  }
}

fun isTransactionRefundable(tx: BaseTransaction): Boolean {
  val wasAuthorizationRequested = wasAuthorizationRequested(tx)
  val wasSendPaymentResultOutcomeKO = wasSendPaymentResultOutcomeKO(tx)
  val wasAuthorizationDenied = wasAuthorizationDenied(tx)
  val wasClosePaymentResponseOutcomeKO = wasClosePaymentResponseOutcomeKo(tx)

  val isTransactionRefundable =
    when (tx) {
      // transaction for which a send payment result was received --> refund =
      // sendPaymentResultOutcome == KO
      is BaseTransactionWithRequestedUserReceipt -> wasSendPaymentResultOutcomeKO
      // transaction stuck after closePayment --> refund = closePaymentResponseOutcome == KO
      is BaseTransactionClosed -> wasClosePaymentResponseOutcomeKO
      // transaction stuck at closure error (no close payment vs Nodo) --> refund =
      // check previous transaction status
      is TransactionWithClosureError -> isTransactionRefundable(tx.transactionAtPreviousState)
      // transaction stuck at authorization completed status --> refund = PGS auth outcome != KO
      is BaseTransactionWithCompletedAuthorization -> !wasAuthorizationDenied
      // transaction in expired status (expiration event sent by batch) --> refund =
      // check previous transaction status
      is BaseTransactionExpired -> isTransactionRefundable(tx.transactionAtPreviousState)
      // transaction stuck at previous steps (authorization requested, activation...) --> refund =
      // authorization was requested to PGS
      else -> wasAuthorizationRequested
    }
  logger.info(
    "Transaction with id ${tx.transactionId.value()} : authorization requested: $wasAuthorizationRequested, authorization denied: $wasAuthorizationDenied, closePaymentResponse.outcome KO: $wasClosePaymentResponseOutcomeKO sendPaymentResult.outcome KO : $wasSendPaymentResultOutcomeKO --> is refundable: $isTransactionRefundable")
  return isTransactionRefundable
}

fun wasSendPaymentResultOutcomeKO(tx: BaseTransaction): Boolean =
  when (tx) {
    is BaseTransactionWithRequestedUserReceipt ->
      tx.transactionUserReceiptData.responseOutcome == TransactionUserReceiptData.Outcome.KO
    is BaseTransactionExpired -> wasSendPaymentResultOutcomeKO(tx.transactionAtPreviousState)
    else -> false
  }

fun wasAuthorizationRequested(tx: BaseTransaction): Boolean =
  when (tx) {
    is BaseTransactionWithRequestedAuthorization -> true
    is TransactionWithClosureError ->
      tx
        .transactionAtPreviousState()
        .map { txAtPreviousStep -> txAtPreviousStep.isRight }
        .orElse(false)
    else -> false
  }

fun getGatewayAuthorizationOutcome(
  gatewayAuthorizationData: TransactionGatewayAuthorizationData
): AuthorizationResultDto {
  return when (gatewayAuthorizationData) {
    is NpgTransactionGatewayAuthorizationData ->
      if (gatewayAuthorizationData.operationResult == OperationResultDto.EXECUTED) {
        AuthorizationResultDto.OK
      } else {
        AuthorizationResultDto.KO
      }
    is RedirectTransactionGatewayAuthorizationData ->
      if (gatewayAuthorizationData.outcome ==
        RedirectTransactionGatewayAuthorizationData.Outcome.OK) {
        AuthorizationResultDto.OK
      } else {
        AuthorizationResultDto.KO
      }
    is PgsTransactionGatewayAuthorizationData ->
      throw IllegalArgumentException(
        "Unhandled or invalid auth data type 'PgsTransactionGatewayAuthorizationData'")
  }
}

fun getAuthorizationOutcome(tx: BaseTransaction): AuthorizationResultDto? =
  when (tx) {
    is BaseTransactionWithCompletedAuthorization ->
      getGatewayAuthorizationOutcome(
        tx.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData)
    is TransactionWithClosureError -> getAuthorizationOutcome(tx.transactionAtPreviousState)
    is BaseTransactionExpired -> getAuthorizationOutcome(tx.transactionAtPreviousState)
    else -> null
  }

/*
 * @formatter:off
 *
 * Warning kotlin:S1871 - Two branches in a conditional structure should not have exactly the same implementation
 * Suppressed because different branches correspond to different types and cannot be unified
 *
 * @formatter:on
 */
@SuppressWarnings("kotlin:S1871")
fun getAuthorizationCompletedData(
  tx: BaseTransaction,
  npgService: NpgService
): Mono<TransactionGatewayAuthorizationData> =
  when (tx) {
    is BaseTransactionWithCompletedAuthorization ->
      tx.transactionAuthorizationCompletedData.transactionGatewayAuthorizationData.toMono()
    is BaseTransactionWithRefundRequested ->
      tx.transactionAuthorizationGatewayData
        .map { it.toMono() }
        .orElse(getAuthorizationCompletedData(tx.transactionAtPreviousState, npgService))
    is TransactionWithClosureError ->
      getAuthorizationCompletedData(tx.transactionAtPreviousState, npgService)
    is BaseTransactionExpired ->
      getAuthorizationCompletedData(tx.transactionAtPreviousState, npgService)
    is BaseTransactionWithRequestedAuthorization ->
      if (tx.transactionAuthorizationRequestData.paymentGateway ==
        TransactionAuthorizationRequestData.PaymentGateway.NPG) {
        npgService.getAuthorizationDataFromNpgOrder(tx)
      } else Mono.empty()
    else -> Mono.empty()
  }

fun wasAuthorizationDenied(tx: BaseTransaction): Boolean =
  getAuthorizationOutcome(tx) == AuthorizationResultDto.KO

fun isTransactionExpired(tx: BaseTransaction): Boolean =
  tx.status == TransactionStatusDto.EXPIRED ||
    tx.status == TransactionStatusDto.EXPIRED_NOT_AUTHORIZED ||
    tx.status == TransactionStatusDto.CANCELLATION_EXPIRED

fun reduceEvents(
  transactionId: Mono<String>,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>
): Mono<BaseTransaction> =
  reduceEvents(transactionId, transactionsEventStoreRepository, EmptyTransaction())

fun reduceEvents(
  transactionId: Mono<String>,
  transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  emptyTransaction: EmptyTransaction
): Mono<BaseTransaction> =
  reduceEvents(
    transactionId.flatMapMany {
      transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(it).map {
        it as TransactionEvent<Any>
      }
    },
    emptyTransaction)

fun <T> reduceEvents(events: Flux<TransactionEvent<T>>): Mono<BaseTransaction> =
  reduceEvents(events, EmptyTransaction())

fun <T> reduceEvents(
  events: Flux<TransactionEvent<T>>,
  emptyTransaction: EmptyTransaction
): Mono<BaseTransaction> =
  events
    .reduce(emptyTransaction, it.pagopa.ecommerce.commons.domain.v2.Transaction::applyEvent)
    .cast(BaseTransaction::class.java)

fun updateNotifiedTransactionStatus(
  transaction: BaseTransactionWithRequestedUserReceipt,
  transactionsViewRepository: TransactionsViewRepository,
  transactionUserReceiptRepository: TransactionsEventStoreRepository<TransactionUserReceiptData>
): Mono<BaseTransactionWithUserReceipt> {
  val newStatus =
    when (transaction.transactionUserReceiptData.responseOutcome!!) {
      TransactionUserReceiptData.Outcome.OK -> TransactionStatusDto.NOTIFIED_OK
      TransactionUserReceiptData.Outcome.KO -> TransactionStatusDto.NOTIFIED_KO
      else ->
        throw RuntimeException(
          "Unexpected transaction user receipt data response outcome ${transaction.transactionUserReceiptData.responseOutcome} for transaction with id: ${transaction.transactionId.value()}")
    }
  val event =
    TransactionUserReceiptAddedEvent(
      transaction.transactionId.value(), transaction.transactionUserReceiptData)
  logger.info("Updating transaction {} status to {}", transaction.transactionId.value(), newStatus)

  return transactionsViewRepository
    .findByTransactionId(transaction.transactionId.value())
    .cast(Transaction::class.java)
    .flatMap { tx ->
      tx.status = newStatus
      transactionsViewRepository.save(tx)
    }
    .flatMap { transactionUserReceiptRepository.save(event) }
    .thenReturn(
      (transaction as it.pagopa.ecommerce.commons.domain.v2.Transaction).applyEvent(event)
        as BaseTransactionWithUserReceipt)
}

fun updateNotificationErrorTransactionStatus(
  transaction: BaseTransactionWithRequestedUserReceipt,
  transactionsViewRepository: TransactionsViewRepository,
  transactionUserReceiptRepository: TransactionsEventStoreRepository<TransactionUserReceiptData>
): Mono<TransactionUserReceiptAddErrorEvent> {
  val newStatus = TransactionStatusDto.NOTIFICATION_ERROR
  val event =
    TransactionUserReceiptAddErrorEvent(
      transaction.transactionId.value(), transaction.transactionUserReceiptData)
  logger.info("Updating transaction {} status to {}", transaction.transactionId.value(), newStatus)

  return transactionsViewRepository
    .findByTransactionId(transaction.transactionId.value())
    .cast(Transaction::class.java)
    .flatMap { tx ->
      tx.status = newStatus
      transactionsViewRepository.save(tx)
    }
    .flatMap { transactionUserReceiptRepository.save(event) }
}

/*
 * @formatter:off
 *
 * Warning kotlin:S107 - Functions should not have too many parameters
 * Suppressed because the inner business logic is complex
 * TODO: will refactor into separate functions down the line
 *
 * @formatter:on
 */
@SuppressWarnings("kotlin:S107")
fun notificationRefundTransactionPipeline(
  transaction: BaseTransactionWithRequestedUserReceipt,
  transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  transactionsViewRepository: TransactionsViewRepository,
  npgService: NpgService,
  tracingInfo: TracingInfo?,
  refundRequestedAsyncClient: QueueAsyncClient,
  transientQueueTTLSeconds: Duration
): Mono<BaseTransaction> {
  val userReceiptOutcome = transaction.transactionUserReceiptData.responseOutcome
  val toBeRefunded = userReceiptOutcome == TransactionUserReceiptData.Outcome.KO
  logger.info(
    "Transaction Nodo sendPaymentResult response outcome: $userReceiptOutcome --> to be refunded: $toBeRefunded")
  return Mono.just(transaction)
    .filter { toBeRefunded }
    .flatMap {
      requestRefundTransaction(
        transaction,
        transactionsRefundedEventStoreRepository,
        transactionsViewRepository,
        npgService,
        tracingInfo,
        refundRequestedAsyncClient,
        transientQueueTTLSeconds)
    }
}

fun <T> runTracedPipelineWithDeadLetterQueue(
  checkPointer: Checkpointer,
  pipeline: Mono<T>,
  queueEvent: QueueEvent<*>,
  deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  tracingUtils: TracingUtils,
  spanName: String,
  jsonSerializerProviderV2: StrictJsonSerializerProvider
): Mono<Unit> {
  val nullTransactionId = "00000000000000000000000000000000" // null event ID used in warmup phase
  val eventLogString = "${queueEvent.event.id}, transactionId: ${queueEvent.event.transactionId}"

  val deadLetterPipeline =
    checkPointer
      .success()
      .doOnSuccess { logger.info("Checkpoint performed successfully for event $eventLogString") }
      .doOnError { logger.error("Error performing checkpoint for event $eventLogString", it) }
      .then(pipeline)
      .then(Mono.just(Unit))
      .onErrorResume { pipelineException ->
        val errorCategory: DeadLetterTracedQueueAsyncClient.ErrorCategory =
          when (pipelineException) {
            is NoRetryAttemptsLeftException ->
              DeadLetterTracedQueueAsyncClient.ErrorCategory.RETRY_EVENT_NO_ATTEMPTS_LEFT
            is RefundError ->
              DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED
            else -> DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR
          }
        logger.error("Exception processing event $eventLogString", pipelineException)
        if (queueEvent.event.transactionId != nullTransactionId) {
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            binaryData =
              BinaryData.fromObject(queueEvent, jsonSerializerProviderV2.createInstance()),
            errorContext =
              DeadLetterTracedQueueAsyncClient.ErrorContext(
                transactionId = TransactionId(queueEvent.event.transactionId),
                transactionEventCode = queueEvent.event.eventCode,
                errorCategory = errorCategory))
        } else {
          logger.info("Skipping dead letter queue for warmup event with null transaction ID")
          Mono.just(Unit)
        }
      }

  return tracingUtils
    .traceMonoWithRemoteSpan(queueEvent.tracingInfo, spanName, deadLetterPipeline)
    .then(Mono.just(Unit))
}

fun getClosePaymentOutcome(tx: BaseTransaction): TransactionClosureData.Outcome? =
  when (tx) {
    is BaseTransactionClosed -> tx.transactionClosureData.responseOutcome
    is BaseTransactionExpired -> getClosePaymentOutcome(tx.transactionAtPreviousState)
    else -> null
  }

fun wasClosePaymentResponseOutcomeKo(tx: BaseTransaction) =
  getClosePaymentOutcome(tx) == TransactionClosureData.Outcome.KO

fun <T> timeLeftForSendPaymentResult(
  tx: BaseTransaction,
  sendPaymentResultTimeoutSeconds: Int,
  events: Flux<TransactionEvent<T>>
): Mono<Duration> {
  val timeout = Duration.ofSeconds(sendPaymentResultTimeoutSeconds.toLong())
  val transactionStatus = tx.status
  return if (transactionStatus == TransactionStatusDto.CLOSED) {
    events
      .filter {
        TransactionEventCode.valueOf(it.eventCode) ==
          TransactionEventCode.TRANSACTION_CLOSED_EVENT &&
          (it as TransactionClosedEvent).data.responseOutcome == TransactionClosureData.Outcome.OK
      }
      .next()
      .map {
        val closePaymentDate = ZonedDateTime.parse(it.creationDate)
        val now = ZonedDateTime.now()
        val timeLeft = Duration.between(now, closePaymentDate.plus(timeout))
        logger.info("Transaction close payment done at: $closePaymentDate, time left: $timeLeft")
        return@map timeLeft
      }
  } else {
    Mono.empty()
  }
}

fun isRefundableCheckRequired(tx: BaseTransaction): Boolean =
  when (tx) {
    is BaseTransactionExpired -> isRefundableCheckRequired(tx.transactionAtPreviousState)
    is BaseTransactionWithClosureError -> isRefundableCheckRequired(tx.transactionAtPreviousState)
    else ->
      setOf(TransactionStatusDto.AUTHORIZATION_COMPLETED, TransactionStatusDto.CLOSURE_REQUESTED)
        .contains(tx.status) && !wasAuthorizationDenied(tx)
  }

fun <T> computeRefundProcessingRequestDelay(
  tx: BaseTransaction,
  timeToWaitFromAuthRequestMinutes: Long,
  events: Flux<TransactionEvent<T>>
): Mono<Duration> {
  val authOutcome = getAuthorizationOutcome(tx)
  // safe here, transaction here must be in auth requested state due the check in the caller
  val authData = getTransactionAuthorizationRequestData(tx)!!
  val authorizationRequestedDate =
    events
      .filter {
        TransactionEventCode.valueOf(it.eventCode) ==
          TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT
      }
      .next()
      .map { ZonedDateTime.parse(it.creationDate) }
      .switchIfEmpty(
        Mono.error(
          RuntimeException("No auth requested event found for a transaction to be refunded")))

  if (authOutcome != null) {
    return Mono.just(Duration.ZERO)
  }
  // refund can be postponed only for NPG gateway
  if (authData.paymentGateway != TransactionAuthorizationRequestData.PaymentGateway.NPG) {
    return Mono.just(Duration.ZERO)
  }
  return authorizationRequestedDate.map { authRequestedDate ->
    val now = ZonedDateTime.now()
    val refundNotBefore = authRequestedDate + Duration.ofMinutes(timeToWaitFromAuthRequestMinutes)
    val refundTimeout =
      if (now.isAfter(refundNotBefore)) {
        Duration.ZERO
      } else {
        Duration.between(now, refundNotBefore)
      }
    logger.info(
      "Transaction with id: [{}], authorization requested at: [{}], refund to be processed at: [{}]",
      tx.transactionId,
      authRequestedDate,
      refundNotBefore)
    refundTimeout
  }
}
