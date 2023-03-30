package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithUserReceiptError
import it.pagopa.ecommerce.commons.domain.v1.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NoRetryAttemptsLeftException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.NotificationRetryService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.UserReceiptMailBuilder
import it.pagopa.generated.notifications.templates.success.*
import java.util.*
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class TransactionNotificationsRetryQueueConsumer(
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionUserReceiptRepository:
    TransactionsEventStoreRepository<TransactionUserReceiptData>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val notificationRetryService: NotificationRetryService,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val refundRetryService: RefundRetryService,
  @Autowired private val userReceiptMailBuilder: UserReceiptMailBuilder,
  @Autowired private val notificationsServiceClient: NotificationsServiceClient
) {
  var logger: Logger =
    LoggerFactory.getLogger(TransactionNotificationsRetryQueueConsumer::class.java)

  private fun getTransactionIdFromPayload(data: BinaryData): Mono<String> {
    val idFromClosureErrorEvent =
      data.toObjectAsync(TransactionUserReceiptAddErrorEvent::class.java).map { it.transactionId }
    val idFromClosureRetriedEvent =
      data.toObjectAsync(TransactionUserReceiptAddRetriedEvent::class.java).map { it.transactionId }
    return Mono.firstWithValue(idFromClosureErrorEvent, idFromClosureRetriedEvent)
  }

  private fun getRetryCountFromPayload(data: BinaryData): Mono<Int> {
    return data
      .toObjectAsync(TransactionUserReceiptAddRetriedEvent::class.java)
      .map { Optional.ofNullable(it.data.retryCount).orElse(0) }
      .onErrorResume { Mono.just(0) }
  }

  @ServiceActivator(
    inputChannel = "transactionretrynotificationschannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
  ) = messageReceiver(payload, checkPointer, EmptyTransaction())

  fun messageReceiver(
    payload: ByteArray,
    checkPointer: Checkpointer,
    emptyTransaction: EmptyTransaction
  ): Mono<Void> {
    val checkpoint = checkPointer.success()
    val binaryData = BinaryData.fromBytes(payload)
    val transactionId = getTransactionIdFromPayload(binaryData)
    val retryCount = getRetryCountFromPayload(binaryData)
    val baseTransaction =
      reduceEvents(transactionId, transactionsEventStoreRepository, emptyTransaction)
    val notificationResendPipeline =
      baseTransaction
        .flatMap {
          logger.info("Status for transaction ${it.transactionId.value}: ${it.status}")

          if (it.status != TransactionStatusDto.NOTIFICATION_ERROR) {
            Mono.error(
              BadTransactionStatusException(
                transactionId = it.transactionId,
                expected = TransactionStatusDto.NOTIFICATION_ERROR,
                actual = it.status))
          } else {
            Mono.just(it)
          }
        }
        .cast(TransactionWithUserReceiptError::class.java)
        .flatMap { tx ->
          mono { userReceiptMailBuilder.buildNotificationEmailRequestDto(tx) }
            .flatMap { notificationsServiceClient.sendNotificationEmail(it) }
            .flatMap { updateTransactionStatus(tx) }
            .then()
            .onErrorResume { exception ->
              baseTransaction.flatMap { baseTransaction ->
                logger.error(
                  "Got exception while retrying user receipt mail sending for transaction with id ${baseTransaction.transactionId}!",
                  exception)
                retryCount
                  .flatMap { retryCount ->
                    notificationRetryService.enqueueRetryEvent(baseTransaction, retryCount)
                  }
                  .onErrorResume(NoRetryAttemptsLeftException::class.java) { exception ->
                    logger.error(
                      "No more attempts left for closure retry, refunding transaction", exception)
                    /*
                     * The refund process is started only iff the Nodo sent sendPaymentResult with outcome KO
                     */
                    refundTransactionPipeline(tx).then()
                  }
                  .then()
              }
            }
        }
    return checkpoint.then(notificationResendPipeline).then()
  }

  private fun refundTransactionPipeline(
    transaction: TransactionWithUserReceiptError,
  ): Mono<BaseTransaction> {
    val userReceiptOutcome = transaction.transactionUserReceiptData.responseOutcome
    val toBeRefunded = userReceiptOutcome == TransactionUserReceiptData.Outcome.KO
    logger.info(
      "Transaction Nodo sendPaymentResult response outcome: $userReceiptOutcome --> to be refunded: $toBeRefunded")
    return Mono.just(transaction)
      .filter { toBeRefunded }
      .flatMap { tx ->
        updateTransactionToRefundRequested(
          tx, transactionsRefundedEventStoreRepository, transactionsViewRepository)
      }
      .flatMap {
        refundTransaction(
          transaction,
          transactionsRefundedEventStoreRepository,
          transactionsViewRepository,
          paymentGatewayClient,
          refundRetryService)
      }
  }

  private fun updateTransactionStatus(
    transaction: TransactionWithUserReceiptError,
  ): Mono<TransactionUserReceiptAddedEvent> {
    val newStatus =
      when (transaction.transactionUserReceiptData.responseOutcome!!) {
        TransactionUserReceiptData.Outcome.OK -> TransactionStatusDto.NOTIFIED_OK
        TransactionUserReceiptData.Outcome.KO -> TransactionStatusDto.NOTIFIED_KO
      }
    val event =
      TransactionUserReceiptAddedEvent(
        transaction.transactionId.value.toString(), transaction.transactionUserReceiptData)
    logger.info("Updating transaction {} status to {}", transaction.transactionId.value, newStatus)

    return transactionsViewRepository
      .findByTransactionId(transaction.transactionId.value.toString())
      .flatMap { tx ->
        tx.status = newStatus
        transactionsViewRepository.save(tx)
      }
      .flatMap { transactionUserReceiptRepository.save(event) }
  }
}
