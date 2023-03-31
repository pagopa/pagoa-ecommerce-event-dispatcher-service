package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
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
class TransactionNotificationsQueueConsumer(
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
  var logger: Logger = LoggerFactory.getLogger(TransactionNotificationsQueueConsumer::class.java)

  private fun getTransactionIdFromPayload(data: BinaryData) =
    data.toObjectAsync(TransactionUserReceiptAddedEvent::class.java).map { it.transactionId }

  @ServiceActivator(inputChannel = "transactionnotificationschannel", outputChannel = "nullChannel")
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
    val baseTransaction =
      reduceEvents(transactionId, transactionsEventStoreRepository, emptyTransaction)
    val notificationResendPipeline =
      baseTransaction
        .flatMap {
          logger.info("Status for transaction ${it.transactionId.value}: ${it.status}")

          if (it.status != TransactionStatusDto.NOTIFICATION_REQUESTED) {
            Mono.error(
              BadTransactionStatusException(
                transactionId = it.transactionId,
                expected = TransactionStatusDto.NOTIFICATION_REQUESTED,
                actual = it.status))
          } else {
            Mono.just(it)
          }
        }
        .cast(BaseTransactionWithUserReceipt::class.java)
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

                notificationRetryService
                  .enqueueRetryEvent(baseTransaction, 0)
                  .doOnError { exception ->
                    logger.error("Exception enqueueing notification retry event", exception)
                  }
                  .then()
              }
            }
        }
    return checkpoint.then(notificationResendPipeline).then()
  }

  private fun updateTransactionStatus(
    transaction: BaseTransactionWithUserReceipt,
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
