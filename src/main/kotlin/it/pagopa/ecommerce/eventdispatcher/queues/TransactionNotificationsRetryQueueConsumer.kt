package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithUserReceiptError
import it.pagopa.ecommerce.commons.domain.v1.pojos.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingUtils
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
import org.springframework.beans.factory.annotation.Value
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
  @Autowired private val notificationsServiceClient: NotificationsServiceClient,
  @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
  @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
  private val deadLetterTTLSeconds: Int,
  @Autowired private val tracingUtils: TracingUtils
) {
  var logger: Logger =
    LoggerFactory.getLogger(TransactionNotificationsRetryQueueConsumer::class.java)

  private fun getTransactionIdFromPayload(
    event:
      Either<
        QueueEvent<TransactionUserReceiptAddErrorEvent>,
        QueueEvent<TransactionUserReceiptAddRetriedEvent>>
  ): String {
    return event.fold({ it.event.transactionId }, { it.event.transactionId })
  }

  private fun getRetryCountFromPayload(
    event:
      Either<
        QueueEvent<TransactionUserReceiptAddErrorEvent>,
        QueueEvent<TransactionUserReceiptAddRetriedEvent>>
  ): Int {
    return event.fold({ 0 }, { Optional.ofNullable(it.event.data.retryCount).orElse(0) })
  }

  private fun parseEvent(
    data: BinaryData
  ): Mono<
    Either<
      QueueEvent<TransactionUserReceiptAddErrorEvent>,
      QueueEvent<TransactionUserReceiptAddRetriedEvent>>> {
    val notificationErrorEvent =
      data.toObjectAsync(
        object : TypeReference<QueueEvent<TransactionUserReceiptAddErrorEvent>>() {})

    val notificationRetryEvent =
      data.toObjectAsync(
        object : TypeReference<QueueEvent<TransactionUserReceiptAddRetriedEvent>>() {})

    return notificationErrorEvent
      .map<
        Either<
          QueueEvent<TransactionUserReceiptAddErrorEvent>,
          QueueEvent<TransactionUserReceiptAddRetriedEvent>>> { Either.left(it) }
      .onErrorResume { notificationRetryEvent.map { Either.right(it) } }
  }

  @ServiceActivator(
    inputChannel = "transactionretrynotificationschannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
  ): Mono<Void> {
    val binaryData = BinaryData.fromBytes(payload)
    val event = parseEvent(binaryData)
    val transactionId = event.map { getTransactionIdFromPayload(it) }
    val retryCount = event.map { getRetryCountFromPayload(it) }
    val baseTransaction =
      reduceEvents(transactionId, transactionsEventStoreRepository, EmptyTransaction())
    val notificationResendPipeline =
      baseTransaction
        .flatMap {
          logger.info("Status for transaction ${it.transactionId.value()}: ${it.status}")

          if (it.status != TransactionStatusDto.NOTIFICATION_ERROR) {
            Mono.error(
              BadTransactionStatusException(
                transactionId = it.transactionId,
                expected = listOf(TransactionStatusDto.NOTIFICATION_ERROR),
                actual = it.status))
          } else {
            Mono.just(it)
          }
        }
        .cast(TransactionWithUserReceiptError::class.java)
        .flatMap { tx ->
          mono { userReceiptMailBuilder.buildNotificationEmailRequestDto(tx) }
            .flatMap { notificationsServiceClient.sendNotificationEmail(it) }
            .flatMap {
              updateNotifiedTransactionStatus(
                  tx, transactionsViewRepository, transactionUserReceiptRepository)
                .flatMap {
                  notificationRefundTransactionPipeline(
                    tx,
                    transactionsRefundedEventStoreRepository,
                    transactionsViewRepository,
                    paymentGatewayClient,
                    refundRetryService)
                }
            }
            .then()
            .onErrorResume { exception ->
              logger.error(
                "Got exception while retrying user receipt mail sending for transaction with id ${tx.transactionId}!",
                exception)
              retryCount
                .flatMap { retryCount ->
                  notificationRetryService.enqueueRetryEvent(tx, retryCount)
                }
                .onErrorResume(NoRetryAttemptsLeftException::class.java) { enqueueException ->
                  logger.error(
                    "No more attempts left for user receipt send retry", enqueueException)

                  notificationRefundTransactionPipeline(
                      tx,
                      transactionsRefundedEventStoreRepository,
                      transactionsViewRepository,
                      paymentGatewayClient,
                      refundRetryService)
                    .then()
                }
                .then()
            }
        }

    return event.flatMap { it ->
      val e = it.fold({ it as QueueEvent<*> }, { it as QueueEvent<*> })

      runTracedPipelineWithDeadLetterQueue(
        checkPointer,
        notificationResendPipeline,
        e,
        deadLetterQueueAsyncClient,
        deadLetterTTLSeconds,
        tracingUtils,
        this::class.simpleName!!)
    }
  }
}
