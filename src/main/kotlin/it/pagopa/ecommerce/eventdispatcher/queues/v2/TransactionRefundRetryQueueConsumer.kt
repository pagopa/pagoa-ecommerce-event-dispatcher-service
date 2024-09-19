package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.BaseTransactionRefundedData
import it.pagopa.ecommerce.commons.documents.v2.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRetriedEvent
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadTransactionStatusException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.RefundService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.v2.RefundRetryService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgService
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Event consumer for events related to refund retry. This consumer's responsibilities are to handle
 * refund process retry for a given transaction
 */
@Service("TransactionRefundRetryQueueConsumerV2")
class TransactionRefundRetryQueueConsumer(
  @Autowired private val paymentGatewayClient: PaymentGatewayClient,
  @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
  @Autowired
  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData>,
  @Autowired private val transactionsViewRepository: TransactionsViewRepository,
  @Autowired private val refundService: RefundService,
  @Autowired private val refundRetryService: RefundRetryService,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val tracingUtils: TracingUtils,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider,
  @Autowired private val ngpService: NpgService,
) {

  var logger: Logger = LoggerFactory.getLogger(TransactionRefundRetryQueueConsumer::class.java)

  fun messageReceiver(
    parsedEvent: QueueEvent<TransactionRefundRetriedEvent>,
    checkPointer: Checkpointer
  ): Mono<Unit> {
    val event = parsedEvent.event
    val tracingInfo = parsedEvent.tracingInfo
    val events =
      transactionsEventStoreRepository
        .findByTransactionIdOrderByCreationDateAsc(event.transactionId)
        .map { it as TransactionEvent<Any> }
    val baseTransaction = Mono.defer { reduceEvents(events) }
    val refundPipeline =
      baseTransaction
        .flatMap {
          logger.info("Status for transaction ${it.transactionId.value()}: ${it.status}")

          if (it.status != TransactionStatusDto.REFUND_ERROR) {
            Mono.error(
              BadTransactionStatusException(
                transactionId = it.transactionId,
                expected = listOf(TransactionStatusDto.REFUND_ERROR),
                actual = it.status))
          } else {
            Mono.just(it)
          }
        }
        .flatMap { tx ->
          refundTransaction(
            tx,
            transactionsRefundedEventStoreRepository,
            transactionsViewRepository,
            refundService,
            refundRetryService,
            ngpService,
            tracingInfo,
            event.data.retryCount,
          )
        }
    return runTracedPipelineWithDeadLetterQueue(
      checkPointer,
      refundPipeline,
      QueueEvent(event, tracingInfo),
      deadLetterTracedQueueAsyncClient,
      tracingUtils,
      this::class.simpleName!!,
      strictSerializerProviderV2)
  }
}
