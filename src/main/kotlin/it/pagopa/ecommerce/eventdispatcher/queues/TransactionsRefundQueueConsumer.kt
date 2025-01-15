package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundRequestedEvent as TransactionRefundRequestedEventV1
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundRetriedEvent as TransactionRefundRetriedEventV1
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRequestedEvent as TransactionRefundRequestedEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRetriedEvent as TransactionRefundRetriedEventV2
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.warmup.annotations.WarmupFunction
import it.pagopa.ecommerce.payment.requests.warmup.utils.DummyCheckpointer
import it.pagopa.ecommerce.payment.requests.warmup.utils.WarmupRequests.getTransactionRefundRequestedEvent
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Event consumer for transactions to refund. These events are input in the event queue only when a
 * transaction is stuck in an REFUND_REQUESTED state **and** needs to be reverted
 */
@Service("TransactionsRefundQueueConsumer")
class TransactionsRefundQueueConsumer(
  @Autowired
  private val queueConsumerV1:
    it.pagopa.ecommerce.eventdispatcher.queues.v1.TransactionsRefundQueueConsumer,
  @Autowired
  private val queueConsumerV2:
    it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionsRefundQueueConsumer,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val strictSerializerProviderV1: StrictJsonSerializerProvider,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) {

  var logger: Logger = LoggerFactory.getLogger(TransactionsRefundQueueConsumer::class.java)

  fun parseEvent(payload: ByteArray): Mono<Pair<BaseTransactionEvent<*>, TracingInfo?>> {
    val data = BinaryData.fromBytes(payload)
    val jsonSerializerV1 = strictSerializerProviderV1.createInstance()
    val jsonSerializerV2 = strictSerializerProviderV2.createInstance()
    val refundRequestedEventV1 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionRefundRequestedEventV1>>() {},
          jsonSerializerV1)
        .map { it.event to it.tracingInfo }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    val refundRetriedEventV1 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionRefundRetriedEventV1>>() {},
          jsonSerializerV1)
        .map { it.event to it.tracingInfo }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    val untracedRefundRequestedEventV1 =
      data
        .toObjectAsync(
          object : TypeReference<TransactionRefundRequestedEventV1>() {}, jsonSerializerV1)
        .map { it to null }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    val untracedRefundRetriedEventV1 =
      data
        .toObjectAsync(
          object : TypeReference<TransactionRefundRetriedEventV1>() {}, jsonSerializerV1)
        .map { it to null }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    val refundRequestedEventV2 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionRefundRequestedEventV2>>() {},
          jsonSerializerV2)
        .map { it.event to it.tracingInfo }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    val refundRetriedEventV2 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionRefundRetriedEventV2>>() {},
          jsonSerializerV2)
        .map { it.event to it.tracingInfo }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    return Mono.firstWithValue(
        refundRequestedEventV1,
        refundRetriedEventV1,
        untracedRefundRequestedEventV1,
        untracedRefundRetriedEventV1,
        refundRequestedEventV2,
        refundRetriedEventV2,
      )
      .onErrorMap(NoSuchElementException::class.java) { InvalidEventException(data.toBytes(), it) }
  }

  @ServiceActivator(inputChannel = "transactionsrefundchannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
  ): Mono<Unit> {
    val eventWithTracingInfo = parseEvent(payload)
    return eventWithTracingInfo
      .flatMap { (e, tracingInfo) ->
        when (e) {
          is TransactionRefundRequestedEventV1 -> {
            logger.debug("Event {} with tracing info {} dispatched to V1 handler", e, tracingInfo)
            queueConsumerV1.messageReceiver(Pair(Either.right(e), tracingInfo), checkPointer)
          }
          is TransactionRefundRetriedEventV1 -> {
            logger.debug("Event {} with tracing info {} dispatched to V1 handler", e, tracingInfo)
            queueConsumerV1.messageReceiver(Pair(Either.left(e), tracingInfo), checkPointer)
          }
          is TransactionRefundRequestedEventV2 -> {
            logger.debug("Event {} with tracing info {} dispatched to V2 handler", e, tracingInfo)
            queueConsumerV2.messageReceiver(Either.right(QueueEvent(e, tracingInfo)), checkPointer)
          }
          is TransactionRefundRetriedEventV2 -> {
            logger.debug("Event {} with tracing info {} dispatched to V2 handler", e, tracingInfo)
            queueConsumerV2.messageReceiver(Either.left(QueueEvent(e, tracingInfo)), checkPointer)
          }
          else -> {
            logger.error(
              "Event {} with tracing info {} cannot be dispatched to any know handler",
              e,
              tracingInfo)
            Mono.error(InvalidEventException(payload, null))
          }
        }
      }
      .onErrorResume(InvalidEventException::class.java) {
        logger.error("Invalid input event", it)
        writeEventToDeadLetterQueue(
          checkPointer,
          payload,
          it,
          deadLetterTracedQueueAsyncClient,
          DeadLetterTracedQueueAsyncClient.PARSING_EVENT_ERROR_CONTEXT)
      }
  }

  @WarmupFunction
  fun warmupService() {
    messageReceiver(getTransactionRefundRequestedEvent(), DummyCheckpointer).block()
  }
}
