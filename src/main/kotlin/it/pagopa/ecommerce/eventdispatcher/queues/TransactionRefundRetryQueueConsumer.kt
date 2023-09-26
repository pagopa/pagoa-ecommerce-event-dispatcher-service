package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionRefundRetriedEvent as TransactionRefundRetriedEventV1
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRetriedEvent as TransactionRefundRetriedEventV2
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Event consumer for events related to refund retry. This consumer's responsibilities are to handle
 * refund process retry for a given transaction
 */
@Service("TransactionRefundRetryQueueConsumer")
class TransactionRefundRetryQueueConsumer(
  @Autowired
  private val queueConsumerV1:
    it.pagopa.ecommerce.eventdispatcher.queues.v1.TransactionRefundRetryQueueConsumer,
  @Autowired
  private val queueConsumerV2:
    it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionRefundRetryQueueConsumer,
  @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
  @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
  private val deadLetterTTLSeconds: Int,
  @Autowired private val strictSerializerProviderV1: StrictJsonSerializerProvider,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) {

  var logger: Logger = LoggerFactory.getLogger(TransactionRefundRetryQueueConsumer::class.java)

  private fun parseEvent(data: BinaryData): Mono<Pair<BaseTransactionEvent<*>, TracingInfo?>> {
    val jsonSerializerV1 = strictSerializerProviderV1.createInstance()
    val jsonSerializerV2 = strictSerializerProviderV2.createInstance()
    val refundRetriedEventV1 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionRefundRetriedEventV1>>() {},
          jsonSerializerV1)
        .map { it.event to it.tracingInfo }

    val untracedRefundRetriedEventV1 =
      data
        .toObjectAsync(
          object : TypeReference<TransactionRefundRetriedEventV1>() {}, jsonSerializerV1)
        .map { it to null }

    val refundRetriedEventV2 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionRefundRetriedEventV2>>() {},
          jsonSerializerV2)
        .map { it.event to it.tracingInfo }

    return Mono.firstWithValue(
        refundRetriedEventV1, untracedRefundRetriedEventV1, refundRetriedEventV2)
      .onErrorMap { InvalidEventException(data.toBytes(), it) }
  }

  @ServiceActivator(inputChannel = "transactionrefundretrychannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
  ): Mono<Void> {
    val eventWithTracingInfo = parseEvent(BinaryData.fromBytes(payload))

    return eventWithTracingInfo
      .flatMap { (e, tracingInfo) ->
        when (e) {
          is TransactionRefundRetriedEventV1 -> {
            logger.info("Event {} with tracing info {} dispatched to V1 handler", e, tracingInfo)
            queueConsumerV1.messageReceiver(e to tracingInfo, checkPointer)
          }
          is TransactionRefundRetriedEventV2 -> {
            logger.info("Event {} with tracing info {} dispatched to V2 handler", e, tracingInfo)
            queueConsumerV2.messageReceiver(QueueEvent(e, tracingInfo), checkPointer)
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
          checkPointer, payload, it, deadLetterQueueAsyncClient, deadLetterTTLSeconds)
      }
  }
}
