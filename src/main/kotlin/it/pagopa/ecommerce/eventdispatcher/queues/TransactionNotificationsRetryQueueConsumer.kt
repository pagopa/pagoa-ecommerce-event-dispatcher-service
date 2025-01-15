package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionUserReceiptAddErrorEvent as TransactionUserReceiptAddErrorEventV1
import it.pagopa.ecommerce.commons.documents.v1.TransactionUserReceiptAddRetriedEvent as TransactionUserReceiptAddRetriedEventV1
import it.pagopa.ecommerce.commons.documents.v2.TransactionUserReceiptAddErrorEvent as TransactionUserReceiptAddErrorEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionUserReceiptAddRetriedEvent as TransactionUserReceiptAddRetriedEventV2
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.warmup.annotations.WarmupFunction
import it.pagopa.ecommerce.payment.requests.warmup.utils.DummyCheckpointer
import it.pagopa.ecommerce.payment.requests.warmup.utils.WarmupRequests.getTransactionUserReceiptAddErrorEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service("TransactionNotificationsRetryQueueConsumer")
class TransactionNotificationsRetryQueueConsumer(
  @Autowired
  private val queueConsumerV1:
    it.pagopa.ecommerce.eventdispatcher.queues.v1.TransactionNotificationsRetryQueueConsumer,
  @Autowired
  private val queueConsumerV2:
    it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionNotificationsRetryQueueConsumer,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val strictSerializerProviderV1: StrictJsonSerializerProvider,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) {
  var logger: Logger =
    LoggerFactory.getLogger(TransactionNotificationsRetryQueueConsumer::class.java)

  fun parseEvent(payload: ByteArray): Mono<Pair<BaseTransactionEvent<*>, TracingInfo?>> {
    val data = BinaryData.fromBytes(payload)
    val jsonSerializerV1 = strictSerializerProviderV1.createInstance()
    val jsonSerializerV2 = strictSerializerProviderV2.createInstance()
    val notificationErrorEventV1 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionUserReceiptAddErrorEventV1>>() {},
          jsonSerializerV1)
        .map { it.event to it.tracingInfo }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    val notificationRetryEventV1 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionUserReceiptAddRetriedEventV1>>() {},
          jsonSerializerV1)
        .map { it.event to it.tracingInfo }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    val untracedNotificationErrorEventV1 =
      data
        .toObjectAsync(
          object : TypeReference<TransactionUserReceiptAddErrorEventV1>() {}, jsonSerializerV1)
        .map { it to null }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    val untracedNotificationRetryEventV1 =
      data
        .toObjectAsync(
          object : TypeReference<TransactionUserReceiptAddRetriedEventV1>() {}, jsonSerializerV1)
        .map { it to null }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }
    val notificationErrorEventV2 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionUserReceiptAddErrorEventV2>>() {},
          jsonSerializerV2)
        .map { it.event to it.tracingInfo }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    val notificationRetryEventV2 =
      data
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionUserReceiptAddRetriedEventV2>>() {},
          jsonSerializerV2)
        .map { it.event to it.tracingInfo }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    return Mono.firstWithValue(
        notificationErrorEventV1,
        notificationRetryEventV1,
        untracedNotificationErrorEventV1,
        untracedNotificationRetryEventV1,
        notificationErrorEventV2,
        notificationRetryEventV2)
      .onErrorMap { InvalidEventException(data.toBytes(), it) }
  }

  @ServiceActivator(
    inputChannel = "transactionretrynotificationschannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
  ): Mono<Unit> {
    val eventWithTracingInfo = parseEvent(payload)
    return eventWithTracingInfo
      .flatMap { (e, tracingInfo) ->
        when (e) {
          is TransactionUserReceiptAddErrorEventV1 -> {
            logger.debug("Event {} with tracing info {} dispatched to V1 handler", e, tracingInfo)
            queueConsumerV1.messageReceiver(Pair(Either.left(e), tracingInfo), checkPointer)
          }
          is TransactionUserReceiptAddRetriedEventV1 -> {
            logger.debug("Event {} with tracing info {} dispatched to V1 handler", e, tracingInfo)
            queueConsumerV1.messageReceiver(Pair(Either.right(e), tracingInfo), checkPointer)
          }
          is TransactionUserReceiptAddErrorEventV2 -> {
            logger.debug("Event {} with tracing info {} dispatched to V2 handler", e, tracingInfo)
            queueConsumerV2.messageReceiver(Either.left(QueueEvent(e, tracingInfo)), checkPointer)
          }
          is TransactionUserReceiptAddRetriedEventV2 -> {
            logger.debug("Event {} with tracing info {} dispatched to V2 handler", e, tracingInfo)
            queueConsumerV2.messageReceiver(Either.right(QueueEvent(e, tracingInfo)), checkPointer)
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
    messageReceiver(getTransactionUserReceiptAddErrorEvent(), DummyCheckpointer).block()
  }
}
