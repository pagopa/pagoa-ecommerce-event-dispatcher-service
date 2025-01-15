package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionUserCanceledEvent as TransactionUserCanceledEventV1
import it.pagopa.ecommerce.commons.documents.v2.TransactionClosureRequestedEvent as TransactionClosureRequestedEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionUserCanceledEvent as TransactionUserCanceledEventV2
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.warmup.annotations.WarmupFunction
import it.pagopa.ecommerce.payment.requests.warmup.utils.DummyCheckpointer
import it.pagopa.ecommerce.payment.requests.warmup.utils.WarmupRequests.getTransactionClosureRequestedEvent
import java.util.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service("TransactionClosePaymentQueueConsumer")
class TransactionClosePaymentQueueConsumer(
  @Autowired
  private val queueConsumerV1:
    it.pagopa.ecommerce.eventdispatcher.queues.v1.TransactionClosePaymentQueueConsumer,
  @Autowired
  private val queueConsumerV2:
    it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionClosePaymentQueueConsumer,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val strictSerializerProviderV1: StrictJsonSerializerProvider,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) {
  var logger: Logger = LoggerFactory.getLogger(TransactionClosePaymentQueueConsumer::class.java)

  fun parseEvent(payload: ByteArray): Mono<Pair<BaseTransactionEvent<Void>, TracingInfo?>> {
    val binaryData = BinaryData.fromBytes(payload)
    val jsonSerializerV1 = strictSerializerProviderV1.createInstance()
    val jsonSerializerV2 = strictSerializerProviderV2.createInstance()
    val queueEventV1 =
      binaryData
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionUserCanceledEventV1>>() {}, jsonSerializerV1)
        .map { Pair(it.event, it.tracingInfo) }

    val untracedEventV1 =
      binaryData
        .toObjectAsync(
          object : TypeReference<TransactionUserCanceledEventV1>() {}, jsonSerializerV1)
        .map { Pair(it, null) }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }
    val transactionUserCanceledEventV2 =
      binaryData
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionUserCanceledEventV2>>() {}, jsonSerializerV2)
        .map { Pair(it.event, it.tracingInfo) }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    val transactionClosureRequestedEventV2 =
      binaryData
        .toObjectAsync(
          object : TypeReference<QueueEvent<TransactionClosureRequestedEventV2>>() {},
          jsonSerializerV2)
        .map { Pair(it.event, it.tracingInfo) }
        .onErrorResume {
          logger.debug(ERROR_PARSING_EVENT_ERROR, it)
          Mono.empty()
        }

    return Mono.firstWithValue(
        queueEventV1,
        untracedEventV1,
        transactionUserCanceledEventV2,
        transactionClosureRequestedEventV2)
      .onErrorMap { InvalidEventException(binaryData.toBytes(), it) }
  }

  @ServiceActivator(inputChannel = "transactionclosureschannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer,
  ): Mono<Unit> {
    val parsedEvents = parseEvent(payload)
    return parsedEvents
      .flatMap { (e, tracingInfo) ->
        when (e) {
          is TransactionUserCanceledEventV1 -> {
            logger.debug("Event {} with tracing info {} dispatched to V1 handler", e, tracingInfo)
            queueConsumerV1.messageReceiver(e to tracingInfo, checkPointer)
          }
          is TransactionUserCanceledEventV2 -> {
            logger.debug("Event {} with tracing info {} dispatched to V2 handler", e, tracingInfo)
            queueConsumerV2.messageReceiver(Either.left(QueueEvent(e, tracingInfo)), checkPointer)
          }
          is TransactionClosureRequestedEventV2 -> {
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
    messageReceiver(getTransactionClosureRequestedEvent(), DummyCheckpointer).block()
  }
}
