package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationOutcomeWaitingEvent
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.StrictJsonSerializerProvider
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.warmup.annotations.WarmupFunction
import it.pagopa.ecommerce.payment.requests.warmup.utils.DummyCheckpointer
import it.pagopa.ecommerce.payment.requests.warmup.utils.WarmupRequests.getTransactionAuthorizationOutcomeWaitingEvent
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * Event consumer for transactions for which retrieve authorization outcome status. This consumer
 * will handle TransactionAuthorizationRequestedEvent V2 events and inquiry gateway in order to
 * retrieve authorization outcome
 */
@Service("TransactionAuthorizationOutcomeWaitingQueueConsumer")
class TransactionAuthorizationOutcomeWaitingQueueConsumer(
  @Autowired
  private val queueConsumerV2:
    it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionAuthorizationOutcomeWaitingQueueConsumer,
  @Autowired private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient,
  @Autowired private val strictSerializerProviderV2: StrictJsonSerializerProvider
) {

  var logger: Logger =
    LoggerFactory.getLogger(TransactionAuthorizationOutcomeWaitingQueueConsumer::class.java)

  fun parseEvent(
    payload: ByteArray
  ): Mono<QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>> {
    val data = BinaryData.fromBytes(payload)
    val jsonSerializerV2 = strictSerializerProviderV2.createInstance()

    return data
      .toObjectAsync(
        object : TypeReference<QueueEvent<TransactionAuthorizationOutcomeWaitingEvent>>() {},
        jsonSerializerV2)
      .onErrorMap {
        logger.debug(ERROR_PARSING_EVENT_ERROR, it)
        InvalidEventException(data.toBytes(), it)
      }
  }

  @ServiceActivator(
    inputChannel = "transactionsauthorizationoutcomewaitingchannel", outputChannel = "nullChannel")
  fun messageReceiver(
    @Payload payload: ByteArray,
    @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
  ): Mono<Unit> {
    val eventWithTracingInfo = parseEvent(payload)
    return eventWithTracingInfo
      .flatMap { e ->
        when (e.event) {
          is TransactionAuthorizationOutcomeWaitingEvent -> {
            logger.debug(
              "Event {} with tracing info {} dispatched to V2 handler", e.event, e.tracingInfo)
            queueConsumerV2.messageReceiver(e, checkPointer)
          }
          else -> {
            logger.error(
              "Event {} with tracing info {} cannot be dispatched to any know handler",
              e.event,
              e.tracingInfo)
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
    messageReceiver(getTransactionAuthorizationOutcomeWaitingEvent(), DummyCheckpointer).block()
  }
}
