package it.pagopa.ecommerce.payment.requests.warmup.utils

import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.documents.BaseTransactionEvent
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.warmup.utils.EventsUtil
import reactor.core.publisher.Mono

object DummyCheckpointer : Checkpointer {
  override fun success(): Mono<Void> = Mono.empty()
  override fun failure(): Mono<Void> = Mono.empty()
}

object WarmupRequests {

  private val queuesConsumerConfig = QueuesConsumerConfig()
  val strictSerializerProviderV2 = queuesConsumerConfig.strictSerializerProviderV2()

  fun getTransactionAuthorizationOutcomeWaitingEvent(): ByteArray {
    val event = EventsUtil.getTransactionAuthorizationOutcomeWaitingEventObject()
    return traceAndSerializeEvent(event)
  }

  fun getTransactionUserReceiptAddErrorEvent(): ByteArray {
    val event = EventsUtil.getTransactionUserReceiptAddErrorEventObject()
    return traceAndSerializeEvent(event)
  }

  fun getTransactionAuthorizationRequestedEvent(): ByteArray {
    val event = EventsUtil.getTransactionAuthorizationRequestedEventObject()
    return traceAndSerializeEvent(event)
  }

  fun getTransactionClosureRequestedEvent(): ByteArray {
    val event = EventsUtil.getTransactionClosureRequestedEventObject()
    return traceAndSerializeEvent(event)
  }

  fun getTransactionClosureErrorEvent(): ByteArray {
    val event = EventsUtil.getTransactionClosureErrorEventObject()
    return traceAndSerializeEvent(event)
  }

  fun getTransactionExpiredEvent(): ByteArray {
    val event = EventsUtil.getTransactionExpiredEventObject()
    return traceAndSerializeEvent(event)
  }

  fun getTransactionUserReceiptRequestedEvent(): ByteArray {
    val event = EventsUtil.getTransactionUserReceiptRequestedEventObject()
    return traceAndSerializeEvent(event)
  }

  fun getTransactionRefundRetriedEvent(): ByteArray {
    val event = EventsUtil.getTransactionRefundRetriedEventObject()
    return traceAndSerializeEvent(event)
  }

  fun getTransactionRefundRequestedEvent(): ByteArray {
    val event = EventsUtil.getTransactionRefundRequestedEventObject()
    return traceAndSerializeEvent(event)
  }

  private fun traceAndSerializeEvent(event: BaseTransactionEvent<*>): ByteArray {
    val queueEvent = QueueEvent(event, null)
    val objectMapper = strictSerializerProviderV2.objectMapper
    var jsonString = objectMapper.writeValueAsString(queueEvent)

    // Replace the "tracingInfo": null with the desired structure
    val tracingInfoReplacement =
      """
        "tracingInfo": {
          "traceparent": "00-5868efa082297543570dafff7d53c70b-56f1d9262e6ee6cf-00",
          "tracestate": null,
          "baggage": null
        }
    """.trimIndent()

    // Use regular expression or string replacement to perform the substitution
    jsonString = jsonString.replace("\"tracingInfo\":null", tracingInfoReplacement)
    return jsonString.toByteArray()
  }
}
