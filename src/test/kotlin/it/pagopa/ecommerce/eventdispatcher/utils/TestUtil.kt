package it.pagopa.ecommerce.eventdispatcher.utils

import com.azure.core.http.rest.Response
import com.azure.core.http.rest.ResponseBase
import com.azure.core.serializer.json.jackson.JacksonJsonSerializer
import com.azure.core.serializer.json.jackson.JacksonJsonSerializerBuilder
import com.azure.storage.queue.models.SendMessageResult
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator
import io.mockk.every
import io.mockk.mockkStatic
import io.opentelemetry.api.trace.Span
import it.pagopa.ecommerce.commons.documents.v1.TransactionAuthorizationRequestData
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.queues.createSpanWithRemoteTracingContext
import it.pagopa.ecommerce.eventdispatcher.queues.traceMonoWithSpan
import it.pagopa.generated.ecommerce.gateway.v1.dto.VposDeleteResponseDto
import it.pagopa.generated.ecommerce.gateway.v1.dto.XPayRefundResponse200Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import java.time.OffsetDateTime
import java.util.*
import kotlin.reflect.KFunction
import reactor.core.publisher.Mono

val OBJECT_MAPPER: ObjectMapper =
  ObjectMapper()
    .activateDefaultTyping(
      BasicPolymorphicTypeValidator.builder()
        .allowIfBaseType("it.pagopa.ecommerce")
        .allowIfBaseType(List::class.java)
        .build(),
      ObjectMapper.DefaultTyping.EVERYTHING,
      JsonTypeInfo.As.PROPERTY)
    .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
    .configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, true)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)

val JSON_SERIALIZER: JacksonJsonSerializer =
  JacksonJsonSerializerBuilder().serializer(OBJECT_MAPPER).build()

fun getMockedClosePaymentRequest(
  transactionId: UUID,
  outcome: ClosePaymentRequestV2Dto.OutcomeEnum
): ClosePaymentRequestV2Dto {

  val authEventData =
    TransactionAuthorizationRequestData(
      100,
      1,
      "paymentInstrumentId",
      "pspId",
      "paymentTypeCode",
      "brokerName",
      "pspChannelCode",
      "requestId",
      "pspBusinessName",
      "authorizationRequestId",
      TransactionAuthorizationRequestData.PaymentGateway.VPOS,
      TransactionTestUtils.LOGO_URI,
      TransactionAuthorizationRequestData.CardBrand.VISA)

  return ClosePaymentRequestV2Dto().apply {
    paymentTokens = listOf(UUID.randomUUID().toString())
    this.outcome = outcome
    idPSP = authEventData.pspId
    paymentMethod = authEventData.paymentTypeCode
    idBrokerPSP = authEventData.brokerName
    idChannel = authEventData.pspChannelCode
    this.transactionId = transactionId.toString()
    totalAmount = (authEventData.amount + authEventData.fee).toBigDecimal()
    timestampOperation = OffsetDateTime.now()
  }
}

fun getMockedXPayRefundRequest(
  paymentId: String?,
  result: String = "success",
): XPayRefundResponse200Dto {
  if (result == "success") {
    return XPayRefundResponse200Dto()
      .requestId(UUID.randomUUID().toString())
      .status(XPayRefundResponse200Dto.StatusEnum.CANCELLED)
      .error("")
  } else {
    return XPayRefundResponse200Dto()
      .requestId(UUID.randomUUID().toString())
      .status(XPayRefundResponse200Dto.StatusEnum.CREATED)
      .error("err")
  }
}

fun getMockedVPosRefundRequest(
  paymentId: String?,
  result: String = "success",
): VposDeleteResponseDto {
  if (result == "success") {
    return VposDeleteResponseDto()
      .requestId(UUID.randomUUID().toString())
      .status(VposDeleteResponseDto.StatusEnum.CANCELLED)
      .error("")
  } else {
    return VposDeleteResponseDto()
      .requestId(UUID.randomUUID().toString())
      .status(VposDeleteResponseDto.StatusEnum.CREATED)
      .error("err")
  }
}

fun queueSuccessfulResponse(): Mono<Response<SendMessageResult>> {
  val sendMessageResult = SendMessageResult()
  sendMessageResult.messageId = "msgId"
  sendMessageResult.timeNextVisible = OffsetDateTime.now()
  return Mono.just(ResponseBase(null, 200, null, sendMessageResult, null))
}

const val TRANSIENT_QUEUE_TTL_SECONDS = 30

const val DEAD_LETTER_QUEUE_TTL_SECONDS = -1

fun setupTracingMocks() {
  val traceMonoWithSpanFunction: (Span, Mono<Any>) -> Mono<Any> = ::traceMonoWithSpan
  val traceMonoWithSpanKFunction = traceMonoWithSpanFunction as KFunction<*>
  mockkStatic(traceMonoWithSpanKFunction)
  every { traceMonoWithSpan(any(), any<Mono<Any>>()) } returnsArgument (1)

  mockkStatic(::createSpanWithRemoteTracingContext)
  every { createSpanWithRemoteTracingContext(any(), any(), any(), any()) } returns Span.getInvalid()
}
