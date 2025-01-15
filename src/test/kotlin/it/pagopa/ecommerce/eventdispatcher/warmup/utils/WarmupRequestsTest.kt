package it.pagopa.ecommerce.eventdispatcher.warmup.utils

import it.pagopa.ecommerce.commons.documents.v2.TransactionAuthorizationRequestedEvent as TransactionAuthorizationRequestedEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionClosureErrorEvent as TransactionClosureErrorEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionClosureRequestedEvent as TransactionClosureRequestedEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionExpiredEvent as TransactionExpiredEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRequestedEvent
import it.pagopa.ecommerce.commons.documents.v2.TransactionRefundRetriedEvent as TransactionRefundRetriedEventV2
import it.pagopa.ecommerce.commons.documents.v2.TransactionUserReceiptAddErrorEvent
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.queues.*
import it.pagopa.ecommerce.eventdispatcher.utils.DeadLetterTracedQueueAsyncClient
import it.pagopa.ecommerce.eventdispatcher.validation.BeanValidationConfiguration
import it.pagopa.ecommerce.payment.requests.warmup.utils.WarmupRequests
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.springframework.context.annotation.Import
import org.springframework.test.context.TestPropertySource
import reactor.test.StepVerifier

@Import(BeanValidationConfiguration::class)
@TestPropertySource(locations = ["classpath:application.test.properties"])
class WarmupRequestsTest {

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()
  private val queuesConsumerConfig = QueuesConsumerConfig()

  val strictSerializerProviderV1 = queuesConsumerConfig.strictSerializerProviderV1()
  val strictSerializerProviderV2 = queuesConsumerConfig.strictSerializerProviderV2()

  private val transactionRefundedEventsConsumer =
    TransactionsRefundQueueConsumer(
      queueConsumerV1 = mock(),
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV1 = strictSerializerProviderV1,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  private val transactionRefundedRetryEventsConsumer =
    TransactionRefundRetryQueueConsumer(
      queueConsumerV1 = mock(),
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV1 = strictSerializerProviderV1,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  private val transactionNotificationsRetryQueueConsumer =
    TransactionNotificationsRetryQueueConsumer(
      queueConsumerV1 = mock(),
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV1 = strictSerializerProviderV1,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  private val transactionClosePaymentRetryQueueConsumer =
    TransactionClosePaymentRetryQueueConsumer(
      queueConsumerV1 = mock(),
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV1 = strictSerializerProviderV1,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  private val transactionClosePaymentQueueConsumer =
    TransactionClosePaymentQueueConsumer(
      queueConsumerV1 = mock(),
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV1 = strictSerializerProviderV1,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  private val transactionExpirationQueueConsumer =
    TransactionExpirationQueueConsumer(
      queueConsumerV1 = mock(),
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV1 = strictSerializerProviderV1,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  private val transactionAuthorizationRequestedQueueConsumer =
    TransactionAuthorizationRequestedQueueConsumer(
      queueConsumerV2 = mock(),
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      strictSerializerProviderV2 = strictSerializerProviderV2)

  @Test
  fun `test parse TransactionUserReceiptAddErrorEvent`() {
    val payload = WarmupRequests.getTransactionUserReceiptAddErrorEvent()
    val result = transactionNotificationsRetryQueueConsumer.parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionUserReceiptAddErrorEvent::class.java, event.first::class.java)
        assertEquals("TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT", event.first.eventCode)
      }
      .verifyComplete()
  }

  @Test
  fun `test parse TransactionAuthorizationRequestedEventV2`() {
    val payload = WarmupRequests.getTransactionAuthorizationRequestedEvent()
    val result = transactionAuthorizationRequestedQueueConsumer.parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionAuthorizationRequestedEventV2::class.java, event.event::class.java)
        assertEquals("TRANSACTION_AUTHORIZATION_REQUESTED_EVENT", event.event.eventCode)
      }
      .verifyComplete()
  }

  @Test
  fun `test parse TransactionClosureRequestedEventV2`() {
    val payload = WarmupRequests.getTransactionClosureRequestedEvent()
    val result = transactionClosePaymentQueueConsumer.parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionClosureRequestedEventV2::class.java, event.first::class.java)
        assertEquals("TRANSACTION_CLOSURE_REQUESTED_EVENT", event.first.eventCode)
      }
      .verifyComplete()
  }

  @Test
  fun `test parse TransactionClosureErrorEventV2`() {
    val payload = WarmupRequests.getTransactionClosureErrorEvent()
    val result = transactionClosePaymentRetryQueueConsumer.parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionClosureErrorEventV2::class.java, event.first::class.java)
        assertEquals("TRANSACTION_CLOSURE_ERROR_EVENT", event.first.eventCode)
      }
      .verifyComplete()
  }

  @Test
  fun `test parse TransactionExpiredEventV2`() {
    val payload = WarmupRequests.getTransactionExpiredEvent()
    val result = transactionExpirationQueueConsumer.parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionExpiredEventV2::class.java, event.first::class.java)
        assertEquals("TRANSACTION_EXPIRED_EVENT", event.first.eventCode)
      }
      .verifyComplete()
  }

  @Test
  fun `test parse TransactionRefundRequestedEvent`() {
    val payload = WarmupRequests.getTransactionRefundRequestedEvent()
    val result = transactionRefundedEventsConsumer.parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionRefundRequestedEvent::class.java, event.first::class.java)
        assertEquals("TRANSACTION_REFUND_REQUESTED_EVENT", event.first.eventCode)
      }
      .verifyComplete()
  }

  @Test
  fun `test parse TransactionRefundRetriedEventV2`() {
    val payload = WarmupRequests.getTransactionRefundRetriedEvent()
    val result = transactionRefundedRetryEventsConsumer.parseEvent(payload)

    StepVerifier.create(result)
      .assertNext { event ->
        assertEquals(TransactionRefundRetriedEventV2::class.java, event.first::class.java)
        assertEquals("TRANSACTION_REFUND_RETRIED_EVENT", event.first.eventCode)
      }
      .verifyComplete()
  }
}
