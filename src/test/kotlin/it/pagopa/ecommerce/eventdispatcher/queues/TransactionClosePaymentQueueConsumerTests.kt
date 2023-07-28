package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.TransactionEventCode
import it.pagopa.ecommerce.commons.domain.v1.TransactionId
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.exceptions.BadClosePaymentRequest
import it.pagopa.ecommerce.eventdispatcher.exceptions.TransactionNotFound
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.NodeService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.ClosureRetryService
import it.pagopa.ecommerce.eventdispatcher.utils.DEAD_LETTER_QUEUE_TTL_SECONDS
import it.pagopa.ecommerce.eventdispatcher.utils.queueSuccessfulResponse
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import java.time.Duration
import java.time.ZonedDateTime
import java.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.test.StepVerifier

@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionClosePaymentQueueConsumerTests {
  private val checkpointer: Checkpointer = mock()

  private val nodeService: NodeService = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val transactionClosureErrorEventStoreRepository: TransactionsEventStoreRepository<Void> =
    mock()

  private val closureRetryService: ClosureRetryService = mock()

  private val transactionClosedEventRepository:
    TransactionsEventStoreRepository<TransactionClosureData> =
    mock()

  private val deadLetterQueueAsyncClient: QueueAsyncClient = mock()

  private val tracingUtils: TracingUtils = TracingUtilsTests.getMock()

  @Captor private lateinit var viewArgumentCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var closedEventStoreRepositoryCaptor:
    ArgumentCaptor<TransactionEvent<TransactionClosureData>>

  @Captor
  private lateinit var closureErrorEventStoreRepositoryCaptor:
    ArgumentCaptor<TransactionClosureErrorEvent>

  private val transactionClosureEventsConsumer =
    TransactionClosePaymentQueueConsumer(
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      transactionClosureSentEventRepository = transactionClosedEventRepository,
      transactionClosureErrorEventStoreRepository = transactionClosureErrorEventStoreRepository,
      transactionsViewRepository = transactionsViewRepository,
      nodeService = nodeService,
      closureRetryService = closureRetryService,
      deadLetterQueueAsyncClient = deadLetterQueueAsyncClient,
      deadLetterTTLSeconds = DEAD_LETTER_QUEUE_TTL_SECONDS,
      tracingUtils = tracingUtils)

  @Test
  fun `consumer processes bare close message correctly with OK closure outcome`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

    val events = listOf(activationEvent, cancelRequestEvent)

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CANCELLATION_REQUESTED,
        ZonedDateTime.parse(activationEvent.creationDate))

    val expectedUpdatedTransactionCanceled =
      transactionDocument(
        TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

    val transactionId = TransactionId(TRANSACTION_ID)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(transactionId, ClosePaymentRequestV2Dto.OutcomeEnum.KO))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.OK })

    /* test */

    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(
          BinaryData.fromObject(QueueEvent(cancelRequestEvent, MOCK_TRACING_INFO)).toBytes(),
          checkpointer))
      .expectNext()
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(transactionId, ClosePaymentRequestV2Dto.OutcomeEnum.KO)
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransactionCanceled)
    verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
    assertEquals(TransactionStatusDto.CANCELED, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      closedEventStoreRepositoryCaptor.value.eventCode)
    assertEquals(
      TransactionClosureData.Outcome.OK,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)
  }

  @Test
  fun `consumer processes bare close message correctly with KO closure outcome`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

    val events = listOf(activationEvent, cancelRequestEvent)

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CANCELLATION_REQUESTED,
        ZonedDateTime.parse(activationEvent.creationDate))

    val expectedUpdatedTransactionCanceled =
      transactionDocument(
        TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

    val transactionId = TransactionId(TRANSACTION_ID)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionClosedEventRepository.save(closedEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(nodeService.closePayment(transactionId, ClosePaymentRequestV2Dto.OutcomeEnum.KO))
      .willReturn(
        ClosePaymentResponseDto().apply { outcome = ClosePaymentResponseDto.OutcomeEnum.KO })

    /* test */

    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(
          BinaryData.fromObject(QueueEvent(cancelRequestEvent, MOCK_TRACING_INFO)).toBytes(),
          checkpointer))
      .expectNext()
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1))
      .closePayment(transactionId, ClosePaymentRequestV2Dto.OutcomeEnum.KO)
    verify(transactionClosedEventRepository, Mockito.times(1))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransactionCanceled)
    verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
    assertEquals(TransactionStatusDto.CANCELED, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSED_EVENT,
      closedEventStoreRepositoryCaptor.value.eventCode)
    assertEquals(
      TransactionClosureData.Outcome.KO,
      closedEventStoreRepositoryCaptor.value.data.responseOutcome)
  }

  @Test
  fun `consumer receive error from close payment and send a retry event`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

    val events = listOf(activationEvent, cancelRequestEvent)

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CANCELLATION_REQUESTED,
        ZonedDateTime.parse(activationEvent.creationDate))

    val expectedUpdatedTransactionCanceled =
      transactionDocument(
        TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

    val transactionId = TransactionId(TRANSACTION_ID)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(nodeService.closePayment(transactionId, ClosePaymentRequestV2Dto.OutcomeEnum.KO))
      .willThrow(RuntimeException("Nodo error"))

    given(
        transactionClosureErrorEventStoreRepository.save(
          closureErrorEventStoreRepositoryCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }

    given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(closureRetryService.enqueueRetryEvent(any(), any())).willReturn(Mono.empty())
    /* test */

    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(
          BinaryData.fromObject(QueueEvent(cancelRequestEvent, MOCK_TRACING_INFO)).toBytes(),
          checkpointer))
      .expectNext()
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(1)).closePayment(any(), any())
    verify(transactionClosedEventRepository, Mockito.times(0))
      .save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
    // mocking
    verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransactionCanceled)
    verify(closureRetryService, times(1)).enqueueRetryEvent(any(), any())
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, viewArgumentCaptor.value.status)
    assertEquals(
      TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT,
      closureErrorEventStoreRepositoryCaptor.value.eventCode)
  }

  @Test
  fun `consumer process doesn't modify db on invalid transaction status`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

    val events = listOf(activationEvent)

    val transactionDocument =
      transactionDocument(
        TransactionStatusDto.CANCELLATION_REQUESTED,
        ZonedDateTime.parse(activationEvent.creationDate))
    val payload = BinaryData.fromObject(QueueEvent(cancelRequestEvent, MOCK_TRACING_INFO))
    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(Mono.just(transactionDocument))
    given(deadLetterQueueAsyncClient.sendMessageWithResponse(any<BinaryData>(), any(), anyOrNull()))
      .willReturn(queueSuccessfulResponse())

    /* test */

    StepVerifier.create(
        transactionClosureEventsConsumer.messageReceiver(payload.toBytes(), checkpointer))
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(nodeService, Mockito.times(0)).closePayment(any(), any())
    verify(transactionClosedEventRepository, Mockito.times(0)).save(any())
    verify(transactionsViewRepository, Mockito.times(0)).save(any())
    verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
    verify(deadLetterQueueAsyncClient, times(1))
      .sendMessageWithResponse(
        argThat<BinaryData> {
          this.toObject(object : TypeReference<QueueEvent<TransactionUserCanceledEvent>>() {})
            .event
            .eventCode == TransactionEventCode.TRANSACTION_USER_CANCELED_EVENT
        },
        eq(Duration.ZERO),
        eq(Duration.ofSeconds(DEAD_LETTER_QUEUE_TTL_SECONDS.toLong())))
  }

  @Test
  fun `consumer receive unrecoverable error (400 Bad Request) error from close payment and do not send a retry event`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

      val events = listOf(activationEvent, cancelRequestEvent)

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CANCELLATION_REQUESTED,
          ZonedDateTime.parse(activationEvent.creationDate))

      val expectedUpdatedTransactionCanceled =
        transactionDocument(
          TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionId = TransactionId(TRANSACTION_ID)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(nodeService.closePayment(transactionId, ClosePaymentRequestV2Dto.OutcomeEnum.KO))
        .willThrow(BadClosePaymentRequest("Bad request"))

      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }

      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(closureRetryService.enqueueRetryEvent(any(), any())).willReturn(Mono.empty())
      /* test */

      StepVerifier.create(
          transactionClosureEventsConsumer.messageReceiver(
            BinaryData.fromObject(QueueEvent(cancelRequestEvent, MOCK_TRACING_INFO)).toBytes(),
            checkpointer))
        .expectNext()
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1)).closePayment(any(), any())
      verify(transactionClosedEventRepository, Mockito.times(0))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransactionCanceled)
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
    }

  @Test
  fun `consumer receive unrecoverable error (404 Not found) error from close payment and do not send a retry event`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val cancelRequestEvent = transactionUserCanceledEvent() as TransactionEvent<Any>

      val events = listOf(activationEvent, cancelRequestEvent)

      val transactionDocument =
        transactionDocument(
          TransactionStatusDto.CANCELLATION_REQUESTED,
          ZonedDateTime.parse(activationEvent.creationDate))

      val expectedUpdatedTransactionCanceled =
        transactionDocument(
          TransactionStatusDto.CANCELED, ZonedDateTime.parse(activationEvent.creationDate))

      val transactionId = TransactionId(TRANSACTION_ID)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            TRANSACTION_ID))
        .willReturn(events.toFlux())
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(Mono.just(transactionDocument))
      given(nodeService.closePayment(transactionId, ClosePaymentRequestV2Dto.OutcomeEnum.KO))
        .willThrow(TransactionNotFound(transactionId.uuid))

      given(
          transactionClosureErrorEventStoreRepository.save(
            closureErrorEventStoreRepositoryCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }

      given(transactionsViewRepository.save(viewArgumentCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(closureRetryService.enqueueRetryEvent(any(), any())).willReturn(Mono.empty())
      /* test */

      StepVerifier.create(
          transactionClosureEventsConsumer.messageReceiver(
            BinaryData.fromObject(QueueEvent(cancelRequestEvent, MOCK_TRACING_INFO)).toBytes(),
            checkpointer))
        .expectNext()
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, Mockito.times(1)).success()
      verify(nodeService, Mockito.times(1)).closePayment(any(), any())
      verify(transactionClosedEventRepository, Mockito.times(0))
        .save(
          any()) // FIXME: Unable to use better argument captor because of misbehaviour in static
      // mocking
      verify(transactionsViewRepository, Mockito.times(0)).save(expectedUpdatedTransactionCanceled)
      verify(closureRetryService, times(0)).enqueueRetryEvent(any(), any())
    }
}
