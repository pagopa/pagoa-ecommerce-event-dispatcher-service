package it.pagopa.ecommerce.eventdispatcher.queues.v2

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.TransactionId
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.*
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfoTest.MOCK_TRACING_INFO
import it.pagopa.ecommerce.commons.queues.TracingUtilsTests
import it.pagopa.ecommerce.commons.utils.v2.TransactionUtils
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.config.QueuesConsumerConfig
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidNPGResponseException
import it.pagopa.ecommerce.eventdispatcher.exceptions.NpgBadRequestException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.v2.AuthorizationStateRetrieverService
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgService
import it.pagopa.ecommerce.eventdispatcher.utils.*
import java.time.Duration
import java.time.ZonedDateTime
import java.util.*
import java.util.stream.Stream
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentCaptor
import org.mockito.Captor
import org.mockito.Mockito
import org.mockito.kotlin.*
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.messaging.MessageHeaders
import org.springframework.test.context.TestPropertySource
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.core.publisher.toMono
import reactor.kotlin.test.test
import reactor.test.StepVerifier

@SpringBootTest
@TestPropertySource(locations = ["classpath:application.test.properties"])
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionExpirationQueueConsumerTests {

  private val checkpointer: Checkpointer = mock()

  private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

  private val transactionsExpiredEventStoreRepository:
    TransactionsEventStoreRepository<TransactionExpiredData> =
    mock()

  private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<BaseTransactionRefundedData> =
    mock()

  private val transactionsViewRepository: TransactionsViewRepository = mock()

  private val authorizationStateRetrieverService: AuthorizationStateRetrieverService = mock()

  @Captor private lateinit var transactionViewRepositoryCaptor: ArgumentCaptor<Transaction>

  @Captor
  private lateinit var transactionRefundEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<BaseTransactionRefundedData>>

  @Captor
  private lateinit var transactionExpiredEventStoreCaptor:
    ArgumentCaptor<TransactionEvent<TransactionExpiredData>>

  @Captor private lateinit var queueEventCaptor: ArgumentCaptor<BinaryData>

  @Captor private lateinit var binaryDataCaptor: ArgumentCaptor<BinaryData>

  @Captor private lateinit var visibilityTimeoutCaptor: ArgumentCaptor<Duration>

  private val transactionUtils = TransactionUtils()

  private val refundRequestedAsyncClient: it.pagopa.ecommerce.commons.client.QueueAsyncClient =
    mock()

  private val deadLetterTracedQueueAsyncClient: DeadLetterTracedQueueAsyncClient = mock()

  private val expirationQueueAsyncClient: QueueAsyncClient = mock()

  private val sendPaymentResultTimeout = 120

  private val sendPaymentResultOffset = 10

  private val tracingUtils = TracingUtilsTests.getMock()

  private val strictJsonSerializerProviderV2 = QueuesConsumerConfig().strictSerializerProviderV2()

  private val jsonSerializerV2 = strictJsonSerializerProviderV2.createInstance()

  private val transactionExpirationQueueConsumer =
    TransactionExpirationQueueConsumer(
      transactionsEventStoreRepository = transactionsEventStoreRepository,
      transactionsExpiredEventStoreRepository = transactionsExpiredEventStoreRepository,
      transactionsRefundedEventStoreRepository = transactionsRefundedEventStoreRepository,
      transactionsViewRepository = transactionsViewRepository,
      transactionUtils = transactionUtils,
      refundRequestedAsyncClient = refundRequestedAsyncClient,
      deadLetterTracedQueueAsyncClient = deadLetterTracedQueueAsyncClient,
      expirationQueueAsyncClient = expirationQueueAsyncClient,
      sendPaymentResultTimeoutSeconds = sendPaymentResultTimeout,
      sendPaymentResultTimeoutOffsetSeconds = sendPaymentResultOffset,
      transientQueueTTLSeconds = TRANSIENT_QUEUE_TTL_SECONDS,
      tracingUtils = tracingUtils,
      strictSerializerProviderV2 = strictJsonSerializerProviderV2,
      npgService =
        NpgService(
          authorizationStateRetrieverService = authorizationStateRetrieverService,
          refundDelayFromAuthRequestMinutes = npgDelayRefundFromAuthRequestMinutes,
        ),
    )

  @AfterEach
  fun shouldReadEventFromEventStoreJustOnce() {
    verify(transactionsEventStoreRepository, times(1))
      .findByTransactionIdOrderByCreationDateAsc(any())
  }

  @Test
  fun `messageReceiver receives activated messages successfully`() {
    val activatedEvent = transactionActivateEvent()
    val transactionId = activatedEvent.transactionId

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId,
        ))
      .willReturn(Flux.just(activatedEvent as TransactionEvent<Any>))
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsExpiredEventStoreRepository.save(any())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())))

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
  }

  @Test
  fun `messageReceiver receives legacy activated messages successfully`() {
    val activatedEvent = transactionActivateEvent()
    val transactionId = activatedEvent.transactionId

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId,
        ))
      .willReturn(Flux.just(activatedEvent as TransactionEvent<Any>))
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsExpiredEventStoreRepository.save(any())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())))

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left(QueueEvent(activatedEvent, null)), checkpointer, MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
  }

  @Test
  fun `messageReceiver receives legacy expiration messages successfully`() {
    val activatedEvent = transactionActivateEvent()
    val tx = reduceEvents(activatedEvent)

    val expiredEvent = transactionExpiredEvent(tx)

    val transactionId = activatedEvent.transactionId

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          transactionId,
        ))
      .willReturn(
        Flux.just(activatedEvent as TransactionEvent<Any>, expiredEvent as TransactionEvent<Any>))
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsExpiredEventStoreRepository.save(any())).willAnswer {
      Mono.just(it.arguments[0])
    }

    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.EXPIRED_NOT_AUTHORIZED, ZonedDateTime.now())))

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.right(QueueEvent(expiredEvent, null)), checkpointer, MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
  }

  @Test
  fun `messageReceiver requests refund on transaction with authorization request`() = runTest {
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))
    val refundedEvent =
      transactionRefundedEvent(transactionActivated(ZonedDateTime.now().toString()))
        as TransactionEvent<BaseTransactionRefundedData>

    val transaction =
      transactionDocument(
        TransactionStatusDto.EXPIRED, ZonedDateTime.parse(activatedEvent.creationDate))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(
        Flux.just(
          activatedEvent as TransactionEvent<Any>,
          authorizationRequestedEvent as TransactionEvent<Any>))

    given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
    given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.just(refundedEvent))
    given(transactionsViewRepository.save(any())).willReturn(Mono.just(transaction))
    given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
      .willReturn(queueSuccessfulResponse())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())))
    given(authorizationStateRetrieverService.performGetOrder(any()))
      .willReturn(npgAuthorizedOrderResponse("operationId", "paymentEnd2EndId"))
    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(refundRequestedAsyncClient, times(1))
      .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
  }

  @Test
  fun `messageReceiver generate new expired event with error in eventstore`() = runTest {
    val activatedEvent = transactionActivateEvent()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(Flux.just(activatedEvent as TransactionEvent<Any>))

    given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
    given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.empty())
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())))
    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(refundRequestedAsyncClient, times(0))
      .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
  }

  @Test
  fun `messageReceiver fails to generate new expired event`() = runTest {
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))
    val refundedEvent =
      transactionRefundedEvent(transactionActivated(ZonedDateTime.now().toString()))
        as TransactionEvent<BaseTransactionRefundedData>

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(
        Flux.just(
          activatedEvent as TransactionEvent<Any>,
          authorizationRequestedEvent as TransactionEvent<Any>))

    given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
    given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.just(refundedEvent))
    given(transactionsViewRepository.findByTransactionId(any()))
      .willReturn(
        Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))
    given(transactionsViewRepository.save(any()))
      .willReturn(Mono.error(RuntimeException("error while trying to save event")))
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> {
          TransactionEventCode.valueOf(
            this.toObject(
                object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                jsonSerializerV2)
              .event
              .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
        },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = TransactionId(TRANSACTION_ID),
            transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
  }

  @Test
  fun `messageReceiver fails to generate new refund event`() = runTest {
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))
    val refundedEvent =
      transactionRefundedEvent(transactionActivated(ZonedDateTime.now().toString()))
        as TransactionEvent<BaseTransactionRefundedData>

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(
        Flux.just(
          activatedEvent as TransactionEvent<Any>,
          authorizationRequestedEvent as TransactionEvent<Any>))

    given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
    given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.just(refundedEvent))
    given(transactionsViewRepository.findByTransactionId(any()))
      .willReturn(
        Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))
    given(transactionsViewRepository.save(any()))
      .willReturn(Mono.error(RuntimeException("error while saving data")))
    given(
        deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(any<BinaryData>(), any()))
      .willReturn(mono {})

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .verifyComplete()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(deadLetterTracedQueueAsyncClient, times(1))
      .sendAndTraceDeadLetterQueueEvent(
        argThat<BinaryData> {
          TransactionEventCode.valueOf(
            this.toObject(
                object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                jsonSerializerV2)
              .event
              .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
        },
        eq(
          DeadLetterTracedQueueAsyncClient.ErrorContext(
            transactionId = TransactionId(TRANSACTION_ID),
            transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
            errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
  }

  @Test
  fun `messageReceiver calls update transaction to EXPIRED_NOT_AUTHORIZED for activated only expired transaction`() =
    runTest {
      val activatedEvent = transactionActivateEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(Flux.just(activatedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(transactionExpiredEventStoreCaptor.value.eventCode))
      assertEquals(
        TransactionStatusDto.EXPIRED_NOT_AUTHORIZED,
        transactionViewRepositoryCaptor.value.status,
      )
    }

  @Test
  fun `messageReceiver does nothing on a expiration event received for a transaction in EXPIRED_NOT_AUTHORIZED status`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val transactionExpiredEvent = transactionExpiredEvent(reduceEvents(activatedEvent))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            transactionExpiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver requests refund on transaction with authorization request after transaction expiration`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val expiredEvent =
        transactionExpiredEvent(reduceEvents(activatedEvent, authorizationRequestedEvent))
      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(authorizationStateRetrieverService.performGetOrder(any()))
        .willReturn(npgAuthorizedOrderResponse("operationId", "paymentEnd2EndId"))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(1)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val expectedRefundEventStatuses =
        listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
      val viewExpectedStatuses = listOf(TransactionStatusDto.REFUND_REQUESTED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver requests refund on transaction expired in NOTIFIED_KO status`() = runTest {
    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val authorizationCompletedEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
    val closureRequestedEvent = transactionClosureRequestedEvent()
    val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
    val userReceiptRequestedEvent = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
    val addUserReceiptEvent = transactionUserReceiptAddedEvent(transactionUserReceiptData)
    val expiredEvent =
      transactionExpiredEvent(
        reduceEvents(
          activatedEvent,
          authorizationRequestedEvent,
          authorizationCompletedEvent,
          closureRequestedEvent,
          closedEvent,
          userReceiptRequestedEvent,
          addUserReceiptEvent))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(
        Flux.just(
          activatedEvent as TransactionEvent<Any>,
          authorizationRequestedEvent as TransactionEvent<Any>,
          authorizationCompletedEvent as TransactionEvent<Any>,
          closureRequestedEvent as TransactionEvent<Any>,
          closedEvent as TransactionEvent<Any>,
          addUserReceiptEvent as TransactionEvent<Any>,
          userReceiptRequestedEvent as TransactionEvent<Any>,
          expiredEvent as TransactionEvent<Any>,
        ))

    given(
        transactionsExpiredEventStoreRepository.save(transactionExpiredEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(transactionRefundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
      .willReturn(queueSuccessfulResponse())

    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturnConsecutively(
        listOf(
          Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
          Mono.just(
            transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
    verify(refundRequestedAsyncClient, times(1))
      .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
    verify(transactionsRefundedEventStoreRepository, times(1)).save(any())
    verify(transactionsViewRepository, times(1)).save(any())
    /*
     * check view update statuses and events stored into event store
     */
    val expectedRefundEventStatuses =
      listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
    val viewExpectedStatuses = listOf(TransactionStatusDto.REFUND_REQUESTED)
    viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
      assertEquals(
        expectedStatus,
        transactionViewRepositoryCaptor.allValues[idx].status,
        "Unexpected view status on idx: $idx")
    }

    expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
      assertEquals(
        expectedStatus,
        TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
        "Unexpected event code on idx: $idx")
    }
  }

  @Test
  fun `messageReceiver requests refund on transaction in NOTIFICATION_ERROR status and send payment result outcome KO`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      val userReceiptRequestedEvent =
        transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val userReceiptErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>,
            userReceiptRequestedEvent as TransactionEvent<Any>,
            userReceiptErrorEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(1)).save(any())
      verify(transactionsViewRepository, times(2)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val expectedRefundEventStatuses =
        listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
      val viewExpectedStatuses =
        listOf(TransactionStatusDto.EXPIRED, TransactionStatusDto.REFUND_REQUESTED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
          "Unexpected event code on idx: $idx")
      }
      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(
        TransactionStatusDto.NOTIFICATION_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not request refund on transaction in NOTIFICATION_ERROR status and send payment result outcome OK`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      val userReceiptRequestedEvent =
        transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val userReceiptErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            userReceiptRequestedEvent as TransactionEvent<Any>,
            userReceiptErrorEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */

      assertEquals(TransactionStatusDto.EXPIRED, transactionViewRepositoryCaptor.value.status)
      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(
        TransactionStatusDto.NOTIFICATION_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver requests refund on transaction expired in NOTIFICATION_ERROR status and send payment result outcome KO`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      val userReceiptRequestedEvent =
        transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val userReceiptErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
            closureRequestedEvent,
            closedEvent,
            userReceiptRequestedEvent,
            userReceiptErrorEvent))
      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>,
            userReceiptRequestedEvent as TransactionEvent<Any>,
            userReceiptErrorEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(1)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val expectedRefundEventStatuses =
        listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
      val viewExpectedStatuses = listOf(TransactionStatusDto.REFUND_REQUESTED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver should not calls refund on transaction expired in NOTIFICATION_ERROR status and send payment result outcome OK`() =
    runTest {
      val transactionUserReceiptData =
        transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      val userReceiptRequestedEvent =
        transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val userReceiptErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
            closureRequestedEvent,
            closedEvent,
            userReceiptRequestedEvent,
            userReceiptErrorEvent,
          ))
      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>,
            userReceiptRequestedEvent as TransactionEvent<Any>,
            userReceiptErrorEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver should not process transaction in REFUND_REQUESTED status`() = runTest {
    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val authorizationCompletedEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED, "", "", "", ""))
    val closureRequestedEvent = transactionClosureRequestedEvent()
    val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
    val userReceiptRequestedEvent = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
    val userReceiptErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
    val refundRequestedEvent =
      transactionRefundRequestedEvent(
        reduceEvents(
          activatedEvent,
          authorizationRequestedEvent,
          authorizationCompletedEvent,
          closureRequestedEvent,
          closedEvent,
          userReceiptRequestedEvent,
          userReceiptErrorEvent))
    val gatewayClientResponse = RefundResponseDto()
    gatewayClientResponse.operationId("operationId").operationTime("operationTime")

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(
        Flux.just(
          activatedEvent as TransactionEvent<Any>,
          authorizationRequestedEvent as TransactionEvent<Any>,
          authorizationCompletedEvent as TransactionEvent<Any>,
          closureRequestedEvent as TransactionEvent<Any>,
          closedEvent as TransactionEvent<Any>,
          userReceiptRequestedEvent as TransactionEvent<Any>,
          userReceiptErrorEvent as TransactionEvent<Any>,
          refundRequestedEvent as TransactionEvent<Any>))

    given(
        transactionsExpiredEventStoreRepository.save(transactionExpiredEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(transactionRefundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
      .willReturn(queueSuccessfulResponse())

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
    verify(refundRequestedAsyncClient, times(0))
      .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
    verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
    verify(transactionsViewRepository, times(0)).save(any())
  }

  @Test
  fun `messageReceiver should not process transaction in REFUND_ERROR status`() = runTest {
    val transactionUserReceiptData =
      transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val authorizationCompletedEvent =
      transactionAuthorizationCompletedEvent(
        NpgTransactionGatewayAuthorizationData(
          OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
    val closureRequestedEvent = transactionClosureRequestedEvent()
    val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
    val userReceiptRequestedEvent = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
    val userReceiptErrorEvent = transactionUserReceiptAddErrorEvent(transactionUserReceiptData)
    val refundRequestedEvent =
      transactionRefundRequestedEvent(
        reduceEvents(
          activatedEvent,
          authorizationRequestedEvent,
          authorizationCompletedEvent,
          closureRequestedEvent,
          closedEvent,
          userReceiptRequestedEvent,
          userReceiptErrorEvent))
    val refundErrorEvent =
      transactionRefundErrorEvent(
        reduceEvents(
          activatedEvent,
          authorizationRequestedEvent,
          authorizationCompletedEvent,
          closureRequestedEvent,
          closedEvent,
          userReceiptRequestedEvent,
          userReceiptErrorEvent,
          refundRequestedEvent))
    val gatewayClientResponse = RefundResponseDto()
    gatewayClientResponse.operationId("operationId").operationTime("operationTime")

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(
        Flux.just(
          activatedEvent as TransactionEvent<Any>,
          authorizationRequestedEvent as TransactionEvent<Any>,
          authorizationCompletedEvent as TransactionEvent<Any>,
          closureRequestedEvent as TransactionEvent<Any>,
          closedEvent as TransactionEvent<Any>,
          userReceiptRequestedEvent as TransactionEvent<Any>,
          userReceiptErrorEvent as TransactionEvent<Any>,
          refundRequestedEvent as TransactionEvent<Any>,
          refundErrorEvent as TransactionEvent<Any>))

    given(
        transactionsExpiredEventStoreRepository.save(transactionExpiredEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(transactionRefundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
      .willReturn(queueSuccessfulResponse())

    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
    verify(refundRequestedAsyncClient, times(0))
      .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
    verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
    verify(transactionsViewRepository, times(0)).save(any())
  }

  @Test
  fun `messageReceiver calls update transaction to CANCELLATION_EXPIRED for transaction expired in CANCELLATION_REQUESTED status`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val cancellationRequested = transactionUserCanceledEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            cancellationRequested as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(transactionExpiredEventStoreCaptor.value.eventCode))
      assertEquals(
        TransactionStatusDto.CANCELLATION_EXPIRED,
        transactionViewRepositoryCaptor.value.status,
      )
    }

  @Test
  fun `messageReceiver calls update transaction to CANCELLATION_EXPIRED for transaction expired in CLOSURE_ERROR coming from user cancellation`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val cancellationRequested = transactionUserCanceledEvent()
      val closureError = transactionClosureErrorEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            cancellationRequested as TransactionEvent<Any>,
            closureError as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(transactionExpiredEventStoreCaptor.value.eventCode))
      assertEquals(
        TransactionStatusDto.CANCELLATION_EXPIRED,
        transactionViewRepositoryCaptor.value.status,
      )
    }

  @Test
  fun `messageReceiver does nothing on a expiration event received for a transaction in CANCELLATION_EXPIRED status`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val cancellationEvent = transactionUserCanceledEvent()
      val transactionExpiredEvent = transactionExpiredEvent(reduceEvents(activatedEvent))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            cancellationEvent as TransactionEvent<Any>,
            transactionExpiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturn(
          Mono.just(transactionDocument(TransactionStatusDto.ACTIVATED, ZonedDateTime.now())))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver does not request refund on transaction in CLOSURE_ERROR status for an authorized transaction`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closureErrorEvent = transactionClosureErrorEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closureErrorEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})
      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */

      val viewExpectedStatuses =
        listOf(
          TransactionStatusDto.EXPIRED,
        )
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, expiredEvent.data.statusBeforeExpiration)
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED)))
    }

  @Test
  fun `messageReceiver does not request refund on transaction in CLOSURE_ERROR status for an authorized transaction that has expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closureErrorEvent = transactionClosureErrorEvent()
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
            closureRequestedEvent,
            closureErrorEvent))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closureErrorEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})
      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED)))
    }

  @Test
  fun `messageReceiver should not calls refund on transaction in CLOSURE_ERROR status for an user canceled transaction`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val userCanceledEvent = transactionUserCanceledEvent()
      val closureErrorEvent = transactionClosureErrorEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            userCanceledEvent as TransactionEvent<Any>,
            closureErrorEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val viewExpectedStatuses = listOf(TransactionStatusDto.CANCELLATION_EXPIRED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }
      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not call refund on transaction in CLOSURE_ERROR status for an unauthorized transaction`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.DECLINED, "operationId", "paymentEnd2EndId", "errorCode", null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closureErrorEvent = transactionClosureErrorEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closureErrorEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */

      val viewExpectedStatuses = listOf(TransactionStatusDto.EXPIRED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSURE_ERROR, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should do nothing on transaction in CLOSURE_ERROR status for an unauthorized transaction that has expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.DECLINED, "operationId", "paymentEnd2EndId", "errorCode", null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closureErrorEvent = transactionClosureErrorEvent()
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
            closureRequestedEvent,
            closureErrorEvent))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closureErrorEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver should not request refund for a transaction in AUTHORIZATION_COMPLETED status that with outcome KO`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.DECLINED, "operationId", "paymentEnd2EndId", "errorCode", null))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now()))))

      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */

      val viewExpectedStatuses = listOf(TransactionStatusDto.EXPIRED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }
      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(
        TransactionStatusDto.AUTHORIZATION_COMPLETED, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not request refund for a transaction in AUTHORIZATION_COMPLETED status that with outcome KO expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.DECLINED, "operationId", "paymentEnd2EndId", "errorCode", null))
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
          ))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver forward event into dead letter queue for exception processing the event`() =
    runTest {
      /* preconditions */

      val activatedEvent = transactionActivateEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(Flux.error(RuntimeException("Error finding event from event store")))
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(mono {})

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .verifyComplete()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
    }

  @Test
  fun `messageReceiver processing should fail for error forward event into dead letter queue for exception processing the event`() =
    runTest {
      /* preconditions */

      val activatedEvent = transactionActivateEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(Flux.error(RuntimeException("Error finding event from event store")))
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            any<BinaryData>(), any()))
        .willReturn(Mono.error(RuntimeException("Error sending event to dead letter queue")))

      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectErrorMatches { it.message == "Error sending event to dead letter queue" }
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory = DeadLetterTracedQueueAsyncClient.ErrorCategory.PROCESSING_ERROR)))
    }

  @Test
  fun `messageReceiver should not request refund for a transaction in AUTHORIZATION_COMPLETED status with auth outcome OK`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.AUTHORIZATION_COMPLETED, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED)))
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val viewExpectedStatuses = listOf(TransactionStatusDto.EXPIRED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(
        TransactionStatusDto.AUTHORIZATION_COMPLETED, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not request refund for a transaction in AUTHORIZATION_COMPLETED status with auth outcome OK expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(activatedEvent, authorizationRequestedEvent, authorizationCompletedEvent))
      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(capture(transactionExpiredEventStoreCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(capture(transactionRefundEventStoreCaptor)))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(
                TransactionStatusDto.AUTHORIZATION_COMPLETED, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})
      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED)))
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver should request refund for a transaction in CLOSED status with close payment response outcome KO`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.KO)
      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.CLOSED, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(1)).save(any())
      verify(transactionsViewRepository, times(2)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val expectedRefundEventStatuses =
        listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
      val viewExpectedStatuses =
        listOf(TransactionStatusDto.EXPIRED, TransactionStatusDto.REFUND_REQUESTED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
          "Unexpected event code on idx: $idx")
      }
      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSED, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should request refund for a transaction in CLOSED status with close payment response outcome KO expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.KO)
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
            closureRequestedEvent,
            closedEvent))
      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(1)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */
      val expectedRefundEventStatuses =
        listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT)
      val viewExpectedStatuses = listOf(TransactionStatusDto.REFUND_REQUESTED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      expectedRefundEventStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          TransactionEventCode.valueOf(transactionRefundEventStoreCaptor.allValues[idx].eventCode),
          "Unexpected event code on idx: $idx")
      }
    }

  @Test
  fun `messageReceiver should not request refund for a transaction in CLOSED status with close payment response outcome OK`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      closedEvent.creationDate =
        ZonedDateTime.now().minus(Duration.ofSeconds(sendPaymentResultTimeout.toLong())).toString()
      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(Mono.just(transactionDocument(TransactionStatusDto.CLOSED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory
                  .SEND_PAYMENT_RESULT_RECEIVING_TIMEOUT)))
      /*
       * check view update statuses and events stored into event store
       */

      val viewExpectedStatuses = listOf(TransactionStatusDto.EXPIRED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSED, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver should not request refund for a transaction in CLOSED status with close payment response outcome OK expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
            closureRequestedEvent,
            closedEvent))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(Mono.just(transactionDocument(TransactionStatusDto.CLOSED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
    }

  @Test
  fun `messageReceiver should enqueue expiration event to wait for send payment result to be received`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closePaymentDate = ZonedDateTime.now()
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      closedEvent.creationDate = closePaymentDate.toString()
      val event =
        Either.left<QueueEvent<TransactionActivatedEvent>, QueueEvent<TransactionExpiredEvent>>(
          QueueEvent(activatedEvent, MOCK_TRACING_INFO))
      val expectedRetryEventVisibilityTimeout =
        Duration.ofSeconds(sendPaymentResultTimeout.toLong())
      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>))

      given(
          expirationQueueAsyncClient.sendMessageWithResponse(
            queueEventCaptor.capture(), visibilityTimeoutCaptor.capture(), anyOrNull()))
        .willReturn(queueSuccessfulResponse())

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            event, checkpointer, MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(expirationQueueAsyncClient, times(1))
        .sendMessageWithResponse(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          argThat<Duration> {
            expectedRetryEventVisibilityTimeout.toSeconds() - this.toSeconds() <= 1
          },
          eq(Duration.ofSeconds(TRANSIENT_QUEUE_TTL_SECONDS.toLong())))
    }

  @Test
  fun `messageReceiver should not request refund for a transaction in CLOSED status with closePayment authorization outcome OK near sendPaymentResult expiration offset`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val closedEvent = transactionClosedEvent(TransactionClosureData.Outcome.OK)
      closedEvent.creationDate =
        ZonedDateTime.now()
          .minus(Duration.ofSeconds(sendPaymentResultTimeout.toLong()))
          .plus(Duration.ofSeconds(sendPaymentResultOffset.toLong() / 2))
          .toString()
      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            closedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(Mono.just(transactionDocument(TransactionStatusDto.CLOSED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }

      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})

      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory
                  .SEND_PAYMENT_RESULT_RECEIVING_TIMEOUT)))
      /*
       * check view update statuses and events stored into event store
       */

      val viewExpectedStatuses = listOf(TransactionStatusDto.EXPIRED)
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSED, expiredEvent.data.statusBeforeExpiration)
    }

  @Test
  fun `messageReceiver does not request refund on transaction in CLOSURE_REQUESTED status for an authorized transaction`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.CLOSURE_REQUESTED, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})
      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())
      /*
       * check view update statuses and events stored into event store
       */

      val viewExpectedStatuses =
        listOf(
          TransactionStatusDto.EXPIRED,
        )
      viewExpectedStatuses.forEachIndexed { idx, expectedStatus ->
        assertEquals(
          expectedStatus,
          transactionViewRepositoryCaptor.allValues[idx].status,
          "Unexpected view status on idx: $idx")
      }

      val expiredEvent = transactionExpiredEventStoreCaptor.value
      assertEquals(
        TransactionEventCode.TRANSACTION_EXPIRED_EVENT,
        TransactionEventCode.valueOf(expiredEvent.eventCode))
      assertEquals(TransactionStatusDto.CLOSURE_REQUESTED, expiredEvent.data.statusBeforeExpiration)
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED)))
    }

  @Test
  fun `messageReceiver does not request refund on transaction in CLOSURE_REQUESTED status for an authorized transaction that has expired by batch`() =
    runTest {
      val activatedEvent = transactionActivateEvent()
      val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
      val authorizationCompletedEvent =
        transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
      val closureRequestedEvent = transactionClosureRequestedEvent()
      val expiredEvent =
        transactionExpiredEvent(
          reduceEvents(
            activatedEvent,
            authorizationRequestedEvent,
            authorizationCompletedEvent,
            closureRequestedEvent,
          ))

      /* preconditions */
      given(checkpointer.success()).willReturn(Mono.empty())
      given(
          transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
            any(),
          ))
        .willReturn(
          Flux.just(
            activatedEvent as TransactionEvent<Any>,
            authorizationRequestedEvent as TransactionEvent<Any>,
            authorizationCompletedEvent as TransactionEvent<Any>,
            closureRequestedEvent as TransactionEvent<Any>,
            expiredEvent as TransactionEvent<Any>))

      given(
          transactionsExpiredEventStoreRepository.save(
            transactionExpiredEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(
          transactionsRefundedEventStoreRepository.save(
            transactionRefundEventStoreCaptor.capture()))
        .willAnswer { Mono.just(it.arguments[0]) }
      given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
        .willReturnConsecutively(
          listOf(
            Mono.just(
              transactionDocument(TransactionStatusDto.CLOSURE_REQUESTED, ZonedDateTime.now())),
            Mono.just(transactionDocument(TransactionStatusDto.EXPIRED, ZonedDateTime.now())),
            Mono.just(
              transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()))))
      given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
        Mono.just(it.arguments[0])
      }
      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})
      Hooks.onOperatorDebug()
      /* test */
      StepVerifier.create(
          transactionExpirationQueueConsumer.messageReceiver(
            Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
            checkpointer,
            MessageHeaders(mapOf())))
        .expectNext(Unit)
        .expectComplete()
        .verify()

      /* Asserts */
      verify(checkpointer, times(1)).success()
      verify(transactionsExpiredEventStoreRepository, times(0)).save(any())
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(0)).save(any())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(
          argThat<BinaryData> {
            TransactionEventCode.valueOf(
              this.toObject(
                  object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {},
                  jsonSerializerV2)
                .event
                .eventCode) == TransactionEventCode.TRANSACTION_ACTIVATED_EVENT
          },
          eq(
            DeadLetterTracedQueueAsyncClient.ErrorContext(
              transactionId = TransactionId(TRANSACTION_ID),
              transactionEventCode = TransactionEventCode.TRANSACTION_ACTIVATED_EVENT.toString(),
              errorCategory =
                DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED)))
    }

  @Test
  fun `messageReceiver does not request refund if authorization was not requested`() {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val expiredEvent =
      transactionExpiredEvent(reduceEvents(activationEvent)) as TransactionEvent<Any>

    val events = listOf(activationEvent)

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(TRANSACTION_ID))
      .willReturn(events.toFlux())
    given(
        transactionsExpiredEventStoreRepository.save(transactionExpiredEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        mono { transactionDocument(TransactionStatusDto.REFUND_REQUESTED, ZonedDateTime.now()) })

    /* test */
    transactionExpirationQueueConsumer
      .messageReceiver(
        Either.right(QueueEvent(expiredEvent as TransactionExpiredEvent, MOCK_TRACING_INFO)),
        checkpointer,
        MessageHeaders(mapOf()),
      )
      .block()

    /* Asserts */
    verify(checkpointer, Mockito.times(1)).success()
    verify(refundRequestedAsyncClient, times(0))
      .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
    verify(transactionsRefundedEventStoreRepository, Mockito.times(0)).save(any())

    verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
    assertEventCodesEquals(
      listOf(TransactionEventCode.TRANSACTION_EXPIRED_EVENT),
      transactionExpiredEventStoreCaptor.allValues)

    verify(transactionsViewRepository, times(1)).save(transactionViewRepositoryCaptor.capture())
    assetTransactionStatusEquals(
      listOf(TransactionStatusDto.EXPIRED_NOT_AUTHORIZED),
      transactionViewRepositoryCaptor.allValues)
  }

  @Nested
  inner class StuckAuthorizationRequestedTransactionTest {
    @Test
    fun `messageReceiver should request a refund from AUTHORIZATION_REQUEST status`() {
      val operationId = UUID.randomUUID().toString()
      val paymentEndToEndId = UUID.randomUUID().toString()
      val events =
        listOf(
          transactionActivateEvent(npgTransactionGatewayActivationData()),
          transactionAuthorizationRequestedEvent(
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            npgTransactionGatewayAuthorizationRequestedData()))
      given(checkpointer.success()).willReturn(Mono.empty())
      setupTransactionStorageMock(events)
      given { authorizationStateRetrieverService.performGetOrder(any()) }
        .willAnswer { npgAuthorizedOrderResponse(operationId, paymentEndToEndId).toMono() }

      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})

      transactionExpirationQueueConsumer
        .messageReceiver(
          Either.left(
            QueueEvent(
              events.filterIsInstance<TransactionActivatedEvent>().first(), MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf()),
        )
        .block()

      verify(transactionsRefundedEventStoreRepository, times(1)).save(any())
      assertEventCodesEquals(
        listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT),
        transactionRefundEventStoreCaptor.allValues)

      val authorizationData =
        transactionRefundEventStoreCaptor.allValues
          .filterIsInstance<TransactionRefundRequestedEvent>()
          .first()
          .data
          .gatewayAuthData as NpgTransactionGatewayAuthorizationData
      val gatewayAuthorizationData =
        transactionRefundEventStoreCaptor.allValues
          .filterIsInstance<TransactionRefundRequestedEvent>()
          .first()
          .data
          .gatewayAuthData as NpgTransactionGatewayAuthorizationData
      assertEquals(authorizationData.operationId, operationId)
      assertEquals(authorizationData.paymentEndToEndId, paymentEndToEndId)
      assertEquals(gatewayAuthorizationData.operationId, operationId)

      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      assertEventCodesEquals(
        listOf(TransactionEventCode.TRANSACTION_EXPIRED_EVENT),
        transactionExpiredEventStoreCaptor.allValues)

      verify(transactionsViewRepository, times(2)).save(any())
      assetTransactionStatusEquals(
        listOf(
          TransactionStatusDto.EXPIRED,
          TransactionStatusDto.REFUND_REQUESTED,
        ),
        transactionViewRepositoryCaptor.allValues)
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any(), any())
      verify(refundRequestedAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
    }

    @Test
    fun `messageReceiver should request a refund from EXPIRED transaction previously stuck on AUTHORIZATION_REQUEST`() {
      val operationId = UUID.randomUUID().toString()
      val paymentEndToEndId = UUID.randomUUID().toString()
      val events =
        listOf(
          transactionActivateEvent(npgTransactionGatewayActivationData()),
          transactionAuthorizationRequestedEvent(
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            npgTransactionGatewayAuthorizationRequestedData()),
          transactionExpiredEvent(TransactionStatusDto.AUTHORIZATION_REQUESTED))
      setupTransactionStorageMock(events)
      given(checkpointer.success()).willReturn(Mono.empty())
      given { authorizationStateRetrieverService.performGetOrder(any()) }
        .willAnswer { npgAuthorizedOrderResponse(operationId, paymentEndToEndId).toMono() }

      given(refundRequestedAsyncClient.sendMessageWithResponse(any<QueueEvent<*>>(), any(), any()))
        .willReturn(queueSuccessfulResponse())
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), any()))
        .willReturn(mono {})

      transactionExpirationQueueConsumer
        .messageReceiver(
          Either.right(
            QueueEvent(
              events.filterIsInstance<TransactionExpiredEvent>().first(), MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf()),
        )
        .block()

      verify(transactionsRefundedEventStoreRepository, times(1)).save(any())
      assertEventCodesEquals(
        listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT),
        transactionRefundEventStoreCaptor.allValues)
      verify(transactionsViewRepository, times(1)).save(any())
      assetTransactionStatusEquals(
        listOf(TransactionStatusDto.REFUND_REQUESTED), transactionViewRepositoryCaptor.allValues)
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any(), any())
      verify(refundRequestedAsyncClient, times(1))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())

      val authorizationData =
        transactionRefundEventStoreCaptor.allValues
          .filterIsInstance<TransactionRefundRequestedEvent>()
          .first()
          .data
          .gatewayAuthData as NpgTransactionGatewayAuthorizationData
      val gatewayAuthorizationData =
        transactionRefundEventStoreCaptor.allValues
          .filterIsInstance<TransactionRefundRequestedEvent>()
          .first()
          .data
          .gatewayAuthData as NpgTransactionGatewayAuthorizationData
      assertEquals(authorizationData.operationId, operationId)
      assertEquals(authorizationData.paymentEndToEndId, paymentEndToEndId)
      assertEquals(gatewayAuthorizationData.operationId, operationId)
    }

    @Test
    fun `messageReceiver should not request REFUND when NPG has not authorized the transaction`() {
      val events =
        listOf(
          transactionActivateEvent(npgTransactionGatewayActivationData()),
          transactionAuthorizationRequestedEvent(
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            npgTransactionGatewayAuthorizationRequestedData()))
      setupTransactionStorageMock(events)
      given(checkpointer.success()).willReturn(Mono.empty())
      given { authorizationStateRetrieverService.performGetOrder(any()) }
        .willAnswer {
          OrderResponseDto()
            .orderStatus(OrderStatusDto().lastOperationType(OperationTypeDto.AUTHORIZATION))
            .addOperationsItem(
              OperationDto()
                .orderId(UUID.randomUUID().toString())
                .operationType(OperationTypeDto.AUTHORIZATION)
                .operationResult(OperationResultDto.CANCELED)
                .paymentEndToEndId(UUID.randomUUID().toString())
                .operationTime(ZonedDateTime.now().toString()))
            .toMono()
        }

      transactionExpirationQueueConsumer
        .messageReceiver(
          Either.right(
            QueueEvent(
              transactionExpiredEvent(TransactionStatusDto.AUTHORIZATION_REQUESTED),
              MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf()),
        )
        .test()
        .expectNext(Unit)
        .verifyComplete()

      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(transactionsRefundedEventStoreRepository, times(0)).save(any())
      verify(transactionsViewRepository, times(1)).save(any())

      assertEventCodesEquals(
        listOf(TransactionEventCode.TRANSACTION_EXPIRED_EVENT),
        transactionExpiredEventStoreCaptor.allValues)
      assetTransactionStatusEquals(
        listOf(
          TransactionStatusDto.EXPIRED,
        ),
        transactionViewRepositoryCaptor.allValues)
      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(deadLetterTracedQueueAsyncClient, times(0))
        .sendAndTraceDeadLetterQueueEvent(any(), any())
    }

    @ParameterizedTest
    @MethodSource(
      "it.pagopa.ecommerce.eventdispatcher.queues.v2.TransactionExpirationQueueConsumerTests#manualCheckRequiredNPGResponses")
    fun `messageReceiver should require manual check refund for AUTHORIZATION_REQUESTED stuck transaction when NPG fail to get order or has inconsistent state`(
      npgResponse: Either<Exception, OrderResponseDto>
    ) {
      val events =
        listOf(
          transactionActivateEvent(npgTransactionGatewayActivationData()),
          transactionAuthorizationRequestedEvent(
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            npgTransactionGatewayAuthorizationRequestedData()))
      setupTransactionStorageMock(events)
      given(checkpointer.success()).willReturn(Mono.empty())
      given { authorizationStateRetrieverService.performGetOrder(any()) }
        .willAnswer { npgResponse.fold({ Mono.error(it) }, { Mono.just(it) }) }

      val errorContextCaptor = argumentCaptor<DeadLetterTracedQueueAsyncClient.ErrorContext>()
      given(
          deadLetterTracedQueueAsyncClient.sendAndTraceDeadLetterQueueEvent(
            capture(binaryDataCaptor), errorContextCaptor.capture()))
        .willReturn(mono {})

      transactionExpirationQueueConsumer
        .messageReceiver(
          Either.right(
            QueueEvent(
              transactionExpiredEvent(TransactionStatusDto.AUTHORIZATION_REQUESTED),
              MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf()),
        )
        .block()

      verify(transactionsExpiredEventStoreRepository, times(1)).save(any())
      verify(transactionsRefundedEventStoreRepository, times(1)).save(any())
      verify(transactionsViewRepository, times(2)).save(any())

      assertEventCodesEquals(
        listOf(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT),
        transactionRefundEventStoreCaptor.allValues)

      assertEventCodesEquals(
        listOf(TransactionEventCode.TRANSACTION_EXPIRED_EVENT),
        transactionExpiredEventStoreCaptor.allValues)

      assetTransactionStatusEquals(
        listOf(
          TransactionStatusDto.EXPIRED,
          TransactionStatusDto.REFUND_REQUESTED,
        ),
        transactionViewRepositoryCaptor.allValues)

      verify(refundRequestedAsyncClient, times(0))
        .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
      verify(deadLetterTracedQueueAsyncClient, times(1))
        .sendAndTraceDeadLetterQueueEvent(any(), any())
      assertEquals(
        DeadLetterTracedQueueAsyncClient.ErrorCategory.REFUND_MANUAL_CHECK_REQUIRED,
        errorContextCaptor.firstValue.errorCategory)
    }
  }

  private fun setupTransactionStorageMock(events: List<TransactionEvent<*>>) {
    val transaction = reduceEvents(events.map { it as TransactionEvent<Any> }.toFlux()).block()
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(events.map { it as TransactionEvent<Any> }.toFlux())
    given(
        transactionsExpiredEventStoreRepository.save(transactionExpiredEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(
        transactionsRefundedEventStoreRepository.save(transactionRefundEventStoreCaptor.capture()))
      .willAnswer { Mono.just(it.arguments[0]) }
    given(transactionsViewRepository.save(transactionViewRepositoryCaptor.capture())).willAnswer {
      Mono.just(it.arguments[0])
    }
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID)).willAnswer {
      transactionDocument(transaction!!.status, ZonedDateTime.now()).toMono()
    }
  }

  @ParameterizedTest
  @MethodSource("npgRefundDelayTestMethodSource")
  fun `messageReceiver requests refund on a NPG transaction with authorization request with expected delay`(
    elapsedTimeFromAuthorization: Duration,
    expectedRefundEventDelay: Duration
  ) = runTest {
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    authorizationRequestedEvent.creationDate =
      ZonedDateTime.parse(authorizationRequestedEvent.creationDate)
        .minus(elapsedTimeFromAuthorization)
        .toString()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))
    val refundedEvent =
      transactionRefundedEvent(transactionActivated(ZonedDateTime.now().toString()))
        as TransactionEvent<BaseTransactionRefundedData>

    val transaction =
      transactionDocument(
        TransactionStatusDto.EXPIRED, ZonedDateTime.parse(activatedEvent.creationDate))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(
        Flux.just(
          activatedEvent as TransactionEvent<Any>,
          authorizationRequestedEvent as TransactionEvent<Any>))

    given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
    given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.just(refundedEvent))
    given(transactionsViewRepository.save(any())).willReturn(Mono.just(transaction))
    given(
        refundRequestedAsyncClient.sendMessageWithResponse(
          any<QueueEvent<*>>(), visibilityTimeoutCaptor.capture(), any()))
      .willReturn(queueSuccessfulResponse())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())))
    given(authorizationStateRetrieverService.performGetOrder(any()))
      .willReturn(npgAuthorizedOrderResponse("operationId", "paymentEnd2EndId"))
    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(refundRequestedAsyncClient, times(1))
      .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
    // there is time diff due to the fact that code calculate time diffs using now(), check event
    // delay with a 1 sec toleration
    println(
      "expected refund event delay: [$expectedRefundEventDelay], actual delay: [${visibilityTimeoutCaptor.value}]")
    assertTrue(expectedRefundEventDelay.minus(visibilityTimeoutCaptor.value).toMillis() < 1000)
  }

  @ParameterizedTest
  @MethodSource("npgRefundDelayTestMethodSource")
  fun `messageReceiver requests refund on a REDIRECT transaction without delay`(
    elapsedTimeFromAuthorization: Duration
  ) = runTest {
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent =
      transactionAuthorizationRequestedEvent(
        TransactionAuthorizationRequestData.PaymentGateway.REDIRECT,
        redirectTransactionGatewayAuthorizationRequestedData())
    authorizationRequestedEvent.creationDate =
      ZonedDateTime.parse(authorizationRequestedEvent.creationDate)
        .minus(elapsedTimeFromAuthorization)
        .toString()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))
    val refundedEvent =
      transactionRefundedEvent(transactionActivated(ZonedDateTime.now().toString()))
        as TransactionEvent<BaseTransactionRefundedData>

    val transaction =
      transactionDocument(
        TransactionStatusDto.EXPIRED, ZonedDateTime.parse(activatedEvent.creationDate))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(
        Flux.just(
          activatedEvent as TransactionEvent<Any>,
          authorizationRequestedEvent as TransactionEvent<Any>))

    given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
    given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.just(refundedEvent))
    given(transactionsViewRepository.save(any())).willReturn(Mono.just(transaction))
    given(
        refundRequestedAsyncClient.sendMessageWithResponse(
          any<QueueEvent<*>>(), visibilityTimeoutCaptor.capture(), any()))
      .willReturn(queueSuccessfulResponse())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())))
    given(authorizationStateRetrieverService.performGetOrder(any()))
      .willReturn(npgAuthorizedOrderResponse("operationId", "paymentEnd2EndId"))
    /* test */
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(refundRequestedAsyncClient, times(1))
      .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())
    assertEquals(Duration.ZERO, visibilityTimeoutCaptor.value)
  }

  @ParameterizedTest
  @MethodSource("npgRefundDelayTestMethodSource")
  fun `messageReceiver requests refund on a NPG transaction with no delay for transactions in authorization completed status`(
    elapsedTimeFromAuthorization: Duration
  ) = runTest {
    val activatedEvent = transactionActivateEvent()
    val authorizationRequestedEvent = transactionAuthorizationRequestedEvent()
    val authorizationCompletedEvent =
      transactionAuthorizationCompletedEvent(
        npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))
    val closureRequestedEvent = transactionClosureRequestedEvent()
    val closureFailedEvent = transactionClosedEvent(TransactionClosureData.Outcome.KO)
    authorizationRequestedEvent.creationDate =
      ZonedDateTime.parse(authorizationRequestedEvent.creationDate)
        .minus(elapsedTimeFromAuthorization)
        .toString()
    val expiredEvent = transactionExpiredEvent(transactionActivated(ZonedDateTime.now().toString()))
    val refundedEvent =
      transactionRefundedEvent(transactionActivated(ZonedDateTime.now().toString()))
        as TransactionEvent<BaseTransactionRefundedData>

    val transaction =
      transactionDocument(
        TransactionStatusDto.EXPIRED, ZonedDateTime.parse(activatedEvent.creationDate))

    /* preconditions */
    given(checkpointer.success()).willReturn(Mono.empty())
    given(
        transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(
          any(),
        ))
      .willReturn(
        Flux.just(
          activatedEvent as TransactionEvent<Any>,
          authorizationRequestedEvent as TransactionEvent<Any>,
          authorizationCompletedEvent as TransactionEvent<Any>,
          closureRequestedEvent as TransactionEvent<Any>,
          closureFailedEvent as TransactionEvent<Any>))

    given(transactionsExpiredEventStoreRepository.save(any())).willReturn(Mono.just(expiredEvent))
    given(transactionsRefundedEventStoreRepository.save(any())).willReturn(Mono.just(refundedEvent))
    given(transactionsViewRepository.save(any())).willReturn(Mono.just(transaction))
    given(
        refundRequestedAsyncClient.sendMessageWithResponse(
          any<QueueEvent<*>>(), visibilityTimeoutCaptor.capture(), any()))
      .willReturn(queueSuccessfulResponse())
    given(transactionsViewRepository.findByTransactionId(TRANSACTION_ID))
      .willReturn(
        Mono.just(
          transactionDocument(TransactionStatusDto.NOTIFICATION_ERROR, ZonedDateTime.now())))
    given(authorizationStateRetrieverService.performGetOrder(any()))
      .willReturn(npgAuthorizedOrderResponse("operationId", "paymentEnd2EndId"))
    /* test */
    Hooks.onOperatorDebug()
    StepVerifier.create(
        transactionExpirationQueueConsumer.messageReceiver(
          Either.left(QueueEvent(activatedEvent, MOCK_TRACING_INFO)),
          checkpointer,
          MessageHeaders(mapOf())))
      .expectNext(Unit)
      .expectComplete()
      .verify()

    /* Asserts */
    verify(checkpointer, times(1)).success()
    verify(refundRequestedAsyncClient, times(1))
      .sendMessageWithResponse(any<QueueEvent<*>>(), any(), any())

    assertEquals(Duration.ZERO, visibilityTimeoutCaptor.value)
  }

  companion object {
    const val npgDelayRefundFromAuthRequestMinutes = 10L

    @JvmStatic
    fun manualCheckRequiredNPGResponses(): Stream<Arguments> =
      Stream.of<Either<Exception, OrderResponseDto>>(
          Either.right(
            OrderResponseDto() // executed without operationId
              .orderStatus(OrderStatusDto().lastOperationType(OperationTypeDto.AUTHORIZATION))
              .addOperationsItem(
                OperationDto()
                  .orderId(UUID.randomUUID().toString())
                  .operationType(OperationTypeDto.AUTHORIZATION)
                  .operationResult(OperationResultDto.EXECUTED)
                  .paymentEndToEndId(UUID.randomUUID().toString())
                  .operationTime(ZonedDateTime.now().toString()))),
          Either.right(
            OrderResponseDto()
              .orderStatus(OrderStatusDto().lastOperationType(OperationTypeDto.AUTHORIZATION))
              .addOperationsItem(
                OperationDto()
                  .orderId(UUID.randomUUID().toString())
                  .operationType(OperationTypeDto.VOID)
                  .operationResult(OperationResultDto.DECLINED)
                  .paymentEndToEndId(UUID.randomUUID().toString())
                  .operationTime(ZonedDateTime.now().toString()))),
          Either.right(
            OrderResponseDto()
              .orderStatus(OrderStatusDto().lastOperationType(OperationTypeDto.AUTHORIZATION))),
          Either.right(
            OrderResponseDto()
              .orderStatus(OrderStatusDto().lastOperationType(OperationTypeDto.AUTHORIZATION))
              .operations(emptyList())),
          Either.right( // Unexpected NPG already refunded
            OrderResponseDto()
              .orderStatus(OrderStatusDto().lastOperationType(OperationTypeDto.REFUND))
              .addOperationsItem(
                OperationDto()
                  .orderId(UUID.randomUUID().toString())
                  .operationType(OperationTypeDto.AUTHORIZATION)
                  .operationResult(OperationResultDto.AUTHORIZED)
                  .paymentEndToEndId(UUID.randomUUID().toString())
                  .operationTime(ZonedDateTime.now().toString()))
              .addOperationsItem(
                OperationDto()
                  .orderId(UUID.randomUUID().toString())
                  .operationType(OperationTypeDto.REFUND)
                  .operationResult(OperationResultDto.VOIDED)
                  .paymentEndToEndId(UUID.randomUUID().toString())
                  .operationTime(ZonedDateTime.now().toString()))),
          Either.right(
            OrderResponseDto() // Unexpected NPG already refunded
              .orderStatus(OrderStatusDto().lastOperationType(OperationTypeDto.REFUND))
              .addOperationsItem(
                OperationDto()
                  .orderId(UUID.randomUUID().toString())
                  .operationType(OperationTypeDto.REFUND)
                  .operationResult(OperationResultDto.VOIDED)
                  .paymentEndToEndId(UUID.randomUUID().toString())
                  .operationTime(ZonedDateTime.now().toString()))
              .addOperationsItem(
                OperationDto()
                  .orderId(UUID.randomUUID().toString())
                  .operationType(OperationTypeDto.AUTHORIZATION)
                  .operationResult(OperationResultDto.AUTHORIZED)
                  .paymentEndToEndId(UUID.randomUUID().toString())
                  .operationTime(ZonedDateTime.now().toString()))),
          Either.left(
            NpgBadRequestException(TransactionId(TRANSACTION_ID), "N/A")), // 4xx error code
          Either.left(InvalidNPGResponseException()), // a generic invalid response
        )
        .map { Arguments.of(it) }

    @JvmStatic
    fun npgRefundDelayTestMethodSource(): Stream<Arguments> =
      Stream.of(
        Arguments.of(Duration.ZERO, Duration.ofMinutes(npgDelayRefundFromAuthRequestMinutes)),
        Arguments.of(
          Duration.ofMinutes(npgDelayRefundFromAuthRequestMinutes / 2),
          Duration.ofMinutes(npgDelayRefundFromAuthRequestMinutes / 2)),
        Arguments.of(
          Duration.ofMinutes(npgDelayRefundFromAuthRequestMinutes).plus(Duration.ofSeconds(1)),
          Duration.ZERO))
  }
}
