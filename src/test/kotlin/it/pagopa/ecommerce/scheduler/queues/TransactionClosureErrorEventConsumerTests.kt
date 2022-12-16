package it.pagopa.ecommerce.scheduler.queues

import com.azure.core.util.BinaryData
import com.azure.spring.messaging.checkpoint.Checkpointer
import it.pagopa.ecommerce.commons.TransactionTestUtils.*
import it.pagopa.ecommerce.commons.documents.TransactionClosureSendData
import it.pagopa.ecommerce.commons.documents.TransactionClosureSentEvent
import it.pagopa.ecommerce.commons.documents.TransactionEvent
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.scheduler.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.scheduler.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.scheduler.services.NodeService
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.*
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import java.time.ZonedDateTime
import java.util.*


@ExtendWith(MockitoExtension::class)
@OptIn(ExperimentalCoroutinesApi::class)
class TransactionClosureErrorEventConsumerTests {
    private val checkpointer: Checkpointer = mock()

    private val nodeService: NodeService = mock()

    private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any> = mock()

    private val transactionsViewRepository: TransactionsViewRepository = mock()

    private val transactionClosureSentEventRepository: TransactionsEventStoreRepository<TransactionClosureSendData> =
        mock()

    private val transactionClosureErrorEventsConsumer = TransactionClosureErrorEventConsumer(
        transactionsEventStoreRepository,
        transactionClosureSentEventRepository,
        transactionsViewRepository,
        nodeService
    )


    @Test
    fun `consumer processes bare message correctly with OK closure outcome`() = runTest {
        val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
        val authorizationRequestEvent = transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
        val authorizationUpdateEvent = transactionAuthorizationStatusUpdatedEvent(AuthorizationResultDto.OK) as TransactionEvent<Any>
        val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

        val events = listOf(activationEvent, authorizationRequestEvent, authorizationUpdateEvent, closureErrorEvent)

        val expectedUpdatedTransaction = transactionDocument(TransactionStatusDto.CLOSED, ZonedDateTime.parse(activationEvent.creationDate))

        val transactionDocument = transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

        val uuidFromStringWorkaround = "00000000-0000-0000-0000-000000000000" // FIXME: Workaround for static mocking apparently not working
        val expectedClosureEvent = TransactionClosureSentEvent(
            uuidFromStringWorkaround,
            RPT_ID,
            PAYMENT_TOKEN,
            TransactionClosureSendData(
                ClosePaymentResponseDto.OutcomeEnum.OK,
                TransactionStatusDto.CLOSED
            )
        )

        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(transactionsEventStoreRepository.findByTransactionId(any()))
            .willReturn(events.toFlux())
        given(transactionsViewRepository.findByTransactionId(any())).willReturn(Mono.just(transactionDocument))
        given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
        given(nodeService.closePayment(UUID.fromString(uuidFromStringWorkaround), ClosePaymentRequestV2Dto.OutcomeEnum.OK)).willReturn(ClosePaymentResponseDto().apply {
            outcome = ClosePaymentResponseDto.OutcomeEnum.OK
        })

        /* test */
        
        val closureEventId = UUID.fromString(expectedClosureEvent.id)

        Mockito.mockStatic(UUID::class.java).use { uuid ->
            uuid.`when`<Any>(UUID::randomUUID).thenReturn(closureEventId)
            uuid.`when`<Any> { UUID.fromString(any()) }.thenCallRealMethod()

            given(transactionClosureSentEventRepository.save(any())).willReturn(Mono.just(expectedClosureEvent))

            transactionClosureErrorEventsConsumer.messageReceiver(
                BinaryData.fromObject(closureErrorEvent).toBytes(),
                checkpointer
            )

            /* Asserts */
            verify(checkpointer, Mockito.times(1)).success()
            verify(nodeService, Mockito.times(1)).closePayment(UUID.fromString(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.OK)
            verify(transactionClosureSentEventRepository, Mockito.times(1)).save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static mocking
            verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
        }
    }

    @Test
    fun `consumer processes bare message correctly with KO closure outcome`() = runTest {
        val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
        val authorizationRequestEvent = transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
        val authorizationUpdateEvent = transactionAuthorizationStatusUpdatedEvent(AuthorizationResultDto.KO) as TransactionEvent<Any>
        val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

        val events = listOf(activationEvent, authorizationRequestEvent, authorizationUpdateEvent, closureErrorEvent)

        val expectedUpdatedTransaction = transactionDocument(TransactionStatusDto.CLOSURE_FAILED, ZonedDateTime.parse(activationEvent.creationDate))

        val transactionDocument = transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

        val uuidFromStringWorkaround = "00000000-0000-0000-0000-000000000000" // FIXME: Workaround for static mocking apparently not working
        val expectedClosureEvent = TransactionClosureSentEvent(
            uuidFromStringWorkaround,
            RPT_ID,
            PAYMENT_TOKEN,
            TransactionClosureSendData(
                ClosePaymentResponseDto.OutcomeEnum.KO,
                TransactionStatusDto.CLOSED
            )
        )

        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(transactionsEventStoreRepository.findByTransactionId(any()))
            .willReturn(events.toFlux())
        given(transactionsViewRepository.findByTransactionId(any())).willReturn(Mono.just(transactionDocument))
        given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
        given(nodeService.closePayment(UUID.fromString(uuidFromStringWorkaround), ClosePaymentRequestV2Dto.OutcomeEnum.KO)).willReturn(ClosePaymentResponseDto().apply {
            outcome = ClosePaymentResponseDto.OutcomeEnum.KO
        })

        /* test */

        val closureEventId = UUID.fromString(expectedClosureEvent.id)

        Mockito.mockStatic(UUID::class.java).use { uuid ->
            uuid.`when`<Any>(UUID::randomUUID).thenReturn(closureEventId)
            uuid.`when`<Any> { UUID.fromString(any()) }.thenCallRealMethod()

            given(transactionClosureSentEventRepository.save(any())).willReturn(Mono.just(expectedClosureEvent))

            transactionClosureErrorEventsConsumer.messageReceiver(
                BinaryData.fromObject(closureErrorEvent).toBytes(),
                checkpointer
            )

            /* Asserts */
            verify(checkpointer, Mockito.times(1)).success()
            verify(nodeService, Mockito.times(1)).closePayment(UUID.fromString(TRANSACTION_ID), ClosePaymentRequestV2Dto.OutcomeEnum.KO)
            verify(transactionClosureSentEventRepository, Mockito.times(1)).save(any()) // FIXME: Unable to use better argument captor because of misbehaviour in static mocking
            verify(transactionsViewRepository, Mockito.times(1)).save(expectedUpdatedTransaction)
        }
    }

    @Test
    fun `consumer process doesn't modify db on invalid transaction status`() = runTest {
        val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
        val authorizationRequestEvent = transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
        val authorizationUpdateEvent = transactionAuthorizationStatusUpdatedEvent(AuthorizationResultDto.KO) as TransactionEvent<Any>

        val events = listOf(activationEvent, authorizationRequestEvent, authorizationUpdateEvent)

        val transactionDocument = transactionDocument(TransactionStatusDto.CLOSURE_ERROR, ZonedDateTime.parse(activationEvent.creationDate))

        val uuidFromStringWorkaround = "00000000-0000-0000-0000-000000000000" // FIXME: Workaround for static mocking apparently not working

        /* preconditions */
        given(checkpointer.success()).willReturn(Mono.empty())
        given(transactionsEventStoreRepository.findByTransactionId(any()))
            .willReturn(events.toFlux())
        given(transactionsViewRepository.findByTransactionId(any())).willReturn(Mono.just(transactionDocument))
        given(transactionsViewRepository.save(any())).willAnswer { Mono.just(it.arguments[0]) }
        given(nodeService.closePayment(UUID.fromString(uuidFromStringWorkaround), ClosePaymentRequestV2Dto.OutcomeEnum.KO)).willReturn(ClosePaymentResponseDto().apply {
            outcome = ClosePaymentResponseDto.OutcomeEnum.KO
        })

        /* test */
        val closureErrorEvent = transactionClosureErrorEvent() as TransactionEvent<Any>

        transactionClosureErrorEventsConsumer.messageReceiver(
            BinaryData.fromObject(closureErrorEvent).toBytes(),
            checkpointer
        )

        /* Asserts */
        verify(checkpointer, Mockito.times(1)).success()
        verify(nodeService, Mockito.times(0)).closePayment(any(), any())
        verify(transactionClosureSentEventRepository, Mockito.times(0)).save(any())
        verify(transactionsViewRepository, Mockito.times(0)).save(any())
    }
}