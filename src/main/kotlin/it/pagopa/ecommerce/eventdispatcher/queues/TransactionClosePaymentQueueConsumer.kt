package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.EmptyTransaction
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithCancellationRequested
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithCancellationRequested
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.commons.redis.templatewrappers.v1.PaymentRequestInfoRedisTemplateWrapper
import it.pagopa.ecommerce.eventdispatcher.exceptions.*
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.NodeService
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.ClosureRetryService
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentRequestV2Dto
import it.pagopa.generated.ecommerce.nodo.v2.dto.ClosePaymentResponseDto
import kotlinx.coroutines.reactor.mono
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.util.*

@Service
class TransactionClosePaymentQueueConsumer(
    @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
    @Autowired
    private val transactionClosureSentEventRepository:
    TransactionsEventStoreRepository<TransactionClosureData>,
    @Autowired
    private val transactionClosureErrorEventStoreRepository: TransactionsEventStoreRepository<Void>,
    @Autowired private val transactionsViewRepository: TransactionsViewRepository,
    @Autowired private val nodeService: NodeService,
    @Autowired private val closureRetryService: ClosureRetryService,
    @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
    @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
    private val deadLetterTTLSeconds: Int,
    @Autowired private val tracingUtils: TracingUtils,
    @Autowired
    private val paymentRequestInfoRedisTemplateWrapper: PaymentRequestInfoRedisTemplateWrapper,
) {
    var logger: Logger = LoggerFactory.getLogger(TransactionClosePaymentQueueConsumer::class.java)

    private fun parseEvent(
        binaryData: BinaryData
    ): Mono<Pair<TransactionUserCanceledEvent, TracingInfo?>> {
        val queueEvent =
            binaryData
                .toObjectAsync(object : TypeReference<QueueEvent<TransactionUserCanceledEvent>>() {})
                .map { Pair(it.event, it.tracingInfo) }

        val untracedEvent =
            binaryData.toObjectAsync(object : TypeReference<TransactionUserCanceledEvent>() {}).map {
                Pair(it, null)
            }

        return queueEvent.onErrorResume { untracedEvent }
    }

    @ServiceActivator(inputChannel = "transactionclosureschannel", outputChannel = "nullChannel")
    fun messageReceiver(
        @Payload payload: ByteArray,
        @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer
    ) = messageReceiver(payload, checkPointer, EmptyTransaction())

    fun messageReceiver(
        payload: ByteArray,
        checkPointer: Checkpointer,
        emptyTransaction: EmptyTransaction
    ): Mono<Void> {
        val binaryData = BinaryData.fromBytes(payload)
        val queueEvent = parseEvent(binaryData)
        val transactionId = queueEvent.map { it.first.transactionId }
        val baseTransaction =
            reduceEvents(transactionId, transactionsEventStoreRepository, emptyTransaction)
        val closurePipeline =
            baseTransaction
                .flatMap {
                    logger.info("Status for transaction ${it.transactionId.value()}: ${it.status}")

                    if (it.status != TransactionStatusDto.CANCELLATION_REQUESTED) {
                        Mono.error(
                            BadTransactionStatusException(
                                transactionId = it.transactionId,
                                expected = listOf(TransactionStatusDto.CANCELLATION_REQUESTED),
                                actual = it.status
                            )
                        )
                    } else {
                        Mono.just(it)
                    }
                }
                .cast(TransactionWithCancellationRequested::class.java)
                .flatMap { tx ->
                    mono {
                        nodeService.closePayment(tx.transactionId, ClosePaymentRequestV2Dto.OutcomeEnum.KO)
                    }
                        .flatMap { closePaymentResponse ->
                            updateTransactionStatus(
                                transaction = tx, closePaymentResponseDto = closePaymentResponse
                            )
                        }
                        .then()
                        .onErrorResume { exception ->
                            baseTransaction.flatMap { baseTransaction ->
                                when (exception) {
                                    is BadClosePaymentRequest ->
                                        mono { baseTransaction }
                                            .flatMap {
                                                logger.error(
                                                    "Got unrecoverable error (400 - Bad Request) while calling closePaymentV2 for transaction with id ${it.transactionId}!",
                                                    exception
                                                )
                                                Mono.empty()
                                            }

                                    is TransactionNotFound ->
                                        mono { baseTransaction }
                                            .flatMap {
                                                logger.error(
                                                    "Got unrecoverable error (404 - Not Founds) while calling closePaymentV2 for transaction with id ${it.transactionId}!",
                                                    exception
                                                )
                                                Mono.empty()
                                            }

                                    else -> {
                                        logger.error(
                                            "Got exception while calling closePaymentV2 for transaction with id ${baseTransaction.transactionId}!",
                                            exception
                                        )

                                        mono { baseTransaction }
                                            .map { tx ->
                                                TransactionClosureErrorEvent(tx.transactionId.value().toString())
                                            }
                                            .flatMap { transactionClosureErrorEvent ->
                                                transactionClosureErrorEventStoreRepository.save(
                                                    transactionClosureErrorEvent
                                                )
                                            }
                                            .flatMap {
                                                transactionsViewRepository.findByTransactionId(
                                                    baseTransaction.transactionId.value()
                                                )
                                            }
                                            .flatMap { tx ->
                                                tx.status = TransactionStatusDto.CLOSURE_ERROR
                                                transactionsViewRepository.save(tx)
                                            }
                                            .flatMap {
                                                reduceEvents(
                                                    transactionId, transactionsEventStoreRepository, emptyTransaction
                                                )
                                            }
                                            .zipWith(queueEvent, ::Pair)
                                            .flatMap { (transactionUpdated, eventData) ->
                                                closureRetryService
                                                    .enqueueRetryEvent(transactionUpdated, 0, eventData.second)
                                                    .doOnError(NoRetryAttemptsLeftException::class.java) { exception ->
                                                        logger.error(
                                                            "No more attempts left for closure retry",
                                                            exception
                                                        )
                                                    }
                                            }
                                    }
                                }
                            }
                        }
                        .doFinally {
                            tx.paymentNotices.forEach { el ->
                                logger.info("Invalidate cache for RptId : {}", el.rptId().value())
                                paymentRequestInfoRedisTemplateWrapper.deleteById(el.rptId().value())
                            }
                        }
                }
                .then()

        return queueEvent
            .onErrorMap { InvalidEventException(payload) }
            .flatMap { (event, tracingInfo) ->
                if (tracingInfo != null) {
                    runTracedPipelineWithDeadLetterQueue(
                        checkPointer,
                        closurePipeline,
                        QueueEvent(event, tracingInfo),
                        deadLetterQueueAsyncClient,
                        deadLetterTTLSeconds,
                        tracingUtils,
                        this::class.simpleName!!
                    )
                } else {
                    runPipelineWithDeadLetterQueue(
                        checkPointer,
                        closurePipeline,
                        BinaryData.fromObject(event).toBytes(),
                        deadLetterQueueAsyncClient,
                        deadLetterTTLSeconds,
                    )
                }
            }.onErrorResume(InvalidEventException::class.java) {
                logger.error("Invalid input event", it)
                runPipelineWithDeadLetterQueue(
                    checkPointer,
                    closurePipeline,
                    payload,
                    deadLetterQueueAsyncClient,
                    deadLetterTTLSeconds
                )
            }
    }

    private fun updateTransactionStatus(
        transaction: BaseTransactionWithCancellationRequested,
        closePaymentResponseDto: ClosePaymentResponseDto,
    ): Mono<TransactionClosedEvent> {
        val outcome =
            when (closePaymentResponseDto.outcome) {
                ClosePaymentResponseDto.OutcomeEnum.OK -> TransactionClosureData.Outcome.OK
                ClosePaymentResponseDto.OutcomeEnum.KO -> TransactionClosureData.Outcome.KO
            }

        val event =
            TransactionClosedEvent(transaction.transactionId.value(), TransactionClosureData(outcome))

        /*
         * the transaction was canceled by the user then it
         * will go to CANCELED status regardless the Nodo ClosePayment outcome
         */
        val newStatus = TransactionStatusDto.CANCELED

        logger.info(
            "Updating transaction {} status to {}", transaction.transactionId.value(), newStatus
        )

        val transactionUpdate =
            transactionsViewRepository.findByTransactionId(transaction.transactionId.value())

        return transactionClosureSentEventRepository.save(event).flatMap { closedEvent ->
            transactionUpdate
                .flatMap { tx ->
                    tx.status = newStatus
                    transactionsViewRepository.save(tx)
                }
                .thenReturn(closedEvent)
        }
    }
}
