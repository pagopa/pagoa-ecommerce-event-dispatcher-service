package it.pagopa.ecommerce.eventdispatcher.queues

import com.azure.core.util.BinaryData
import com.azure.core.util.serializer.TypeReference
import com.azure.spring.messaging.AzureHeaders
import com.azure.spring.messaging.checkpoint.Checkpointer
import com.azure.storage.queue.QueueAsyncClient
import io.vavr.control.Either
import it.pagopa.ecommerce.commons.documents.v1.*
import it.pagopa.ecommerce.commons.domain.v1.TransactionWithClosureError
import it.pagopa.ecommerce.commons.queues.QueueEvent
import it.pagopa.ecommerce.commons.queues.TracingInfo
import it.pagopa.ecommerce.commons.queues.TracingUtils
import it.pagopa.ecommerce.commons.utils.v1.TransactionUtils
import it.pagopa.ecommerce.eventdispatcher.client.PaymentGatewayClient
import it.pagopa.ecommerce.eventdispatcher.exceptions.InvalidEventException
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsEventStoreRepository
import it.pagopa.ecommerce.eventdispatcher.repositories.TransactionsViewRepository
import it.pagopa.ecommerce.eventdispatcher.services.eventretry.RefundRetryService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.messaging.MessageHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Headers
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.*

/**
 * Event consumer for events related to transaction activation. This consumer's responsibilities are
 * to handle expiration of transactions and subsequent refund for transaction stuck in a
 * pending/transient state.
 */
@Service
class TransactionExpirationQueueConsumer(
    @Autowired private val paymentGatewayClient: PaymentGatewayClient,
    @Autowired private val transactionsEventStoreRepository: TransactionsEventStoreRepository<Any>,
    @Autowired
    private val transactionsExpiredEventStoreRepository:
    TransactionsEventStoreRepository<TransactionExpiredData>,
    @Autowired
    private val transactionsRefundedEventStoreRepository:
    TransactionsEventStoreRepository<TransactionRefundedData>,
    @Autowired private val transactionsViewRepository: TransactionsViewRepository,
    @Autowired private val transactionUtils: TransactionUtils,
    @Autowired private val refundRetryService: RefundRetryService,
    @Autowired private val deadLetterQueueAsyncClient: QueueAsyncClient,
    @Autowired private val expirationQueueAsyncClient: QueueAsyncClient,
    @Value("\${sendPaymentResult.timeoutSeconds}") private val sendPaymentResultTimeoutSeconds: Int,
    @Value("\${sendPaymentResult.expirationOffset}")
    private val sendPaymentResultTimeoutOffsetSeconds: Int,
    @Value("\${azurestorage.queues.transientQueues.ttlSeconds}")
    private val transientQueueTTLSeconds: Int,
    @Value("\${azurestorage.queues.deadLetterQueue.ttlSeconds}")
    private val deadLetterTTLSeconds: Int,
    @Autowired private val tracingUtils: TracingUtils
) {

    val logger: Logger = LoggerFactory.getLogger(TransactionExpirationQueueConsumer::class.java)

    private fun parseEvent(
        data: BinaryData
    ): Mono<Pair<Either<TransactionActivatedEvent, TransactionExpiredEvent>, TracingInfo?>> {
        val transactionActivatedEvent =
            data.toObjectAsync(object : TypeReference<QueueEvent<TransactionActivatedEvent>>() {}).map {
                Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(it.event) to it.tracingInfo
            }

        val transactionExpiredEvent =
            data.toObjectAsync(object : TypeReference<QueueEvent<TransactionExpiredEvent>>() {}).map {
                Either.right<TransactionActivatedEvent, TransactionExpiredEvent>(it.event) to it.tracingInfo
            }

        val untracedTransactionActivatedEvent =
            data.toObjectAsync(object : TypeReference<TransactionActivatedEvent>() {}).map {
                Either.left<TransactionActivatedEvent, TransactionExpiredEvent>(it) to null
            }

        val untracedTransactionExpiredEvent =
            data.toObjectAsync(object : TypeReference<TransactionExpiredEvent>() {}).map {
                Either.right<TransactionActivatedEvent, TransactionExpiredEvent>(it) to null
            }

        return Mono.firstWithValue(
            transactionActivatedEvent,
            transactionExpiredEvent,
            untracedTransactionActivatedEvent,
            untracedTransactionExpiredEvent
        )
    }

    @ServiceActivator(inputChannel = "transactionexpiredchannel", outputChannel = "nullChannel")
    fun messageReceiver(
        @Payload payload: ByteArray,
        @Header(AzureHeaders.CHECKPOINTER) checkPointer: Checkpointer,
        @Headers headers: MessageHeaders
    ): Mono<Void> {
        val binaryData = BinaryData.fromBytes(payload)
        val queueEvent = parseEvent(binaryData)
        val transactionId =
            queueEvent.map { e -> e.first.fold({ it.transactionId }, { it.transactionId }) }
        val events =
            transactionId.flatMapMany {
                transactionsEventStoreRepository.findByTransactionIdOrderByCreationDateAsc(it)
            }
        val baseTransaction = reduceEvents(events)
        val refundPipeline =
            baseTransaction
                .filter {
                    val isTransient = transactionUtils.isTransientStatus(it.status)
                    logger.info(
                        "Transaction ${it.transactionId.value()} in status ${it.status}, is transient: $isTransient"
                    )
                    isTransient
                }
                .filterWhen {
                    val sendPaymentResultTimeLeft =
                        timeLeftForSendPaymentResult(it, sendPaymentResultTimeoutSeconds, events)
                    sendPaymentResultTimeLeft
                        .zipWith(queueEvent, ::Pair)
                        .flatMap { (timeLeft) ->
                            val sendPaymentResultOffset =
                                Duration.ofSeconds(sendPaymentResultTimeoutOffsetSeconds.toLong())
                            val expired = timeLeft < sendPaymentResultOffset
                            logger.info(
                                "Transaction ${it.transactionId.value()} - Time left for send payment result: $timeLeft, timeout offset: $sendPaymentResultOffset  --> expired: $expired"
                            )
                            if (expired) {
                                logger.error(
                                    "Transaction ${it.transactionId.value()} - No send payment result received on time! Transaction will be expired."
                                )
                                deadLetterQueueAsyncClient
                                    .sendMessageWithResponse(
                                        binaryData,
                                        Duration.ZERO,
                                        Duration.ofSeconds(deadLetterTTLSeconds.toLong()),
                                    )
                                    .thenReturn(true)
                            } else {
                                logger.info(
                                    "Transaction ${it.transactionId.value()} still waiting for sendPaymentResult outcome, expiration event sent with visibility timeout: $timeLeft"
                                )

                                expirationQueueAsyncClient
                                    .sendMessageWithResponse(
                                        binaryData,
                                        timeLeft,
                                        Duration.ofSeconds(transientQueueTTLSeconds.toLong()),
                                    )
                                    .thenReturn(false)
                            }
                        }
                        .switchIfEmpty(Mono.just(true))
                }
                .flatMap { tx ->
                    val isTransactionExpired = isTransactionExpired(tx)
                    logger.info("Transaction ${tx.transactionId.value()} is expired: $isTransactionExpired")
                    if (!isTransactionExpired) {
                        updateTransactionToExpired(
                            tx, transactionsExpiredEventStoreRepository, transactionsViewRepository
                        )
                    } else {
                        Mono.just(tx)
                    }
                }
                .filter {
                    val refundable = isTransactionRefundable(it)
                    logger.info(
                        "Transaction ${it.transactionId.value()} in status ${it.status}, refundable: $refundable"
                    )
                    refundable
                }
                .flatMap {
                    updateTransactionToRefundRequested(
                        it, transactionsRefundedEventStoreRepository, transactionsViewRepository
                    )
                }
                .zipWith(queueEvent, ::Pair)
                .flatMap { (tx, event) ->
                    val tracingInfo = event.second
                    val transaction =
                        if (tx is TransactionWithClosureError) {
                            tx.transactionAtPreviousState
                        } else {
                            tx
                        }
                    refundTransaction(
                        transaction,
                        transactionsRefundedEventStoreRepository,
                        transactionsViewRepository,
                        paymentGatewayClient,
                        refundRetryService,
                        tracingInfo
                    )
                }

        return queueEvent
            .onErrorMap { InvalidEventException(payload) }
            .flatMap { e ->
                val tracingInfo = e.second

                val event = e.first.fold({ it }, { it })

                if (tracingInfo != null) {
                    runTracedPipelineWithDeadLetterQueue(
                        checkPointer,
                        refundPipeline,
                        QueueEvent(event, tracingInfo),
                        deadLetterQueueAsyncClient,
                        deadLetterTTLSeconds,
                        tracingUtils,
                        this::class.simpleName!!
                    )
                } else {
                    runPipelineWithDeadLetterQueue(
                        checkPointer,
                        refundPipeline,
                        BinaryData.fromObject(event).toBytes(),
                        deadLetterQueueAsyncClient,
                        deadLetterTTLSeconds
                    )
                }
            }.onErrorResume(InvalidEventException::class.java) {
                logger.error("Invalid input event", it)
                runPipelineWithDeadLetterQueue(
                    checkPointer,
                    refundPipeline,
                    payload,
                    deadLetterQueueAsyncClient,
                    deadLetterTTLSeconds
                )
            }
    }
}
