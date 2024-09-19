package it.pagopa.ecommerce.eventdispatcher.queues.v2

import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.v2.pojos.*
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto
import it.pagopa.ecommerce.commons.utils.v2.TransactionUtils
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.services.v2.NpgService
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock

class CommonTests {

  companion object {

    val testedStatuses = mutableSetOf<TransactionStatusDto>()

    private val transactionUtils = TransactionUtils()

    private val npgService: NpgService = mock()

    @AfterAll
    @JvmStatic
    fun checkAllStatusesCovered() {
      TransactionStatusDto.values().forEach {
        /*
         * This test covers only transient statuses
         */
        if (transactionUtils.isTransientStatus(it)) {
          assertTrue(
            testedStatuses.contains(it), "Error: Transaction in status [$it] NOT covered by tests!")
        } else {
          // and expects that transient statuses are not covered by this test suite
          assertFalse(
            testedStatuses.contains(it),
            "Error: Transaction in status [$it] NOT expected to be covered by tests!")
        }
      }
    }
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in ACTIVATED status`() {
    val baseTransaction = reduceEventsAndMarkTestedStatus(transactionActivateEvent())
    assertFalse(isTransactionRefundable(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertEquals(TransactionStatusDto.ACTIVATED, baseTransaction.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in CANCELLATION_REQUESTED status`() {
    val baseTransaction =
      reduceEventsAndMarkTestedStatus(transactionActivateEvent(), transactionUserCanceledEvent())
    assertFalse(isTransactionRefundable(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertEquals(TransactionStatusDto.CANCELLATION_REQUESTED, baseTransaction.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in CLOSURE_ERROR status (user cancellation flow)`() {
    val baseTransaction =
      reduceEventsAndMarkTestedStatus(
        transactionActivateEvent(),
        transactionUserCanceledEvent(),
        transactionClosureErrorEvent(),
      )
    assertFalse(isTransactionRefundable(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, baseTransaction.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in AUTHORIZATION_REQUESTED status`() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(), transactionAuthorizationRequestedEvent())
    assertTrue(isTransactionRefundable(baseTransaction))
    assertTrue(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.AUTHORIZATION_REQUESTED, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in AUTHORIZATION_COMPLETED status (authorization OK)`() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)))
    assertTrue(isTransactionRefundable(baseTransaction))
    assertTrue(isTransactionRefundable(baseTransactionExpired))
    assertTrue(isRefundableCheckRequired(baseTransaction))
    assertTrue(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.AUTHORIZATION_COMPLETED, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in AUTHORIZATION_COMPLETED status (authorization KO)`() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.KO, null)))
    assertFalse(isTransactionRefundable(baseTransaction))
    assertFalse(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.AUTHORIZATION_COMPLETED, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in CLOSURE_REQUESTED status (authorization OK)`() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)),
        transactionClosureRequestedEvent(),
      )
    assertTrue(isTransactionRefundable(baseTransaction))
    assertTrue(isTransactionRefundable(baseTransactionExpired))
    assertTrue(isRefundableCheckRequired(baseTransaction))
    assertTrue(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.CLOSURE_REQUESTED, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in CLOSURE_REQUESTED status (authorization KO)`() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.KO, null)),
        transactionClosureRequestedEvent(),
      )
    assertFalse(isTransactionRefundable(baseTransaction))
    assertFalse(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.CLOSURE_REQUESTED, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in CLOSURE_ERROR status (authorization OK)`() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)),
        transactionClosureRequestedEvent(),
        transactionClosureErrorEvent())
    assertTrue(isTransactionRefundable(baseTransaction))
    assertTrue(isTransactionRefundable(baseTransactionExpired))
    assertTrue(isRefundableCheckRequired(baseTransaction))
    assertTrue(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in CLOSURE_ERROR status (authorization KO)`() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.KO, null)),
        transactionClosureRequestedEvent(),
        transactionClosureErrorEvent())
    assertFalse(isTransactionRefundable(baseTransaction))
    assertFalse(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.CLOSURE_ERROR, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in CLOSED status (authorization OK, close payment OK)`() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)),
        transactionClosureRequestedEvent(),
        transactionClosedEvent(TransactionClosureData.Outcome.OK))
    assertFalse(isTransactionRefundable(baseTransaction))
    assertFalse(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.CLOSED, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in CLOSED status (authorization OK, close payment KO)`() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)),
        transactionClosureRequestedEvent(),
        transactionClosedEvent(TransactionClosureData.Outcome.KO))
    assertTrue(isTransactionRefundable(baseTransaction))
    assertTrue(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.CLOSED, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in CLOSED status (authorization OK, close payment OK) recovering from CLOSURE_ERROR`() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)),
        transactionClosureRequestedEvent(),
        transactionClosureErrorEvent(),
        transactionClosedEvent(TransactionClosureData.Outcome.OK))
    assertFalse(isTransactionRefundable(baseTransaction))
    assertFalse(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.CLOSED, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in CLOSED status (authorization OK, close payment KO) recovering from CLOSURE_ERROR`() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)),
        transactionClosureRequestedEvent(),
        transactionClosureRequestedEvent(),
        transactionClosureErrorEvent(),
        transactionClosedEvent(TransactionClosureData.Outcome.KO))
    assertTrue(isTransactionRefundable(baseTransaction))
    assertTrue(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.CLOSED, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in NOTIFICATION_REQUESTED status (authorization OK, close payment OK, send payment OK) `() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)),
        transactionClosureRequestedEvent(),
        transactionClosureErrorEvent(),
        transactionClosedEvent(TransactionClosureData.Outcome.OK),
        transactionUserReceiptRequestedEvent(
          transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)))
    assertFalse(isTransactionRefundable(baseTransaction))
    assertFalse(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.NOTIFICATION_REQUESTED, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in NOTIFICATION_ERROR status (authorization OK, close payment OK, send payment OK) `() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)),
        transactionClosureRequestedEvent(),
        transactionClosureErrorEvent(),
        transactionClosedEvent(TransactionClosureData.Outcome.OK),
        transactionUserReceiptRequestedEvent(
          transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)),
        transactionUserReceiptAddErrorEvent(
          transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)))
    assertFalse(isTransactionRefundable(baseTransaction))
    assertFalse(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.NOTIFICATION_ERROR, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in NOTIFICATION_REQUESTED status (authorization OK, close payment OK, send payment KO) `() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)),
        transactionClosureRequestedEvent(),
        transactionClosureErrorEvent(),
        transactionClosedEvent(TransactionClosureData.Outcome.OK),
        transactionUserReceiptRequestedEvent(
          transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)))
    assertTrue(isTransactionRefundable(baseTransaction))
    assertTrue(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.NOTIFICATION_REQUESTED, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in NOTIFICATION_ERROR status (authorization OK, close payment OK, send payment KO) `() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)),
        transactionClosureRequestedEvent(),
        transactionClosureErrorEvent(),
        transactionClosedEvent(TransactionClosureData.Outcome.OK),
        transactionUserReceiptRequestedEvent(
          transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)),
        transactionUserReceiptAddErrorEvent(
          transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)))
    assertTrue(isTransactionRefundable(baseTransaction))
    assertTrue(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.NOTIFICATION_ERROR, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  @Test
  fun `Should calculate refund flags correctly for transaction in NOTIFIED_KO status (authorization OK, close payment OK, send payment KO) `() {
    val (baseTransaction, baseTransactionExpired) =
      reduceEventsAndExpireTransaction(
        transactionActivateEvent(),
        transactionAuthorizationRequestedEvent(
          redirectTransactionGatewayAuthorizationRequestedData()),
        transactionAuthorizationCompletedEvent(
          redirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)),
        transactionClosureRequestedEvent(),
        transactionClosureErrorEvent(),
        transactionClosedEvent(TransactionClosureData.Outcome.OK),
        transactionUserReceiptRequestedEvent(
          transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)),
        transactionUserReceiptAddErrorEvent(
          transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)),
        transactionUserReceiptAddedEvent(
          transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)))
    assertTrue(isTransactionRefundable(baseTransaction))
    assertTrue(isTransactionRefundable(baseTransactionExpired))
    assertFalse(isRefundableCheckRequired(baseTransaction))
    assertFalse(isRefundableCheckRequired(baseTransactionExpired))
    assertEquals(TransactionStatusDto.NOTIFIED_KO, baseTransaction.status)
    assertEquals(TransactionStatusDto.EXPIRED, baseTransactionExpired.status)
  }

  private fun reduceEventsAndExpireTransaction(
    vararg events: TransactionEvent<*>
  ): Pair<BaseTransaction, BaseTransaction> {
    val eventsList = events.toList()
    val baseTransaction = reduceEvents(*eventsList.toTypedArray())
    val expiredBaseTransaction =
      reduceEvents(*eventsList.plus(transactionExpiredEvent(baseTransaction)).toTypedArray())
    testedStatuses.add(baseTransaction.status)
    testedStatuses.add(expiredBaseTransaction.status)
    return Pair(
      baseTransaction,
      expiredBaseTransaction,
    )
  }

  private fun reduceEventsAndMarkTestedStatus(vararg events: TransactionEvent<*>): BaseTransaction {
    val eventsList = events.toList()
    val baseTransaction = reduceEvents(*eventsList.toTypedArray())
    testedStatuses.add(baseTransaction.status)
    return baseTransaction
  }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionWithRefundRequested`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
      TransactionAuthorizationRequestData.PaymentGateway.NPG

    val transactionGatewayAuthorizationData =
      npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)

    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(transactionGatewayAuthorizationData)
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent() as TransactionEvent<Any>
    val closedEvent =
      transactionClosedEvent(TransactionClosureData.Outcome.KO) as TransactionEvent<Any>
    val refundRequestedEvent =
      TransactionRefundRequestedEvent(
        TRANSACTION_ID, TransactionRefundRequestedData(null, TransactionStatusDto.REFUND_REQUESTED))
        as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closedEvent,
        refundRequestedEvent)

    val transaction = reduceEvents(*events.toTypedArray()) as BaseTransactionWithRefundRequested

    assertEquals(
      getAuthorizationCompletedData(transaction, npgService).block(),
      transactionGatewayAuthorizationData)
  }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionWithCompletedAuthorization`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
        TransactionAuthorizationRequestData.PaymentGateway.NPG

      val transactionGatewayAuthorizationData =
        npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)

      val authorizationCompleteEvent =
        transactionAuthorizationCompletedEvent(transactionGatewayAuthorizationData)
          as TransactionEvent<Any>

      val events = listOf(activationEvent, authorizationRequestEvent, authorizationCompleteEvent)

      val transaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithCompletedAuthorization

      assertEquals(
        getAuthorizationCompletedData(transaction, npgService).block(),
        transactionGatewayAuthorizationData)
    }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionWithClosureError`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
      TransactionAuthorizationRequestData.PaymentGateway.NPG

    val transactionGatewayAuthorizationData =
      npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)

    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(transactionGatewayAuthorizationData)
        as TransactionEvent<Any>
    val closureRequestedEvent = transactionClosureRequestedEvent()
    val closureError = transactionClosureErrorEvent() as TransactionEvent<Any>

    val events =
      listOf(
        activationEvent,
        authorizationRequestEvent,
        authorizationCompleteEvent,
        closureRequestedEvent,
        closureError)

    val transaction = reduceEvents(*events.toTypedArray()) as BaseTransactionWithClosureError

    assertEquals(
      getAuthorizationCompletedData(transaction, npgService).block(),
      transactionGatewayAuthorizationData)
  }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionExpired`() = runTest {
    val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
    val authorizationRequestEvent =
      transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
    (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
      TransactionAuthorizationRequestData.PaymentGateway.NPG

    val transactionGatewayAuthorizationData =
      npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)

    val authorizationCompleteEvent =
      transactionAuthorizationCompletedEvent(transactionGatewayAuthorizationData)
        as TransactionEvent<Any>

    val events: MutableList<TransactionEvent<Any>> =
      mutableListOf(activationEvent, authorizationRequestEvent, authorizationCompleteEvent)

    val expiredEvent =
      transactionExpiredEvent(reduceEvents(*events.toTypedArray())) as TransactionEvent<Any>

    events.add(expiredEvent)

    val transaction = reduceEvents(*events.toTypedArray()) as BaseTransactionExpired

    assertEquals(
      getAuthorizationCompletedData(transaction, npgService).block(),
      transactionGatewayAuthorizationData)
  }

  @Test
  fun `test getAuthorizationCompletedData with BaseTransactionWithRequestedAuthorization`() =
    runTest {
      val activationEvent = transactionActivateEvent() as TransactionEvent<Any>
      val authorizationRequestEvent =
        transactionAuthorizationRequestedEvent() as TransactionEvent<Any>
      (authorizationRequestEvent.data as TransactionAuthorizationRequestData).paymentGateway =
        TransactionAuthorizationRequestData.PaymentGateway.REDIRECT

      val events = listOf(activationEvent, authorizationRequestEvent)

      val transaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedAuthorization

      assertNull(getAuthorizationCompletedData(transaction, npgService).block())
    }
}
