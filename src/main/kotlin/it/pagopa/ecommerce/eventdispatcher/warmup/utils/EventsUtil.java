package it.pagopa.ecommerce.eventdispatcher.warmup.utils;
import it.pagopa.ecommerce.commons.documents.v2.*;
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationRequestedData;
import it.pagopa.ecommerce.commons.domain.v2.TransactionEventCode;
import it.pagopa.ecommerce.commons.generated.server.model.TransactionStatusDto;
import it.pagopa.ecommerce.commons.queues.TracingInfo;
import it.pagopa.ecommerce.commons.queues.TracingUtils;
import org.springframework.http.HttpStatus;
import java.net.URI;

public class EventsUtil {

    public static TransactionAuthorizationRequestedEvent getTransactionAuthorizationRequestedEventObject() {
        // Create the event object
        TransactionAuthorizationRequestedEvent event = new TransactionAuthorizationRequestedEvent();

        // Set properties
        event.setId("7ee814b9-8bb8-4f61-9204-2aa55cb56773");
        event.setTransactionId("00000000000000000000000000000000");
        event.setCreationDate("2025-01-10T14:28:47.843515440Z[Etc/UTC]");

        // Initialize the data object for the event
        TransactionAuthorizationRequestData data = new TransactionAuthorizationRequestData(
                1000, // amount
                50, // fee
                "instrumentId", // paymentInstrumentId
                "pspId", // pspId
                "CP", // paymentTypeCode
                "brokerName", // brokerName
                "pspChannelCode", // pspChannelCode
                "paymentMethodName", // paymentMethodName
                "pspBusinessName", // pspBusinessName
                true, // isPspOnUs
                "authorizationRequestId", // authorizationRequestId
                TransactionAuthorizationRequestData.PaymentGateway.NPG, // paymentGateway
                "paymentMethodDescription", // paymentMethodDescription
                new NpgTransactionGatewayAuthorizationRequestedData(
                        URI.create("http://test.com"), // URI
                        "VISA", // cardType
                        "NPG_SESSION_ID", // sessionId
                        "NPG_CONFIRM_PAYMENT_SESSION_ID", // confirmSessionId
                        null // transactionData (nullable)
                ),
                "idBundle" // idBundle
        );

        // Set the event data
        event.setData(data);

        // Set the event code
        event.setEventCode(TransactionEventCode.TRANSACTION_AUTHORIZATION_REQUESTED_EVENT.toString());

        return event;
    }

    public static TransactionAuthorizationOutcomeWaitingEvent getTransactionAuthorizationOutcomeWaitingEventObject() {
        // Create the event object
        TransactionAuthorizationOutcomeWaitingEvent event = new TransactionAuthorizationOutcomeWaitingEvent();

        // Set properties
        event.setTransactionId("00000000000000000000000000000000");
        event.setCreationDate("2025-01-10T14:28:47.843515440Z[Etc/UTC]");

        // Initialize the data object for the event
        TransactionRetriedData data = new TransactionRetriedData(1);

        // Set the event data
        event.setData(data);

        // Set the event code
        event.setEventCode(TransactionEventCode.TRANSACTION_AUTHORIZATION_OUTCOME_WAITING_EVENT.toString());

        return event;
    }


    public static TransactionUserReceiptAddErrorEvent getTransactionUserReceiptAddErrorEventObject() {
        // Create the event object
        TransactionUserReceiptAddErrorEvent event = new TransactionUserReceiptAddErrorEvent();

        // Set properties
        event.setId("12345678-1234-1234-1234-123456789012");
        event.setTransactionId("00000000000000000000000000000000");
        event.setCreationDate("2025-01-13T10:26:33.000Z[Etc/UTC]");

        // Initialize the data object for the event
        TransactionUserReceiptData data = new TransactionUserReceiptData();
        data.setLanguage("en");
        data.setPaymentDate("2025-01-12T10:00:00.000Z");
        data.setResponseOutcome(TransactionUserReceiptData.Outcome.OK); // Ensure Outcome is set correctly

        // Set the data and event code in the event object
        event.setData(data);
        event.setEventCode(TransactionEventCode.TRANSACTION_ADD_USER_RECEIPT_ERROR_EVENT.toString());

        return event;
    }

    public static TransactionClosureRequestedEvent getTransactionClosureRequestedEventObject() {
        // Create the event object
        TransactionClosureRequestedEvent event = new TransactionClosureRequestedEvent();

        // Set properties
        event.setId("7ee814b9-8bb8-4f61-9204-2aa55cb56773");
        event.setTransactionId("00000000000000000000000000000000");
        event.setCreationDate("2025-01-10T14:28:47.843515440Z[Etc/UTC]");

        // Set the event code
        event.setEventCode(TransactionEventCode.TRANSACTION_CLOSURE_REQUESTED_EVENT.toString());

        return event;
    }

    public static TransactionClosureErrorEvent getTransactionClosureErrorEventObject() {
        // Create the event object
        TransactionClosureErrorEvent event = new TransactionClosureErrorEvent();

        // Set properties
        event.setId("7ee814b9-8bb8-4f61-9204-2aa55cb56773");
        event.setTransactionId("00000000000000000000000000000000");
        event.setCreationDate("2025-01-10T14:28:47.843515440Z[Etc/UTC]");

        // Create and set the data object
        ClosureErrorData data = new ClosureErrorData();
        data.setHttpErrorCode(HttpStatus.INTERNAL_SERVER_ERROR);  // Set the HTTP error code
        data.setErrorDescription("Sample error message");  // Set the error description
        data.setErrorType(ClosureErrorData.ErrorType.KO_RESPONSE_RECEIVED);  // Set the error type

        // Set the data object in the event
        event.setData(data);

        // Set the event code
        event.setEventCode(TransactionEventCode.TRANSACTION_CLOSURE_ERROR_EVENT.toString());

        return event;
    }

    public static TransactionExpiredEvent getTransactionExpiredEventObject() {
        // Create the event object
        TransactionExpiredEvent event = new TransactionExpiredEvent();

        // Set properties
        event.setId("7ee814b9-8bb8-4f61-9204-2aa55cb56773");
        event.setTransactionId("00000000000000000000000000000000");
        event.setCreationDate("2025-01-10T14:28:47.843515440Z[Etc/UTC]");

        // Create and set the data object
        TransactionExpiredData data = new TransactionExpiredData();
        data.setStatusBeforeExpiration(TransactionStatusDto.REFUND_REQUESTED);  // Set the status before expiration

        // Set the data in the event
        event.setData(data);

        // Set the event code
        event.setEventCode(TransactionEventCode.TRANSACTION_EXPIRED_EVENT.toString());

        return event;
    }

    public static TransactionUserReceiptRequestedEvent getTransactionUserReceiptRequestedEventObject() {
        // Create the event object
        TransactionUserReceiptRequestedEvent event = new TransactionUserReceiptRequestedEvent();

        // Set properties
        event.setId("7ee814b9-8bb8-4f61-9204-2aa55cb56773");
        event.setTransactionId("00000000000000000000000000000000");
        event.setCreationDate("2025-01-10T14:28:47.843515440Z[Etc/UTC]");

        // Create and set the data object
        TransactionUserReceiptData data = new TransactionUserReceiptData();
        data.setResponseOutcome(TransactionUserReceiptData.Outcome.OK); // Set the response outcome
        data.setLanguage("en"); // Set the language
        data.setPaymentDate("2025-01-10T14:28:47.843515440Z[Etc/UTC]"); // Set the payment date

        // Set the data in the event
        event.setData(data);

        // Set the event code
        event.setEventCode(TransactionEventCode.TRANSACTION_USER_RECEIPT_REQUESTED_EVENT.toString());

        return event;
    }

    public static TransactionRefundRetriedEvent getTransactionRefundRetriedEventObject() {
        // Create the event object
        TransactionRefundRetriedEvent event = new TransactionRefundRetriedEvent();

        // Set properties
        event.setId("7ee814b9-8bb8-4f61-9204-2aa55cb56773");
        event.setTransactionId("00000000000000000000000000000000");
        event.setCreationDate("2025-01-10T14:28:47.843515440Z[Etc/UTC]");

        // Create and set the data object
        TransactionRefundRetriedData data = new TransactionRefundRetriedData();
        data.setTransactionGatewayAuthorizationData(null);  // Set transactionGatewayAuthorizationData to null
        data.setRetryCount(1);  // Set the retry count

        // Set the data in the event
        event.setData(data);

        // Set the event code
        event.setEventCode(TransactionEventCode.TRANSACTION_REFUND_RETRIED_EVENT.toString());

        return event;
    }

    public static TransactionRefundRequestedEvent getTransactionRefundRequestedEventObject() {
        // Create the event object
        TransactionRefundRequestedEvent event = new TransactionRefundRequestedEvent();

        // Set properties
        event.setId("abcdef12-3456-7890-abcd-ef1234567890");
        event.setTransactionId("00000000000000000000000000000000");
        event.setCreationDate("2025-01-13T10:30:00.000Z[Etc/UTC]");

        // Create and set the data object
        TransactionRefundRequestedData data = new TransactionRefundRequestedData();
        data.setGatewayAuthData(null);  // Set gatewayAuthData to null
        data.setStatusBeforeRefunded(TransactionStatusDto.AUTHORIZATION_REQUESTED);  // Set status before refunded

        // Set the data in the event
        event.setData(data);

        // Set the event code
        event.setEventCode(TransactionEventCode.TRANSACTION_REFUND_REQUESTED_EVENT.toString());

        return event;
    }
}
