package it.pagopa.ecommerce.eventdispatcher.utils

import com.fasterxml.jackson.databind.ObjectMapper
import it.pagopa.ecommerce.commons.documents.v1.TransactionClosureData
import it.pagopa.ecommerce.commons.documents.v1.TransactionEvent
import it.pagopa.ecommerce.commons.documents.v1.TransactionUserReceiptData
import it.pagopa.ecommerce.commons.domain.v1.Email
import it.pagopa.ecommerce.commons.domain.v1.RptId
import it.pagopa.ecommerce.commons.domain.v1.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.commons.generated.server.model.AuthorizationResultDto
import it.pagopa.ecommerce.commons.v1.TransactionTestUtils
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.generated.notifications.templates.ko.KoTemplate
import it.pagopa.generated.notifications.templates.success.*
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import java.time.LocalDateTime
import java.time.Month
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.given
import org.mockito.kotlin.mock

@OptIn(ExperimentalCoroutinesApi::class)
class UserReceiptMailBuilderTest {

  private val confidentialMailUtils: ConfidentialMailUtils = mock()

  private val userReceiptMailBuilder = UserReceiptMailBuilder(confidentialMailUtils)

  @Test
  fun `Should build success email for notified transaction with send payment result outcome OK`() =
    runTest {
      /*
       * Prerequisites
       */
      given(confidentialMailUtils.toEmail(any()))
        .willReturn(Email(TransactionTestUtils.EMAIL_STRING))
      val events =
        listOf<TransactionEvent<*>>(
          TransactionTestUtils.transactionActivateEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
            as TransactionEvent<*>,
          TransactionTestUtils.transactionClosedEvent(TransactionClosureData.Outcome.OK)
            as TransactionEvent<*>,
          TransactionTestUtils.transactionUserReceiptRequestedEvent(
            TransactionTestUtils.transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)),
        )
      val baseTransaction =
        TransactionTestUtils.reduceEvents(*events.toTypedArray())
          as BaseTransactionWithRequestedUserReceipt
      val amountString =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.paymentNotices
            .map { it.transactionAmount.value }
            .reduce { a, b -> a + b })
      val feeString =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.transactionAuthorizationRequestData.fee)
      val dateString =
        userReceiptMailBuilder.dateTimeToHumanReadableString(
          baseTransaction.transactionUserReceiptData.paymentDate,
          Locale.forLanguageTag(TransactionTestUtils.LANGUAGE))
      val successTemplateRequest =
        NotificationsServiceClient.SuccessTemplateRequest(
          TransactionTestUtils.EMAIL_STRING,
          "Il riepilogo del tuo pagamento",
          TransactionTestUtils.LANGUAGE,
          SuccessTemplate(
            TransactionTemplate(
              baseTransaction.transactionId.value.toString(),
              dateString,
              amountString,
              PspTemplate(TransactionTestUtils.PSP_BUSINESS_NAME, FeeTemplate(feeString)),
              baseTransaction.transactionAuthorizationRequestData.authorizationRequestId,
              baseTransaction.transactionAuthorizationCompletedData.authorizationCode,
              PaymentMethodTemplate(
                TransactionTestUtils.PAYMENT_METHOD_NAME,
                TransactionTestUtils.PAYMENT_METHOD_LOGO_URL.toString(),
                null,
                false)),
            UserTemplate(null, TransactionTestUtils.EMAIL_STRING),
            CartTemplate(
              listOf(
                ItemTemplate(
                  RefNumberTemplate(
                    RefNumberTemplate.Type.CODICE_AVVISO,
                    RptId(TransactionTestUtils.RPT_ID).noticeId),
                  null,
                  PayeeTemplate(
                    TransactionTestUtils.RECEIVING_OFFICE_NAME,
                    RptId(TransactionTestUtils.RPT_ID).fiscalCode),
                  TransactionTestUtils.PAYMENT_DESCRIPTION,
                  amountString)),
              amountString)))
      val expected =
        NotificationEmailRequestDto()
          .language(successTemplateRequest.language)
          .subject(successTemplateRequest.subject)
          .to(successTemplateRequest.to)
          .templateId(NotificationsServiceClient.SuccessTemplateRequest.TEMPLATE_ID)
          .parameters(successTemplateRequest.templateParameters)
      /*
       * Test
       */
      val notificationEmailRequest =
        userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction)
      /*
       * Assertions
       */
      val objectMapper = ObjectMapper()
      assertEquals(
        objectMapper.writeValueAsString(expected),
        objectMapper.writeValueAsString(notificationEmailRequest))
    }

  @Test
  fun `Should build ko email for notified transaction with send payment result outcome KO`() =
    runTest {
      /*
       * Prerequisites
       */
      given(confidentialMailUtils.toEmail(any()))
        .willReturn(Email(TransactionTestUtils.EMAIL_STRING))
      val events =
        listOf<TransactionEvent<*>>(
          TransactionTestUtils.transactionActivateEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationCompletedEvent(AuthorizationResultDto.OK)
            as TransactionEvent<*>,
          TransactionTestUtils.transactionClosedEvent(TransactionClosureData.Outcome.OK)
            as TransactionEvent<*>,
          TransactionTestUtils.transactionUserReceiptRequestedEvent(
            TransactionTestUtils.transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)),
        )
      val baseTransaction =
        TransactionTestUtils.reduceEvents(*events.toTypedArray())
          as BaseTransactionWithRequestedUserReceipt
      val amountString =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.paymentNotices
            .map { it.transactionAmount.value }
            .reduce { a, b -> a + b })
      val dateString =
        userReceiptMailBuilder.dateTimeToHumanReadableString(
          baseTransaction.creationDate.toOffsetDateTime(),
          Locale.forLanguageTag(TransactionTestUtils.LANGUAGE))
      val koTemplateRequest =
        NotificationsServiceClient.KoTemplateRequest(
          TransactionTestUtils.EMAIL_STRING,
          "Il pagamento non è riuscito",
          TransactionTestUtils.LANGUAGE,
          KoTemplate(
            it.pagopa.generated.notifications.templates.ko.TransactionTemplate(
              baseTransaction.transactionId.value.toString(), dateString, amountString)))
      val expected =
        NotificationEmailRequestDto()
          .language(koTemplateRequest.language)
          .subject(koTemplateRequest.subject)
          .to(koTemplateRequest.to)
          .templateId(NotificationsServiceClient.KoTemplateRequest.TEMPLATE_ID)
          .parameters(koTemplateRequest.templateParameters)
      /*
       * Test
       */
      val notificationEmailRequest =
        userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction)
      /*
       * Assertions
       */
      val objectMapper = ObjectMapper()
      assertEquals(
        objectMapper.writeValueAsString(expected),
        objectMapper.writeValueAsString(notificationEmailRequest))
    }

  @Test
  fun `Should convert amount to human readable string successfully`() {
    var convertedAmount = userReceiptMailBuilder.amountToHumanReadableString(1)
    assertEquals("0,01", convertedAmount)
    convertedAmount = userReceiptMailBuilder.amountToHumanReadableString(154)
    assertEquals("1,54", convertedAmount)
  }

  @Test
  fun `Should convert date to human readable string successfully`() {
    val locale = Locale.ITALY
    val offsetDateTime =
      OffsetDateTime.of(LocalDateTime.of(2023, Month.JANUARY, 1, 1, 0), ZoneOffset.UTC)
    val humanReadableDate =
      userReceiptMailBuilder.dateTimeToHumanReadableString(offsetDateTime, locale)
    assertEquals("01 gennaio 2023, 01:00:00", humanReadableDate)
  }
}