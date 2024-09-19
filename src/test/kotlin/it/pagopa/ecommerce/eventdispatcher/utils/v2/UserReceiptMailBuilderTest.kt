package it.pagopa.ecommerce.eventdispatcher.utils.v2

import com.fasterxml.jackson.databind.ObjectMapper
import it.pagopa.ecommerce.commons.client.NpgClient
import it.pagopa.ecommerce.commons.documents.PaymentNotice
import it.pagopa.ecommerce.commons.documents.v2.*
import it.pagopa.ecommerce.commons.documents.v2.activation.EmptyTransactionGatewayActivationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.documents.v2.authorization.NpgTransactionGatewayAuthorizationRequestedData
import it.pagopa.ecommerce.commons.documents.v2.authorization.RedirectTransactionGatewayAuthorizationData
import it.pagopa.ecommerce.commons.domain.Email
import it.pagopa.ecommerce.commons.domain.v2.pojos.BaseTransactionWithRequestedUserReceipt
import it.pagopa.ecommerce.commons.generated.npg.v1.dto.OperationResultDto
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils
import it.pagopa.ecommerce.commons.v2.TransactionTestUtils.*
import it.pagopa.ecommerce.eventdispatcher.client.NotificationsServiceClient
import it.pagopa.ecommerce.eventdispatcher.utils.ConfidentialDataUtils
import it.pagopa.ecommerce.eventdispatcher.utils.PaymentCode
import it.pagopa.generated.notifications.templates.ko.KoTemplate
import it.pagopa.generated.notifications.templates.success.*
import it.pagopa.generated.notifications.v1.dto.NotificationEmailRequestDto
import java.time.LocalDateTime
import java.time.Month
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.*
import java.util.stream.Stream
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.any
import org.mockito.kotlin.given
import org.mockito.kotlin.mock
import reactor.core.publisher.Hooks

@OptIn(ExperimentalCoroutinesApi::class)
class UserReceiptMailBuilderTest {

  private val confidentialDataUtils: ConfidentialDataUtils = mock()

  private val userReceiptMailBuilder = UserReceiptMailBuilder(confidentialDataUtils)

  @Test
  fun `Should build success email for NPG payments with rrn for notified transaction with send payment result outcome OK`() =
    runTest {
      /*
       * Prerequisites
       */
      given(confidentialDataUtils.toEmail(any())).willReturn(Email(EMAIL_STRING))
      val events =
        listOf<TransactionEvent<*>>(
          transactionActivateEvent() as TransactionEvent<*>,
          transactionAuthorizationRequestedEvent(
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            npgTransactionGatewayAuthorizationRequestedData())
            as TransactionEvent<*>,
          transactionAuthorizationCompletedEvent(
            NpgTransactionGatewayAuthorizationData(
              OperationResultDto.EXECUTED, "operationId", "paymentEndToEndId", null, null))
            as TransactionEvent<*>,
          transactionClosureRequestedEvent() as TransactionEvent<*>,
          transactionClosedEvent(TransactionClosureData.Outcome.OK) as TransactionEvent<*>,
          transactionClosureRequestedEvent() as TransactionEvent<*>,
          transactionUserReceiptRequestedEvent(
            transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)),
        )
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val totalAmountWithFeeString =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.paymentNotices
            .map { it.transactionAmount.value }
            .reduce { a, b -> a + b } + baseTransaction.transactionAuthorizationRequestData.fee)

      val totalAmount =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.paymentNotices
            .map { it.transactionAmount.value }
            .reduce { a, b -> a + b })
      val feeString =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.transactionAuthorizationRequestData.fee)
      val dateString =
        userReceiptMailBuilder.dateTimeToHumanReadableString(
          ZonedDateTime.parse(baseTransaction.transactionUserReceiptData.paymentDate),
          Locale.forLanguageTag(LANGUAGE))
      val successTemplateRequest =
        NotificationsServiceClient.SuccessTemplateRequest(
          EMAIL_STRING,
          "Il riepilogo del tuo pagamento",
          LANGUAGE,
          SuccessTemplate(
            TransactionTemplate(
              baseTransaction.transactionId.value(),
              dateString,
              totalAmountWithFeeString,
              PspTemplate(PSP_BUSINESS_NAME, FeeTemplate(feeString)),
              baseTransaction.transactionAuthorizationCompletedData.rrn,
              baseTransaction.transactionAuthorizationCompletedData.authorizationCode,
              PaymentMethodTemplate(PAYMENT_METHOD_DESCRIPTION, LOGO_URI.toString(), null, false)),
            UserTemplate(null, EMAIL_STRING),
            CartTemplate(
              baseTransaction.paymentNotices.map {
                ItemTemplate(
                  RefNumberTemplate(RefNumberTemplate.Type.CODICE_AVVISO, it.rptId.noticeId),
                  null,
                  PayeeTemplate(it.companyName.value, it.rptId.fiscalCode),
                  it.transactionDescription.value,
                  userReceiptMailBuilder.amountToHumanReadableString(it.transactionAmount.value))
              },
              totalAmount),
          ))
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
      print(objectMapper.writeValueAsString(expected))
      assertEquals(
        objectMapper.writeValueAsString(expected),
        objectMapper.writeValueAsString(notificationEmailRequest))
    }

  @Test
  fun `Should build success email for VPOS payments with rrn for notified transaction with send payment result outcome OK`() =
    runTest {
      /*
       * Prerequisites
       */
      given(confidentialDataUtils.toEmail(any())).willReturn(Email(EMAIL_STRING))
      val events =
        listOf<TransactionEvent<*>>(
          TransactionTestUtils.transactionActivateEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationCompletedEvent(
            NpgTransactionGatewayAuthorizationData(
              OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
            as TransactionEvent<*>,
          transactionClosureRequestedEvent() as TransactionEvent<*>,
          transactionClosedEvent(TransactionClosureData.Outcome.OK) as TransactionEvent<*>,
          transactionUserReceiptRequestedEvent(
            transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)),
        )
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val totalAmountWithFeeString =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.paymentNotices
            .map { it.transactionAmount.value }
            .reduce { a, b -> a + b } + baseTransaction.transactionAuthorizationRequestData.fee)

      val totalAmount =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.paymentNotices
            .map { it.transactionAmount.value }
            .reduce { a, b -> a + b })
      val feeString =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.transactionAuthorizationRequestData.fee)
      val dateString =
        userReceiptMailBuilder.dateTimeToHumanReadableString(
          ZonedDateTime.parse(baseTransaction.transactionUserReceiptData.paymentDate),
          Locale.forLanguageTag(LANGUAGE))
      val successTemplateRequest =
        NotificationsServiceClient.SuccessTemplateRequest(
          EMAIL_STRING,
          "Il riepilogo del tuo pagamento",
          LANGUAGE,
          SuccessTemplate(
            TransactionTemplate(
              baseTransaction.transactionId.value(),
              dateString,
              totalAmountWithFeeString,
              PspTemplate(PSP_BUSINESS_NAME, FeeTemplate(feeString)),
              baseTransaction.transactionAuthorizationCompletedData.rrn,
              baseTransaction.transactionAuthorizationCompletedData.authorizationCode,
              PaymentMethodTemplate(PAYMENT_METHOD_DESCRIPTION, LOGO_URI.toString(), null, false)),
            UserTemplate(null, EMAIL_STRING),
            CartTemplate(
              baseTransaction.paymentNotices.map {
                ItemTemplate(
                  RefNumberTemplate(RefNumberTemplate.Type.CODICE_AVVISO, it.rptId.noticeId),
                  null,
                  PayeeTemplate(it.companyName.value, it.rptId.fiscalCode),
                  it.transactionDescription.value,
                  userReceiptMailBuilder.amountToHumanReadableString(it.transactionAmount.value))
              },
              totalAmount),
          ))
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
      print(objectMapper.writeValueAsString(expected))
      assertEquals(
        objectMapper.writeValueAsString(expected),
        objectMapper.writeValueAsString(notificationEmailRequest))
    }

  @Test
  fun `Should build success email for notified transaction with send payment result outcome OK`() =
    runTest {
      /*
       * Prerequisites
       */
      given(confidentialDataUtils.toEmail(any())).willReturn(Email(EMAIL_STRING))
      val events =
        listOf<TransactionEvent<*>>(
          TransactionTestUtils.transactionActivateEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationCompletedEvent(
            NpgTransactionGatewayAuthorizationData(
              OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
            as TransactionEvent<*>,
          transactionClosureRequestedEvent() as TransactionEvent<*>,
          transactionClosedEvent(TransactionClosureData.Outcome.OK) as TransactionEvent<*>,
          transactionUserReceiptRequestedEvent(
            transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)),
        )
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val totalAmountWithFeeString =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.paymentNotices
            .map { it.transactionAmount.value }
            .reduce { a, b -> a + b } + baseTransaction.transactionAuthorizationRequestData.fee)

      val totalAmount =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.paymentNotices
            .map { it.transactionAmount.value }
            .reduce { a, b -> a + b })
      val feeString =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.transactionAuthorizationRequestData.fee)
      val dateString =
        userReceiptMailBuilder.dateTimeToHumanReadableString(
          ZonedDateTime.parse(baseTransaction.transactionUserReceiptData.paymentDate),
          Locale.forLanguageTag(LANGUAGE))
      val successTemplateRequest =
        NotificationsServiceClient.SuccessTemplateRequest(
          EMAIL_STRING,
          "Il riepilogo del tuo pagamento",
          LANGUAGE,
          SuccessTemplate(
            TransactionTemplate(
              baseTransaction.transactionId.value(),
              dateString,
              totalAmountWithFeeString,
              PspTemplate(PSP_BUSINESS_NAME, FeeTemplate(feeString)),
              baseTransaction.transactionAuthorizationCompletedData.rrn,
              baseTransaction.transactionAuthorizationCompletedData.authorizationCode,
              PaymentMethodTemplate(PAYMENT_METHOD_DESCRIPTION, LOGO_URI.toString(), null, false)),
            UserTemplate(null, EMAIL_STRING),
            CartTemplate(
              baseTransaction.paymentNotices.map {
                ItemTemplate(
                  RefNumberTemplate(RefNumberTemplate.Type.CODICE_AVVISO, it.rptId.noticeId),
                  null,
                  PayeeTemplate(it.companyName.value, it.rptId.fiscalCode),
                  it.transactionDescription.value,
                  userReceiptMailBuilder.amountToHumanReadableString(it.transactionAmount.value))
              },
              totalAmount),
          ))
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
      given(confidentialDataUtils.toEmail(any())).willReturn(Email(EMAIL_STRING))
      val events =
        listOf<TransactionEvent<*>>(
          TransactionTestUtils.transactionActivateEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationCompletedEvent(
            NpgTransactionGatewayAuthorizationData(
              OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
            as TransactionEvent<*>,
          transactionClosureRequestedEvent() as TransactionEvent<*>,
          transactionClosedEvent(TransactionClosureData.Outcome.OK) as TransactionEvent<*>,
          transactionUserReceiptRequestedEvent(
            transactionUserReceiptData(TransactionUserReceiptData.Outcome.KO)),
        )
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val amountString =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.paymentNotices
            .map { it.transactionAmount.value }
            .reduce { a, b -> a + b })
      val dateString =
        userReceiptMailBuilder.dateTimeToHumanReadableString(
          baseTransaction.creationDate, Locale.forLanguageTag(LANGUAGE))
      val koTemplateRequest =
        NotificationsServiceClient.KoTemplateRequest(
          EMAIL_STRING,
          "Il pagamento non è riuscito",
          LANGUAGE,
          KoTemplate(
            it.pagopa.generated.notifications.templates.ko.TransactionTemplate(
              baseTransaction.transactionId.value(), dateString, amountString)))
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
  fun `Should throw exception for notified transaction with invalid send payment result outcome`() =
    runTest {
      /*
       * Prerequisites
       */
      given(confidentialDataUtils.toEmail(any())).willReturn(Email(EMAIL_STRING))
      val events =
        listOf<TransactionEvent<*>>(
          TransactionTestUtils.transactionActivateEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<*>,
          TransactionTestUtils.transactionAuthorizationCompletedEvent(
            NpgTransactionGatewayAuthorizationData(
              OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
            as TransactionEvent<*>,
          transactionClosureRequestedEvent() as TransactionEvent<*>,
          transactionClosedEvent(TransactionClosureData.Outcome.OK) as TransactionEvent<*>,
          transactionUserReceiptRequestedEvent(
            transactionUserReceiptData(TransactionUserReceiptData.Outcome.NOT_RECEIVED)),
        )
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      /*
       * Test
       */
      assertThrows<RuntimeException> {
        userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction)
      }
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
      ZonedDateTime.of(LocalDateTime.of(2023, Month.JANUARY, 1, 1, 0), ZoneOffset.UTC)
    val humanReadableDate =
      userReceiptMailBuilder.dateTimeToHumanReadableString(offsetDateTime, locale)
    assertEquals("01 gennaio 2023, 01:00:00", humanReadableDate)
  }

  @Test
  fun `Should build success email for transaction with a cart`() = runTest {
    /*
     * Prerequisites
     */
    given(confidentialDataUtils.toEmail(any())).willReturn(Email(EMAIL_STRING))
    val transactionActivatedEvent = transactionActivateEvent()
    val paymentNotices = mutableListOf<PaymentNotice>()
    repeat(5) {
      paymentNotices.add(
        PaymentNotice().apply {
          paymentToken = UUID.randomUUID().toString().replace("-", "")
          rptId = RPT_ID
          description = "description_$it"
          amount = it * 100
          paymentContextCode = null
          transferList = listOf()
          isAllCCP = false
          companyName = "companyName_$it"
        })
    }
    transactionActivatedEvent.data.paymentNotices = paymentNotices
    val events =
      listOf<TransactionEvent<*>>(
        transactionActivatedEvent as TransactionEvent<*>,
        TransactionTestUtils.transactionAuthorizationRequestedEvent() as TransactionEvent<*>,
        TransactionTestUtils.transactionAuthorizationCompletedEvent(
          NpgTransactionGatewayAuthorizationData(
            OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null))
          as TransactionEvent<*>,
        transactionClosureRequestedEvent() as TransactionEvent<*>,
        transactionClosedEvent(TransactionClosureData.Outcome.OK) as TransactionEvent<*>,
        transactionUserReceiptRequestedEvent(
          transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)),
      )
    val baseTransaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
    val totalAmountWithFeeString =
      userReceiptMailBuilder.amountToHumanReadableString(
        baseTransaction.paymentNotices.map { it.transactionAmount.value }.reduce { a, b -> a + b } +
          baseTransaction.transactionAuthorizationRequestData.fee)

    val totalAmount =
      userReceiptMailBuilder.amountToHumanReadableString(
        baseTransaction.paymentNotices.map { it.transactionAmount.value }.reduce { a, b -> a + b })
    val feeString =
      userReceiptMailBuilder.amountToHumanReadableString(
        baseTransaction.transactionAuthorizationRequestData.fee)
    val dateString =
      userReceiptMailBuilder.dateTimeToHumanReadableString(
        ZonedDateTime.parse(baseTransaction.transactionUserReceiptData.paymentDate),
        Locale.forLanguageTag(LANGUAGE))
    val successTemplateRequest =
      NotificationsServiceClient.SuccessTemplateRequest(
        EMAIL_STRING,
        "Il riepilogo del tuo pagamento",
        LANGUAGE,
        SuccessTemplate(
          TransactionTemplate(
            baseTransaction.transactionId.value(),
            dateString,
            totalAmountWithFeeString,
            PspTemplate(PSP_BUSINESS_NAME, FeeTemplate(feeString)),
            baseTransaction.transactionAuthorizationCompletedData.rrn,
            baseTransaction.transactionAuthorizationCompletedData.authorizationCode,
            PaymentMethodTemplate(PAYMENT_METHOD_DESCRIPTION, LOGO_URI.toString(), null, false)),
          UserTemplate(null, EMAIL_STRING),
          CartTemplate(
            baseTransaction.paymentNotices.map {
              ItemTemplate(
                RefNumberTemplate(RefNumberTemplate.Type.CODICE_AVVISO, it.rptId.noticeId),
                null,
                PayeeTemplate(it.companyName.value, it.rptId.fiscalCode),
                it.transactionDescription.value,
                userReceiptMailBuilder.amountToHumanReadableString(it.transactionAmount.value))
            },
            totalAmount),
        ))
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
    repeat(5) {
      assertEquals(
        "companyName_$it",
        (notificationEmailRequest.parameters as SuccessTemplate).cart.items[it].payee.name)
      assertEquals(
        "description_$it",
        (notificationEmailRequest.parameters as SuccessTemplate).cart.items[it].subject)
    }
  }

  companion object {
    @JvmStatic
    fun `template fields method source`(): Stream<Arguments> =
      Stream.of(
        Arguments.of(
          PaymentCode.BPAY.name,
          "-",
          "npgOperationId",
          transactionAuthorizationCompletedEvent(
            npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))),
        Arguments.of(
          PaymentCode.CP.name,
          "rrn",
          "authorizationCode",
          transactionAuthorizationCompletedEvent(
            npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))),
        Arguments.of(
          PaymentCode.CP.name,
          AUTHORIZATION_REQUEST_ID,
          "authorizationCode",
          TransactionAuthorizationCompletedEvent(
            TRANSACTION_ID,
            TransactionAuthorizationCompletedData(
              "authorizationCode",
              null,
              "2023-01-01T01:02:03+01:00",
              npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)))),
        Arguments.of(
          PaymentCode.CP.name,
          "rrn",
          "-",
          TransactionAuthorizationCompletedEvent(
            TRANSACTION_ID,
            TransactionAuthorizationCompletedData(
              null,
              "rrn",
              "2023-01-01T01:02:03+01:00",
              npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)))),
        Arguments.of(
          PaymentCode.PPAL.name,
          "-",
          "-",
          transactionAuthorizationCompletedEvent(
            npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))),
        Arguments.of(
          PaymentCode.MYBK.name,
          "-",
          "-",
          transactionAuthorizationCompletedEvent(
            npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))),
        createRedirectTemplateFieldsMethodSource(PaymentCode.RBPB.name),
        Arguments.of(
          PaymentCode.RBPP.name,
          "-",
          "-",
          TransactionAuthorizationCompletedEvent(
            TRANSACTION_ID,
            TransactionAuthorizationCompletedData(
              null,
              "rrn",
              "2023-01-01T01:02:03+01:00",
              npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)))),
        createRedirectTemplateFieldsMethodSource(PaymentCode.RBPR.name),
        createRedirectTemplateFieldsMethodSource(PaymentCode.RBPS.name),
        createRedirectTemplateFieldsMethodSource(PaymentCode.RPIC.name),
        createRedirectTemplateFieldsMethodSource(PaymentCode.RICO.name),
        Arguments.of(
          PaymentCode.SATY.name,
          "-",
          "-",
          transactionAuthorizationCompletedEvent(
            npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))),
        Arguments.of(
          PaymentCode.APPL.name,
          "rrn",
          "authorizationCode",
          transactionAuthorizationCompletedEvent(
            npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED))),
        Arguments.of(
          PaymentCode.APPL.name,
          AUTHORIZATION_REQUEST_ID,
          "authorizationCode",
          TransactionAuthorizationCompletedEvent(
            TRANSACTION_ID,
            TransactionAuthorizationCompletedData(
              "authorizationCode",
              null,
              "2023-01-01T01:02:03+01:00",
              npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)))),
        Arguments.of(
          PaymentCode.APPL.name,
          "rrn",
          "-",
          TransactionAuthorizationCompletedEvent(
            TRANSACTION_ID,
            TransactionAuthorizationCompletedData(
              null,
              "rrn",
              "2023-01-01T01:02:03+01:00",
              npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)))),
      )

    private fun createRedirectTemplateFieldsMethodSource(code: String): Arguments {
      return Arguments.of(
        code,
        "-",
        "authorizationCode",
        transactionAuthorizationCompletedEvent(
          npgTransactionGatewayAuthorizationData(OperationResultDto.EXECUTED)))
    }
  }

  @ParameterizedTest
  @MethodSource("template fields method source")
  fun `Should build success email for transaction with correct rrn an authorization code`(
    paymentTypeCode: String,
    expectedRRN: String,
    expectedAuthCode: String,
    transactionAuthorizationCompletedEvent: TransactionAuthorizationCompletedEvent
  ) = runTest {
    /*
     * Prerequisites
     */
    given(confidentialDataUtils.toEmail(any())).willReturn(Email(EMAIL_STRING))
    val transactionActivatedEvent = transactionActivateEvent()
    val paymentNotices = mutableListOf<PaymentNotice>()
    repeat(5) {
      paymentNotices.add(
        PaymentNotice().apply {
          paymentToken = UUID.randomUUID().toString().replace("-", "")
          rptId = RPT_ID
          description = "description_$it"
          amount = it * 100
          paymentContextCode = null
          transferList = listOf()
          isAllCCP = false
          companyName = "companyName_$it"
        })
    }
    val paypalTransactionGatewayAuthorizationRequestedData =
      NpgTransactionGatewayAuthorizationRequestedData(
        LOGO_URI,
        NpgClient.PaymentMethod.PAYPAL.toString(),
        "npgSessionId",
        "npgConfirmPaymentSessionId",
        null)
    val authEvent =
      TransactionAuthorizationRequestedEvent(
        TRANSACTION_ID,
        TransactionAuthorizationRequestData(
          100,
          10,
          "paymentInstrumentId",
          "pspId",
          paymentTypeCode,
          "brokerName",
          "pspChannelCode",
          "paymentMethodName",
          "pspBusinessName",
          false,
          AUTHORIZATION_REQUEST_ID,
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          "paymentMethodDescription",
          paypalTransactionGatewayAuthorizationRequestedData))

    transactionActivatedEvent.data.paymentNotices = paymentNotices
    val events =
      listOf<TransactionEvent<*>>(
        transactionActivatedEvent as TransactionEvent<*>,
        authEvent,
        transactionAuthorizationCompletedEvent,
        transactionClosureRequestedEvent() as TransactionEvent<*>,
        transactionClosedEvent(TransactionClosureData.Outcome.OK) as TransactionEvent<*>,
        transactionUserReceiptRequestedEvent(
          transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)),
      )
    val baseTransaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
    val totalAmountWithFeeString =
      userReceiptMailBuilder.amountToHumanReadableString(
        baseTransaction.paymentNotices.map { it.transactionAmount.value }.reduce { a, b -> a + b } +
          baseTransaction.transactionAuthorizationRequestData.fee)

    val totalAmount =
      userReceiptMailBuilder.amountToHumanReadableString(
        baseTransaction.paymentNotices.map { it.transactionAmount.value }.reduce { a, b -> a + b })
    val feeString =
      userReceiptMailBuilder.amountToHumanReadableString(
        baseTransaction.transactionAuthorizationRequestData.fee)
    val dateString =
      userReceiptMailBuilder.dateTimeToHumanReadableString(
        ZonedDateTime.parse(baseTransaction.transactionUserReceiptData.paymentDate),
        Locale.forLanguageTag(LANGUAGE))

    val transactionExpected =
      TransactionTemplate(
        baseTransaction.transactionId.value(),
        dateString,
        totalAmountWithFeeString,
        PspTemplate(PSP_BUSINESS_NAME, FeeTemplate(feeString)),
        expectedRRN,
        expectedAuthCode,
        PaymentMethodTemplate(PAYMENT_METHOD_DESCRIPTION, LOGO_URI.toString(), null, false))
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
      objectMapper.writeValueAsString(transactionExpected),
      objectMapper.writeValueAsString(
        (notificationEmailRequest.parameters as SuccessTemplate).transaction))
  }

  @Test
  fun `Should throw error for wrong gateway with paymentTypeCode BPAY`() = runTest {
    /*
     * Prerequisites
     */
    given(confidentialDataUtils.toEmail(any())).willReturn(Email(EMAIL_STRING))
    val paypalTransactionGatewayAuthorizationRequestedData =
      NpgTransactionGatewayAuthorizationRequestedData(
        LOGO_URI,
        NpgClient.PaymentMethod.PAYPAL.toString(),
        "npgSessionId",
        "npgConfirmPaymentSessionId",
        null)
    val authEvent =
      TransactionAuthorizationRequestedEvent(
        TRANSACTION_ID,
        TransactionAuthorizationRequestData(
          100,
          10,
          "paymentInstrumentId",
          "pspId",
          PaymentCode.BPAY.name,
          "brokerName",
          "pspChannelCode",
          "paymentMethodName",
          "pspBusinessName",
          false,
          AUTHORIZATION_REQUEST_ID,
          TransactionAuthorizationRequestData.PaymentGateway.NPG,
          "paymentMethodDescription",
          paypalTransactionGatewayAuthorizationRequestedData))
    val transactionActivatedEvent = transactionActivateEvent()
    val paymentNotices = mutableListOf<PaymentNotice>()
    repeat(5) {
      paymentNotices.add(
        PaymentNotice().apply {
          paymentToken = UUID.randomUUID().toString().replace("-", "")
          rptId = RPT_ID
          description = "description_$it"
          amount = it * 100
          paymentContextCode = null
          transferList = listOf()
          isAllCCP = false
          companyName = "companyName_$it"
        })
    }

    transactionActivatedEvent.data.paymentNotices = paymentNotices
    val events =
      listOf<TransactionEvent<*>>(
        transactionActivatedEvent as TransactionEvent<*>,
        authEvent,
        transactionAuthorizationCompletedEvent(
          RedirectTransactionGatewayAuthorizationData(
            RedirectTransactionGatewayAuthorizationData.Outcome.OK, null)),
        transactionClosureRequestedEvent() as TransactionEvent<*>,
        transactionClosedEvent(TransactionClosureData.Outcome.OK) as TransactionEvent<*>,
        transactionUserReceiptRequestedEvent(
          transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)),
      )
    val baseTransaction =
      reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt

    val exec: RuntimeException =
      assertThrows("wrong gateway") {
        userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction)
      }
    assertEquals(
      "Unexpected TransactionGatewayAuthorization for paymentTypeCode BPAY. Expected NPG gateway.",
      exec.message)
  }

  @Test
  fun `Should throw error for invalid payment type code during the build of success template`() =
    runTest {
      /*
       * Prerequisites
       */
      val paymentTypeCode = "invalid"
      given(confidentialDataUtils.toEmail(any())).willReturn(Email(EMAIL_STRING))
      val paypalTransactionGatewayAuthorizationRequestedData =
        NpgTransactionGatewayAuthorizationRequestedData(
          LOGO_URI,
          NpgClient.PaymentMethod.PAYPAL.toString(),
          "npgSessionId",
          "npgConfirmPaymentSessionId",
          null)
      val authEvent =
        TransactionAuthorizationRequestedEvent(
          TRANSACTION_ID,
          TransactionAuthorizationRequestData(
            100,
            10,
            "paymentInstrumentId",
            "pspId",
            paymentTypeCode,
            "brokerName",
            "pspChannelCode",
            "paymentMethodName",
            "pspBusinessName",
            false,
            AUTHORIZATION_REQUEST_ID,
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            "paymentMethodDescription",
            paypalTransactionGatewayAuthorizationRequestedData))
      val transactionActivatedEvent = transactionActivateEvent()
      val paymentNotices = mutableListOf<PaymentNotice>()
      repeat(5) {
        paymentNotices.add(
          PaymentNotice().apply {
            paymentToken = UUID.randomUUID().toString().replace("-", "")
            rptId = RPT_ID
            description = "description_$it"
            amount = it * 100
            paymentContextCode = null
            transferList = listOf()
            isAllCCP = false
            companyName = "companyName_$it"
          })
      }

      transactionActivatedEvent.data.paymentNotices = paymentNotices
      val events =
        listOf<TransactionEvent<*>>(
          transactionActivatedEvent as TransactionEvent<*>,
          authEvent,
          transactionAuthorizationCompletedEvent(
            NpgTransactionGatewayAuthorizationData(
              OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null)),
          transactionClosureRequestedEvent() as TransactionEvent<*>,
          transactionClosedEvent(TransactionClosureData.Outcome.OK) as TransactionEvent<*>,
          transactionUserReceiptRequestedEvent(
            transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)),
        )
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt

      val exec: RuntimeException =
        assertThrows("wrong gateway") {
          userReceiptMailBuilder.buildNotificationEmailRequestDto(baseTransaction)
        }
      assertEquals("Unhandled or invalid payment type code: ${paymentTypeCode}", exec.message)
    }

  @Test
  fun `when transaction client is WISP_REDIRECT should build success email with payment notice id equals to reference creditor Id`() =
    runTest {
      /*
       * Prerequisites
       */
      given(confidentialDataUtils.toEmail(any())).willReturn(Email(EMAIL_STRING))
      val events =
        listOf<TransactionEvent<*>>(
          transactionActivateEvent(
            ZonedDateTime.now().toString(),
            EmptyTransactionGatewayActivationData(),
            USER_ID,
            Transaction.ClientId.WISP_REDIRECT)
            as TransactionEvent<*>,
          transactionAuthorizationRequestedEvent(
            TransactionAuthorizationRequestData.PaymentGateway.NPG,
            npgTransactionGatewayAuthorizationRequestedData())
            as TransactionEvent<*>,
          transactionAuthorizationCompletedEvent(
            NpgTransactionGatewayAuthorizationData(
              OperationResultDto.EXECUTED, "operationId", "paymentEndToEndId", null, null))
            as TransactionEvent<*>,
          transactionClosureRequestedEvent() as TransactionEvent<*>,
          transactionClosedEvent(TransactionClosureData.Outcome.OK) as TransactionEvent<*>,
          transactionClosureRequestedEvent() as TransactionEvent<*>,
          transactionUserReceiptRequestedEvent(
            transactionUserReceiptData(TransactionUserReceiptData.Outcome.OK)),
        )
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      val totalAmountWithFeeString =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.paymentNotices
            .map { it.transactionAmount.value }
            .reduce { a, b -> a + b } + baseTransaction.transactionAuthorizationRequestData.fee)

      val totalAmount =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.paymentNotices
            .map { it.transactionAmount.value }
            .reduce { a, b -> a + b })
      val feeString =
        userReceiptMailBuilder.amountToHumanReadableString(
          baseTransaction.transactionAuthorizationRequestData.fee)
      val dateString =
        userReceiptMailBuilder.dateTimeToHumanReadableString(
          ZonedDateTime.parse(baseTransaction.transactionUserReceiptData.paymentDate),
          Locale.forLanguageTag(LANGUAGE))
      val successTemplateRequest =
        NotificationsServiceClient.SuccessTemplateRequest(
          EMAIL_STRING,
          "Il riepilogo del tuo pagamento",
          LANGUAGE,
          SuccessTemplate(
            TransactionTemplate(
              baseTransaction.transactionId.value(),
              dateString,
              totalAmountWithFeeString,
              PspTemplate(PSP_BUSINESS_NAME, FeeTemplate(feeString)),
              baseTransaction.transactionAuthorizationCompletedData.rrn,
              baseTransaction.transactionAuthorizationCompletedData.authorizationCode,
              PaymentMethodTemplate(PAYMENT_METHOD_DESCRIPTION, LOGO_URI.toString(), null, false)),
            UserTemplate(null, EMAIL_STRING),
            CartTemplate(
              baseTransaction.paymentNotices.map {
                ItemTemplate(
                  RefNumberTemplate(RefNumberTemplate.Type.CODICE_AVVISO, it.creditorReferenceId),
                  null,
                  PayeeTemplate(it.companyName.value, it.rptId.fiscalCode),
                  it.transactionDescription.value,
                  userReceiptMailBuilder.amountToHumanReadableString(it.transactionAmount.value))
              },
              totalAmount),
          ))
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
  fun `Should set right value string to payee template name field when TransactionUserReceiptData receivingOfficeName is not null`() =
    runTest {
      val confidentialDataUtils: ConfidentialDataUtils = mock()
      given(confidentialDataUtils.toEmail(any())).willReturn(Email("to@to.it"))
      val userReceiptBuilder = UserReceiptMailBuilder(confidentialDataUtils)
      val transactionUserReceiptData =
        TransactionUserReceiptData(TransactionUserReceiptData.Outcome.OK, "it-IT", PAYMENT_DATE)
      val companyName = "testCompanyName"
      val transactionActivatedEvent = transactionActivateEvent()
      transactionActivatedEvent.data.paymentNotices.forEach { it.companyName = companyName }
      val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val events =
        listOf(
          transactionActivatedEvent,
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(
            NpgTransactionGatewayAuthorizationData(
              OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null)),
          transactionClosureRequestedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          notificationRequested)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      Hooks.onOperatorDebug()

      val notificationEmailRequestDto =
        userReceiptBuilder.buildNotificationEmailRequestDto(baseTransaction)
      assertEquals(
        companyName,
        (notificationEmailRequestDto.parameters as SuccessTemplate)
          .cart
          .items
          .filter { i -> i.payee != null }[0]
          .payee
          .name)
    }

  @Test
  fun `Should set empty string to payee template name field when TransactionUserReceiptData receivingOfficeName is null`() =
    runTest {
      val confidentialDataUtils: ConfidentialDataUtils = mock()
      given(confidentialDataUtils.toEmail(any())).willReturn(Email("to@to.it"))
      val userReceiptBuilder = UserReceiptMailBuilder(confidentialDataUtils)
      val transactionUserReceiptData =
        TransactionUserReceiptData(TransactionUserReceiptData.Outcome.OK, "it-IT", PAYMENT_DATE)
      val notificationRequested = transactionUserReceiptRequestedEvent(transactionUserReceiptData)
      val transactionActivatedEvent = transactionActivateEvent()
      transactionActivatedEvent.data.paymentNotices.forEach { it.companyName = null }
      val events =
        listOf(
          transactionActivatedEvent,
          transactionAuthorizationRequestedEvent(),
          transactionAuthorizationCompletedEvent(
            NpgTransactionGatewayAuthorizationData(
              OperationResultDto.EXECUTED, "operationId", "paymentEnd2EndId", null, null)),
          transactionClosureRequestedEvent(),
          transactionClosedEvent(TransactionClosureData.Outcome.OK),
          notificationRequested)
          as List<TransactionEvent<Any>>
      val baseTransaction =
        reduceEvents(*events.toTypedArray()) as BaseTransactionWithRequestedUserReceipt
      Hooks.onOperatorDebug()

      val notificationEmailRequestDto =
        userReceiptBuilder.buildNotificationEmailRequestDto(baseTransaction)
      assertEquals(
        "",
        (notificationEmailRequestDto.parameters as SuccessTemplate)
          .cart
          .items
          .filter { i -> i.payee != null }[0]
          .payee
          .name)
    }
}
