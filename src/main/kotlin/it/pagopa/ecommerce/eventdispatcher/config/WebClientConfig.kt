package it.pagopa.ecommerce.eventdispatcher.config

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.netty.channel.ChannelOption
import io.netty.handler.timeout.ReadTimeoutHandler
import it.pagopa.generated.ecommerce.gateway.v1.ApiClient as GatewayApiClient
import it.pagopa.generated.ecommerce.gateway.v1.api.VposInternalApi
import it.pagopa.generated.ecommerce.gateway.v1.api.XPayInternalApi
import it.pagopa.generated.ecommerce.nodo.v2.ApiClient as NodoApiClient
import it.pagopa.generated.ecommerce.nodo.v2.api.NodoApi
import it.pagopa.generated.notifications.v1.ApiClient
import it.pagopa.generated.notifications.v1.api.DefaultApi
import java.util.concurrent.TimeUnit
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.codec.StringDecoder
import org.springframework.http.MediaType
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.http.codec.ClientCodecConfigurer
import org.springframework.http.codec.json.Jackson2JsonDecoder
import org.springframework.http.codec.json.Jackson2JsonEncoder
import org.springframework.web.reactive.function.client.ExchangeStrategies
import reactor.netty.Connection
import reactor.netty.http.client.HttpClient

@Configuration
class WebClientConfig {

  fun getNodeObjectMapper(): ObjectMapper {
    val mapper = ObjectMapper()
    mapper.registerModule(JavaTimeModule())
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    return mapper
  }
  @Bean
  fun nodoApi(
    @Value("\${nodo.uri}") nodoUri: String,
    @Value("\${nodo.readTimeout}") nodoReadTimeout: Long,
    @Value("\${nodo.connectionTimeout}") nodoConnectionTimeout: Int,
    @Value("\${nodo.closepayment.apikey}") nodoClosePaymentApiKey: String
  ): NodoApi {
    val httpClient: HttpClient =
      HttpClient.create()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nodoConnectionTimeout)
        .doOnConnected { connection ->
          connection.addHandlerLast(ReadTimeoutHandler(nodoReadTimeout, TimeUnit.MILLISECONDS))
        }

    val exchangeStrategies =
      ExchangeStrategies.builder()
        .codecs { clientCodecConfigurer: ClientCodecConfigurer ->
          val mapper = getNodeObjectMapper()
          clientCodecConfigurer.registerDefaults(false)
          clientCodecConfigurer.customCodecs().register(StringDecoder.allMimeTypes())
          clientCodecConfigurer
            .customCodecs()
            .register(Jackson2JsonDecoder(mapper, MediaType.APPLICATION_JSON))
          clientCodecConfigurer
            .customCodecs()
            .register(Jackson2JsonEncoder(mapper, MediaType.APPLICATION_JSON))
        }
        .build()

    val webClient =
      NodoApiClient.buildWebClientBuilder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .defaultHeader("ocp-apim-subscription-key", nodoClosePaymentApiKey)
        .exchangeStrategies(exchangeStrategies)
        .baseUrl(nodoUri)
        .build()

    val nodoApiClient = NodoApiClient(webClient).apply { basePath = nodoUri }

    return NodoApi(nodoApiClient)
  }

  @Bean(name = ["VposApiWebClient"])
  fun vposApiWebClient(
    @Value("\${paymentTransactionsGateway.uri}") paymentTransactionGatewayUri: String,
    @Value("\${paymentTransactionsGateway.readTimeout}") paymentTransactionGatewayReadTimeout: Int,
    @Value("\${paymentTransactionsGateway.connectionTimeout}")
    paymentTransactionGatewayConnectionTimeout: Int,
    @Value("\${paymentTransactionsGateway.apiKey}") paymentTransactionGatewayApiKey: String
  ): VposInternalApi {
    val httpClient =
      HttpClient.create()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, paymentTransactionGatewayConnectionTimeout)
        .doOnConnected { connection: Connection ->
          connection.addHandlerLast(
            ReadTimeoutHandler(
              paymentTransactionGatewayReadTimeout.toLong(), TimeUnit.MILLISECONDS))
        }
    val webClient =
      GatewayApiClient.buildWebClientBuilder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .baseUrl(paymentTransactionGatewayUri)
        .build()

    val gatewayApiClient = GatewayApiClient(webClient)
    gatewayApiClient.setApiKey(paymentTransactionGatewayApiKey)
    gatewayApiClient.basePath = paymentTransactionGatewayUri

    return VposInternalApi(gatewayApiClient)
  }

  @Bean(name = ["XpayApiWebClient"])
  fun xpayApiWebClient(
    @Value("\${paymentTransactionsGateway.uri}") paymentTransactionGatewayUri: String,
    @Value("\${paymentTransactionsGateway.readTimeout}") paymentTransactionGatewayReadTimeout: Int,
    @Value("\${paymentTransactionsGateway.connectionTimeout}")
    paymentTransactionGatewayConnectionTimeout: Int,
    @Value("\${paymentTransactionsGateway.apiKey}") paymentTransactionGatewayApiKey: String
  ): XPayInternalApi {
    val httpClient =
      HttpClient.create()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, paymentTransactionGatewayConnectionTimeout)
        .doOnConnected { connection: Connection ->
          connection.addHandlerLast(
            ReadTimeoutHandler(
              paymentTransactionGatewayReadTimeout.toLong(), TimeUnit.MILLISECONDS))
        }
    val webClient =
      GatewayApiClient.buildWebClientBuilder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .baseUrl(paymentTransactionGatewayUri)
        .build()

    val gatewayApiClient = GatewayApiClient(webClient)
    gatewayApiClient.setApiKey(paymentTransactionGatewayApiKey)
    gatewayApiClient.basePath = paymentTransactionGatewayUri

    return XPayInternalApi(gatewayApiClient)
  }

  @Bean(name = ["notificationsServiceWebClient"])
  fun notificationsServiceWebClient(
    @Value("\${notificationsService.uri}") notificationsServiceUri: String,
    @Value("\${notificationsService.readTimeout}") notificationsServiceReadTimeout: Int,
    @Value("\${notificationsService.connectionTimeout}") notificationsServiceConnectionTimeout: Int
  ): DefaultApi {
    val httpClient =
      HttpClient.create()
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, notificationsServiceConnectionTimeout)
        .doOnConnected { connection: Connection ->
          connection.addHandlerLast(
            ReadTimeoutHandler(notificationsServiceReadTimeout.toLong(), TimeUnit.MILLISECONDS))
        }
    val webClient =
      ApiClient.buildWebClientBuilder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .baseUrl(notificationsServiceUri)
        .build()
    return DefaultApi(ApiClient(webClient).setBasePath(notificationsServiceUri))
  }
}
