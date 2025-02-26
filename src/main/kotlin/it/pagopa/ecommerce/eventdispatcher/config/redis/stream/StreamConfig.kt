package it.pagopa.ecommerce.eventdispatcher.config.redis.stream

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherCommandMixin
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherGenericCommand
import java.time.Duration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.hash.Jackson2HashMapper
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.data.redis.stream.StreamReceiver

/**
 * Redis stream configuration class. This class contains all Redis Stream integration specific
 * configurations
 */
@Configuration
class StreamConfig {

  @Bean
  fun redisStreamReceiver(
    reactiveRedisConnectionFactory: ReactiveRedisConnectionFactory,
  ): StreamReceiver<String, ObjectRecord<String, LinkedHashMap<*, *>>>? {
    val objectMapper =
      jacksonObjectMapper()
        .addMixIn(
          EventDispatcherGenericCommand::class.java, EventDispatcherCommandMixin::class.java)
    val streamReceiverOptions =
      StreamReceiver.StreamReceiverOptions.builder()
        .pollTimeout(Duration.ofMillis(1000))
        .batchSize(1) // read one item per poll
        .keySerializer(
          RedisSerializationContext.SerializationPair.fromSerializer(StringRedisSerializer()))
        .objectMapper(Jackson2HashMapper(objectMapper, false))
        .targetType(LinkedHashMap::class.java)
        .build()
    return StreamReceiver.create(reactiveRedisConnectionFactory, streamReceiverOptions)
  }

  /**
   * InboundChannelAdapter has only method scope, used this producer method to bound custom
   * RedisStreamMessageSource to inbound channel adapter
   */
  /*@Bean
  @InboundChannelAdapter(
    channel = "eventDispatcherReceiverCommandChannel",
    poller = [Poller(fixedDelay = "1000", maxMessagesPerPoll = "1")],
    autoStartup = "false")
  @EndpointId("eventDispatcherReceiverCommandChannelEndpoint")
  fun eventDispatcherReceiverCommandMessageSource(
    redisStreamReceiver: StreamReceiver<String, ObjectRecord<String, LinkedHashMap<*, *>>>,
    eventDispatcherCommandsTemplateWrapper: EventDispatcherCommandsTemplateWrapper,
    redisStreamConf: RedisStreamEventControllerConfigs
  ): RedisStreamMessageSource =
    RedisStreamMessageSource(
      redisStreamReceiver = redisStreamReceiver,
      eventDispatcherCommandsTemplateWrapper = eventDispatcherCommandsTemplateWrapper,
      redisStreamConf = redisStreamConf)
   */
}
