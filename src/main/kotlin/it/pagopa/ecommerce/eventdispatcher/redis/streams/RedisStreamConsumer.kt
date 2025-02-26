package it.pagopa.ecommerce.eventdispatcher.redis.streams

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import it.pagopa.ecommerce.eventdispatcher.config.RedisStreamEventControllerConfigs
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherCommandMixin
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherGenericCommand
import it.pagopa.ecommerce.eventdispatcher.redis.streams.commands.EventDispatcherReceiverCommand
import it.pagopa.ecommerce.eventdispatcher.services.InboundChannelAdapterLifecycleHandlerService
import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto
import java.time.Duration
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.data.redis.connection.stream.ObjectRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.stream.StreamReceiver
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry

@Service
class RedisStreamConsumer(
  @Autowired
  private val redisStreamReceiver:
    StreamReceiver<String, ObjectRecord<String, LinkedHashMap<*, *>>>,
  @Autowired private val redisStreamConf: RedisStreamEventControllerConfigs,
  @Autowired
  private val inboundChannelAdapterLifecycleHandlerService:
    InboundChannelAdapterLifecycleHandlerService,
  @Value("\${eventController.deploymentVersion}")
  private val deploymentVersion: DeploymentVersionDto,
) : ApplicationListener<ApplicationReadyEvent> {

  private val objectMapper: ObjectMapper =
    jacksonObjectMapper().apply {
      addMixIn(EventDispatcherGenericCommand::class.java, EventDispatcherCommandMixin::class.java)
    }

  private val logger = LoggerFactory.getLogger(javaClass)

  override fun onApplicationEvent(event: ApplicationReadyEvent) {
    logger.info("Starting Redis stream receiver")
    eventStreamPipelineWithRetry().subscribeOn(Schedulers.parallel()).subscribe { record ->
      runCatching {
          val event =
            objectMapper.convertValue(record.value, EventDispatcherGenericCommand::class.java)
          processStreamEvent(event)
        }
        .onFailure { ex -> logger.error("Error processing redis stream event", ex) }
    }
  }

  fun eventStreamPipelineWithRetry(): Flux<ObjectRecord<String, LinkedHashMap<*, *>>> =
    Mono.just(1)
      .flatMapMany {
        redisStreamReceiver.receive(
          StreamOffset.create(redisStreamConf.streamKey, ReadOffset.from(RecordId.of(0, 0))))
      }
      .retryWhen(
        Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(1)).doBeforeRetry {
          logger.warn("Detected error in redis stream connection, reconnecting", it.failure())
        })

  fun processStreamEvent(event: EventDispatcherGenericCommand) {
    logger.info("Received event: ${event.type}")
    when (event) {
      is EventDispatcherReceiverCommand -> hanldeEventReceiverCommand(event)
    }
  }

  fun hanldeEventReceiverCommand(command: EventDispatcherReceiverCommand) {
    val currentDeploymentVersion = deploymentVersion
    val commandTargetVersion = command.version

    val isTargetedByCommand =
      commandTargetVersion == null || currentDeploymentVersion == commandTargetVersion
    logger.info(
      "Event dispatcher receiver command event received. Current deployment version: [{}], command deployment version: [{}] -> targeted: [{}]",
      currentDeploymentVersion,
      commandTargetVersion ?: "ALL",
      isTargetedByCommand)

    if (isTargetedByCommand) {
      val commandToSend =
        when (command.receiverCommand) {
          EventDispatcherReceiverCommand.ReceiverCommand.START -> "start"
          EventDispatcherReceiverCommand.ReceiverCommand.STOP -> "stop"
        }
      inboundChannelAdapterLifecycleHandlerService.invokeCommandForAllEndpoints(commandToSend)
    } else {
      logger.info(
        "Current deployment version not targeted by command, command will not be processed")
    }
  }
}
