package it.pagopa.ecommerce.eventdispatcher.warmup

import it.pagopa.ecommerce.eventdispatcher.services.InboundChannelAdapterLifecycleHandlerService
import it.pagopa.ecommerce.eventdispatcher.warmup.annotations.WarmupFunction
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.reflect.full.hasAnnotation
import kotlin.system.measureTimeMillis
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationListener
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.util.ClassUtils

@Component
class ServicesWarmup(
  @Autowired
  private val inboundChannelAdapterLifecycleHandlerService:
    InboundChannelAdapterLifecycleHandlerService
) : ApplicationListener<ContextRefreshedEvent> {

  private val logger = LoggerFactory.getLogger(this.javaClass)

  override fun onApplicationEvent(event: ContextRefreshedEvent) {
    val eventReceiverServices =
      event.applicationContext
        .getBeansWithAnnotation(Service::class.java)
        .map { it.value }
        .filter { service ->
          service.javaClass.kotlin.declaredMemberFunctions.any {
            it.hasAnnotation<WarmupFunction>()
          }
        }
    logger.info("Found services: [{}]", eventReceiverServices.size)

    try {
      eventReceiverServices.forEach(this::warmUpService)
    } catch (e: Exception) {
      logger.error("Exception during service warm-up", e)
    } finally {
      inboundChannelAdapterLifecycleHandlerService.invokeCommandForAllEndpoints("start")
      inboundChannelAdapterLifecycleHandlerService.invokeCommandForRedisStreamMessageSource("start")
    }
  }

  private fun warmUpService(serviceToWarmUpInstance: Any) {
    var warmUpMethods = 0
    val serviceToWarmUpKClass = ClassUtils.getUserClass(serviceToWarmUpInstance).kotlin
    val elapsedTime = measureTimeMillis {
      runCatching {
          serviceToWarmUpKClass.declaredMemberFunctions
            .filter { it.hasAnnotation<WarmupFunction>() }
            .forEach {
              warmUpMethods++
              val result: Result<*>
              val intertime = measureTimeMillis {
                result = runCatching {
                  logger.info("Invoking function: [{}]", it.toString())
                  it.call(serviceToWarmUpInstance)
                }
              }
              logger.info(
                "Warmup function: [{}] -> elapsed time: [{}]. Is ok: [{}] ",
                it.toString(),
                intertime,
                result)
            }
        }
        .getOrElse { logger.error("Exception performing service warm up ", it) }
    }
    logger.info(
      "service: [{}] warm-up executed functions: [{}], elapsed time: [{}] ms",
      serviceToWarmUpKClass,
      warmUpMethods,
      elapsedTime)
  }
}
