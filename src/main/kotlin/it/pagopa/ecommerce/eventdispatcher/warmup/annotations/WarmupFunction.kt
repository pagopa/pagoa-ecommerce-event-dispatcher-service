package it.pagopa.ecommerce.eventdispatcher.warmup.annotations

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
/**
 * Annotation used to annotate a queue service function to be called during module warm-up phase.
 * Warm-up function can be used to simulate a message v2 received from the queue in order to
 * initialize all it's resource before the module being ready to serve requests
 */
annotation class WarmupFunction {}
