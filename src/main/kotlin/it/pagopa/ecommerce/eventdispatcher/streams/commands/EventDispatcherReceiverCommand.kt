package it.pagopa.ecommerce.eventdispatcher.streams.commands

import it.pagopa.generated.eventdispatcher.server.model.DeploymentVersionDto

data class EventDispatcherReceiverCommand(
    val receiverCommand: ReceiverCommand,
    val version: DeploymentVersionDto?
) : EventDispatcherGenericCommand(type = CommandType.RECEIVER_COMMAND) {
    enum class ReceiverCommand {
        START,
        STOP
    }
}