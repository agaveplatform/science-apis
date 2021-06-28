package org.agaveplatform.service.transfers.model;

import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;

import javax.validation.constraints.NotNull;

public class TransferTaskNotificationMessage extends TransferTaskMessage {
    public static final String SOURCE_MESSAGE_TYPE_FIELD = "originalAddress";
    private String sourceMessageType;

    public TransferTaskNotificationMessage(JsonObject jsonTransferTask) {
        super(jsonTransferTask);

        if (jsonTransferTask.containsKey(SOURCE_MESSAGE_TYPE_FIELD)) {
            this.sourceMessageType = jsonTransferTask.getString(SOURCE_MESSAGE_TYPE_FIELD);
        }
    }

    public TransferTaskNotificationMessage(@NotNull TransferTask transferTask, @NotNull String sourceMessageType) {
        super(transferTask);
        this.sourceMessageType = sourceMessageType;
    }


    /**
     * Fetches the source {@link MessageType} constant for this message
     * @return a valid {@link MessageType} constant or null if no value is set or no match
     */
    public String getSourceMessageType() {
        return this.sourceMessageType;
    }

    /**
     * Sets the {@link MessageType} constant value for the {@link #SOURCE_MESSAGE_TYPE_FIELD} in the json representation
     * of this message
     * @param sourceMessageType a valid {@link MessageType} constant value
     */
    public void setSourceMessageType(String sourceMessageType) {
        this.sourceMessageType = sourceMessageType;
    }


    /**
     * Marshals the {@link TransferTask} to json, and inserts a {@link #SOURCE_MESSAGE_TYPE_FIELD} wih the value of
     * {@link #getSourceMessageType()}.
     * @return the marshalled message
     */
    public JsonObject toJson() {
        return this.transferTask.toJson().put(SOURCE_MESSAGE_TYPE_FIELD, getSourceMessageType());
    }
}
