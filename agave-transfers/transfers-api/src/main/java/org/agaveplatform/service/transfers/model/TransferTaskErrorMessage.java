package org.agaveplatform.service.transfers.model;

import io.vertx.core.json.JsonObject;

import javax.validation.constraints.NotNull;

public class TransferTaskErrorMessage extends TransferTaskMessage {
    public static final String ERROR_CLASS_NAME_FIELD = "cause";
    public static final String ERROR_MESSAGE_FIELD = "mesasge";

    private String errorClassName;
    private String errorMessage;

    public TransferTaskErrorMessage(@NotNull TransferTask transferTask, Throwable cause) {
        this(transferTask, cause, cause == null ? null : cause.getMessage());
    }

    public TransferTaskErrorMessage(@NotNull TransferTask transferTask, Throwable cause, String errorMessage) {
        super(transferTask);
        this.errorClassName = cause == null ? null : cause.getClass().getSimpleName();
        this.errorMessage = errorMessage;
    }

    public TransferTaskErrorMessage(JsonObject jsonTransferTask) {
        super(jsonTransferTask);
        this.errorClassName = jsonTransferTask.getString(ERROR_CLASS_NAME_FIELD);
        this.errorMessage = jsonTransferTask.getString(ERROR_MESSAGE_FIELD);
    }

    /**
     * Fetches the class name of the original exception from the {@link #ERROR_CLASS_NAME_FIELD}
     * @return the original exception type
     */
    public String getErrorClassName() {
        return this.errorClassName;
    }

    /**
     * Sets the value for the {@link #ERROR_CLASS_NAME_FIELD} in the json representation
     * of this message
     * @param errorClassName the errorClassName to set
     */
    public void setErrorClassName(String errorClassName) {
        this.errorClassName = errorClassName;
    }

    /**
     * Sets the value for the {@link #ERROR_CLASS_NAME_FIELD} in the json representation
     * of this message
     * @param cause the cause to attribute to this message
     */
    public void setErrorClassName(Throwable cause) {
        this.errorClassName = cause == null ? null : cause.getClass().getSimpleName();
    }

    /**
     * Fetches the error message from the {@link #ERROR_MESSAGE_FIELD}
     * @return the original exception type
     */
    public String getErrorMessage() {
        return this.errorMessage;
    }

    /**
     * Sets the value for the {@link #ERROR_MESSAGE_FIELD} in the json representation
     * of this message
     * @param errorMessage a valid error message
     */
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * Marshals the {@link TransferTask} to json, and inserts {@link #ERROR_CLASS_NAME_FIELD} and {@link #ERROR_MESSAGE_FIELD}
     * fields wih their respective value.
     * @return the marshalled message
     */
    public JsonObject toJson() {
        return this.transferTask.toJson()
                .put(ERROR_CLASS_NAME_FIELD, getErrorClassName())
                .put(ERROR_MESSAGE_FIELD, getErrorMessage());
    }
}