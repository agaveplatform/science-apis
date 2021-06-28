package org.agaveplatform.service.transfers.model;

import io.vertx.core.json.JsonObject;

import javax.validation.constraints.NotNull;
import java.util.Objects;

public class TransferTaskMessage {
    protected TransferTask transferTask;

    public TransferTaskMessage(@NotNull TransferTask transferTask) {
        Objects.requireNonNull(transferTask);
        this.transferTask = transferTask;
    }

    public TransferTaskMessage(@NotNull JsonObject jsonTransferTask) {
        Objects.requireNonNull(jsonTransferTask);
        this.transferTask = new TransferTask(jsonTransferTask);
    }

    public TransferTask getTransferTask() {
        return this.transferTask;
    }

    public JsonObject toJson() {
        return this.transferTask.toJson();
    }

    public String toString() {
        return toJson().encode();
    }
}
