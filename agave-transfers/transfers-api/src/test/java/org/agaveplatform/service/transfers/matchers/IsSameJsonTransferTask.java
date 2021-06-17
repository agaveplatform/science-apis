package org.agaveplatform.service.transfers.matchers;

import io.vertx.core.json.JsonObject;
import org.mockito.ArgumentMatcher;

public class IsSameJsonTransferTask extends ArgumentMatcher<JsonObject> {
        JsonObject expectedJsonTransferTask;

        public IsSameJsonTransferTask(JsonObject expectedJsonTransferTask) {
            this.expectedJsonTransferTask = expectedJsonTransferTask;
        }

        /**
         * Returns whether this matcher accepts the given argument.
         * <p>
         * The method should <b>never</b> assert if the argument doesn't match. It
         * should only return false.
         *
         * @param actualJsonTransferTask the argument
         * @return whether this matcher accepts the given argument.
         */
        @Override
        public boolean matches(Object actualJsonTransferTask) {
            if (!(actualJsonTransferTask instanceof JsonObject)) return false;

            this.expectedJsonTransferTask.remove("transferRate");
            ((JsonObject) actualJsonTransferTask).remove("transferRate");

            this.expectedJsonTransferTask.remove("lastUpdated");
            ((JsonObject) actualJsonTransferTask).remove("lastUpdated");

//            this.expectedJsonTransferTask.remove("endTime");
//            ((JsonObject) actualJsonTransferTask).remove("endTime");

            return actualJsonTransferTask.equals(this.expectedJsonTransferTask);
        }
    }