package org.agaveplatform.service.transfers.matchers;

import org.agaveplatform.service.transfers.model.TransferTask;
import org.mockito.ArgumentMatcher;

public class IsSameTransferTask extends ArgumentMatcher<TransferTask> {
        TransferTask expectedTransferTask;

        public IsSameTransferTask(TransferTask expectedTransferTask) {
            this.expectedTransferTask = expectedTransferTask;
        }

        /**
         * Returns whether this matcher accepts the given argument.
         * <p>
         * The method should <b>never</b> assert if the argument doesn't match. It
         * should only return false.
         *
         * @param actualTransferTask the argument
         * @return whether this matcher accepts the given argument.
         */

        @Override
        public boolean matches(Object actualTransferTask) {
            if (!(actualTransferTask instanceof TransferTask)) return false;
            return actualTransferTask.equals(expectedTransferTask);
        }
    }