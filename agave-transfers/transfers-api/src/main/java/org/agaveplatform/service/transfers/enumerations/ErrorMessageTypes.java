package org.agaveplatform.service.transfers.enumerations;

import org.apache.commons.lang.StringUtils;

import java.util.List;

public enum ErrorMessageTypes {
    TRANSFER_COMPLETED,
    TRANSFERTASK_ERROR,
    TRANSFERTASK_PARENT_ERROR,
    TRANSFER_FAILED;


    /**
     * Turns {@link #values()} into a comma separated string
     * @return the values joined by ", "
     */
    public static String supportedValuesAsString() {
        return StringUtils.join(values(), ", ");
    }

    /**
     * Returns {@link ErrorMessageTypes} that represent error message statuses
     * @return immutable list of the active {@link TransferStatusType}
     */
    public static List<ErrorMessageTypes> getError() {
        return List.of(
            TRANSFER_COMPLETED,
            TRANSFERTASK_ERROR,
            TRANSFERTASK_PARENT_ERROR);
    }

    /**
     * Checks that the message is one of the error states.
     * @return true if this {@link TransferStatusType} is present in {@link #getError()}, false otherwise
     * @see #getError()
     */
    public boolean isError() {
        return getError().contains(this);
    }
}
