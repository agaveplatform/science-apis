package org.iplantc.service.transfer.exceptions;

public class RemotePermissionException extends RemoteDataException {
    /**
     *
     */
    public RemotePermissionException() {
    }

    /**
     * @param message
     */
    public RemotePermissionException(String message) {
        super(message);
    }

    /**
     * @param message
     */
    public RemotePermissionException(Throwable message) {
        super(message);
    }

    /**
     * @param message
     * @param throwable
     */
    public RemotePermissionException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
