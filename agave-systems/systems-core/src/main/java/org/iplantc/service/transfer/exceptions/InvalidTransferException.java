package org.iplantc.service.transfer.exceptions;

public class InvalidTransferException extends RemoteDataException {
    public InvalidTransferException() {
    }

    public InvalidTransferException(String arg0) {
        super(arg0);
    }

    public InvalidTransferException(Throwable arg0) {
        super(arg0);
    }

    public InvalidTransferException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }
}
