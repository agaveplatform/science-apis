package org.agaveplatform.service.transfers.exception;

import org.iplantc.service.common.exceptions.MessagingException;

public class DuplicateMessageException extends MessagingException {
    public DuplicateMessageException() {
    }

    public DuplicateMessageException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    public DuplicateMessageException(String arg0) {
        super(arg0);
    }

    public DuplicateMessageException(Throwable arg0) {
        super(arg0);
    }
}
