package org.iplantc.service.common.messaging;

import org.iplantc.service.common.exceptions.MessageProcessingException;

public interface MessageQueueListener {
	
	void processMessage(String message) throws MessageProcessingException;
	
	void stop();
}
