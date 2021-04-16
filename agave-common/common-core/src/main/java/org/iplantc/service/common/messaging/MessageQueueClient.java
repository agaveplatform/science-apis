package org.iplantc.service.common.messaging;

import org.apache.commons.lang.NotImplementedException;
import org.iplantc.service.common.exceptions.MessageProcessingException;
import org.iplantc.service.common.exceptions.MessagingException;

import java.util.List;

/**
 * Interface used to provide an abstraction layer over a pluggable messaging service. This exposes the minimal
 * methods needed to push, pop, search, discovery, and renew messages..
 */
public interface MessageQueueClient {

	/**
	 * Publishes a message onto the given queue
	 * @param queue name of queue
	 * @param message the message content to persist
	 * @throws MessagingException if communication with the queue fails
	 */
	public void push(String exchange, String queue, String message) throws MessagingException;
	
	/**
	 * Publishes a message onto the given queue with a delayed execution
	 * @param queue name of queue
	 * @param message the message body to persist
	 * @param secondsToDelay the number of seconds to delay the message from being processed
	 * @throws MessagingException if communication with the queue fails
	 */
	public void push(String exchange, String queue, String message, int secondsToDelay) throws MessagingException;
	
	/**
	 * Returns a message back to the queue for consumption by a different process.
	 * 
	 * @param messageId the unique id of the message to reject
	 * @param message the message body to reject
	 * @throws MessagingException if communication with the queue fails
	 */
	public void reject(String exchange, String queue, Object messageId, String message) throws MessagingException;
	
	/**
	 * Deletes a message from the queue.
	 * 
	 * @param messageId the unique id of the message
	 * @throws MessagingException if communication with the queue fails
	 */
	public void delete(String exchange, String queue, Object messageId) throws MessagingException;
	
	/**
	 * Gets the next event from the queue. The distribution of messages is controlled by 
	 * the messaging system. This will simply pull the next messages.
	 *
	 * @param queue name of queue
	 * @return the next message on the queue
	 * @throws MessagingException if communication with the queue fails
	 */
	public Message pop(String exchange, String queue) throws MessagingException;
	
	/**
	 * Gets the next count events from the queue. The distribution of messages is controlled by 
	 * the messaging system. This will simply pull the next count messages.
	 *
	 * @param exchange name of the exchange
	 * @param queue name of queue
	 * @param count the number of messages to pull from the queue
	 * @return the messages popped form the queue
	 * @throws MessagingException if communication with the queue fails
	 */
	public List<Message> pop(String exchange, String queue, int count) throws MessagingException;
	
//	/**
//	 * Explicitly acknowledge receipt of a message. This is needed in long running tasks.
//	 * @param messageId the unique id of the message
//	 * @return
//	 * @throws MessagingException if communication with the queue fails
//	 */
//	public void acknowledge(String exchange,Object messageId) throws MessagingException;
//	
	
	/**
	 * Touches a message or job in the queue to keep it alive. Used for long running tasks.
	 *
	 * @param messageId the unique id of the message
	 * @param queue the name of the queue where the message lives
	 * @return true if the refresh succeeded, false otherwise
	 * @throws MessagingException if communication with the queue fails
	 */
	public boolean touch(Object messageId, String queue) throws MessagingException;
	
	/**
	 * Starts a watch loop that will listen to the event queue and process anything that comes 
	 * until {@link MessageQueueListener#stop()} is called.
	 *
	 * @param exchange name of the exchange
	 * @param queue name of queue
	 * @param listener the listener with which to listen
	 * @throws MessagingException if communication with the queue fails
	 */
	public void listen(String exchange, String queue, MessageQueueListener listener) 
	throws MessagingException, MessageProcessingException;
	
	/**
	 * Forces a listening loop to terminate
	 */
	public void stop();
	
	/**
	 * Returns a list of all the queues available in the underlying
	 * service. This may not be available uniformly.
	 * 
	 * @return List of all the available queues on the server
	 * @throws MessagingException if communication with the queue fails
	 * @throws NotImplementedException if the underlying service does not support messaging
	 */
	public List<Object> listQueues() throws MessagingException, NotImplementedException;
	
	/**
     * Returns a list of the names of all the queues available in the underlying
     * service. This may not be available uniformly.
     * 
     * @return List of the available queue names
     * @throws MessagingException if communication with the queue fails
     * @throws NotImplementedException if the underlying service does not support messaging
     */
    public List<String> listQueueNames() throws MessagingException, NotImplementedException;
	
	/**
	 * Returns true if a queue exists match the given name.
	 * 
	 * @param queue name of queue
	 * @return true if the queue exists, false otherwise
	 * @throws MessagingException if communication with the queue fails
	 */
	public boolean queueExist(String queueName) throws MessagingException;
	
	/**
	 * Find a queue whose name matches the given regular expression. There is no guarantee
	 * that this will be the best fit match, just that it will match. 
	 * 
	 * @param regex the regular expression used to match the queue name
	 * @return name of queue matching the regex
	 * @throws MessagingException if communication with the queue fails
	 */
	public String findQueueMatching(String regex) throws MessagingException;
	
	/**
	 * Checks for the existence of a given message.
	 * 
	 * @param queue name of queue
	 * @param messageId the unique id of the message
	 * @return true if a message with the given ID exists. False otherwise.
	 * @throws MessagingException if communication with the queue fails
	 */
	public boolean messageExist(String queue, Object messageId) throws MessagingException;

	/**
	 * Identical to {@link #pop(String, String)}, but waits <code>timeout</code> to
	 * recieve a reserved message. 
	 * 
	 * @param exchange name of the exchange
	 * @param queue name of queue
	 * @param timeout the timeout of the message in seconds.
	 * @return Successfully reserved message
	 * @throws MessagingException if a message is not received during timeout or the operation fails
	 */
	public Message reserve(String exchange, String queue, int timeout)
	throws MessagingException;

    
	
}
