/**
 * 
 */
package org.iplantc.service.common.messaging;

import com.surftools.BeanstalkClient.Job;
import com.surftools.BeanstalkClientImpl.ClientImpl;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.exceptions.MessageProcessingException;
import org.iplantc.service.common.exceptions.MessagingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.nats.streaming.*;

/**
 * @author dooley
 *
 */
public class NatsClient implements MessageQueueClient
{
	private static final Logger log = Logger.getLogger(NatsClient.class);

	private StreamingConnection connection;
	private boolean stop = false;

	class MessageAckHandler implements AckHandler {

		@Override
		public void onAck(String s, Exception e) {
			if (e != null) {
				log.debug("Message ack received " + s);
			} else {
				log.error("Failed to put message on queue.", e);
			}
		}

		@Override
		public void onAck(String nuid, String subject, byte[] data, Exception ex) {
			if (ex != null) {
				log.debug("Message ack received " + nuid);
			} else {
				log.error("Failed to put message on queue.", ex);
			}
		}

		@Override
		public boolean includeDataWithAck() {
			return false;
		}
	};

	public NatsClient() throws IOException, InterruptedException {
		String natsUrl = String.format("nats://%s:%d", Settings.MESSAGING_SERVICE_HOST, Settings.MESSAGING_SERVICE_PORT);
		Options options = new Options.Builder().natsUrl(natsUrl).build();
				//.errorListener(new NatsClient.ErrorListener());
		connection = NatsStreaming.connect("testing", Settings.getLocalHostname(), options);
	}
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.common.messaging.MessageQueueClient#push(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void push(String exchange, String queue, String message)
	throws MessagingException
	{
		try {
			String connection.publish(queue, message.getBytes(), new MessageAckHandler() );
		} catch (Exception e) {
			throw new MessagingException("Failed to push message to the " + queue + " tube", e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.common.messaging.MessageQueueClient#push(java.lang.String, java.lang.String, java.lang.String, int)
	 */
	@Override
	public void push(String exchange, String queue, String message, int secondsToDelay)
	throws MessagingException
	{
		try {
			connection.publish(queue, message.getBytes(), new MessageAckHandler() );
		} catch (Exception e) {
			throw new MessagingException("Failed to push message to the " + queue + " tube", e);
		}
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.common.messaging.MessageQueueClient#reject(java.lang.String, java.lang.String, java.lang.Object, java.lang.String)
	 */
	@Override
	public void reject(String exchange, String queue, Object messageId,
			String message) throws MessagingException
	{
		try {
			connection.watch(queue);
			connection.useTube(queue);
			
			connection.release((Long)messageId, 65536, 10);
		} 
		catch (Exception e) {
			throw new MessagingException("Failed to reject message and remove from the tube", e);
		}
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.common.messaging.MessageQueueClient#pop(java.lang.String, java.lang.String)
	 */
	@Override
	public Message pop(String exchange, String queue) throws MessagingException
	{
		try
		{
			connection.watch(queue);
			connection.useTube(queue);
//			if (getActiveJobCount(exchange, queue) > 0) 
//			{
				Job beanstalkJob = connection.reserve(null);
				
				connection.ignore(queue);
				return new Message(beanstalkJob.getJobId(), new String(beanstalkJob.getData()));
//			} 
//			else
//			{
//				return null;
//			}
		}
		catch (Exception e)
		{
			throw new MessagingException("Failed to retrieve message from the " + queue + " tube", e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.common.messaging.MessageQueueClient#reserve(java.lang.String, java.lang.String, int)
	 */
	@Override
	public Message reserve(String exchange, String queue, int timeout) throws MessagingException
	{
		try
		{
			connection.watch(queue);
			connection.useTube(queue);
			Job beanstalkJob = connection.reserve(timeout);
			
			connection.ignore(queue);
			if (beanstalkJob != null) {
				return new Message(beanstalkJob.getJobId(), new String(beanstalkJob.getData()));
			} else {
				throw new MessagingException("Failed to retrieve message from the " + queue + " tube");
			}
		}
		catch (MessagingException e) {
			throw e;
		}
		catch (Exception e) {
			throw new MessagingException("Failed to retrieve message from the " + queue + " tube", e);
		}
	}
	
	/* 
	 * Pulls multiple messages off a tube. Beanstalk locks the client when you try to read
	 * a message and none are available, so don't use this if you are not sure there are
	 * <pre>count</pre> messages available.
	 */
	@Override
	public List<Message> pop(String exchange, String queue, int count) throws MessagingException
	{
		List<Message> messages = new ArrayList<Message>();
		try
		{
			for (int i=0;i<Math.min(count, getActiveJobCount(exchange, queue));i++) 
			{
				connection.watch(queue);
				connection.useTube(queue);
				Job beanstalkJob = connection.reserve(null);
				if (beanstalkJob == null) {
					break;
				}
				connection.ignore(queue);
				messages.add(new Message(beanstalkJob.getJobId(), new String(beanstalkJob.getData())));
			}
			return messages;
		}
		catch (Exception e)
		{
			throw new MessagingException("Failed to retrieve messages from the " + queue + " tube", e);
		}
	}

	@Override
	public boolean touch(Object messageId, String queue) throws MessagingException
	{
		try 
		{
			connection.useTube(queue);
			return connection.touch((Long)messageId);
		} 
		catch (Exception e)
		{
			throw new MessagingException("Failed to touch message " + messageId + " from the " + queue + " tube", e);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.common.messaging.MessageQueueClient#listen(java.lang.String, java.lang.String, org.iplantc.service.common.messaging.MessageQueueListener)
	 */
	@Override
	public void listen(String exchange, String queue, MessageQueueListener listener) 
	throws MessagingException
	{	
		try
		{
			stop = false;
			
			connection.useTube(queue);
			connection.watch(queue);
			while (!stop && !Thread.currentThread().isInterrupted())
			{
				Job job = null;
				String body = null;
				try 
				{
					job = connection.reserve(null);
					body = new String(job.getData());
					System.out.println(" [x] Received '" + body + "'");
					
					listener.processMessage(body);
					connection.delete(job.getJobId());
				} catch (MessageProcessingException e) {
					log.error(e);
					if (job != null)
						connection.release(job.getJobId(), 65536, 0);
					// message will be returned to the queue
				} catch (Throwable e) {
					throw new MessageProcessingException("Failed to process message " + body, e);
				}
				
				System.out.println(" [x] Done");
			}
		}
		catch (Exception e)
		{
			throw new MessagingException("Listener to event queue " + queue + " failed.", e);
		}
		finally
		{
			try { connection.ignore(queue);} catch (Throwable e) {}
			try { connection.close();} catch (Throwable e) {}
		}
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.common.messaging.MessageQueueClient#stop()
	 */
	@Override
	public void stop()
	{
		stop = true;
		try {
			connection.close();} catch (Throwable ignored) {}
		connection = null;
	}
	
	private int getActiveJobCount(String exchange, String queue)
	throws MessagingException
	{	
		try {
			connection.watch(queue);
			connection.useTube(queue);
			
			Map<String, String> map = connection.statsTube(queue);
			
			return NumberUtils.toInt(map.get("current-jobs-ready"));
		} 
		catch (Exception e) {
			throw new MessagingException("Failed to retrieve active job count", e);
		}
	}

	@Override
	public void delete(String exchange, String queue, Object messageId) throws MessagingException
	{
		try {
			connection.watch(queue);
			connection.useTube(queue);
			
			//client.release((Long)messageId, 65536, 0);
			connection.delete((Long)messageId);
		} 
		catch (Exception e) {
			throw new MessagingException("Failed to delete message from the tube", e);
		}
		
	}

    @Override
    public List<Object> listQueues() throws MessagingException, NotImplementedException {
        try
        {
            return Arrays.asList(connection.listTubes().toArray());
        }
        catch (Exception e)
        {
            throw new MessagingException("Failed to retrieve info about available queue", e);
        }
    }
    
    /* (non-Javadoc)
     * @see org.iplantc.service.common.messaging.MessageQueueClient#listQueueNames()
     */
    @Override
    public List<String> listQueueNames() throws MessagingException, NotImplementedException {
        try
        {
            return connection.listTubes();
        }
        catch (Exception e)
        {
            throw new MessagingException("Failed to retrieve info about available queue", e);
        }
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.common.messaging.MessageQueueClient#queueExist(java.lang.String)
     */
    @Override
    public boolean queueExist(String queueName) throws MessagingException {
        return listQueueNames().contains(queueName);
    }
    
    /* (non-Javadoc)
     * @see org.iplantc.service.common.messaging.MessageQueueClient#findQueueMatching(java.lang.String)
     */
    @Override
    public String findQueueMatching(String regex) throws MessagingException {
        for(String q: listQueueNames()) {
            if (q.matches(regex)) return q;
        }
        
        return null;
    }
    
    @Override
    public boolean messageExist(String queue, Object messageId) throws MessagingException {
        try {
            connection.watch(queue);
            connection.useTube(queue);
            
            Map<String, String> stats = connection.statsJob((Long)messageId);
            
            return (stats != null && !stats.isEmpty());
        } 
        catch (Exception e) {
            throw new MessagingException("Failed to reject message and remove from the tube", e);
        }
    }	
}
