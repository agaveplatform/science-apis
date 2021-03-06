package org.iplantc.service.jobs.managers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.jobs.dao.AbstractDaoTest;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.dao.JobEventDao;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.JobEvent;
import org.iplantc.service.jobs.model.enumerations.JobEventType;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.model.Notification;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test(enabled=false, groups={"broken", "integration"})
public class JobManagerIT extends AbstractDaoTest
{
	private final boolean pass = false;
	private final boolean fail = true;
	private final ObjectMapper mapper = new ObjectMapper();
	
	private static final Answer<Boolean> ANSWER_TRUE = new Answer<Boolean>() {
		 public Boolean answer(InvocationOnMock invocation) throws Throwable {
	         return true;
	     }
	};
	
	@BeforeClass
	public void beforeClass() throws Exception {
		super.beforeClass();
		drainQueue();
	}

	@AfterClass
	public void afterClass() throws Exception {
		super.afterClass();
		drainQueue();
	}
	
	/**
	 * Creates a test array of jobs job with every {@link JobStatusType}.
	 * 
	 * @return an array of object arrays with the following structure: 
	 * <ul>
	 * 	<li>Job job: a visible job with {@link JobStatusType}</li>
	 * 	<li>boolean shouldThrowException: {@code false} this should never throw an exception</li> 
	 * 	<li>String message: a meaningful message of why the test should not have failed.</li>
	 * </ul> 
	 * @throws Exception
	 */
	@DataProvider
	protected Object[][] hideVisibleJobProvider() throws Exception {
		List<Object[]> testCases = new ArrayList<Object[]>();
	
		for (JobStatusType status: JobStatusType.values()) {
//			if (JobStatusType.isRunning(status)) {
				testCases.add( new Object[]{ status,
										false, 
										"Hiding " + status.name() + " job should not throw exception"});
		}
	
		return testCases.toArray(new Object[][]{});
	}

	@Test(dataProvider="hideVisibleJobProvider", groups="jobManagement", enabled=true)
	public void hideVisibleJob(JobStatusType status, boolean shouldThrowException, String message)
	throws Exception 
	{

		try {
			Software software = createSoftware();
			Job job = createJob(status, software);

			int eventCount = job.getEvents().size();
			
			boolean hasRunningStatus = job.isRunning();
			
			JobManager.hide(job.getId(), job.getOwner());
			
			Job hiddenJob = JobDao.getById(job.getId());
			
			Assert.assertNotNull(hiddenJob, "Hidden job should return when queried by ID");
			
			Assert.assertFalse(hiddenJob.isVisible(), "Hidden job should not be visible.");
			
			int expectedEventCount = eventCount + (hasRunningStatus ? 2 : 1);
			
			Assert.assertEquals(hiddenJob.getEvents().size(), expectedEventCount, 
					"Unexpected number of events present after hiding the job.");
			
			JobEvent restoredJobEvent = hiddenJob.getEvents().get(hiddenJob.getEvents().size()-1);
			
			Assert.assertTrue(restoredJobEvent.getStatus().equalsIgnoreCase(JobEventType.DELETED.name()), 
					"DELETED event was not written to the job event history after being restored.");
			
			if (hasRunningStatus) {
				JobEvent stoppedJobEvent = hiddenJob.getEvents().get(hiddenJob.getEvents().size()-2);
				
				Assert.assertTrue(stoppedJobEvent.getStatus().equalsIgnoreCase(JobEventType.STOPPED.name()), 
						"STOPPED event was not written to the job event history prior to being restored.");
			}
		}
		catch (Exception e) {
			Assert.fail(message);
		}
	}

	/**
	 * Creates a test array of jobs job with every {@link JobStatusType}.
	 *
	 * @return an array of object arrays with the following structure:
	 * <ul>
	 * 	<li>Job job: a hidden job with {@link JobStatusType}</li>
	 * 	<li>boolean shouldThrowException: {@code false} this should never throw an exception</li>
	 * 	<li>String message: a meaningful message of why the test should not have failed.</li>
	 * </ul>
	 */
	@DataProvider
	protected Object[][] restoreHiddenJobProvider() {
		List<Object[]> testCases = new ArrayList<Object[]>();

		for (JobStatusType status: JobStatusType.values()) {
			testCases.add( new Object[]{
					status,
					false,
					"Restoring " + status.name() + " job should not throw exception"});
		}

		return testCases.toArray(new Object[][]{});
	}

	@Test(dataProvider="restoreHiddenJobProvider", groups="jobManagement", enabled=false)
	protected void hideHiddenJob(JobStatusType status, boolean shouldThrowException, String message)
	{
		try {

			Software software = createSoftware();
			Job job = createJob(status, software);
			job.setVisible(false);
			JobDao.persist(job);

			int eventCount = job.getEvents().size();
			
			JobManager.hide(job.getId(), job.getOwner());
			
			Job hiddenJob = JobDao.getById(job.getId());
			
			Assert.assertNotNull(hiddenJob, "Hidden job should return when queried by ID");
			
			Assert.assertFalse(hiddenJob.isVisible(), "Hidden job should not be visible.");
			
			int expectedEventCount = eventCount + 1;
			
			Assert.assertEquals(hiddenJob.getEvents().size(), expectedEventCount, 
					"Unexpected number of events present after hiding the job.");
			
			JobEvent restoredJobEvent = hiddenJob.getEvents().get(hiddenJob.getEvents().size()-1);
			
			Assert.assertTrue(restoredJobEvent.getStatus().equalsIgnoreCase(JobEventType.RESTORED.name()), 
					"RESTORED event was not written to the job event history after being restored.");
			
//			if (hasRunningStatus) {
//				JobEvent stoppedJobEvent = hiddenJob.getEvents().get(hiddenJob.getEvents().size()-2);
//				
//				Assert.assertTrue(stoppedJobEvent.getStatus().equalsIgnoreCase(JobEventType.STOPPED.name()), 
//						"STOPPED event was not written to the job event history prior to being restored.");
//			}
		}
		catch (Exception e) {
			Assert.fail(message);
		}
	}

	@Test(dataProvider="restoreHiddenJobProvider", groups="jobManagement")
	public void restoreHiddenJob(JobStatusType status, boolean shouldThrowException, String message) {
		try {

			Software software = createSoftware();
			Job job = createJob(status, software);
			job.setVisible(false);
			JobDao.persist(job);

			int eventCount = job.getEvents().size();
			
			JobManager.restore(job.getId(), job.getOwner());
			
			Job restoredJob = JobDao.getById(job.getId());
			
			Assert.assertNotNull(restoredJob, "Restored job should return when queried by ID");
			
			Assert.assertTrue(restoredJob.isVisible(), "Restored job should not be visible.");
			
			Assert.assertEquals(restoredJob.getEvents().size(), eventCount+1, 
					"Unexpected number of events present after restoring the job.");
			
			JobEvent restoredJobEvent = restoredJob.getEvents().get(restoredJob.getEvents().size()-1);
			
			Assert.assertTrue(restoredJobEvent.getStatus().equalsIgnoreCase(JobEventType.RESTORED.name()), 
					"RESTORED event was not written to the job event history after being restored.");
		}
		catch (Exception e) {
			Assert.fail(message);
		}
	}
	
	@Test(enabled=false)
	public void kill(Job job, boolean shouldThrowException, String message) {
		throw new RuntimeException("Test not implemented");
	}


	@DataProvider
	protected Object[][] updateStatusJobStatusTypeJobStatusTypeStringProvider()
	{
		List<Object[]> testData = new ArrayList<Object[]>();
		String customStatusMessage = "This is a different new status message, so the same status should update";
		for (JobStatusType currentStatus: JobStatusType.values())
		{
			for (JobStatusType newStatus: JobStatusType.values()) {
				testData.add(new Object[]{ currentStatus, newStatus, newStatus.getDescription(), null, false,
						String.format("Status update from %s to %s same message should not throw an exception", currentStatus.name(), newStatus.name()) } );
				testData.add(new Object[]{ currentStatus, newStatus, newStatus.getDescription(), newStatus.name(), false,
						String.format("Status update from %s to %s same message should not throw an exception", currentStatus.name(), newStatus.name()) } );
				testData.add(new Object[]{ currentStatus, newStatus, newStatus.getDescription(), "NOTAREALEVENT", false,
						String.format("Status update from %s to %s same message should not throw an exception", currentStatus.name(), newStatus.name()) } );
				testData.add(new Object[]{ currentStatus, newStatus, newStatus.getDescription(), "*", false,
						String.format("Status update from %s to %s same message should not throw an exception", currentStatus.name(), newStatus.name()) } );

				if (currentStatus.equals(newStatus))
				{
					testData.add(new Object[]{ currentStatus, newStatus, customStatusMessage, null, false,
							String.format("Status update from %s to %s different message should not throw an exception", currentStatus.name(), newStatus.name()) } );
					testData.add(new Object[]{ currentStatus, newStatus, customStatusMessage, newStatus.name(), false,
							String.format("Status update from %s to %s different message should not throw an exception", currentStatus.name(), newStatus.name()) } );
					testData.add(new Object[]{ currentStatus, newStatus, customStatusMessage, "NOTAREALEVENT", false,
							String.format("Status update from %s to %s different message should not throw an exception", currentStatus.name(), newStatus.name()) } );
					testData.add(new Object[]{ currentStatus, newStatus, customStatusMessage, "*", false,
							String.format("Status update from %s to %s different message should not throw an exception", currentStatus.name(), newStatus.name()) } );

				}
			}
		}
		return testData.toArray(new Object[][]{});
	}

	/**
	 * Tests that the job status is updated (if not redundant), and a 
	 * notification is sent when the job has one.
	 * 
	 * @param originalJobStatus the status of the job prior to updating
	 * @param newJobStatus the expected status of the job after updating
	 * @param message
	 */
	@Test(dataProvider="updateStatusJobStatusTypeJobStatusTypeStringProvider", enabled=false)
	public void updateStatusJobJobStatusType(JobStatusType originalJobStatus, JobStatusType newJobStatus, String notificatonEvent, String message)
	{
		Job job = null;
		try 
		{
			NotificationDao notificationDao = new NotificationDao();

			Software software = createSoftware();
			job = createJob(originalJobStatus, software);
//			JobDao.persist(job);
			
			Notification notification = null;
			if (!StringUtils.isEmpty(notificatonEvent)) {
				notification = new Notification(job.getUuid(), job.getOwner(), notificatonEvent, "http://example.com", false);
				notificationDao.persist(notification);
			}
			
			JobManager.updateStatus(job, newJobStatus);
			
			// verify status update
			Assert.assertEquals(job.getStatus(), newJobStatus,
					"Job status did not update after status update.");
			Assert.assertEquals(job.getErrorMessage(), newJobStatus.getDescription(),
					"Job description did not update after status update.");

			// verify event creation
			List<JobEvent> events = JobEventDao.getByJobIdAndStatus(job.getId(), job.getStatus());
			Assert.assertEquals(events.size(), 1,
					"Wrong number of events found. Expected " + 
					1 + ", found " + events.size());
			
			JobEvent event = events.get(0);
			Assert.assertEquals(
					event.getDescription(),
					newJobStatus.getDescription(),
					"Wrong event description found. Expected '"
							+ newJobStatus.getDescription() + "', found '"
							+ event.getDescription() + "'");

			
			if (originalJobStatus != newJobStatus &&
					(StringUtils.equals(notificatonEvent, newJobStatus.name()) ||
							StringUtils.equals(notificatonEvent, "*")))
			{
				int messageCount = getMessageCount(Settings.NOTIFICATION_QUEUE);
				Assert.assertEquals(messageCount, 1, "Wrong number of messages found");
				
				// verify notification message was sent
				MessageQueueClient client = MessageClientFactory.getMessageClient();
				Message queuedMessage = null;
				try {
					queuedMessage = client.pop(Settings.NOTIFICATION_TOPIC,
							Settings.NOTIFICATION_QUEUE);
				} catch (Throwable e) { 
					Assert.fail("Failed to remove message from the queue. Further tests will fail.", e);
				}
				finally {
					client.delete(Settings.NOTIFICATION_TOPIC,
							Settings.NOTIFICATION_QUEUE, queuedMessage.getId());
				}
				Assert.assertNotNull(queuedMessage,
						"Null message found on the queue");
				JsonNode json = mapper.readTree(queuedMessage.getMessage());
				Assert.assertEquals(notification.getUuid(), json.get("uuid").textValue(),
						"Notification message has wrong uuid");
				Assert.assertEquals(job.getStatus().name(), json.get("event").textValue(),
						"Notification message has wrong event");
			}
			else
			{
				// check for messages in the queue?
				Assert.assertEquals(getMessageCount(Settings.NOTIFICATION_QUEUE), 0, 
						"Messages found in the queue when none should be there.");
			}
		} catch (Exception e) {
			Assert.fail(message, e);
		}
	}
	


	/**
	 * Tests that the job status is updated when a new status or message value is given. Also verifies a message
	 * is added to the queue if a notification for the job status is set. 
	 * @param originalJobStatus
	 * @param newJobStatus
	 * @param notificationMessage
	 * @param notificatonEventName
	 * @param shouldThrowException
	 * @param message
	 */
	@Test(dataProvider="updateStatusJobStatusTypeJobStatusTypeStringProvider", enabled=false)
	public void updateStatusJobJobStatusTypeString(JobStatusType originalJobStatus, JobStatusType newJobStatus, String notificationMessage, String notificatonEventName, boolean shouldThrowException, String message)
	{
		Job job = null;
		try 
		{
			NotificationDao notificationDao = new NotificationDao();

			Software software = createSoftware();
			job = createJob(originalJobStatus, software);
			
			Notification notification = null;
			if (!StringUtils.isEmpty(notificatonEventName)) {
				notification = new Notification(job.getUuid(), job.getOwner(), notificatonEventName, "http://example.com", false);
				notificationDao.persist(notification);
			}
			
			JobManager.updateStatus(job, newJobStatus, notificationMessage);

			// verify status update
			Assert.assertEquals(job.getStatus(), newJobStatus,
					"Job status did not update after status update.");
			Assert.assertEquals(job.getErrorMessage(), notificationMessage,
					"Job description did not update after status update.");

			// verify event creation
			List<JobEvent> events = JobEventDao.getByJobIdAndStatus(job.getId(), job.getStatus());
			int expectedEvents = (originalJobStatus.equals(newJobStatus) && !originalJobStatus.getDescription().equals(notificationMessage)) ? 2 : 1;
			Assert.assertEquals(expectedEvents, events.size(),
					"Wrong number of events found. Expected " + expectedEvents + ", found " + events.size());

			// this test will fail if the events do not come back ordered by created asc 
			JobEvent event = events.get(events.size() -1);
			Assert.assertEquals(event.getDescription(), notificationMessage,
					"Wrong event description found. Expected '" + notificationMessage
							+ "', found '" + event.getDescription() + "'");

			if (!(originalJobStatus.equals(newJobStatus) && originalJobStatus.getDescription().equals(notificationMessage)) &&
					(StringUtils.equals(notificatonEventName, newJobStatus.name()) || StringUtils.equals(notificatonEventName, "*")))
			{
				// verify notification message was sent
				MessageQueueClient client = MessageClientFactory.getMessageClient();
				Message queuedMessage = null;
				try {
					queuedMessage = client.pop(Settings.NOTIFICATION_TOPIC,
							Settings.NOTIFICATION_QUEUE);
				} catch (Throwable e) { 
					Assert.fail("Failed to remove message from the queue. Further tests will fail.", e);
				}
				finally {
					client.delete(Settings.NOTIFICATION_TOPIC,
							Settings.NOTIFICATION_QUEUE, queuedMessage.getId());
				}
				Assert.assertNotNull(queuedMessage,
						"Null message found on the queue");
				JsonNode json = mapper.readTree(queuedMessage.getMessage());
				Assert.assertEquals(notification.getUuid(), json.get("uuid").textValue(),
						"Notification message has wrong uuid");
				Assert.assertEquals(job.getStatus().name(), json.get("event").textValue(),
						"Notification message has wrong event");
			}
			else
			{
				// check for messages in the queue?
				Assert.assertEquals(getMessageCount(Settings.NOTIFICATION_QUEUE), 0, 
						"Messages found in the queue when none should be there.");
			}
		} catch (Exception e) {
			if (!shouldThrowException) {
				Assert.fail(message, e);
			}
		}
		finally {
			try { clearJobs(); } catch (Exception ignored) {}
		}
	}

	/*********************************************************************************
	 * 								NOT YET IMPLEMENTED
	 *********************************************************************************/
	
	
	@Test(enabled=false)
	public void checkStatus(Job job, boolean shouldThrowException, String message) {
		throw new RuntimeException("Test not implemented");
	}

	@Test(enabled=false)
	public void archive() {
		throw new RuntimeException("Test not implemented");
	}
	


}
