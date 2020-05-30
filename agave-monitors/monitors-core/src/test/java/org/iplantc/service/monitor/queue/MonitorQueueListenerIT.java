package org.iplantc.service.monitor.queue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.monitor.AbstractMonitorIT;
import org.iplantc.service.monitor.Settings;
import org.iplantc.service.monitor.dao.MonitorDao;
import org.iplantc.service.monitor.exceptions.MonitorException;
import org.iplantc.service.monitor.managers.MonitorManager;
import org.iplantc.service.monitor.model.Monitor;
import org.iplantc.service.monitor.model.MonitorCheck;
import org.iplantc.service.monitor.model.enumeration.MonitorStatusType;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Tests the listeners that pull messages off the notification queue and process
 * them.
 * 
 * @author dooley
 *
 */
@Test(groups={"integration"})
public class MonitorQueueListenerIT extends AbstractMonitorIT
{
//	protected MonitorManager manager = new MonitorManager();
	
	@BeforeMethod
	public void beforeMethod() throws Exception {
		clearMonitors();
		clearNotifications();
		clearQueues();
	}
	
	@AfterMethod
	public void afterMethod() throws Exception{
		clearMonitors();
		clearNotifications();
		clearQueues();
	}
	
	@DataProvider(name="processMessageProvider")
	private Object[][] processMessageProvider() throws MonitorException, JSONException, IOException, SystemArgumentException, SystemArgumentException {
		Monitor storageMonitor = createStorageMonitor();
		Monitor executionMonitor = createExecutionMonitor();
	
		return new Object[][] {
			{ storageMonitor, "Valid storage monitor failed", false },
			{ executionMonitor, "Valid execution monitor failed", false },
		};
	}
	
	@Test
	public void processStorageSystemMonitorMessage()
	{
		MonitorQueueListener listener = null;
		MonitorManager manager = new MonitorManager();
		Monitor monitor = null;
		try
		{
			monitor = createStorageMonitor();

			dao.persist(monitor);

			Assert.assertNotNull(monitor.getId(), "Failed to persist monitor prior to test.");

			JsonNode json = new ObjectMapper().createObjectNode()
					.put("uuid", monitor.getUuid())
					.put("target", monitor.getSystem().getSystemId())
					.put("owner", monitor.getOwner());
			
			listener = new MonitorQueueListener();

			listener.processMessage(json.toString());

			MonitorCheck check = checkDao.getLastMonitorCheck(monitor.getId());
			
			Assert.assertNotNull(check, "No check found for storage monitor after processing message.");
			Assert.assertEquals(check.getResult(), MonitorStatusType.PASSED, "Storage monitor check did not pass");
			Assert.assertTrue(monitor.isActive(), "Storage monitor is still active.");
			
			monitor = new MonitorDao().findByUuid(monitor.getUuid());
			
			Assert.assertTrue(monitor.getLastUpdated().after(monitor.getCreated()),
									"Monitor last updated time was not updated.");
		} catch (MonitorException e) {
			Assert.fail("Valid monitor check should not throw MonitorException", e);
		} catch (Exception e) {
			Assert.fail("Unexpected exception thrown", e);
		}
		finally {
			try {
				if (listener != null) {
					listener.stop();
				}
			} catch (Exception ignore) {}
		}
	}

	@Test(dependsOnMethods={"processStorageSystemMonitorMessage"})
	public void processExecutionSystemMonitorMessage()
	{
		MonitorQueueListener listener = null;
		MonitorManager manager = new MonitorManager();
		Monitor monitor = null;
		try
		{
			monitor = createExecutionMonitor();

			dao.persist(monitor);

			Assert.assertNotNull(monitor.getId(), "Failed to persist execution monitor prior to test.");

			JsonNode json = new ObjectMapper().createObjectNode()
					.put("uuid", monitor.getUuid())
					.put("target", monitor.getSystem().getSystemId())
					.put("owner", monitor.getOwner());

			listener = new MonitorQueueListener();

			listener.processMessage(json.toString());

			MonitorCheck check = checkDao.getLastMonitorCheck(monitor.getId());

			Assert.assertNotNull(check, "No check found for execution monitor after processing message.");
			Assert.assertEquals(check.getResult(), MonitorStatusType.PASSED, "Execution monitor check did not pass");
			Assert.assertTrue(monitor.isActive(), "Execution monitor is still active.");

			monitor = new MonitorDao().findByUuid(monitor.getUuid());

			Assert.assertTrue(monitor.getLastUpdated().after(monitor.getCreated()),
					"Execution monitor last updated time was not updated.");
		} catch (MonitorException e) {
			Assert.fail("Valid execution monitor check should not throw MonitorException", e);
		} catch (Exception e) {
			Assert.fail("Unexpected exception thrown", e);
		}
		finally {
			try {
				if (listener != null) {
					listener.stop();
				}
			} catch (Exception ignore) {}
		}
	}

	@Test(dependsOnMethods={"processExecutionSystemMonitorMessage"})
	public void testExecuteReadsMessageFromQueue()
	{
		MonitorQueueListener listener = null;
		MessageQueueClient messageClient = null;
		try
		{
			Monitor monitor = createAndSavePendingStorageMonitor();
			
			Assert.assertNotNull(monitor.getId(), "Failed to persist monitor.");
			
			messageClient = MessageClientFactory.getMessageClient();
			
			JsonNode json = new ObjectMapper().createObjectNode()
					.put("uuid", monitor.getUuid())
					.put("target", monitor.getSystem().getSystemId())
					.put("owner", monitor.getOwner());
					
			messageClient.push(Settings.MONITOR_TOPIC, Settings.MONITOR_QUEUE, json.toString());
			
			listener = new MonitorQueueListener();
			
			listener.execute(null);
			
			MonitorCheck check = checkDao.getLastMonitorCheck(monitor.getId());
			
			Assert.assertNotNull(check, "No check found for monitor");
			Assert.assertEquals(check.getResult(), MonitorStatusType.PASSED, "Monitor check did not pass");
			Assert.assertTrue(check.getMonitor().isActive(), "Monitor is still active.");
			
			Assert.assertNotEquals(check.getMonitor().getLastUpdated().getTime(), 
									check.getMonitor().getCreated().getTime(), 
									"Monitor last updated time was not updated.");

			// this is updated on the cron to avoid it being reset if a forced check occurs
//			Assert.assertTrue(check.getMonitor().getNextUpdateTime().getTime() >= 
//								new DateTime(check.getMonitor().getLastUpdated()).plusMinutes(check.getMonitor().getFrequency()).toDate().getTime(), 
//								"Monitor last sent time was not updated.");
		}
		catch (MonitorException e) 
		{
			Assert.fail("Failed to process monitor message queue.", e);
		}
		catch (Throwable e) {
			Assert.fail("Unexpected exception thrown", e);
		}
		finally {
			try {
				if (messageClient != null) {
					messageClient.stop();
				}
			} catch (Exception ignore) {}

			try {
				if (listener != null) {
					listener.stop();
				}
			} catch (Exception ignore) {}
		}
	}
}