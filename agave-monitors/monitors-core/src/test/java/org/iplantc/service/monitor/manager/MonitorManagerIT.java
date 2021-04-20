package org.iplantc.service.monitor.manager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.hibernate.ObjectNotFoundException;
import org.iplantc.service.common.Settings;
import org.iplantc.service.monitor.AbstractMonitorIT;
import org.iplantc.service.monitor.TestDataHelper;
import org.iplantc.service.monitor.events.DomainEntityEvent;
import org.iplantc.service.monitor.events.DomainEntityEventDao;
import org.iplantc.service.monitor.exceptions.MonitorException;
import org.iplantc.service.monitor.managers.MonitorManager;
import org.iplantc.service.monitor.model.Monitor;
import org.iplantc.service.monitor.model.MonitorCheck;
import org.iplantc.service.monitor.model.enumeration.MonitorCheckType;
import org.iplantc.service.monitor.model.enumeration.MonitorEventType;
import org.iplantc.service.monitor.model.enumeration.MonitorStatusType;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

@Test(groups={"integration"})
public class MonitorManagerIT extends AbstractMonitorIT {

	protected MonitorManager manager = new MonitorManager();
	DomainEntityEventDao eventDao = new DomainEntityEventDao();

	@BeforeMethod
	protected void beforeMethod() throws Exception {
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
	
	@DataProvider(name = "checkProvider")
	protected Object[][] checkProvider() throws Exception
	{	
		return new Object[][] {
			{ createStorageMonitor(), "Valid storage monitor check succeds", true },
			{ createExecutionMonitor(), "Valid execution monitor check succeeds", true }
		};
	}

	@Test(dataProvider = "checkProvider")
	public void check(Monitor monitor, String errorMessage, boolean shouldSucceed)
	{
		try {
			dao.persist(monitor);
			
			MonitorCheck check = manager.check(monitor, monitor.getOwner());
			Assert.assertEquals(shouldSucceed, check.getResult().equals(MonitorStatusType.PASSED), errorMessage);
		}
		catch (Throwable e) {
			Assert.fail("Unexpected error occurred", e);
		}
	}
	
	@Test(dependsOnMethods={"check"})
	public void checkNotificationsOnFailure()
	{
		try {
			JsonNode storageJson = TestDataHelper.getInstance().getTestDataObject(TestDataHelper.TEST_STORAGE_SYSTEM_FILE);
			((ObjectNode)storageJson.get("storage")).put("host", "missing.ssh.example.com");
			((ObjectNode)storageJson).put("id", "missing.ssh.example.com");
			
			StorageSystem system = StorageSystem.fromJSON(new JSONObject(storageJson.toString()));
			system.setOwner(TEST_USER);
			systemDao.persist(system);
			
			Monitor monitor = createStorageMonitor();
			monitor.setSystem(system);
			dao.persist(monitor);
			Assert.assertNotNull(monitor.getId(), "Failed to save monitor");
			
			NotificationDao notificationDao = new NotificationDao();
			Notification n = new Notification(monitor.getUuid(), monitor.getOwner(), "STATUS_CHANGE", System.getProperty("user.name") + "@example.com", false);
			notificationDao.persist(n);
			
			MonitorCheck check = manager.check(monitor, monitor.getOwner());
			Assert.assertEquals(check.getResult(), MonitorStatusType.FAILED, 
					"Storage check on " + system.getStorageConfig().getHost() + " should fail.");
			
			monitor = dao.findByUuid(monitor.getUuid());
			assertNull(monitor.getLastSuccess(),
					"The test monitor's lastSuccess value should not be updated on a failed test.");
			
			// notification message for the event should have been placed in queue
			Assert.assertEquals(getMessageQueueSize(Settings.NOTIFICATION_QUEUE),  1, 
					"Invalid number of messages found on the notification queue after a failed monitor test.");
		}
		catch (MonitorException e) {
			Assert.fail("Failed to process monitor check", e);
		}
		catch (Exception e) {
			Assert.fail("Unexpected error occurred", e);
		}
	}
	
	@Test(dependsOnMethods={"checkNotificationsOnFailure"})
	public void checkNotificationsOnSuccess()
	{
		try 
		{
			Monitor monitor = createStorageMonitor();

			RemoteSystem system = monitor.getSystem();
			system.setStatus(SystemStatusType.DOWN);
			systemDao.persist(system);
			
			dao.persist(monitor);
			Assert.assertNotNull(monitor.getId(), "Failed to save monitor");
			
			NotificationDao notificationDao = new NotificationDao();
			Notification n = new Notification(monitor.getUuid(), monitor.getOwner(), "STATUS_CHANGE", System.getProperty("user.name") + "@example.com", false);
			notificationDao.persist(n);
		
			MonitorCheck check = manager.check(monitor, monitor.getOwner()); // should succeed
			Assert.assertEquals(check.getResult(), MonitorStatusType.PASSED, 
					"Storage check on " + system.getStorageConfig().getHost() + " should pass.");
			
			monitor = dao.findByUuid(monitor.getUuid());
			Assert.assertTrue(Math.abs(monitor.getLastSuccess().getTime() - check.getCreated().getTime()) <= 1000, 
					"The test monitor's lastSuccess value should be updated to the created date "
					+ "of the last successfull check on success.");
			
			// notification message for the event should have been placed in queue
			Assert.assertEquals(getMessageQueueSize(Settings.NOTIFICATION_QUEUE),  1, 
					"Invalid number of messages found on the notification queue after a successful monitor test.");
		}
		catch (MonitorException e) {
			Assert.fail("Failed to process monitor check", e);
		}
		catch (Exception e) {
			Assert.fail("Unexpected error occurred", e);
		}
	}

	@Test
	public void delete() {
		try {
			Monitor monitor = createStorageMonitor();
			RemoteSystem system = monitor.getSystem();
			system.setStatus(SystemStatusType.DOWN);
			systemDao.persist(system);

			// create monitor
			dao.persist(monitor);
			Assert.assertNotNull(monitor.getId(), "Failed to save monitor");

			// create history events
			for (MonitorEventType eventType: new MonitorEventType[]{MonitorEventType.CREATED, MonitorEventType.ENABLED}) {
				DomainEntityEvent event = new DomainEntityEvent(monitor.getUuid(), MonitorEventType.CREATED,
						eventType.getDescription() + " by " + TEST_USER, TEST_USER);
				eventDao.persist(event);
			}

			// create checks
			for (int i=0; i<5; i++) {
				MonitorStatusType statusType =  i % 2 == 0 ? MonitorStatusType.FAILED : MonitorStatusType.PASSED;
				MonitorCheck check = new MonitorCheck(monitor, statusType, UUID.randomUUID().toString(), MonitorCheckType.STORAGE);
				checkDao.persist(check);

				MonitorEventType eventType = i % 2 == 0 ? MonitorEventType.CHECK_FAILED : MonitorEventType.CHECK_PASSED;
				DomainEntityEvent event = new DomainEntityEvent(monitor.getUuid(), eventType,
						eventType.getDescription() + " by " + TEST_USER, TEST_USER);
				eventDao.persist(event);
			}

			manager.delete(monitor, TEST_USER);

			try {
				assertNull(dao.findByUuid(monitor.getUuid()), "Monitor should not be found after manager deletion");
			} catch(ObjectNotFoundException e) {
				fail("unknown monitor uuid should return null, not throw exception", e);
			}

			List<MonitorCheck> checks = checkDao.getAllChecksByMonitorId(monitor.getId());
			Assert.assertTrue(checks.isEmpty(), "No checks should exist for a monitor after deletion by manager");

			List<DomainEntityEvent> events = eventDao.getEntityEventByEntityUuid(monitor.getUuid());
			Assert.assertTrue(events.isEmpty(), "No events should exist for a monitor after deletion by manager");

		}
		catch (MonitorException e) {
			Assert.fail("Failed to process monitor check", e);
		}
		catch (Exception e) {
			Assert.fail("Unexpected error occurred", e);
		}
	}
}
