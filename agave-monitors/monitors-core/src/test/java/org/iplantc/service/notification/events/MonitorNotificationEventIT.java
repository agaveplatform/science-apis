package org.iplantc.service.notification.events;

import org.iplantc.service.common.clients.RequestBin;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.monitor.AbstractMonitorIT;
import org.iplantc.service.monitor.model.Monitor;
import org.iplantc.service.notification.model.Notification;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups={"broken", "integration"})
public class MonitorNotificationEventIT extends AbstractMonitorIT
{
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
	
	@Test
	public void processWebhookNotificationEvent()
	{
		try
		{
			Monitor monitor = createStorageMonitor();
			dao.persist(monitor);

			RequestBin requestBin = RequestBin.getInstance();

			Notification notification = new Notification(monitor.getUuid(), monitor.getOwner(), "RESULT_CHANGE", requestBin + "?system=${TARGET}&status=${EVENT}", false);
			MonitorNotificationEvent event = new MonitorNotificationEvent(new AgaveUUID(monitor.getUuid()), notification, "RESULT_CHANGE", monitor.getOwner());
			event.setCustomNotificationMessageContextData(monitor.toJSON());
			NotificationMessageProcessor.process(notification, "RESULT_CHANGE", monitor.getOwner(), monitor.getUuid(), monitor.toJSON());

			Assert.assertTrue(notification.isSuccess(), "Notification failed to update to true after sending");

			Assert.assertEquals(requestBin.getRequests().size(), 1, "Requestbin should have 1 request after monitor fires.");
		}
		catch (Exception e) {
			Assert.fail("Test failed unexpectedly");
		}
	}

	@Test
	public void processEmailNotificationEvent()
	{
		try
		{
			Monitor monitor = createStorageMonitor();
			dao.persist(monitor);
			Notification notification = new Notification(monitor.getUuid(), monitor.getOwner(), "RESULT_CHANGE", "dooley@tacc.utexas.edu", false);
			MonitorNotificationEvent event = new MonitorNotificationEvent(new AgaveUUID(monitor.getUuid()), notification, "RESULT_CHANGE", monitor.getOwner());
			event.setCustomNotificationMessageContextData(monitor.toJSON());

			NotificationMessageProcessor.process(notification, "RESULT_CHANGE", monitor.getOwner(), monitor.getUuid(), monitor.toJSON());

			Assert.assertTrue(notification.isSuccess(), "Notification failed to update to true after sending");
		}
		catch (Exception e) {
			Assert.fail("Test failed unexpectedly");
		}
	}
}
