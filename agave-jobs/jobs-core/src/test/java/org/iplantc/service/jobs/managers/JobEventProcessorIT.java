package org.iplantc.service.jobs.managers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.jobs.dao.AbstractDaoTest;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.JobEvent;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

@Test(groups={"integration"})
public class JobEventProcessorIT extends AbstractDaoTest {

	protected NotificationDao ndao = new NotificationDao();

//	@Override
//	@BeforeClass
//	public void beforeClass() throws Exception {
//		super.beforeClass();
//		SoftwareDao.persist(software);
//	}
//
//	@AfterClass
//	public void afterClass() throws Exception {
//		clearJobs();
//		clearSoftware();
//		clearSystems();
//		clearNotifications();
//	}
//
//	@BeforeMethod
//	public void beforeMethod(Method m) throws Exception {
//		clearJobs();
//		clearNotifications();
//	}
//
//	@AfterClass
//	public void clearNotifications() throws NotificationException {
//		Session session = null;
//		try {
//			HibernateUtil.beginTransaction();
//			session = HibernateUtil.getSession();
//			session.clear();
//			HibernateUtil.disableAllFilters();
//			session.createQuery("delete Notification").executeUpdate();
//			session.flush();
//		} catch (HibernateException ex) {
//			throw new SoftwareException(ex);
//		} finally {
//			try {
//				HibernateUtil.commitTransaction();
//			} catch (Throwable e) {
//			}
//		}
//
//	}

	protected Notification createNotification(Job job)
			throws NotificationException, IOException {

		Notification notification = new Notification(job.getUuid(),
				job.getOwner(), job.getStatus().name(),
				"http://httpbin:8000/post", true);

		ndao.persist(notification);

		return notification;
	}

//	// @Test
//	public void process() {
//		throw new RuntimeException("Test not implemented");
//	}

	@DataProvider
	protected Object[][] processJobExecutionSystemEventProvider() {
		return new Object[][] { { JobStatusType.PENDING, "JOB_CREATED" },
				{ JobStatusType.QUEUED, "JOB_QUEUED" },
				{ JobStatusType.RUNNING, "JOB_RUNNING" },
				{ JobStatusType.STOPPED, "JOB_STOPPED" },
				{ JobStatusType.FAILED, "JOB_FAILED" },
				{ JobStatusType.PAUSED, "JOB_PAUSED" },
				{ JobStatusType.FINISHED, "JOB_FINISHED" }, };
	}

	@Test(dataProvider = "processJobExecutionSystemEventProvider")
	public void processJobExecutionSystemEventCalled(JobStatusType status,
	String expectedEvent) throws Exception {

		ObjectMapper mapper = new ObjectMapper();
		
		Job job = mock(Job.class);
		when(job.getUuid()).thenReturn(new AgaveUUID(UUIDType.JOB).toString());
		when(job.getStatus()).thenReturn(status);
		when(job.toJSON()).thenReturn(mapper.createObjectNode().toString());
		when(job.getSystem()).thenReturn("testSystem");
		when(job.getSoftwareName()).thenReturn("testSoftware");

		JsonNode jsonJob = mapper.readTree(job.toJSON());

		JobEvent event = new JobEvent(job, status, status.getDescription(),
				job.getOwner());

		JobEventProcessor mockEventProcessor = Mockito.mock(JobEventProcessor.class);
		when(mockEventProcessor.getEvent()).thenReturn(event);
		doCallRealMethod().when(mockEventProcessor).process();

		mockEventProcessor.process();

		verify(mockEventProcessor).processJobExecutionSystemEvent(
				expectedEvent, jsonJob);

	}
	
	@DataProvider
	protected Object[][] processJobSoftwareEventProvider() {
		return new Object[][] { 
				{ JobStatusType.PENDING, "JOB_CREATED", true},
				{ JobStatusType.QUEUED, "JOB_QUEUED", false},
				{ JobStatusType.RUNNING, "JOB_RUNNING", false },
				{ JobStatusType.STOPPED, "JOB_STOPPED", false },
				{ JobStatusType.FAILED, "JOB_FAILED", false },
				{ JobStatusType.PAUSED, "JOB_PAUSED", false },
				{ JobStatusType.FINISHED, "JOB_FINISHED", false }, };
	}
	
	@Test(dataProvider = "processJobSoftwareEventProvider")
	public void processJobSoftwareEventCalled(JobStatusType status, String expectedEvent, boolean shouldCallSoftwareEvent) 
	throws Exception {

		ObjectMapper mapper = new ObjectMapper();
		
		Job job = mock(Job.class);
		when(job.getUuid()).thenReturn(new AgaveUUID(UUIDType.JOB).toString());
		when(job.getStatus()).thenReturn(status);
		when(job.toJSON()).thenReturn(mapper.createObjectNode().toString());
		when(job.getSystem()).thenReturn("testSystem");
		when(job.getSoftwareName()).thenReturn("testSoftware");

		JsonNode jsonJob = mapper.readTree(job.toJSON());

		JobEvent event = new JobEvent(job, status, status.getDescription(),
				job.getOwner());
		
		JobEventProcessor mockEventProcessor = Mockito.mock(JobEventProcessor.class);
		when(mockEventProcessor.getEvent()).thenReturn(event);
		doCallRealMethod().when(mockEventProcessor).process();
		
		mockEventProcessor.process();
		
		if (shouldCallSoftwareEvent) { 
			verify(mockEventProcessor).processJobSoftwareEvent(
					expectedEvent, jsonJob);
		} else {
			verify(mockEventProcessor, never()).processJobSoftwareEvent(
					expectedEvent, jsonJob);
		}
	}

//	@Test
//	public void processJobSoftwareEvent() {
//		throw new RuntimeException("Test not implemented");
//	}
}
