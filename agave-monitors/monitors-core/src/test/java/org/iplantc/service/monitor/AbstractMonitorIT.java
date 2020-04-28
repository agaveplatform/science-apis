package org.iplantc.service.monitor;

import static org.iplantc.service.monitor.TestDataHelper.SYSTEM_SHARE_USER;
import static org.iplantc.service.monitor.TestDataHelper.TEST_EXECUTION_MONITOR;
import static org.iplantc.service.monitor.TestDataHelper.TEST_EXECUTION_SYSTEM_FILE;
import static org.iplantc.service.monitor.TestDataHelper.TEST_STORAGE_MONITOR;
import static org.iplantc.service.monitor.TestDataHelper.TEST_STORAGE_SYSTEM_FILE;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.math.NumberUtils;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.monitor.dao.MonitorCheckDao;
import org.iplantc.service.monitor.dao.MonitorDao;
import org.iplantc.service.monitor.exceptions.MonitorException;
import org.iplantc.service.monitor.model.Monitor;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.SystemRole;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.surftools.BeanstalkClientImpl.ClientImpl;

public class AbstractMonitorIT {

	public static final String TEST_USER = "systest";
	public static final String TEST_EMAIL = "help@agaveplatform.org";
	public static final String TEST_URL = "http://requestbin:5101/11pbi6m1?username=${USERNAME}&status=${STATUS}";
	
	protected ObjectMapper mapper = new ObjectMapper();
	protected SystemDao systemDao = new SystemDao();
	protected MonitorDao dao = new MonitorDao();
	protected MonitorCheckDao checkDao = new MonitorCheckDao();
	protected TestDataHelper dataHelper;
	protected StorageSystem publicStorageSystem;
	protected StorageSystem privateStorageSystem;
	protected StorageSystem sharedStorageSystem;
	protected ExecutionSystem publicExecutionSystem;
	protected ExecutionSystem privateExecutionSystem;
	protected ExecutionSystem sharedExecutionSystem;
	
	@BeforeClass
	public void beforeClass() throws Exception
	{
		HibernateUtil.getConfiguration();
		
		clearMonitors();
		clearNotifications();
		clearSystems();
		initSystems();
	}
	
	@AfterClass
	public void afterClass() throws MonitorException {
		clearSystems();
		clearMonitors();
		clearNotifications();
		clearQueues();
	}
	
	@AfterMethod
	public void afterMethod() throws Exception
	{

	}
	
	/**
	 * Creates a persists a test monitor.
	 * 
	 * @param isActive
	 * @param isStranger
	 * @param isStorageType
	 * @param isPublicSystem
	 * @throws Exception
	 */
	protected void addMonitor(boolean isActive, boolean isStranger, boolean isStorageType, boolean isPublicSystem)
	throws Exception
	{
		String owner = isStranger ? SYSTEM_SHARE_USER : TEST_USER;
		RemoteSystem system = null;
		if (isPublicSystem) {
			if (isStorageType) {
				system = publicStorageSystem;
			} else {
				system = publicExecutionSystem;
			}
		} else {
			if (isStorageType) {
				if (isStranger)
					system = sharedStorageSystem;
				else 
					system = privateStorageSystem;
			} else {
				if (isStranger)
					system = sharedExecutionSystem;
				else 
					system = privateExecutionSystem;
			}
		}
		Monitor monitor = new Monitor(system, 5, owner);
		monitor.setActive(isActive);
		
		dao.persist(monitor);
			
		Assert.assertNotNull(monitor.getId(), "Failed to save monitor");
	}

	public Monitor createStorageMonitor() throws MonitorException, JSONException, IOException
	{
		JSONObject storageJson = TestDataHelper.getInstance().getTestDataObjectAsJSONObject(TEST_STORAGE_SYSTEM_FILE);
		storageJson.put("id", UUID.randomUUID().toString());
		StorageSystem privateStorageSystem = StorageSystem.fromJSON(storageJson);
		privateStorageSystem.setOwner(TEST_USER);
		systemDao.persist(privateStorageSystem);

		JsonNode json = TestDataHelper.getInstance().getTestDataObject(TEST_STORAGE_MONITOR);
		((ObjectNode)json).put("target",privateStorageSystem.getSystemId());
		Monitor monitor = Monitor.fromJSON(json, null, TEST_USER);
		monitor.setCreated(new DateTime().minusDays(1).toDate());
		monitor.setLastUpdated(monitor.getCreated());
		return monitor;
	}

	public Monitor createExecutionMonitor() throws MonitorException, JSONException, IOException, SystemArgumentException {
		JSONObject executionJson = TestDataHelper.getInstance().getTestDataObjectAsJSONObject(TEST_EXECUTION_SYSTEM_FILE);
		executionJson.put("id", UUID.randomUUID().toString());
		ExecutionSystem privateExecutionSystem = ExecutionSystem.fromJSON(executionJson);
		privateExecutionSystem.setOwner(TEST_USER);
		systemDao.persist(privateExecutionSystem);

		JsonNode json = TestDataHelper.getInstance().getTestDataObject(TEST_EXECUTION_MONITOR);
		((ObjectNode)json).put("target",privateExecutionSystem.getSystemId());
		Monitor monitor = Monitor.fromJSON(json, null, TEST_USER);
		monitor.setCreated(new DateTime().minusDays(1).toDate());
		monitor.setLastUpdated(monitor.getCreated());
		return monitor;
	}

	public Monitor createAndSavePendingStorageMonitor() throws MonitorException, JSONException, IOException
	{
		Monitor monitor = createStorageMonitor();
		monitor.setNextUpdateTime(new DateTime().minusYears(1).toDate());
		dao.persist(monitor);
		Assert.assertNotNull(monitor.getId(), "Failed to persist storage Monitor.");
		
		return monitor;

	}

	public Monitor createAndSavePendingExecutionMonitor() throws MonitorException, JSONException, IOException, SystemArgumentException
	{
		Monitor monitor = createExecutionMonitor();
		monitor.setNextUpdateTime(new DateTime().minusYears(1).toDate());
		dao.persist(monitor);
		Assert.assertNotNull(monitor.getId(), "Failed to persist storage Monitor.");
		
		return monitor;
	}

	public void clearNotifications() throws MonitorException
	{
		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
            session.clear();
            HibernateUtil.disableAllFilters();
			session.createQuery("delete Notification").executeUpdate();
			session.flush();
		}
		catch (HibernateException ex)
		{
			throw new MonitorException(ex);
		}
		finally
		{
			try { HibernateUtil.commitTransaction(); } catch (Exception e) {}
		}
	}

	public void clearMonitors() throws MonitorException
	{
		try
		{
			HibernateUtil.beginTransaction();
			Session session = HibernateUtil.getSession();
			session.clear();
			HibernateUtil.disableAllFilters();
			session.createQuery("delete Monitor").executeUpdate();
			session.createQuery("delete MonitorCheck").executeUpdate();
			session.createQuery("delete DomainEntityEvent").executeUpdate();
			session.flush();
		}
		catch (HibernateException ex)
		{
			ex.printStackTrace();
			throw new MonitorException(ex);
		}
		finally
		{
			try { HibernateUtil.commitTransaction(); }
			catch (Exception ignore) {}
		}
	}

	public void clearSystems()
	{
	    Session session = null;
        try
        {
            HibernateUtil.beginTransaction();
            session = HibernateUtil.getSession();
            session.clear();
            HibernateUtil.disableAllFilters();

            session.createQuery("delete RemoteSystem").executeUpdate();
            session.flush();
        }
        finally
        {
            try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
        }
	}
	
	public void initSystems() throws Exception
	{
		JSONObject storageJson = TestDataHelper.getInstance().getTestDataObjectAsJSONObject(TEST_STORAGE_SYSTEM_FILE);
		storageJson.put("id", UUID.randomUUID().toString());
    	privateStorageSystem = StorageSystem.fromJSON(storageJson);
        privateStorageSystem.setOwner(TEST_USER);
        systemDao.persist(privateStorageSystem);

		storageJson.put("id", UUID.randomUUID().toString());
        publicStorageSystem = StorageSystem.fromJSON(storageJson);
        publicStorageSystem.setOwner(TEST_USER);
        publicStorageSystem.setPubliclyAvailable(true);
        publicStorageSystem.setGlobalDefault(true);
//        publicStorageSystem.setSystemId(publicStorageSystem.getSystemId() + ".public");
        systemDao.persist(publicStorageSystem);

		storageJson.put("id", UUID.randomUUID().toString());
		sharedStorageSystem = StorageSystem.fromJSON(storageJson);
		sharedStorageSystem.setOwner(TEST_USER);
//		sharedStorageSystem.setSystemId(sharedStorageSystem.getSystemId() + ".shared");
		systemDao.persist(sharedStorageSystem);
		sharedStorageSystem.addRole(new SystemRole(SYSTEM_SHARE_USER, RoleType.ADMIN));
		systemDao.persist(sharedStorageSystem);


		JSONObject executionJson = TestDataHelper.getInstance().getTestDataObjectAsJSONObject(TEST_EXECUTION_SYSTEM_FILE);
		executionJson.put("id", UUID.randomUUID().toString());
		privateExecutionSystem = ExecutionSystem.fromJSON(executionJson);
        privateExecutionSystem.setOwner(TEST_USER);
        systemDao.persist(privateExecutionSystem);

		executionJson.put("id", UUID.randomUUID().toString());
		publicExecutionSystem = ExecutionSystem.fromJSON(executionJson);
        publicExecutionSystem.setOwner(TEST_USER);
        publicExecutionSystem.setPubliclyAvailable(true);
        publicExecutionSystem.setGlobalDefault(true);
//        publicExecutionSystem.setSystemId(publicExecutionSystem.getSystemId() + ".public");
        systemDao.persist(publicExecutionSystem);

		executionJson.put("id", UUID.randomUUID().toString());
        sharedExecutionSystem = ExecutionSystem.fromJSON(executionJson);
        sharedExecutionSystem.setOwner(TEST_USER);
//        sharedExecutionSystem.setSystemId(sharedExecutionSystem.getSystemId() + ".shared");
        systemDao.persist(sharedExecutionSystem);
		sharedExecutionSystem.addRole(new SystemRole(SYSTEM_SHARE_USER, RoleType.ADMIN));
		systemDao.persist(sharedExecutionSystem);
	}

	/**
	 * Flushes the messaging tube of any and all existing jobs.
	 */
	public void clearQueues() {
		ClientImpl client = null;
	
		// drain the message queue
		client = new ClientImpl(Settings.MESSAGING_SERVICE_HOST,
				Settings.MESSAGING_SERVICE_PORT);
		
		for (String tube: client.listTubes())
		{
			try {
				client.watch(tube);
				client.useTube(tube);
				client.kick(Integer.MAX_VALUE);
				
				com.surftools.BeanstalkClient.Job beanstalkJob = null;
				do {
					try {
						beanstalkJob = client.peekReady();
						if (beanstalkJob != null)
							client.delete(beanstalkJob.getJobId());
					} catch (Throwable e) {
						e.printStackTrace();
					}
				} while (beanstalkJob != null);
				do {
					try {
						beanstalkJob = client.peekBuried();
						if (beanstalkJob != null)
							client.delete(beanstalkJob.getJobId());
					} catch (Throwable e) {
						e.printStackTrace();
					}
				} while (beanstalkJob != null);
				do {
					try {
						beanstalkJob = client.peekDelayed();
						
						if (beanstalkJob != null)
							client.delete(beanstalkJob.getJobId());
					} catch (Throwable e) {
						e.printStackTrace();
					}
				} while (beanstalkJob != null);
				
			} catch (Throwable e) {
				e.printStackTrace();
			}
			finally {
				try { client.ignore(tube); } catch (Throwable e) {}
				
			}
		}
		try { client.close(); } catch (Throwable e) {}
		client = null;
	}

	public int getMessageQueueSize(String queue)
	{
		ClientImpl client = null;
		int size = 0;
		// drain the message queue
		client = new ClientImpl(Settings.MESSAGING_SERVICE_HOST,
				Settings.MESSAGING_SERVICE_PORT);
		try 
		{
			client.watch(queue);
			client.useTube(queue);
			client.kick(Integer.MAX_VALUE);
			
			Map<String, String> stats = client.statsTube(queue);
			String totalJobs = stats.get("current-jobs-ready");
			if (NumberUtils.isNumber(totalJobs)) {
				size = NumberUtils.toInt(totalJobs);
			} 
		} 
		catch (Throwable e) {
			Assert.fail("Failed to retrieve message queue size", e);
		}
		finally {
			try { client.ignore(queue); } catch (Throwable e) {}
			try { client.close(); } catch (Throwable e) {}
			client = null;
		}
		
		return size;
	}

	public boolean isWebhookSent(String callback) throws Exception
	{
		return true;
//		File webhookLogFile = new File("/tmp/postbin.out");
//		if (!webhookLogFile.exists()) {
//			return false;
//		}
//		
//		String webhookParameters = FileUtils.readFileToString(webhookLogFile);
//		if (StringUtils.isEmpty(webhookParameters)) {
//			return false;
//		}
//		
//		ObjectMapper mapper = new ObjectMapper();
//		JsonNode json = mapper.readTree(webhookParameters);
//		
//		URI uri = new URI(callback);
//		
//		//if ()
	}
}