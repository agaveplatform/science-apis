package org.iplantc.service.monitor.dao;

import com.jcabi.immutable.Array;
import org.apache.commons.lang.ArrayUtils;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.monitor.AbstractMonitorIT;
import org.iplantc.service.monitor.exceptions.MonitorException;
import org.iplantc.service.monitor.model.Monitor;
import org.iplantc.service.monitor.model.MonitorCheck;
import org.iplantc.service.monitor.model.enumeration.MonitorCheckType;
import org.iplantc.service.monitor.model.enumeration.MonitorStatusType;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Test(singleThreaded=true, groups={"integration"})
public class MonitorCheckDaoIT extends AbstractMonitorIT {

	@BeforeClass
	public void beforeClass() throws Exception
	{
		super.beforeClass();
	}

	@AfterClass
	public void afterClass() throws MonitorException {
		super.afterClass();
	}

	@AfterMethod
	public void afterMethod() throws Exception {
		clearNotifications();
		clearMonitors();
		clearSystems();
	}

	@BeforeMethod
	public void beforeMethod() throws Exception {
		initSystems();
	}
	
	@Test
	public void persist() {
		MonitorCheckDao checkDao = new MonitorCheckDao();
		Monitor monitor = null;
		MonitorCheck check = null;
		try {
			monitor = createAndSavePendingStorageMonitor();
			check = new MonitorCheck(monitor, MonitorStatusType.FAILED, null, MonitorCheckType.STORAGE);
			
			checkDao.persist(check);
			Assert.assertNotNull(check.getId(), "Failed to save monitor check");
		}
		catch (Exception e) {
			Assert.fail("No exception should be thrown from a valid check persistence.", e);
		}
	}
	
	@Test(dependsOnMethods = { "persist" })
	public void findByUuid() {
		MonitorCheckDao checkDao = new MonitorCheckDao();
		Monitor monitor = null;
		MonitorCheck check = null;
		try {
			monitor = createAndSavePendingStorageMonitor();
			check = new MonitorCheck(monitor, MonitorStatusType.FAILED, null, MonitorCheckType.STORAGE);
			checkDao.persist(check);
			Assert.assertNotNull(check.getId(), "Failed to save monitor check");
			
			MonitorCheck queryCheck = checkDao.findByUuid(check.getUuid());
			
			Assert.assertNotNull(queryCheck, "findByUuid should return monitor check when the valid uuid is given.");
			Assert.assertEquals(queryCheck.getUuid(), check.getUuid(), "UUID of reutrned monitor check did not match the queried uuid.");
		}
		catch (Exception e) {
			Assert.fail("No exception should be thrown from a valid check persistence.", e);
		}
	}

	@Test(dependsOnMethods = { "findByUuid" })
	public void delete() {
		MonitorCheckDao checkDao = new MonitorCheckDao();
		Monitor monitor = null;
		MonitorCheck check = null;
		try {
			monitor = createAndSavePendingStorageMonitor();
			check = new MonitorCheck(monitor, MonitorStatusType.FAILED, null, MonitorCheckType.STORAGE);
			checkDao.persist(check);
			Assert.assertNotNull(check.getId(), "Failed to save monitor check");
			
			checkDao.delete(check);
			
			MonitorCheck queryCheck = checkDao.findByUuid(check.getUuid());
			Assert.assertNull(queryCheck, "Monitor check was not deleted by dao.");
		}
		catch (Exception e) {
			Assert.fail("No exception shoudl be thrown from a valid check persistence.", e);
		}
	}

	@Test(dependsOnMethods = { "delete" })
	public void getAll() {
		MonitorCheckDao checkDao = new MonitorCheckDao();
		Monitor monitor1 = null;
		Monitor monitor2 = null;
		try {
			monitor1 = createAndSavePendingStorageMonitor();
			monitor2 = createAndSavePendingExecutionMonitor();
			int checkCount = 10;
			for (int i=0; i<checkCount; i++) {
				MonitorCheck check1 = new MonitorCheck(monitor1, i % 2 == 0 ? MonitorStatusType.FAILED : MonitorStatusType.PASSED, UUID.randomUUID().toString(), MonitorCheckType.STORAGE);
				checkDao.persist(check1);
				Assert.assertNotNull(check1.getId(), "Failed to save monitor check");
				
				MonitorCheck check2 = new MonitorCheck(monitor2, i % 2 == 0 ? MonitorStatusType.PASSED : MonitorStatusType.FAILED, UUID.randomUUID().toString(), MonitorCheckType.STORAGE);
				checkDao.persist(check2);
				Assert.assertNotNull(check2.getId(), "Failed to save monitor check");
			}
			
			List<MonitorCheck> checks = checkDao.getAll();
			Assert.assertEquals(checks.size(), checkCount * 2, "Invalid number of check returned by getAll.");
		}
		catch (Exception e) {
			Assert.fail("No exception shoudl be thrown from a valid check persistence.", e);
		}
	}

	@Test(dependsOnMethods = { "getAll" })
	public void getAllChecksByMonitorId() {
		MonitorCheckDao checkDao = new MonitorCheckDao();
		Monitor monitor1 = null;
		Monitor monitor2 = null;
		try {
			monitor1 = createAndSavePendingStorageMonitor();
			monitor2 = createAndSavePendingExecutionMonitor();
			int checkCount = 10;
			for (int i=0; i<checkCount; i++) {
				MonitorCheck check1 = new MonitorCheck(monitor1, i % 2 == 0 ? MonitorStatusType.FAILED : MonitorStatusType.PASSED, UUID.randomUUID().toString(), MonitorCheckType.STORAGE);
				checkDao.persist(check1);
				Assert.assertNotNull(check1.getId(), "Failed to save monitor check");
				
				MonitorCheck check2 = new MonitorCheck(monitor2, i % 2 == 0 ? MonitorStatusType.PASSED : MonitorStatusType.FAILED, UUID.randomUUID().toString(), MonitorCheckType.STORAGE);
				checkDao.persist(check2);
				Assert.assertNotNull(check2.getId(), "Failed to save monitor check");
			}
			
			List<MonitorCheck> check1s = checkDao.getAllChecksByMonitorId(monitor1.getId());
			Assert.assertEquals(check1s.size(), checkCount, "Invalid number of checks for first monitor returned by getAll.");
		
			for (MonitorCheck check: check1s) {
				Assert.assertEquals(check.getMonitor().getUuid(), monitor1.getUuid(), "Check from incorrect monitor returned from getAllChecksByMonitorId");
			}
			
			List<MonitorCheck> check2s = checkDao.getAllChecksByMonitorId(monitor2.getId());
			Assert.assertEquals(check2s.size(), checkCount, "Invalid number of checks for second monitor returned by getAll.");
		
			for (MonitorCheck check: check2s) {
				Assert.assertEquals(check.getMonitor().getUuid(), monitor2.getUuid(), "Check from incorrect monitor returned from getAllChecksByMonitorId");
			}
		}
		catch (Exception e) {
			Assert.fail("No exception shoudl be thrown from a valid check persistence.", e);	
		}
	}

	@Test(dependsOnMethods = { "getAllChecksByMonitorId" })
	public void getLastMonitorCheck() {
		MonitorCheckDao checkDao = new MonitorCheckDao();
		Monitor monitor1 = null;
		try {
			monitor1 = createAndSavePendingStorageMonitor();
			int checkCount = 10;
			String lastCheckUuid = null;
			for (int i=0; i<checkCount; i++) {
				MonitorCheck check1 = new MonitorCheck(monitor1, i % 2 == 0 ? MonitorStatusType.FAILED : MonitorStatusType.PASSED, UUID.randomUUID().toString(), MonitorCheckType.STORAGE);
				checkDao.persist(check1);
				Assert.assertNotNull(check1.getId(), "Failed to save monitor check");
				lastCheckUuid = check1.getUuid();
			}
			
			MonitorCheck check = checkDao.getLastMonitorCheck(monitor1.getId());
			Assert.assertEquals(check.getUuid(), lastCheckUuid, "Check returned from getLastMonitorCheck was not the last check uuid added to the database.");
		}
		catch (Exception e) {
			Assert.fail("No exception shoudl be thrown from a valid check persistence.", e);	
		}
	}

	@Test(dependsOnMethods = { "getLastMonitorCheck" })
	public void getPaginatedByIdAndRange() {
		MonitorCheckDao checkDao = new MonitorCheckDao();
		Monitor monitor1 = null;
		Monitor monitor2 = null;
		try {
			monitor1 = createAndSavePendingStorageMonitor();
			monitor2 = createAndSavePendingExecutionMonitor();
			int totalChecks = 10;
			int checkCount = 5;
			List<String> expectedCheckUUIDs = new ArrayList<String>();
			
			for (int i=0; i<totalChecks; i++) {
				MonitorCheck check1 = new MonitorCheck(monitor1, i % 2 == 0 ? MonitorStatusType.FAILED : MonitorStatusType.PASSED, UUID.randomUUID().toString(), MonitorCheckType.STORAGE);
				checkDao.persist(check1);
				Assert.assertNotNull(check1.getId(), "Failed to save monitor check");
				
				if (i < checkCount) {
					expectedCheckUUIDs.add(check1.getUuid());
				}
				
				MonitorCheck check2 = new MonitorCheck(monitor2, i % 2 == 0 ? MonitorStatusType.PASSED : MonitorStatusType.FAILED, UUID.randomUUID().toString(), MonitorCheckType.STORAGE);
				checkDao.persist(check2);
				Assert.assertNotNull(check2.getId(), "Failed to save monitor check");
			}
			
			List<MonitorCheck> paginatedChecks = checkDao.getPaginatedByIdAndRange(monitor1.getId(), null, null, null, null, checkCount, 0);
			Assert.assertEquals(paginatedChecks.size(), expectedCheckUUIDs.size(), "Invalid number of checks for first monitor returned by getAll.");


			for (MonitorCheck check: paginatedChecks) {
				Assert.assertTrue(expectedCheckUUIDs.contains(check.getUuid()),
						"Check returned from getPaginatedByIdAndRange was not in the correct set of checks for the given range.");
			}
			
		}
		catch (Exception e) {
			Assert.fail("No exception shoudl be thrown from a valid check persistence.", e);	
		}
	}
}
