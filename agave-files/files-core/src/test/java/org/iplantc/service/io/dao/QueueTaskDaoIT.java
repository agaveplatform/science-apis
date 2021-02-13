package org.iplantc.service.io.dao;

import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.model.JSONTestDataUtil;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.QueueTask;
import org.iplantc.service.io.model.StagingTask;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.StorageSystem;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.*;

import java.net.URI;
import java.util.UUID;

@Test(groups={"integration"})
public class QueueTaskDaoIT extends BaseTestCase {
	private StorageSystem system;
	private QueueTask task;
	private LogicalFile file;
	private SystemDao systemDao = new SystemDao();
	private String destPath;
	private URI httpUri;

	@BeforeClass
	protected void beforeClass() throws Exception {
		super.beforeClass();
		
		clearSystems();
		clearLogicalFiles();

		destPath = String.format("/home/%s/%s/%s", SYSTEM_OWNER, UUID.randomUUID().toString(), LOCAL_TXT_FILE_NAME);
		httpUri = new URI("http://example.com/foo/bar/baz");


		system = StorageSystem.fromJSON(jtd.getTestDataObject(JSONTestDataUtil.TEST_STORAGE_SYSTEM_FILE));
		system.setOwner(SYSTEM_OWNER);
		system.setPubliclyAvailable(true);
		system.setGlobalDefault(true);
		system.setAvailable(true);
		
		systemDao.persist(system);
	}
	
	@AfterClass
	protected void afterClass() throws Exception {
		clearSystems();
		clearLogicalFiles();
	}
	
	@BeforeMethod
	protected void setUp() throws Exception 
	{
		clearLogicalFiles();
		
		file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
		file.setStatus(StagingTaskStatus.STAGING_QUEUED);
		LogicalFileDao.persist(file);
	}
	
	@AfterMethod
	protected void tearDown() throws Exception {
		clearLogicalFiles();
	}

	@Test
	public void testPersistNull() {
		try {
			QueueTaskDao.persist(null);
			Assert.fail("null task should throw an exception");
		} catch (Exception e) {
			// null task should throw an exception
		}
	}
	
//	@Test
//	public void testPersistStagingJobEventNull() {
//		task = new StagingTask(file);
//		try {
//			QueueTaskDao.persist(task);
//			AssertJUnit.assertNotNull("Null event in staging task should be persisted", ((StagingTask)task).getId());
//		} catch (Exception e) {
//			e.printStackTrace();
//			Assert.fail("Null event should not throw an exception");
//		}
//	}
	
//	@Test
//	public void testPersistTransformJobEventNull() {
//		task = new EncodingTask(file, system, destPath, destPath, "contrast.traits", "traits.pl");
//		try {
//			QueueTaskDao.persist(task);
//			AssertJUnit.assertNotNull("Null event in staging task should be persisted", ((EncodingTask)task).getId());
//		} catch (Exception e) {
//			e.printStackTrace();
//			Assert.fail("Null event should not throw an exception");
//		}
//	}
//	
//	@Test
//	public void testPersistTransformJobEventEmpty() {
//		task = new EncodingTask(file, system, destPath, destPath, "contrast.traits", "traits.pl");
//		try {
//			QueueTaskDao.persist(task);
//			AssertJUnit.assertNotNull("Empty event in staging task should be persisted", ((EncodingTask)task).getId());
//		} catch (Exception e) {
//			e.printStackTrace();
//			Assert.fail("Empty event should not throw an exception");
//		}
//	}
	
	
	@Test
	public void testRemoveNull() {
		try {
			QueueTaskDao.remove(null);
			Assert.fail("null task should throw an exception");
		} catch (Exception e) {
			// null task should throw an exception
		}
	}
	
	@Test
	public void testGetNextStagingTask() {
		try {
			task = new StagingTask(file, file.getOwner());
			QueueTaskDao.persist(task);
			
			Long nextTask = QueueTaskDao.getNextStagingTask(new String[]{file.getTenantId()});
			AssertJUnit.assertNotNull("Next staging task should not be null", nextTask);
			
		} catch (Exception e) {
			Assert.fail("Retrieving next staging task should not throw an exception", e);
		}
	}
	
	@Test
	public void testGetNextStagingTaskForTenantOfLogicalFile() {
		try {
			task = new StagingTask(file, file.getOwner());
			QueueTaskDao.persist(task);
			
			Long nextTask = QueueTaskDao.getNextStagingTask(new String[] {file.getTenantId()});
			AssertJUnit.assertNotNull("Next staging task should not be null", nextTask);
			
		} catch (Exception e) {
			Assert.fail("Retrieving next staging task should not throw an exception", e);
		}
	}
	
	@Test
	public void testGetNextStagingTaskIsNullForTenantWithNoTask() {
		try {
			task = new StagingTask(file, file.getOwner());
			QueueTaskDao.persist(task);
			
			Long nextTask = QueueTaskDao.getNextStagingTask(new String[] {"asdfasdfasdfasd"});
			AssertJUnit.assertNull("Next staging task should be null for tenant without any tasks", nextTask);
			
		} catch (Exception e) {
			Assert.fail("Retrieving next staging task should not throw an exception", e);
		}
	}

}
