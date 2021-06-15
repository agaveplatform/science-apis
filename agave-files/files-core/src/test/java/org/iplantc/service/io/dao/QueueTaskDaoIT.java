package org.iplantc.service.io.dao;

import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.model.JSONTestDataUtil;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.QueueTask;
import org.iplantc.service.io.model.StagingTask;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.StorageSystem;
import org.quartz.SchedulerException;
import org.testng.Assert;
import org.testng.annotations.*;

import java.net.URI;
import java.util.UUID;

import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertNotNull;

@Test(groups={"integration"})
public class QueueTaskDaoIT extends BaseTestCase {

    private StorageSystem system;
    private QueueTask task;
    private LogicalFile file;
    private final SystemDao systemDao = new SystemDao();
    private final QueueTaskDao queueTaskDao = new QueueTaskDao();
    private String destPath;
    private URI httpUri;

    @BeforeClass
    protected void beforeClass() throws Exception {
        super.beforeClass();

        clearSystems();
        clearLogicalFiles();

        destPath = String.format("/home/%s/%s/%s", SYSTEM_OWNER, UUID.randomUUID(), LOCAL_TXT_FILE_NAME);
        httpUri = new URI("https://httpd:8443/public/test_upload.bin");


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
    public void testEnqueueStagingTask() throws SchedulerException, NotificationException {
        queueTaskDao.enqueueStagingTask(file, SYSTEM_OWNER);

        LogicalFile queuedFile = LogicalFileDao.findById(file.getId());

        Assert.assertEquals(queuedFile.getStatus(), StagingTaskStatus.STAGING_QUEUED.name(), "Logical file " +
                "status should be updated to STAGING_QUEUED when transfer request is successfully sent to the " +
                "agave-transfer service.");
        Assert.assertNotNull(queuedFile.getTransferUuid(), "Logical file should have associated transfer " +
                "uuid on succcessful transfer request");

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

            Long nextTask = QueueTaskDao.getNextStagingTask(new String[]{TENANT_ID});
            assertNotNull("Next staging task should not be null", nextTask);

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
            assertNotNull("Next staging task should not be null", nextTask);

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
            assertNull(nextTask, "Next staging task should be null for tenant without any tasks");

        } catch (Exception e) {
            Assert.fail("Retrieving next staging task should not throw an exception", e);
        }
    }

}
