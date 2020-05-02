package org.iplantc.service.apps.queue.actions;

import static org.iplantc.service.apps.model.JSONTestDataUtil.*;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.manager.SystemManager;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.testng.Assert;
import org.testng.annotations.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.UUID;

@Test(groups = {"integration"})
public class PublishActionIT extends AbstractWorkerActionTest {

    private static final Logger log = Logger.getLogger(PublishActionIT.class);
    private PublishAction publishAction;

    @BeforeClass
    @Override
    public void beforeClass() throws Exception {
        super.beforeClass();
    }

    @AfterClass
    @Override
    public void afterClass() throws Exception {
        super.afterClass();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
//        initSystems();
    }

    @AfterMethod
    public void afterMethod() throws Exception {
        clearSoftware();
        clearSystems();
    }

    @DataProvider
    protected Object[][] publishToNamedExectuionSystemHonorsNameAndVersionProvider() throws Exception {
        ArrayList<Object[]> testCases = new ArrayList<Object[]>();

        testCases.add(new Object[]{createNonce(UUID.randomUUID().toString()), "99.9.9"});
        testCases.add(new Object[]{createNonce(UUID.randomUUID().toString()), ""});
        testCases.add(new Object[]{createNonce(UUID.randomUUID().toString()), null});
        testCases.add(new Object[]{"", "99.9.9"});
        testCases.add(new Object[]{null, "99.9.9"});
        testCases.add(new Object[]{"", ""});
        testCases.add(new Object[]{null, null});

        return testCases.toArray(new Object[][]{});
    }

    @Test(dataProvider = "publishToNamedExectuionSystemHonorsNameAndVersionProvider")
    public void publishToNamedExectuionSystemHonorsNameAndVersion(String expectedName, String expectedVersion) {

        RemoteDataClient client = null;
        Software publishedSoftware = null;
        String nonce = createNonce(UUID.randomUUID().toString());
        Path baseDeploymentDir = Paths.get(nonce);
        Software originalSoftware = null;
        ExecutionSystem expectedExecutionSystem = null;
        StorageSystem publicStorageSystem = null;
        try {

            expectedExecutionSystem = createExecutionSystem();
            expectedExecutionSystem.setOwner(SYSTEM_OWNER);
            expectedExecutionSystem.setPubliclyAvailable(true);
            expectedExecutionSystem.setGlobalDefault(true);
            systemDao.persist(expectedExecutionSystem);

            publicStorageSystem = createStorageSystem();
            publicStorageSystem.setOwner(SYSTEM_OWNER);
            publicStorageSystem.setPubliclyAvailable(true);
            publicStorageSystem.setGlobalDefault(true);
            systemDao.persist(publicStorageSystem);

            originalSoftware = createSoftware();

            originalSoftware.setDeploymentPath(baseDeploymentDir.resolve(originalSoftware.getDeploymentPath()).toString());

            stageRemoteSoftwareAssets(originalSoftware);

            originalSoftware.setName(nonce);

            SoftwareDao.persist(originalSoftware);

            PublishAction publishAction = new PublishAction(originalSoftware,
                    ADMIN_USER,
                    expectedName,
                    expectedVersion,
                    expectedExecutionSystem.getSystemId());

            publishAction.run();
            publishedSoftware = publishAction.getPublishedSoftware();

            Assert.assertNotNull(publishedSoftware,
                    "Published software should not be null after publishing");

            client = publishedSoftware.getStorageSystem().getRemoteDataClient();
            client.authenticate();

            Assert.assertTrue(client.doesExist(publishedSoftware.getDeploymentPath()),
                    "Published software deployment path should exist after publishing");
            Assert.assertTrue(client.isFile(publishedSoftware.getDeploymentPath()),
                    "Published software deployment path should be a file.");
            Assert.assertTrue(publishedSoftware.getDeploymentPath().endsWith(".zip"),
                    "Published software deployment path should be a zip archive.");

            Assert.assertEquals(publicStorageSystem.getSystemId(), publishedSoftware.getStorageSystem().getSystemId(),
                    "Published software storage system should be default public storage system.");

            Assert.assertEquals(expectedExecutionSystem.getSystemId(), publishedSoftware.getExecutionSystem().getSystemId(),
                    "Published software storage system should be published to public system with the provided execution id.");

            if (StringUtils.isBlank(expectedName)) {
                Assert.assertEquals(publishedSoftware.getName(), originalSoftware.getName(),
                        "Published software name should match original software name when not provided");
            } else {
                Assert.assertEquals(publishedSoftware.getName(), expectedName,
                        "Published software name should match provided name");
            }

            if (StringUtils.isBlank(expectedVersion)) {
                Assert.assertEquals(publishedSoftware.getVersion(), originalSoftware.getVersion(),
                        "Published software version should match original software version when not provided");

            } else {
                Assert.assertEquals(publishedSoftware.getVersion(), expectedVersion,
                        "Published software version should match provided version");
            }

            Assert.assertEquals((int) publishedSoftware.getRevisionCount(), 1,
                    "Revision count should be 1 for newly published apps");
        } catch (Throwable t) {
            Assert.fail("Publishing happy path should not throw exception", t);
        } finally {
            if (originalSoftware != null) {
                try {
                    deleteRemoteSoftwareAssets(originalSoftware);
                } catch (Exception ignored) {
                }
//                try { SoftwareDao.delete(originalSoftware); } catch (Exception ignored){}
            }

            if (publishedSoftware != null) {
                try {
                    deleteRemoteSoftwareAssets(publishedSoftware);
                } catch (Exception ignored) {
                }
//                try { SoftwareDao.delete(publishedSoftware); } catch (Exception ignored) {}
            }
        }
    }


//    @Test(enabled = false)
//    public void copyPublicAppArchiveToDeploymentSystem(Software software, String username) {
//
//    }
//
//    @Test
//    public void fetchSoftwareDeploymentPath() {
//
//    }
//
//    @Test
//    public void getDestinationRemoteDataClient() {
//        throw new RuntimeException("Test not implemented");
//    }
//
//    @Test
//    public void getPublishedSoftware() {
//        throw new RuntimeException("Test not implemented");
//    }
//
//    @Test
//    public void getPublishingUsername() {
//        throw new RuntimeException("Test not implemented");
//    }
//
//    @Test
//    public void getSourceRemoteDataClient() {
//        throw new RuntimeException("Test not implemented");
//    }
//
//    @Test
//    public void publish() {
//        throw new RuntimeException("Test not implemented");
//    }
//
//    @Test
//    public void resolveAndCreatePublishedDeploymentPath() {
//        throw new RuntimeException("Test not implemented");
//    }
//
//    @Test(enabled = false)
//    public void schedulePublicAppAssetBundleBackup() {
//        throw new RuntimeException("Test not implemented");
//    }
//
//    @Test(enabled = false)
//    public void setPublishedSoftware() {
//        throw new RuntimeException("Test not implemented");
//    }
//
//    @Test(enabled = false)
//    public void setPublishingUsername() {
//        throw new RuntimeException("Test not implemented");
//    }
}
