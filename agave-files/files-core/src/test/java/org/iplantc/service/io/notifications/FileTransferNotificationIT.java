package org.iplantc.service.io.notifications;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.dao.QueueTaskDao;
import org.iplantc.service.io.manager.FilesTransferListener;
import org.iplantc.service.io.model.JSONTestDataUtil;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.QueueTask;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.StorageSystem;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.quartz.SchedulerException;
import org.testng.Assert;

import java.net.URI;
import java.time.Instant;
import java.util.List;


@ExtendWith(VertxExtension.class)
@DisplayName("Files API integration test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FileTransferNotificationIT extends BaseTestCase {
    private StorageSystem system;
    private QueueTask task;
    private LogicalFile file;
    private SystemDao systemDao = new SystemDao();
    private String destPath;
    private URI httpUri;
    private int port = 8080;
    private NotificationDao notificationDao = new NotificationDao();


    @BeforeAll
    protected void beforeClass() throws Exception {
        super.beforeClass();

        clearSystems();
        clearLogicalFiles();

//        destPath = String.format("agave://sftp/home/%s/%s/%s", SYSTEM_OWNER, UUID.randomUUID().toString(), LOCAL_TXT_FILE_NAME);
        destPath = String.format("agave://sftp/tmp");
        httpUri = new URI("https://httpd:8443/public/test_upload.bin");


        system = StorageSystem.fromJSON(jtd.getTestDataObject(JSONTestDataUtil.TEST_STORAGE_SYSTEM_FILE));
        system.setOwner(SYSTEM_OWNER);
        system.setPubliclyAvailable(true);
        system.setGlobalDefault(true);
        system.setAvailable(true);

        systemDao.persist(system);
    }


    @AfterAll
    protected void afterClass() throws Exception {
        clearSystems();
        clearLogicalFiles();
    }

    @BeforeEach
    protected void setUp() throws Exception {
        clearLogicalFiles();

        file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
        file.setStatus(StagingTaskStatus.STAGING_QUEUED);

        file.setTransferUuid(new AgaveUUID(UUIDType.TRANSFER).toString());
        LogicalFileDao.persist(file);
    }

    @Test
    public void fileTransferNotificationTest(Vertx vertx, VertxTestContext ctx) throws SchedulerException {
        //make a request to the transfer service
        try {
            QueueTaskDao.enqueueStagingTask(file, SYSTEM_OWNER);

            //launch FilesTransferListener
            Checkpoint filesListenerDeploymentCheckpoint = ctx.checkpoint();
            Checkpoint requestCheckpoint = ctx.checkpoint();


            vertx.deployVerticle(FilesTransferListener.class.getName(), new DeploymentOptions(), ctx.succeeding(filesListenerId -> {
                filesListenerDeploymentCheckpoint.flag();

                try {
                    vertx.eventBus().send("transfer.completed", getTransferTask(file, "COMPLETED"), response -> {
                        if (response.succeeded()){
                            ctx.verify(() -> {
//                                JsonObject jsonLogicalFile = (JsonObject) response.result();
//
//                                Assertions.assertNotNull(jsonLogicalFile, "Returned task is not null");
//                                Assertions.assertEquals(jsonLogicalFile.getString("source"), file.getSourceUri(), "Returned task source is equivalent to the original source");
//                                Assertions.assertEquals(jsonLogicalFile.getString("path"), file.getPath(), "Returned task dest is equivalent to the original dest");
//                                Assertions.assertEquals(jsonLogicalFile.getString("owner"), SYSTEM_OWNER, "Returned task owner is equivalent to the jwt user");
//                                Assertions.assertEquals(jsonLogicalFile.getString("tenantId"), file.getTenantId(), "Returned task tenant id is equivalent to the jwt tenant id");
//                                Assertions.assertEquals(jsonLogicalFile.getString("status"), "COMPLETED", "Returned task status is completed on successful transfer");

                                requestCheckpoint.flag();

                                //Verify that the notification sent onto the queue is correct
                                List<Notification> notifications = notificationDao.getActiveForAssociatedUuidAndEvent(file.getTransferUuid(), FileEventType.STAGING_COMPLETED.name());
                                Assert.assertFalse(notifications.isEmpty(), "No notifications found for status " + FileEventType.STAGING_COMPLETED.name());
                                Assert.assertEquals(notifications.size(), 1, "Wrong number of notifications returned for status " + FileEventType.STAGING_COMPLETED.name());

                                Notification n = notifications.get(0);
                                Assert.assertTrue(n.getUuid() == file.getTransferUuid(), "Notification for transfer " + file.getTransferUuid() + " was successfully created");
                                Assert.assertTrue(n.getOwner() == file.getOwner(), "Notification owner for transfer is equivalent to original owner");
                                Assert.assertTrue(n.getTenantId() == file.getTenantId(), "Notification tenant id for transfer is equivalent to original tenant id");

                                ctx.completeNow();
                            });
                        } else {
                            ctx.failNow(response.cause());
                        }
                    });
                } catch (Exception e) {
                    ctx.failNow(e);
                }
            }));

//            //Verify that the notification sent onto the queue is correct
//            List<Notification> notifications = notificationDao.getActiveForAssociatedUuidAndEvent(file.getTransferUuid(), FileEventType.STAGING_COMPLETED.name());
//            Assert.assertFalse(notifications.isEmpty(), "No notifications found for status " + FileEventType.STAGING_COMPLETED.name());
//            Assert.assertEquals(notifications.size(), 1, "Wrong number of notifications returned for status " + FileEventType.STAGING_COMPLETED.name());
//
//            Notification n = notifications.get(0);
//            Assert.assertTrue(n.getUuid() == file.getTransferUuid(), "Notification for transfer " + file.getTransferUuid() + " was successfully created");
//            Assert.assertTrue(n.getOwner() == file.getOwner(), "Notification owner for transfer is equivalent to original owner");
//            Assert.assertTrue(n.getTenantId() == file.getTenantId(), "Notification tenant id for transfer is equivalent to original tenant id");


        } catch (SchedulerException ex) {
            Assertions.fail("Enqueuing a valid staging task should not throw exception: " + ex.getMessage());
        }
//        catch (NotificationException e) {
//            Assertions.fail("Retrieving notification should not throw exception: " + e.getMessage());
//        }
    }

    private JsonObject getTransferTask(LogicalFile file, String status){
        JsonObject transferTask = new JsonObject()
                .put("attempts", 1)
                .put("source", file.getSourceUri())
                .put("dest", file.getPath())
                .put("owner", file.getOwner())
                .put("tenantId", file.getTenantId())
                .put("uuid", file.getTransferUuid())
                .put("created", Instant.now())
                .put("lastUpdated", Instant.now())
                .put("endTime", Instant.now())
                .putNull("parentTask")
                .putNull("rootTask")
                .put("status", status);

        return transferTask;
    };

}
