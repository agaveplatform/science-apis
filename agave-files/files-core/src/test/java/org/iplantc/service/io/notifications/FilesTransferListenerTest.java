package org.iplantc.service.io.notifications;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.manager.FilesTransferListener;
import org.iplantc.service.io.model.JSONTestDataUtil;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.QueueTask;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.StorageSystem;
import org.json.JSONException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;


@ExtendWith(VertxExtension.class)
@DisplayName("Files fileTransferListener tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@org.junit.jupiter.api.TestInstance(org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS)

class FilesTransferListenerTest extends BaseTestCase {
    private StorageSystem system;
    private QueueTask task;
    private LogicalFile file;
    private SystemDao systemDao = new SystemDao();
    private String destPath;
    private URI httpUri;

    @BeforeAll
    protected void beforeClass() throws Exception {
        super.beforeClass();

        clearSystems();
        clearLogicalFiles();

        destPath = String.format("/home/%s/%s/%s", SYSTEM_OWNER, UUID.randomUUID().toString(), LOCAL_TXT_FILE_NAME);
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
    protected void setUp() throws Exception
    {
        clearLogicalFiles();

        file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
        file.setStatus(StagingTaskStatus.STAGING_QUEUED);

        file.setTransferUuid(new AgaveUUID(UUIDType.TRANSFER).toString());
        LogicalFileDao.persist(file);
    }

    private FilesTransferListener getMockFilesTransferListenerInstance (Vertx vertx){
        FilesTransferListener listener = mock(FilesTransferListener.class);
        when(listener.getFailedEventChannel()).thenReturn("transfertask.failed");
        when(listener.getCompletedEventChannel()).thenReturn("transfer.completed");
        when(listener.getVertx()).thenReturn(vertx);
        doCallRealMethod().when(listener).processTransferNotification(any(StagingTaskStatus.class),
                any(JsonObject.class), any(Handler.class));
        return listener;
    }

    private LogicalFileDao getMockTranserTaskDatabaseService(LogicalFile transferTaskToReturn) {
        // mock out the db service so we can can isolate method logic rather than db
        LogicalFileDao dbService = mock(LogicalFileDao.class);
        when(LogicalFileDao.findBySourceUrl(anyString())).thenReturn(transferTaskToReturn);
        return dbService;
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

    private JsonObject getTransferResponse(LogicalFile file, String status){
        JsonObject response = new JsonObject()
                .put("status", "success")
                .putNull("message")
                .put("version", 2)
                .put("result", getTransferTask(file, status));

        return response;
    }

    @Test
    @DisplayName("Files Transfer Listener - Logical file updated to COMPLETED Status on transfer complete")
    public void testTransferCompletedUpdatesLogicalFile(Vertx vertx, VertxTestContext ctx) throws JSONException {
        FilesTransferListener filesTransferListener = getMockFilesTransferListenerInstance(vertx);

        filesTransferListener.processTransferNotification(StagingTaskStatus.STAGING_COMPLETED, getTransferResponse(file, "QUEUED"),
                ctx.succeeding(isProcessed -> ctx.verify(()-> {
                    assertTrue(isProcessed, "Future should return true on successful transfer notification processing.");
                    LogicalFile updatedFile = LogicalFileDao.findById(file.getId());
                    assertEquals(updatedFile.getStatus(), StagingTaskStatus.STAGING_COMPLETED.name(), "Logical File should be updated to COMPLETE.");
                    ctx.completeNow();
        })));
    }

    @Test
    @DisplayName("Files Transfer Listener - Logical file updated to FAILED Status on transfer failed")
    public void testTransferFailedUpdatesLogicalFile(Vertx vertx, VertxTestContext ctx) throws JSONException {
        FilesTransferListener filesTransferListener = getMockFilesTransferListenerInstance(vertx);

        filesTransferListener.processTransferNotification(StagingTaskStatus.STAGING_FAILED, getTransferResponse(file, "QUEUED"),
                ctx.succeeding(isProcessed -> ctx.verify(()-> {
                    assertTrue(isProcessed, "Future should return true on successful transfer notification processing.");
                    LogicalFile updatedFile = LogicalFileDao.findById(file.getId());
                    assertEquals(updatedFile.getStatus(), StagingTaskStatus.STAGING_FAILED.name(), "Logical File should be updated to COMPLETE.");
                    ctx.completeNow();
        })));
    }

}
