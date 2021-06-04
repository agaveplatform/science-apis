package org.iplantc.service.io.notifications;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.manager.FilesTransferListener;
import org.iplantc.service.io.model.JSONTestDataUtil;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.StorageSystem;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(VertxExtension.class)
@DisplayName("Files fileTransferListener tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@org.junit.jupiter.api.TestInstance(org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS)

class FilesTransferListenerTest extends BaseTestCase {
    private StorageSystem system;
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
        when(listener.getVertx()).thenReturn(vertx);
        return listener;
    }

    private LogicalFileDao getMockTranserTaskDatabaseService(LogicalFile transferTaskToReturn) {
        // mock out the db service so we can can isolate method logic rather than db
        LogicalFileDao dbService = mock(LogicalFileDao.class);
        when(LogicalFileDao.findBySourceUrl(anyString())).thenReturn(transferTaskToReturn);
        return dbService;
    }

    private JsonNode getTransferTask(LogicalFile file, String status){
        JsonNode transferTask = new ObjectMapper().createObjectNode()
                .put("attempts", 1)
                .put("source", file.getSourceUri())
                .put("dest", file.getPath())
                .put("owner", file.getOwner())
                .put("tenantId", file.getTenantId())
                .put("uuid", file.getTransferUuid())
                .put("created", String.valueOf(Instant.now()))
                .put("lastUpdated", String.valueOf(Instant.now()))
                .put("endTime", String.valueOf(Instant.now()))
                .putNull("parentTask")
                .putNull("rootTask")
                .put("status", status);

        return transferTask;
    };

    private JsonNode getTransferResponse(LogicalFile file, String status){
        JsonNode response = new ObjectMapper().createObjectNode()
                .put("status", "success")
                .putNull("message")
                .put("version", 2)
                .put("result", getTransferTask(file, status));

        return response;
    }

    public void executeProcessTransferNotification(String status, String expectedStatus) {
        MessageQueueClient messageClient = null;
        FilesTransferListener listener = null;

        try{
            messageClient = MessageClientFactory.getMessageClient();
            listener = new FilesTransferListener();

            JsonNode json = getTransferTask(file, status);

            listener.processTransferNotification(StagingTaskStatus.STAGING, json.toString(), result -> {
                if (result.succeeded()){
                    LogicalFile processedFile = LogicalFileDao.findById(file.getId());
                    Assertions.assertEquals(processedFile.getStatus(), expectedStatus, "Logical file status should match status from queue.");
                } else {
                    Assertions.fail(result.cause().getMessage(), result.cause());
                }
            });

        } catch (Exception e) {
            Assertions.fail(e.getMessage(), e);
        } finally {
            try { messageClient.stop(); } catch (Exception ignored) {}
        }
    }

    @Test
    public void executeProcessTransferNotificationForStagingQueued(){
        executeProcessTransferNotification("transfertask.assigned", StagingTaskStatus.STAGING_QUEUED.name());
    }

    @Test
    public void executeProcessTransferNotificationForStaging(){
        executeProcessTransferNotification("transfertask.created", StagingTaskStatus.STAGING.name());
    }

    @Test
    public void executeProcessTransferNotificationForStagingCompleted(){
        executeProcessTransferNotification("transfertask.completed", StagingTaskStatus.STAGING_COMPLETED.name());
    }

    @Test
    public void executeProcessTransferNotificationForStagingFailed(){
        executeProcessTransferNotification("transfertask.failed", StagingTaskStatus.STAGING_FAILED.name());
    }

    @Test
    public void executeProcessTransferNotificationForCreatedFile(){
        executeProcessTransferNotification("transfer.completed", FileEventType.CREATED.name());
    }



}
