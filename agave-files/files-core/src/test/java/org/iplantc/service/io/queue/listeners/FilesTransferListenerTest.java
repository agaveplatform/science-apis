package org.iplantc.service.io.queue.listeners;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.common.exceptions.MessageProcessingException;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.model.JSONTestDataUtil;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.fail;


@Test(groups={"integration"})
class FilesTransferListenerTest extends BaseTestCase {
    private StorageSystem system;
    private LogicalFile file;
    private final SystemDao systemDao = new SystemDao();
    private String destPath;
    private URI httpUri;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeClass
    protected void beforeClass() throws Exception {
        destPath = String.format("/home/%s/%s/%s", SYSTEM_OWNER, UUID.randomUUID(), LOCAL_TXT_FILE_NAME);
        httpUri = new URI("https://httpd:8443/public/test_upload.bin");


        system = StorageSystem.fromJSON(jtd.getTestDataObject(JSONTestDataUtil.TEST_STORAGE_SYSTEM_FILE));
        system.setOwner(SYSTEM_OWNER);
        system.setPubliclyAvailable(true);
        system.setGlobalDefault(true);
        system.setAvailable(true);
    }

    @AfterClass
    protected void afterClass() throws Exception {
        clearSystems();
        clearLogicalFiles();
    }

    protected LogicalFile getMockLogicalFile() throws Exception
    {
        RemoteSystem system = mock(RemoteSystem.class);
        when(system.getSystemId()).thenReturn(UUID.randomUUID().toString());

        LogicalFile logicalFile = mock(LogicalFile.class);
        when(logicalFile.getSystem()).thenReturn(system);
        when(logicalFile.getPath()).thenReturn(destPath);
        when(logicalFile.getUuid()).thenReturn(new AgaveUUID(UUIDType.FILE).toString());
        when(logicalFile.getTenantId()).thenReturn("foo.tenant");
        when(logicalFile.getStatus()).thenReturn(StagingTaskStatus.STAGING_QUEUED.name());

        return logicalFile;
    }

    private FilesTransferListener getMockFilesTransferListenerInstance (){
        return mock(FilesTransferListener.class);
    }



    private JsonNode getTransferTask(LogicalFile file, String status){
        return objectMapper.createObjectNode()
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
    }

//    private JsonNode getTransferResponse(LogicalFile file, String status){
//        return objectMapper.createObjectNode()
//                .put("status", "success")
//                .putNull("message")
//                .put("version", 2)
//                .put("result", getTransferTask(file, status));
//    }

    @Test
    public void runHandlesMessagingException(String status, String expectedStatus, Boolean checkDestFileCreated) {
        FilesTransferListener listener = mock(FilesTransferListener.class);

        MessageQueueClient client = mock(MessageQueueClient.class);

        try {
            when(client.pop(any(), any())).thenThrow(new MessagingException("This should be handled and swallowed"));
            when(listener.getMessageClient()).thenReturn(client);
            // TODO: verify that the message client was not stopped
            listener.run();
        } catch (Exception e) {
            fail("NO exception should excape the run method", e);
        }
    }

    @Test
    public void runHandlesMessagingProcessingException(String status, String expectedStatus, Boolean checkDestFileCreated) {

        FilesTransferListener listener = getMockFilesTransferListenerInstance();

        MessageQueueClient client = mock(MessageQueueClient.class);

        try {
            LogicalFile logicalFile = getMockLogicalFile();

            when(client.pop(any(), any())).thenReturn(new Message(24, getTransferTask(logicalFile, "transfertask.created").toString()));
            doNothing().when(client).reject(any(),any(),any(),any());
            // TODO: verify that the message client was not stopped
            when(listener.getMessageClient()).thenReturn(client);
            doThrow(new MessageProcessingException("This should be handled and swallowed")).when(listener).processTransferNotification(any());

            listener.run();
        } catch (Exception e) {
            fail("NO exception should excape the run method", e);
        }
    }

    public void executeProcessTransferNotification() {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        MessageQueueClient client = mock(MessageQueueClient.class);

        try {
            LogicalFile logicalFile = getMockLogicalFile();

            when(client.pop(any(), any())).thenReturn();
            doNothing().when(client).reject(any(),any(),any(),any());

            when(listener.getMessageClient()).thenReturn(client);
            when(listener.lookupLogicalFileByUrl()
            when(listener.processTransferNotification(any())).thenCallRealMethod();

            for (String status: List.of(TRANSFERTASK_CREATED,
                    TRANSFERTASK_UPDATED,
                    TRANSFERTASK_FINISHED,
                    TRANSFERTASK_FAILED,
                    TRANSFERTASK_PAUSED_COMPLETED,
                    TRANSFERTASK_CANCELED_COMPLETED)) {

                JsonNode json = getTransferTask(logicalFile, status);

                listener.processTransferNotification(json);
                verify(listener.updateTransferStatus();)


                if (result.succeeded()){
                    LogicalFile processedFile = LogicalFileDao.findById(file.getId());
                    Assert.assertNotNull(processedFile, "Logical file should be present");
                    Assert.assertEquals(processedFile.getStatus(), expectedStatus, "Logical file status should match status from queue.");
                    if (checkDestFileCreated){
                        LogicalFile destFile = LogicalFileDao.findBySourceUrl(file.getPublicLink());
                        Assert.assertNotNull(destFile, "Dest logical file should be created on " + status + " status.");
                        Assert.assertEquals(StagingTaskStatus.STAGING_COMPLETED.name(), processedFile.getStatus(), "Source logical file status should be in completed state when dest file is created.");
                        Assert.assertEquals(expectedStatus, destFile.getStatus(), "Dest logical file status should match status from queue.");
                    } else {
                        Assert.assertEquals(expectedStatus, processedFile.getStatus(), "Logical file status should match status from queue.");
                    }
                } else {
                    fail(result.cause().getMessage(), result.cause());
                }
            })

        } catch (Exception e) {
            fail(e.getMessage(), e);
        } finally {
            try { if (messageClient != null) messageClient.stop(); } catch (Exception ignored) {}
        }
    }

    @Test
    public void executeProcessTransferNotificationForStagingQueued(){
        executeProcessTransferNotification("transfertask.assigned", StagingTaskStatus.STAGING_QUEUED.name(), false);
    }

    @Test
    public void executeProcessTransferNotificationForStaging(){
        executeProcessTransferNotification("transfertask.created", StagingTaskStatus.STAGING.name(), false);
    }

    @Test
    public void executeProcessTransferNotificationForStagingCompleted(){
        executeProcessTransferNotification("transfertask.completed", StagingTaskStatus.STAGING_COMPLETED.name(), false);
    }

    @Test
    public void executeProcessTransferNotificationForStagingFailed(){
        executeProcessTransferNotification("transfertask.failed", StagingTaskStatus.STAGING_FAILED.name(), false);
    }

    @Test
    public void executeProcessTransferNotificationForCreatedFile(){
        executeProcessTransferNotification("transfer.completed", FileEventType.CREATED.name(), true );
    }

}
