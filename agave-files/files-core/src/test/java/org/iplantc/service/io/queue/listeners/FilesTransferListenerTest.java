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
import org.iplantc.service.io.exceptions.LogicalFileException;
import org.iplantc.service.io.model.FileEvent;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.RemoteSystem;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;

import static org.iplantc.service.io.model.enumerations.StagingTaskStatus.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.fail;


@Test(groups = {"integration"})
public class FilesTransferListenerTest extends BaseTestCase {
    private final SystemDao systemDao = new SystemDao();
    private String destPath;
    private URI httpUri;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeClass
    protected void beforeClass() throws Exception {
        destPath = String.format("/home/%s/%s/%s", SYSTEM_OWNER, UUID.randomUUID(), LOCAL_BINARY_FILE_NAME);
        httpUri = new URI("https://httpd:8443/public/test_upload.bin");
    }

    @AfterClass
    protected void afterClass() throws Exception {
        clearSystems();
        clearLogicalFiles();
    }

    protected LogicalFile getMockLogicalFile(){
        RemoteSystem system = mock(RemoteSystem.class);
        when(system.getSystemId()).thenReturn(UUID.randomUUID().toString());

        LogicalFile logicalFile = mock(LogicalFile.class);
        when(logicalFile.getSystem()).thenReturn(system);
        when(logicalFile.getSourceUri()).thenReturn(httpUri.toString());
        when(logicalFile.getPath()).thenReturn(destPath);
        when(logicalFile.getUuid()).thenReturn(new AgaveUUID(UUIDType.FILE).toString());
        when(logicalFile.getTenantId()).thenReturn("foo.tenant");
        when(logicalFile.getStatus()).thenReturn(StagingTaskStatus.STAGING_QUEUED.name());

        return logicalFile;
    }

    private FilesTransferListener getMockFilesTransferListenerInstance() {
        return mock(FilesTransferListener.class);
    }


    private JsonNode getTransferTask(LogicalFile file, String status) {
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

    @Test
    public void runHandlesMessagingException() {
        FilesTransferListener listener = mock(FilesTransferListener.class);

        MessageQueueClient client = mock(MessageQueueClient.class);

        try {
            when(client.pop(any(), any())).thenThrow(new MessagingException("This should be handled and swallowed"));
            when(listener.getMessageClient()).thenReturn(client);
            // TODO: verify that the message client was not stopped
            verify(client, never()).stop();
            listener.run();
        } catch (Exception e) {
            fail("NO exception should escape the run method", e);
        }
    }

    @Test
    public void runHandlesMessagingProcessingException() {

        FilesTransferListener listener = getMockFilesTransferListenerInstance();

        MessageQueueClient client = mock(MessageQueueClient.class);

        try {
            LogicalFile logicalFile = getMockLogicalFile();
            Message msg = new Message(24, getTransferTask(logicalFile, "transfertask.created").toString());
            when(client.pop(any(), any())).thenReturn(msg);
            doNothing().when(client).reject(any(), any(), any(), any());
            // TODO: verify that the message client was not stopped
            verify(client, never()).stop();
            when(listener.getMessageClient()).thenReturn(client);
            doThrow(new MessageProcessingException("This should be handled and swallowed")).when(listener).processTransferNotification(any());

            listener.run();
        } catch (Exception e) {
            fail("NO exception should escape the run method", e);
        }
    }

    @Test(expectedExceptions = MessageProcessingException.class)
    public void executeProcessTransferNotificationHandlesProcessingException() throws Exception {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        LogicalFile logicalFile = getMockLogicalFile();

        doCallRealMethod().when(listener).processTransferNotification(any());
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(null);

        listener.processTransferNotification(getTransferTask(logicalFile, "transfertask.created"));
    }

    @Test(expectedExceptions = MessageProcessingException.class, expectedExceptionsMessageRegExp = "Unable to update transfer status*")
    public void executeProcessTransferNotificationHandlesLogicalFileException() throws Exception {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        LogicalFile logicalFile = getMockLogicalFile();

        doCallRealMethod().when(logicalFile).addContentEvent(any(FileEvent.class));
        doCallRealMethod().when(listener).processTransferNotification(any());
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(logicalFile);
        doNothing().when(listener).updateTransferStatus(any(LogicalFile.class), any(StagingTaskStatus.class), anyString());
        when(listener.updateDestinationLogicalFile(any(LogicalFile.class), anyString(), anyString())).
                thenThrow(LogicalFileException.class);

        listener.processTransferNotification(getTransferTask(logicalFile, "transfer.completed"));
    }


    @DataProvider
    private Object[][] executeProcessTransferNotificationProvider() {
        return new Object[][]{
                {"transfertask.assigned", STAGING_QUEUED},
                {"transfertask.staging", STAGING},
                {"transfertask.completed", STAGING_COMPLETED},
                {"transfertask.failed", STAGING_FAILED}
        };
    }

    @Test(dataProvider = "executeProcessTransferNotificationProvider")
    public void executeProcessTransferNotification(String transferStatus, StagingTaskStatus stagingStatus) {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();

        try {
            LogicalFile logicalFile = getMockLogicalFile();

            when(listener.lookupLogicalFileByUrl(logicalFile.getSourceUri(), logicalFile.getTenantId())).thenReturn(logicalFile);
            doCallRealMethod().when(listener).processTransferNotification(any());
            doNothing().when(listener).updateTransferStatus(any(LogicalFile.class), any(StagingTaskStatus.class), anyString());
            when(listener.updateDestinationLogicalFile(any(LogicalFile.class), anyString(), anyString())).thenReturn(logicalFile);

            JsonNode json = getTransferTask(logicalFile, transferStatus);

            listener.processTransferNotification(json);
            verify(listener, times(1)).updateTransferStatus(logicalFile, stagingStatus, logicalFile.getOwner());

        } catch (Exception e) {
            fail("No exception should be thrown for valid transfer notification", e);
        }
    }

    @Test
    public void executeProcessTransferNotificationTransferCompleteCreatesDestLogicalFile() {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();

        try {
            LogicalFile logicalFile = getMockLogicalFile();

            when(listener.lookupLogicalFileByUrl(logicalFile.getSourceUri(), logicalFile.getTenantId())).thenReturn(logicalFile);
            doCallRealMethod().when(listener).processTransferNotification(any());
            doNothing().when(listener).updateTransferStatus(any(LogicalFile.class), any(StagingTaskStatus.class), anyString());
            when(listener.updateDestinationLogicalFile(any(LogicalFile.class), anyString(), anyString())).thenReturn(logicalFile);

            JsonNode json = getTransferTask(logicalFile, "transfer.completed");
            listener.processTransferNotification(json);
            verify(listener, times(1)).
                    updateTransferStatus(logicalFile, STAGING_COMPLETED, logicalFile.getOwner());
            verify(listener, times(1)).
                    updateDestinationLogicalFile(logicalFile, json.get("dest").textValue(), logicalFile.getOwner());

        } catch (Exception e) {
            fail("No exception should be thrown for valid transfer notification", e);
        }
    }

    @Test (expectedExceptions = MessageProcessingException.class)
    public void executeProcessTransferNotificationHandlesUnknownTransferStatus() throws Exception {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();

        LogicalFile logicalFile = getMockLogicalFile();

        when(listener.lookupLogicalFileByUrl(logicalFile.getSourceUri(), logicalFile.getTenantId())).thenReturn(logicalFile);
        doCallRealMethod().when(listener).processTransferNotification(any());

        JsonNode json = getTransferTask(logicalFile, "transfertask.unknown");
        listener.processTransferNotification(json);
    }

}
