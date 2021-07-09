package org.iplantc.service.io.queue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.systems.model.RemoteSystem;
import org.quartz.SchedulerException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;

import static org.iplantc.service.io.BaseTestCase.LOCAL_BINARY_FILE_NAME;
import static org.iplantc.service.io.BaseTestCase.SYSTEM_OWNER;
import static org.iplantc.service.io.model.enumerations.StagingTaskStatus.STAGING_FAILED;
import static org.iplantc.service.io.model.enumerations.StagingTaskStatus.STAGING_QUEUED;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertNotNull;


public class TransferTaskSchedulerTest {
    private String destPath;
    private URI httpUri;
    private static final ObjectMapper objectMapper = new ObjectMapper();


    @BeforeClass
    protected void beforeClass() throws Exception {
        destPath = String.format("/home/%s/%s/%s", SYSTEM_OWNER, UUID.randomUUID(), LOCAL_BINARY_FILE_NAME);
        httpUri = new URI("https://httpd:8443/public/test_upload.bin");
    }

    /**
     * Create {@link JsonNode} equivalent of a TransferTask for a given LogicalFile
     * @param file {@link LogicalFile} to create TransferTask with
     * @param status status of the TransferTask
     * @return {@link JsonNode} of a TransferTask
     */
    protected JsonNode getTransferTask(LogicalFile file, String status) {
        return objectMapper.createObjectNode()
                .put("attempts", 1)
                .put("source", file.getSourceUri())
                .put("dest", "agave://" + file.getSystem() + "/" + file.getPath())
                .put("owner", file.getOwner())
                .put("tenant_id", file.getTenantId())
                .put("uuid", new AgaveUUID(UUIDType.TRANSFER).toString())
                .put("created", String.valueOf(Instant.now()))
                .put("lastUpdated", String.valueOf(Instant.now()))
                .put("endTime", String.valueOf(Instant.now()))
                .putNull("parentTask")
                .putNull("rootTask")
                .put("status", status);
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
        doCallRealMethod().when(logicalFile).setStatus(anyString());
        when(logicalFile.getOwner()).thenReturn(SYSTEM_OWNER);

        return logicalFile;
    }

    private TransferTaskScheduler getMockTransferTaskScheduler() throws NotificationException, SchedulerException {
        TransferTaskScheduler transferTaskScheduler = mock(TransferTaskScheduler.class);
        doCallRealMethod().when(transferTaskScheduler).enqueueStagingTask(any(LogicalFile.class), anyString());
        doCallRealMethod().when(transferTaskScheduler).enqueueStagingTask(any(LogicalFile.class), anyString(), anyString());
        doNothing().when(transferTaskScheduler).updateLogicalFileAndSwallowException(any(LogicalFile.class));
        doCallRealMethod().when(transferTaskScheduler).enqueueStagingTaskWithNotification(any(LogicalFile.class), anyString());
        return transferTaskScheduler;
    }


    @Test (expectedExceptions = SchedulerException.class)
    public void testEnqueueStagingTaskHandlesSchedulerException() throws SchedulerException, NotificationException {
        TransferTaskScheduler transferTaskScheduler = getMockTransferTaskScheduler();
        when(transferTaskScheduler.callTransferClient(any(LogicalFile.class), anyString())).thenReturn(null);

        LogicalFile file = getMockLogicalFile();
        transferTaskScheduler.enqueueStagingTask(file, SYSTEM_OWNER);
        verify(file, times(1)).setStatus(STAGING_FAILED.name());
    }

    @Test (expectedExceptions = SchedulerException.class)
    public void testEnqueueStagingTaskHandlesIOException() throws SchedulerException, NotificationException{
        TransferTaskScheduler transferTaskScheduler = getMockTransferTaskScheduler();
        when(transferTaskScheduler.callTransferClient(any(LogicalFile.class), anyString())).thenThrow(IOException.class);

        LogicalFile file = getMockLogicalFile();
        transferTaskScheduler.enqueueStagingTask(file, SYSTEM_OWNER);
        verify(file, times(1)).setStatus(STAGING_FAILED.name());
    }

    @Test (expectedExceptions = SchedulerException.class)
    public void testEnqueueStagingTaskHandlesNullResponseFromClient() throws SchedulerException, NotificationException {
        TransferTaskScheduler transferTaskScheduler = getMockTransferTaskScheduler();
        when(transferTaskScheduler.callTransferClient(any(LogicalFile.class), anyString())).thenReturn(null);

        LogicalFile file = getMockLogicalFile();
        transferTaskScheduler.enqueueStagingTask(file, SYSTEM_OWNER);
        verify(file, times(1)).setStatus(STAGING_FAILED.name());
    }

    @Test (expectedExceptions = NotificationException.class)
    public void testEnqueueStagingTaskWithNotificationHandlesNotificationException() throws NotificationException, SchedulerException {
        TransferTaskScheduler transferTaskScheduler = getMockTransferTaskScheduler();
        when(transferTaskScheduler.createNotification(any(LogicalFile.class))).thenThrow(NotificationException.class);
        when((transferTaskScheduler.parseTransferResponse(any(JsonNode.class)))).thenCallRealMethod();

        LogicalFile file = getMockLogicalFile();
        JsonNode jsonTransferTask = getTransferTask(file, STAGING_QUEUED.name());
        when(transferTaskScheduler.callTransferClient(any(LogicalFile.class), anyString(), anyString())).thenReturn(jsonTransferTask);

        String transferUuid = transferTaskScheduler.enqueueStagingTaskWithNotification(file, SYSTEM_OWNER);
        assertNotNull(transferUuid, "Successful enqueue of transfer task should return uuid corresponding to transfer.");
//        verify(file, times(1)).setTransferUuid(jsonTransferTask.get("uuid").asText());
    }

    @Test
    public void testEnqueueStagingTaskWithNotificationCreatesNotification() throws NotificationException, SchedulerException {
        TransferTaskScheduler transferTaskScheduler = getMockTransferTaskScheduler();
        when(transferTaskScheduler.createNotification(any(LogicalFile.class))).thenReturn(mock(Notification.class));
        when((transferTaskScheduler.parseTransferResponse(any(JsonNode.class)))).thenCallRealMethod();

        LogicalFile file = getMockLogicalFile();
        JsonNode jsonTransferTask = getTransferTask(file, STAGING_QUEUED.name());
        when(transferTaskScheduler.callTransferClient(any(LogicalFile.class), anyString(), anyString())).thenReturn(jsonTransferTask);

        transferTaskScheduler.enqueueStagingTaskWithNotification(file, SYSTEM_OWNER);
        verify(transferTaskScheduler, times(1)).createNotification(file);
    }

    @Test
    public void testEnqueueStagingTaskForFileUpload() throws NotificationException, SchedulerException {
        //create a file on test system
        TransferTaskScheduler transferTaskScheduler = getMockTransferTaskScheduler();
        when(transferTaskScheduler.createNotification(any(LogicalFile.class))).thenReturn(mock(Notification.class));
        when((transferTaskScheduler.parseTransferResponse(any(JsonNode.class)))).thenCallRealMethod();

        LogicalFile file = getMockLogicalFile();
        JsonNode jsonTransferTask = getTransferTask(file, STAGING_QUEUED.name());
        when(transferTaskScheduler.callTransferClient(any(LogicalFile.class), anyString(), anyString())).thenReturn(jsonTransferTask);

        transferTaskScheduler.enqueueStagingTaskWithNotification(file, SYSTEM_OWNER);
        verify(transferTaskScheduler, times(1)).createNotification(file);
    }

}
