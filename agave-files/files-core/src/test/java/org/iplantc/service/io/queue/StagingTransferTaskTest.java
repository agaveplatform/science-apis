package org.iplantc.service.io.queue;

import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.exceptions.FileProcessingException;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.systems.model.RemoteSystem;
import org.quartz.SchedulerException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import static org.iplantc.service.io.model.enumerations.StagingTaskStatus.STAGING_FAILED;
import static org.iplantc.service.io.model.enumerations.StagingTaskStatus.STAGING_QUEUED;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class StagingTransferTaskTest extends BaseTestCase {
    private String destPath;
    private URI httpUri;

    @BeforeClass
    protected void beforeClass() throws Exception {
        destPath = String.format("/home/%s/%s/%s", SYSTEM_OWNER, UUID.randomUUID(), LOCAL_BINARY_FILE_NAME);
        httpUri = new URI("https://httpd:8443/public/test_upload.bin");
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
        when(logicalFile.getStatus()).thenReturn(STAGING_QUEUED.name());

        return logicalFile;
    }

    @Test (expectedExceptions = SchedulerException.class)
    public void testEnqueueStagingTaskHandlesSchedulerException() throws SchedulerException, IOException, TenantException, FileProcessingException {
        StagingTransferTask stagingTransferTask = mock(StagingTransferTask.class);
        doCallRealMethod().when(stagingTransferTask).enqueueStagingTask(any(LogicalFile.class), anyString());
        doNothing().when(stagingTransferTask).updateLogicalFileAndSwallowException(any(LogicalFile.class));
        when(stagingTransferTask.callTransferClient(any(LogicalFile.class), anyString())).thenReturn(null);

        LogicalFile file = getMockLogicalFile();
        stagingTransferTask.enqueueStagingTask(file, SYSTEM_OWNER);
        assertEquals(file.getStatus(), STAGING_FAILED.name());
    }

    @Test (expectedExceptions = IOException.class)
    public void testEnqueueStagingTaskHandlesIOException() throws SchedulerException, IOException, TenantException, FileProcessingException {
        StagingTransferTask stagingTransferTask = mock(StagingTransferTask.class);
        doCallRealMethod().when(stagingTransferTask).enqueueStagingTask(any(LogicalFile.class), anyString());
        doNothing().when(stagingTransferTask).updateLogicalFileAndSwallowException(any(LogicalFile.class));
        when(stagingTransferTask.callTransferClient(any(LogicalFile.class), anyString())).thenThrow(IOException.class);

        LogicalFile file = getMockLogicalFile();
        stagingTransferTask.enqueueStagingTask(file, SYSTEM_OWNER);
        assertEquals(file.getStatus(), STAGING_FAILED.name());
    }

    @Test (expectedExceptions = TenantException.class)
    public void testEnqueueStagingTaskHandlesTenantException() throws SchedulerException, IOException, TenantException, FileProcessingException {
        StagingTransferTask stagingTransferTask = mock(StagingTransferTask.class);
        doCallRealMethod().when(stagingTransferTask).enqueueStagingTask(any(LogicalFile.class), anyString());
        doNothing().when(stagingTransferTask).updateLogicalFileAndSwallowException(any(LogicalFile.class));
        when(stagingTransferTask.callTransferClient(any(LogicalFile.class), anyString())).thenThrow(TenantException.class);

        LogicalFile file = getMockLogicalFile();
        stagingTransferTask.enqueueStagingTask(file, SYSTEM_OWNER);
        assertEquals(file.getStatus(), STAGING_FAILED.name());
    }



}
