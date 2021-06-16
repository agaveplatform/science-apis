package org.iplantc.service.io.queue;

import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.testng.annotations.Test;

import java.io.File;
import java.util.UUID;

import static org.iplantc.service.io.model.enumerations.StagingTaskStatus.STAGING_QUEUED;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class UploadRunnableTest extends BaseTestCase {
    private static final String strWebhookUrl = "";

    protected LogicalFile getMockLogicalFile() {
        RemoteSystem system = mock(RemoteSystem.class);
        when(system.getSystemId()).thenReturn(UUID.randomUUID().toString());

        LogicalFile logicalFile = mock(LogicalFile.class);
        when(logicalFile.getSystem()).thenReturn(system);
        when(logicalFile.getSourceUri()).thenReturn("https://httpd:8443/public/test_upload.bin");
        when(logicalFile.getPath()).thenReturn(LOCAL_BINARY_FILE_NAME);
        when(logicalFile.getUuid()).thenReturn(new AgaveUUID(UUIDType.FILE).toString());
        when(logicalFile.getTenantId()).thenReturn("foo.tenant");
        when(logicalFile.getStatus()).thenReturn(STAGING_QUEUED.name());
        when(logicalFile.getAgaveRelativePathFromAbsolutePath()).thenReturn("/home/nryan/test_upload.bin");

        return logicalFile;
    }

    protected UploadRunnable getMockUploadRunnable(LogicalFile logicalFile) throws RemoteDataException {
        UploadRunnable mockUploadRunnable = mock(UploadRunnable.class);

        File mockFile = mock(File.class);
        when(mockFile.getName()).thenReturn(LOCAL_BINARY_FILE_NAME);
        RemoteDataClient mockClient = mock(RemoteDataClient.class);

        when(mockUploadRunnable.getRemoteDataClient(any(LogicalFile.class))).thenReturn(mockClient);
        when(mockUploadRunnable.getDestinationLogicalFile()).thenReturn(logicalFile);
        when(mockUploadRunnable.getLocalFile()).thenReturn(mockFile);
        when(mockUploadRunnable.getUploadUsername()).thenReturn(SYSTEM_OWNER);
        when(mockUploadRunnable.getOriginalFileOwner()).thenReturn(SYSTEM_OWNER);
        doCallRealMethod().when(mockUploadRunnable).run();
        doCallRealMethod().when(mockUploadRunnable).setDestinationLogicalFile(any(LogicalFile.class));
        doNothing().when(mockUploadRunnable).updateLogicalFileStatusAndSwallowException(any(LogicalFile.class),
                any(StagingTaskStatus.class), anyString(), anyString());

        return mockUploadRunnable;
    }

    @Test
    public void testRunCreatesNotification() throws RemoteDataException, NotificationException {
        LogicalFile logicalFile = getMockLogicalFile();
        UploadRunnable mockUploadRunnable = getMockUploadRunnable(logicalFile);
        Notification mockNotification = mock(Notification.class);

        when(mockUploadRunnable.createSingleNotification(eq(logicalFile), any(FileEventType.class),
                anyString(), anyBoolean())).thenReturn(mockNotification);

        mockUploadRunnable.run();

        verify(mockUploadRunnable, times(1)).
                createSingleNotification(eq(logicalFile), eq(FileEventType.STAGING_COMPLETED),
                        eq(strWebhookUrl), eq(true));

        verify(mockUploadRunnable, times(1)).updateLogicalFileStatusAndSwallowException
                (eq(logicalFile), eq(StagingTaskStatus.STAGING_COMPLETED), anyString(), eq(SYSTEM_OWNER));

    }

    @Test
    public void testRunHandlesException() throws RemoteDataException, NotificationException {
        LogicalFile logicalFile = getMockLogicalFile();
        UploadRunnable mockUploadRunnable = getMockUploadRunnable(logicalFile);

        when(mockUploadRunnable.createSingleNotification(eq(logicalFile), any(FileEventType.class), anyString(),
                anyBoolean())).thenThrow(NotificationException.class);

        mockUploadRunnable.run();

        verify(mockUploadRunnable, times(1)).updateLogicalFileStatusAndSwallowException
                (eq(logicalFile), eq(StagingTaskStatus.STAGING_FAILED), anyString(), eq(SYSTEM_OWNER));
    }


}