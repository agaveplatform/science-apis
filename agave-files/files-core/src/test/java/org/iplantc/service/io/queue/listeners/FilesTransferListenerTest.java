package org.iplantc.service.io.queue.listeners;

import com.fasterxml.jackson.databind.JsonNode;
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
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import static org.iplantc.service.io.model.enumerations.StagingTaskStatus.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


@Test(groups = {"integration"})
public class FilesTransferListenerTest extends BaseTestCase {
    private String destPath;
    private URI httpUri;

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
        LogicalFile logicalFile = mock(LogicalFile.class);
        RemoteSystem system = getMockRemoteSystem();
        when(logicalFile.getSystem()).thenReturn(system);
        when(logicalFile.getSourceUri()).thenReturn(httpUri.toString());
        when(logicalFile.getPath()).thenReturn(destPath);
        when(logicalFile.getUuid()).thenReturn(new AgaveUUID(UUIDType.FILE).toString());
        when(logicalFile.getTenantId()).thenReturn("foo.tenant");
        when(logicalFile.getStatus()).thenReturn(StagingTaskStatus.STAGING_QUEUED.name());

        return logicalFile;
    }

    protected RemoteSystem getMockRemoteSystem(){
        RemoteSystem system = mock(RemoteSystem.class);
        when(system.getSystemId()).thenReturn(UUID.randomUUID().toString());
        return system;
    }

    protected RemoteDataClient getMockRemoteDataClient(boolean bolPathDoesExist) throws IOException, RemoteDataException {
        RemoteDataClient mockClient = mock(RemoteDataClient.class);
        when(mockClient.doesExist(anyString())).thenReturn(bolPathDoesExist);
        return mockClient;
    }

    private FilesTransferListener getMockFilesTransferListenerInstance() {
        return mock(FilesTransferListener.class);
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
            Message msg = new Message(24, getNotification(getTransferTask(logicalFile, "CREATED"), "transfertask.created").toString());
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

        listener.processTransferNotification(getNotification(getTransferTask(logicalFile, "CREATED"), "transfertask.created"));
    }

    @Test(expectedExceptions = MessageProcessingException.class, expectedExceptionsMessageRegExp = "Unable to update transfer status*")
    public void executeProcessTransferNotificationHandlesLogicalFileException() throws Exception {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        LogicalFile logicalFile = getMockLogicalFile();

        doCallRealMethod().when(logicalFile).addContentEvent(any(FileEvent.class));
        doCallRealMethod().when(listener).processTransferNotification(any());
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(logicalFile);
        doNothing().when(listener).updateTransferStatus(any(LogicalFile.class), any(StagingTaskStatus.class), anyString());
        when(listener.updateDestinationLogicalFile(anyString(), anyString(), anyString())).
                thenThrow(LogicalFileException.class);

        listener.processTransferNotification(getNotification(getTransferTask(logicalFile, "COMPLETED"), "transfer.completed"));
    }


    @DataProvider
    private Object[][] executeProcessTransferNotificationUpdateStatusProvider() {
        return new Object[][]{
                {"transfertask.assigned", STAGING_QUEUED},
                {"transfertask.created", STAGING},
                {"transfertask.completed", STAGING_COMPLETED},
                {"transfertask.failed", STAGING_FAILED}
        };
    }

    @Test(dataProvider = "executeProcessTransferNotificationUpdateStatusProvider")
    public void executeProcessTransferNotification(String transferStatus, StagingTaskStatus stagingStatus) {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();

        try {
            LogicalFile logicalFile = getMockLogicalFile();

            when(listener.lookupLogicalFileByUrl(logicalFile.getSourceUri(), logicalFile.getTenantId())).thenReturn(logicalFile);
            doCallRealMethod().when(listener).processTransferNotification(any());
            doNothing().when(listener).updateTransferStatus(any(LogicalFile.class), any(StagingTaskStatus.class), anyString());
            when(listener.updateDestinationLogicalFile(anyString(), anyString(), anyString())).thenReturn(logicalFile);

            JsonNode jsonNotification = getNotification(getTransferTask(logicalFile, transferStatus), transferStatus);

            listener.processTransferNotification(jsonNotification);
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
            when(listener.updateDestinationLogicalFile(anyString(), anyString(), anyString())).thenReturn(logicalFile);

            JsonNode jsonNotification = getNotification(getTransferTask(logicalFile, "COMPLETED"), "transfer.completed");
            listener.processTransferNotification(jsonNotification);
            verify(listener, times(1)).
                    updateTransferStatus(logicalFile, STAGING_COMPLETED, logicalFile.getOwner());
            verify(listener, times(1)).
                    updateDestinationLogicalFile(eq(logicalFile.getPath()), eq(logicalFile.getOwner()), eq(logicalFile.getTenantId()));

        } catch (Exception e) {
            fail("No exception should be thrown for valid transfer notification", e);
        }
    }

    @Test (expectedExceptions = MessageProcessingException.class)
    public void executeProcessTransferNotificationHandlesUnknownTransferMessageType() throws Exception {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();

        LogicalFile logicalFile = getMockLogicalFile();

        when(listener.lookupLogicalFileByUrl(logicalFile.getSourceUri(), logicalFile.getTenantId())).thenReturn(logicalFile);
        doCallRealMethod().when(listener).processTransferNotification(any());

        JsonNode jsonNotification = getNotification(getTransferTask(logicalFile, "UNKNOWN"), "transfertask.unknown");
        listener.processTransferNotification(jsonNotification);
    }

    @DataProvider
    private Object[][] executeProcessTransferNotificationProvider() {
        return new Object[][]{
                {"transfertask.notification"},
                {"transfertask.updated"}
        };
    }

    @Test (dataProvider = "executeProcessTransferNotificationProvider")
    public void executeProcessTransferNotificationDoesntUpdateForTransferMessageTypesWithoutMatchingLegacyStatus(String transferMessageType) throws LogicalFileException, MessageProcessingException, IOException, RemoteDataException, RemoteCredentialException {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();

        LogicalFile logicalFile = getMockLogicalFile();

        when(listener.lookupLogicalFileByUrl(logicalFile.getSourceUri(), logicalFile.getTenantId())).thenReturn(logicalFile);
        doCallRealMethod().when(listener).processTransferNotification(any());

        JsonNode jsonNotification = getNotification(getTransferTask(logicalFile, "CREATED"), transferMessageType);
        listener.processTransferNotification(jsonNotification);

        verify(listener, never()).updateTransferStatus(any(LogicalFile.class), any(StagingTaskStatus.class), anyString());
        verify(listener, never()).updateDestinationLogicalFile(anyString(), anyString(), anyString());
    }

    @Test
    public void updateDestinationLogicalFileCreatesLogicalFileIfDoesntExist() throws LogicalFileException, RemoteCredentialException, RemoteDataException, IOException {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();

        LogicalFile logicalFile = getMockLogicalFile();
        RemoteSystem mockRemoteSystem = getMockRemoteSystem();
        RemoteDataClient mockClient = getMockRemoteDataClient(true);

        when(mockRemoteSystem.getRemoteDataClient()).thenReturn(mockClient);
        when(logicalFile.getSystem()).thenReturn(mockRemoteSystem);

        when(listener.updateDestinationLogicalFile(anyString(), anyString(), anyString())).thenCallRealMethod();
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(null);
        when(listener.getSystemById(anyString(), anyString())).thenReturn(mockRemoteSystem);
        doNothing().when(listener).persistLogicalFile(any(LogicalFile.class));

        LogicalFile destFile = listener.updateDestinationLogicalFile(destPath, SYSTEM_OWNER, logicalFile.getTenantId());

        assertEquals(destFile.getStatus(), FileEventType.CREATED.name(), "Newly created dest LogicalFile should have CREATED status.");
        assertEquals(destFile.getSourceUri(), String.format("agave://%s/%s", logicalFile.getSystem(), logicalFile.getPath()), "Source URI for the new dest LogicalFile should match the source LogicalFile");
        verify(listener, times(1)).persistLogicalFile(any(LogicalFile.class));
    }

    @Test
    public void updateDestinationLogicalFileDoesntCreateLogicalFileIfDestSystemDoesntExistAndPathDoesntExistOnSrcSystem() throws RemoteCredentialException, IOException, LogicalFileException, RemoteDataException {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();

        LogicalFile logicalFile = getMockLogicalFile();
        RemoteSystem mockRemoteSystem = getMockRemoteSystem();
        RemoteDataClient mockClient = getMockRemoteDataClient(false);

        when(mockRemoteSystem.getRemoteDataClient()).thenReturn(mockClient);
        when(logicalFile.getSystem()).thenReturn(mockRemoteSystem);

        when(listener.updateDestinationLogicalFile(anyString(), anyString(), anyString())).thenCallRealMethod();
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(null);
        when(listener.getSystemById(anyString(), anyString())).thenReturn(null);

        listener.updateDestinationLogicalFile(destPath, SYSTEM_OWNER, logicalFile.getTenantId());

        verify(listener, never()).persistLogicalFile(any(LogicalFile.class));
    }

    @Test
    public void updateDestinationLogicalFileDoesntCreateLogicalFileIfDestSystemDoesntExistButPathDoesExistOnSrcSystem() throws RemoteCredentialException, IOException, LogicalFileException, RemoteDataException {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();

        LogicalFile logicalFile = getMockLogicalFile();
        RemoteSystem mockRemoteSystem = getMockRemoteSystem();
        RemoteDataClient mockClient = getMockRemoteDataClient(true);

        when(mockRemoteSystem.getRemoteDataClient()).thenReturn(mockClient);
        when(logicalFile.getSystem()).thenReturn(mockRemoteSystem);

        when(listener.updateDestinationLogicalFile(anyString(), anyString(), anyString())).thenCallRealMethod();
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(null);
        when(listener.getSystemById(anyString(), anyString())).thenReturn(null);
        doNothing().when(listener).persistLogicalFile(any(LogicalFile.class));

        LogicalFile destFile = listener.updateDestinationLogicalFile(destPath, SYSTEM_OWNER, logicalFile.getTenantId());

        assertEquals(destFile.getStatus(), FileEventType.CREATED.name(), "Newly created dest LogicalFile should have CREATED status.");
        assertEquals(destFile.getSourceUri(), String.format("agave://%s/%s", logicalFile.getSystem(), logicalFile.getPath()), "Source URI for the new dest LogicalFile should match the source LogicalFile");
        verify(listener, times(1)).persistLogicalFile(any(LogicalFile.class));
    }


    @Test void updateDestinationLogicalFileUpdatesExistingLogicalFile() throws RemoteCredentialException, IOException, LogicalFileException, RemoteDataException {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();

        LogicalFile logicalFile = getMockLogicalFile();
        LogicalFile destLogicalFile = mock(LogicalFile.class);
        doNothing().when(destLogicalFile).addContentEvent(any(FileEventType.class), anyString());

        when(listener.updateDestinationLogicalFile(anyString(), anyString(), anyString())).thenCallRealMethod();
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(destLogicalFile );
        doNothing().when(listener).persistLogicalFile(any(LogicalFile.class));

        listener.updateDestinationLogicalFile(destPath, SYSTEM_OWNER, logicalFile.getTenantId());

        verify(destLogicalFile, times(1)).addContentEvent(FileEventType.OVERWRITTEN, SYSTEM_OWNER);
    }

}
