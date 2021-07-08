package org.iplantc.service.io.queue.listeners;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.common.exceptions.MessageProcessingException;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.notification.queue.messaging.NotificationMessageBody;
import org.iplantc.service.notification.queue.messaging.NotificationMessageContext;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.iplantc.service.io.BaseTestCase.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.fail;


@Test(groups = {"integration"})
public class FilesTransferListenerTest  {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private String destPath;
    private URI httpUri;

    @BeforeClass
    protected void beforeClass() throws Exception {
        destPath = String.format("/home/%s/%s/%s", SYSTEM_OWNER, UUID.randomUUID(), LOCAL_BINARY_FILE_NAME);
        httpUri = new URI("https://httpd:8443/public/test_upload.bin");
    }

    @AfterClass
    protected void afterClass() throws Exception {
//        clearSystems();
//        clearLogicalFiles();
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

    /**
     * Create notification for a given TransferTask event
     * @param transferTask the body of the notification message
     * @param messageType the TranfserTask event to create notification for
     * @return {@link JsonNode} of the notification message
     * @throws IOException
     */
    protected JsonNode getNotification(JsonNode transferTask, String messageType) throws IOException {
        NotificationMessageContext messageBodyContext = new NotificationMessageContext(
                messageType, transferTask.toString(), transferTask.get("uuid").asText());

        NotificationMessageBody messageBody = new NotificationMessageBody(
                transferTask.get("uuid").asText(), transferTask.get("owner").asText(), transferTask.get("tenant_id").asText(),
                messageBodyContext);

        return objectMapper.readTree(messageBody.toJSON());
    }

    protected LogicalFile getMockLogicalFile(){
        LogicalFile logicalFile = mock(LogicalFile.class);
        RemoteSystem system = getMockRemoteSystem();
        when(logicalFile.getSystem()).thenReturn(system);
        when(logicalFile.getOwner()).thenReturn(SYSTEM_OWNER);
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
    public void runHandlesMessagingExceptionOnPop() throws MessagingException {

        MessageQueueClient client = mock(MessageQueueClient.class);
        when(client.pop(any(), any())).thenThrow(new MessagingException("This pop should be handled and swallowed"));
        doThrow(new MessagingException("This pop should be handled and swallowed")).when(client).delete(any(), any(), any());
        doNothing().when(client).stop();

        FilesTransferListener listener = mock(FilesTransferListener.class);
        when(listener.isThreadInterrupted()).thenReturn(false, true);
        when(listener.getMessageClient()).thenReturn(client);
        doCallRealMethod().when(listener).run();

        try {
            listener.run();

            verify(client, times(2)).stop();
            verify(client, never()).delete(any(), any(), any());

            verify(listener, atLeastOnce()).setMessageClient(eq(null));
        }
        catch (Throwable e) {
            fail("NO exception should escape the run method", e);
        }
    }

    @Test
    public void runHandlesMessagingExceptionOnDelete() throws MessagingException, MessageProcessingException {

        MessageQueueClient client = mock(MessageQueueClient.class);
        when(client.pop(any(), any())).thenReturn(new Message(1L, "{}"));
        doThrow(new MessagingException("This pop should be handled and swallowed")).when(client).delete(any(), any(), any());
        doNothing().when(client).stop();

        FilesTransferListener listener = mock(FilesTransferListener.class);
        when(listener.isThreadInterrupted()).thenReturn(false, true);
        when(listener.getMessageClient()).thenReturn(client);
        doNothing().when(listener).processTransferNotification(any());
        doCallRealMethod().when(listener).run();

        try {
            listener.run();

            verify(listener, times(2)).isThreadInterrupted();
            verify(listener, atLeastOnce()).setMessageClient(eq(null));
            verify(client).pop(any(), any());
            verify(listener).processTransferNotification(any(JsonNode.class));
            verify(client, times(2)).delete(any(), any(), eq(1L));
            // after exception, client should be stopped and cleaned up.
            verify(client, times(2)).stop();
        }
        catch (Throwable e) {
            fail("NO exception should escape the run method", e);
        }
    }

    @Test
    public void runHandlesMessagingProcessingException() {

        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        // second value will break the loop
        when(listener.isThreadInterrupted()).thenReturn(false, true);
        MessageQueueClient client = mock(MessageQueueClient.class);
        Message msg = new Message(24, "{}");

        try {
            when(client.pop(any(), any())).thenReturn(msg);
            doNothing().when(client).reject(any(), any(), any(), any());

            when(listener.getMessageClient()).thenReturn(client);
            doThrow(new MessageProcessingException("This should be handled and swallowed")).when(listener).processTransferNotification(any());
            doCallRealMethod().when(listener).run();
        }
        catch (MessageProcessingException|MessagingException ignored) {}

        try {

            listener.run();

            // got a client
            verify(listener, atLeastOnce()).getMessageClient();
            // got a message
            verify(client).pop(any(), any());

            // deleted the message
            verify(client).delete(any(), any(), eq(msg.getId()));

            // deleted the message
            verify(client).stop();
            verify(listener).setMessageClient(null);
        }
        catch (Exception e) {
            fail("No exception should escape the run method", e);
        }
    }

    @DataProvider
    Object[][] knownTransferTaskEventTypesProvider() {
        List<Object[]> testData = new ArrayList<>();
        Arrays.stream(FilesTransferListener.TransferTaskEventType.values())
                .forEach(val -> testData.add(new Object[]{val}));
        return testData.toArray(new Object[][]{});
    }

    @Test(dataProvider = "knownTransferTaskEventTypesProvider")
    void processTransferNotificationAttemptsToProcessKnownTransferTaskEvents(FilesTransferListener.TransferTaskEventType transferTaskEvent) throws IOException, MessageProcessingException {
        LogicalFile logicalFile = getMockLogicalFile();
        JsonNode jsonTransferTask = getTransferTask(logicalFile, transferTaskEvent.getEventName());
        JsonNode jsonBody = getNotification(jsonTransferTask, transferTaskEvent.getEventName());

        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        doCallRealMethod().when(listener).processTransferNotification(any(JsonNode.class));
        doNothing().when(listener).updateSourceLogicalFile(any(), any(), any(FilesTransferListener.TransferTaskEventType.class), any());
        doNothing().when(listener).updateDestinationLogicalFile(any(), any(), any(FilesTransferListener.TransferTaskEventType.class), any());

        listener.processTransferNotification(jsonBody);

        verify(listener).updateSourceLogicalFile(jsonTransferTask.get("source").textValue(), jsonTransferTask.get("owner").textValue(), transferTaskEvent, logicalFile.getTenantId());

        verify(listener).updateDestinationLogicalFile(jsonTransferTask.get("dest").textValue(), jsonTransferTask.get("owner").textValue(), transferTaskEvent, logicalFile.getTenantId());
    }

    @DataProvider
    Object[][] invalidTransferTaskEventNamesProvider() {
        return new Object[][] {
                {"CREATED"},
                {UUID.randomUUID().toString()},
                {""},
                {null},
        };
    }

    @Test(dataProvider = "invalidTransferTaskEventNamesProvider")
    void processTransferNotificationIgnoresUnknownTransferTaskEvents(String transferTaskEventName) throws IOException, MessageProcessingException {
        LogicalFile logicalFile = getMockLogicalFile();
        JsonNode jsonTransferTask = getTransferTask(logicalFile, "CREATED");
        JsonNode jsonBody = getNotification(jsonTransferTask, transferTaskEventName);

        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        doCallRealMethod().when(listener).processTransferNotification(any(JsonNode.class));
        doNothing().when(listener).updateSourceLogicalFile(any(), any(), any(), any());
        doNothing().when(listener).updateDestinationLogicalFile(any(), any(), any(), any());

        listener.processTransferNotification(jsonBody);

        // no source or dest should be processed if the transfer task event type is unknown/unsupported
        verify(listener, never()).updateSourceLogicalFile(any(), any(), any(), any());
        verify(listener, never()).updateSourceLogicalFile(any(), any(), any(), any());
    }

    /**
     * Tests the logical file matching the transfer task destination is updated when a terminal event is received
     * @param transferTaskEventType the transfer task event coming in the message event field
     */
    @Test(dataProvider = "knownTransferTaskEventTypesProvider")
    void updateSourceLogicalFileUpdatesExistingLogicalFileOnKnownEvents(FilesTransferListener.TransferTaskEventType transferTaskEventType) throws FileNotFoundException, RemoteCredentialException, RemoteDataException {
        String srcUri = "agave://sftp.example.com//etc/hosts";
        LogicalFile srcLogicalFile = getMockLogicalFile();
        when(srcLogicalFile.getSourceUri()).thenReturn(srcUri);

        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(srcLogicalFile );
        doNothing().when(listener).persistLogicalFile(any(LogicalFile.class));

        doCallRealMethod().when(listener).updateSourceLogicalFile(anyString(), anyString(), any(), anyString());
        doNothing().when(listener).updateTransferStatus(any(LogicalFile.class), any(StagingTaskStatus.class), anyString());

        listener.updateSourceLogicalFile(srcUri, SYSTEM_OWNER, transferTaskEventType , srcLogicalFile.getTenantId());

        verify(listener).lookupLogicalFileByUrl(srcUri, srcLogicalFile.getTenantId());
        // logical file should always be updated as expected
        verify(listener).updateTransferStatus(any(LogicalFile.class), eq(transferTaskEventType.getStagingTaskStatus()), eq(SYSTEM_OWNER));
    }


//    /**
//     * Tests the logical file matching the transfer task destination is ignored when an unknown event is received
//     * @param transferStatus the transfer task event coming in the message event field
//     */
//    @Test(dataProvider = "updateSourceLogicalFileIgnoresExistingLogicalFileOnUnknownEventsProvider")
//    void updateSourceLogicalFileIgnoresExistingLogicalFileOnUnknownEvents(String transferStatus) {
//        String srcUri = "agave://sftp.example.com//etc/hosts";
//        LogicalFile srcLogicalFile = getMockLogicalFile();
//        when(srcLogicalFile.getSourceUri()).thenReturn(srcUri);
//
//        FilesTransferListener listener = getMockFilesTransferListenerInstance();
//        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(srcLogicalFile );
//        doNothing().when(listener).persistLogicalFile(any(LogicalFile.class));
//
//        doCallRealMethod().when(listener).updateSourceLogicalFile(anyString(), anyString(), any(), anyString());
//        doNothing().when(listener).updateTransferStatus(any(LogicalFile.class), any(StagingTaskStatus.class), anyString());
//
//        listener.updateSourceLogicalFile(srcUri, SYSTEM_OWNER, transferTaskEventType , srcLogicalFile.getTenantId());
//
//        verify(listener).lookupLogicalFileByUrl(eq(srcUri), eq(srcLogicalFile.getTenantId()));
//        // logical file status should never be updated on unknown event
//        verify(listener, never()).updateTransferStatus(any(LogicalFile.class), any(), any());
//    }

    /**
     * Tests updateDestinationLogicalFile fails silently when a {@link RuntimeException} is thrown looking up the
     * logical file.
     */
    @Test void updateSourceLogicalFailsSilentlyOnRuntimeError() throws FileNotFoundException, RemoteCredentialException, RemoteDataException {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        doCallRealMethod().when(listener).updateSourceLogicalFile(any(), any(), any(), any());
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenThrow(new RuntimeException("This should be swallowed"));

        doNothing().when(listener).persistLogicalFile(any(LogicalFile.class));

        String srcUri = "agave://sftp.example.com//etc/hosts";
        listener.updateSourceLogicalFile(srcUri, SYSTEM_OWNER, FilesTransferListener.TransferTaskEventType.TRANSFERTASK_FINISHED , TENANT_ID);

        verify(listener).lookupLogicalFileByUrl(eq(srcUri), eq(TENANT_ID));
        verify(listener, never()).updateTransferStatus(any(), any(), any());
    }

    /**
     * Tests updateDestinationLogicalFile does nothing when no logical file exists for the given transfer task dest
     */
    @Test void updateSourceLogicalDoesNothingOnNullLogicalFile() throws FileNotFoundException, RemoteCredentialException, RemoteDataException {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(null);
        doCallRealMethod().when(listener).updateSourceLogicalFile(any(), any(), any(), any());
        doNothing().when(listener).persistLogicalFile(any(LogicalFile.class));

        listener.updateSourceLogicalFile(httpUri.toString(), SYSTEM_OWNER, FilesTransferListener.TransferTaskEventType.TRANSFERTASK_FINISHED , TENANT_ID);

        verify(listener).lookupLogicalFileByUrl(eq(httpUri.toString()), eq(TENANT_ID));

        // no file to update, this should never be called
        verify(listener, never()).persistLogicalFile(any(LogicalFile.class));
    }



    @DataProvider
    Object[][] updateDestinationLogicalFileUpdatesExistingLogicalFileOnTerminalEventsProvider() {
        List<Object[]> testData = new ArrayList<>();
        Arrays.stream(FilesTransferListener.TransferTaskEventType.values())
                .forEach(val -> {
                    if (val.isTerminal()) {
                        testData.add(new Object[]{val});
                    }
                });
        return testData.toArray(new Object[][]{});
    }

    /**
     * Tests the logical file matching the transfer task destination is updated when a terminal event is received
     * @param transferTaskEventType the transfer task event coming in the message event field
     */
    @Test(dataProvider = "updateDestinationLogicalFileUpdatesExistingLogicalFileOnTerminalEventsProvider")
    void updateDestinationLogicalFileUpdatesExistingLogicalFileOnTerminalEvents(FilesTransferListener.TransferTaskEventType transferTaskEventType) throws FileNotFoundException, RemoteCredentialException, RemoteDataException {
        LogicalFile destLogicalFile = getMockLogicalFile();
        doNothing().when(destLogicalFile).addContentEvent(any(FileEventType.class), anyString());

        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(destLogicalFile );
        doNothing().when(listener).persistLogicalFile(any(LogicalFile.class));

        doCallRealMethod().when(listener).updateDestinationLogicalFile(anyString(), anyString(), eq(transferTaskEventType), anyString());

        listener.updateDestinationLogicalFile(destPath, SYSTEM_OWNER, transferTaskEventType , destLogicalFile.getTenantId());

        verify(listener).lookupLogicalFileByUrl(destPath, destLogicalFile.getTenantId());
        verify(destLogicalFile).addContentEvent(FileEventType.OVERWRITTEN, SYSTEM_OWNER);
        verify(listener).persistLogicalFile(any(LogicalFile.class));
    }

//    @DataProvider
//    Object[][] updateDestinationLogicalFileIgnoresExistingLogicalFileOnNonTerminalEventsProvider() {
//        return new Object[][]{
//                {"transfertask.assigned"},
//                {"transfertask.cancelled"},
//                {"transfertask.created"},
//                {"transfertask.deleted"},
//                {"transfertask.error"},
//                {"transfertask.notification"},
//                {"transfertask.paused"},
//                {"transfertask.finished"},
//                {"transfertask.updated"},
//
//                {"transfer.all"},
//                {"transfer.completed"},
//                {"transfer.failed"},
//                {"transfer.retry"},
//                {"transfer.streaming"},
//                {"transfer.unary"},
//        };
//    }
//
//    /**
//     * Tests the logical file matching the transfer task destination is ingnored when a non-terminal event is received
//     * @param transferStatus the transfer task event coming in the message event field
//     */
//    @Test( dataProvider = "updateDestinationLogicalFileIgnoresExistingLogicalFileOnNonTerminalEventsProvider" )
//    void updateDestinationLogicalFileIgnoresLogicalFileOnNonTerminalEvents(String transferStatus) {
//        LogicalFile destLogicalFile = getMockLogicalFile();
//        doNothing().when(destLogicalFile).addContentEvent(any(FileEventType.class), anyString());
//
//        FilesTransferListener listener = getMockFilesTransferListenerInstance();
//        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(destLogicalFile );
//        doNothing().when(listener).persistLogicalFile(any(LogicalFile.class));
//
//        doCallRealMethod().when(listener).updateDestinationLogicalFile(anyString(), anyString(), any(), anyString());
//
//        listener.updateDestinationLogicalFile(destPath, SYSTEM_OWNER, transferStatus , destLogicalFile.getTenantId());
//
//        verify(listener).lookupLogicalFileByUrl(eq(destPath), eq(destLogicalFile.getTenantId()));
//        verify(destLogicalFile).addContentEvent(eq(FileEventType.OVERWRITTEN), eq(SYSTEM_OWNER));
//        verify(listener).persistLogicalFile(any(LogicalFile.class));
//    }

    /**
     * Tests updateDestinationLogicalFile fails silently when a {@link RuntimeException} is thrown looking up the
     * logical file.
     */
    @Test void updateDestinationLogicalFailsSilentlyOnRuntimeError() throws FileNotFoundException, RemoteCredentialException, RemoteDataException {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenThrow(new RuntimeException("This should be swallowed"));
        doCallRealMethod().when(listener).updateDestinationLogicalFile(any(), any(), any(), any());
        doNothing().when(listener).persistLogicalFile(any(LogicalFile.class));

        listener.updateDestinationLogicalFile(destPath, SYSTEM_OWNER, FilesTransferListener.TransferTaskEventType.TRANSFERTASK_FAILED , TENANT_ID);

        verify(listener).lookupLogicalFileByUrl(eq(destPath), eq(TENANT_ID));

        // no file to update, this should never be called
        verify(listener, never()).persistLogicalFile(any(LogicalFile.class));
    }

    /**
     * Tests updateDestinationLogicalFile does nothing when no logical file exists for the given transfer task dest
     */
    @Test void updateDestinationLogicalDoesNothingOnNullLogicalFile() throws FileNotFoundException, RemoteCredentialException, RemoteDataException {
        FilesTransferListener listener = getMockFilesTransferListenerInstance();
        when(listener.lookupLogicalFileByUrl(anyString(), anyString())).thenReturn(null);
        doCallRealMethod().when(listener).updateDestinationLogicalFile(any(), any(), any(), any());
        doNothing().when(listener).persistLogicalFile(any(LogicalFile.class));

        listener.updateDestinationLogicalFile(destPath, SYSTEM_OWNER, FilesTransferListener.TransferTaskEventType.TRANSFERTASK_FINISHED , TENANT_ID);

        verify(listener).lookupLogicalFileByUrl(eq(destPath), eq(TENANT_ID));

        // no file to update, this should never be called
        verify(listener, never()).persistLogicalFile(any(LogicalFile.class));
    }
}
