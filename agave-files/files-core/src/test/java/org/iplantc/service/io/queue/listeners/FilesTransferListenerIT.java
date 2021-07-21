package org.iplantc.service.io.queue.listeners;

import com.fasterxml.jackson.databind.JsonNode;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.exceptions.MessageProcessingException;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.exceptions.LogicalFileException;
import org.iplantc.service.io.model.JSONTestDataUtil;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.mockito.InOrder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.fail;

@Test(singleThreaded = true, groups = {"integration.listeners.filesTransferListener"}, dependsOnGroups = "integration.clients.transferService")
public class FilesTransferListenerIT extends BaseTestCase {
    private StorageSystem storageSystem;
    private LogicalFile file;
    private final LogicalFileDao logicalFileDao = new LogicalFileDao();
    URI srcURI;
    String destPath;

    @BeforeClass
    public void beforeClass() throws Exception {
        System.out.println("Starting FilesTransferListenerIT");
        jtd = JSONTestDataUtil.getInstance();
        clearQueues();
        clearLogicalFiles();
        clearSystems();

        initSystems();

        destPath = String.format("/home/%s/%s/%s", SYSTEM_OWNER, UUID.randomUUID(), LOCAL_BINARY_FILE_NAME);
        srcURI = URI.create("http://httpbin:8000/stream-bytes/32768");

        storageSystem = systemDao.getUserStorageSystem(SYSTEM_OWNER, "sftp.example.com");

        //create test directories
        storageSystem.getRemoteDataClient().mkdirs(destPath);

    }

    @BeforeMethod
    public void beforeMethod() {
        clearQueues();
        clearLogicalFiles();

        file = new LogicalFile(SYSTEM_OWNER, storageSystem, srcURI, destPath);
        LogicalFileDao.persist(file);
    }

    @AfterClass
    public void afterClass() throws Exception {
        clearQueues();
        clearLogicalFiles();
        clearSystems();
    }

    private List<String> getNotificationEvents() {
        return Arrays.stream(FilesTransferListener.TransferTaskEventType.values())
                .map(val -> val.getEventName()).collect(Collectors.toList());
    }


    private void pushNotification(String body) {
        try {
            MessageQueueClient client = MessageClientFactory.getMessageClient();
            client.push(Settings.FILES_STAGING_TOPIC, Settings.FILES_STAGING_QUEUE, body);
        } catch (MessagingException e) {
            fail("Failed to push notification " + body + " to messaging queue", e);
        }
    }

    /**
     * Create TransferTask notifications and push them onto the FILES_STAGING_QUEUE
     */
    private void pushAllNotifications(List<String> events) throws IOException {
        JsonNode tt = getTransferTask(file, "QUEUED");

        for (String event : events) {
            String body = getNotification(tt, event).toString();
            pushNotification(body);
        }
    }


    /**
     * Create mock of the FilesTransferListener
     *
     * @return
     * @throws MessageProcessingException
     * @throws IOException
     * @throws MessagingException
     * @throws LogicalFileException
     * @throws RemoteDataException
     * @throws RemoteCredentialException
     */
    public FilesTransferListener getMockFilesTransferListener() throws MessageProcessingException, IOException, MessagingException, RemoteDataException, RemoteCredentialException {
        FilesTransferListener listener = mock(FilesTransferListener.class);
        doCallRealMethod().when(listener).run();
        doCallRealMethod().when(listener).getMessageClient();
        doCallRealMethod().when(listener).processTransferNotification(any(JsonNode.class));
        when(listener.lookupLogicalFileByUrl(file.getSourceUri(), file.getTenantId())).thenReturn(file);
        doCallRealMethod().when(listener).updateDestinationLogicalFile(anyString(), anyString(), any(), anyString());
        when(listener.getSystemById(anyString(), anyString())).thenReturn(storageSystem);
        doNothing().when(listener).persistLogicalFile(any(LogicalFile.class));
        doNothing().when(listener).updateTransferStatus(any(LogicalFile.class), any(StagingTaskStatus.class), anyString());

        return listener;
    }


    @Test(priority = 0)
    public void testRunUpdatesStatus() throws MessagingException, MessageProcessingException, IOException, LogicalFileException, RemoteDataException, RemoteCredentialException {
        FilesTransferListener listener = getMockFilesTransferListener();
        doNothing().when(listener).updateSourceLogicalFile(anyString(), anyString(), any(FilesTransferListener.TransferTaskEventType.class), anyString());
        doNothing().when(listener).updateDestinationLogicalFile(anyString(), anyString(), any(FilesTransferListener.TransferTaskEventType.class), anyString());


        try {
            ExecutorService executor = Executors.newCachedThreadPool();

            executor.execute(listener);

            //push all notifications to queue
            pushAllNotifications(getNotificationEvents());

            executor.awaitTermination(50, TimeUnit.MILLISECONDS);

            InOrder verifyOrder = inOrder(listener);

            List<FilesTransferListener.TransferTaskEventType> eventTypes = List.of(FilesTransferListener.TransferTaskEventType.TRANSFERTASK_CREATED, FilesTransferListener.TransferTaskEventType.TRANSFERTASK_ASSIGNED,
                    FilesTransferListener.TransferTaskEventType.TRANSFERTASK_FINISHED);


            for (FilesTransferListener.TransferTaskEventType eventType : eventTypes) {
                verifyOrder.verify(listener).updateSourceLogicalFile(file.getSourceUri(), SYSTEM_OWNER, eventType, file.getTenantId());
                if (eventType.isTerminal()) {
                    verifyOrder.verify(listener).updateDestinationLogicalFile("agave://" + file.getSystem() + "/" + file.getPath(), SYSTEM_OWNER, eventType, file.getTenantId());
                }
            }

            executor.shutdown();
        } catch (Exception e) {
            fail("Exceptions should be swallowed.");
        }
    }

    @Test(dependsOnMethods = "testRunUpdatesStatus", priority = 1)
    public void testRun() throws RemoteCredentialException, MessageProcessingException, LogicalFileException, RemoteDataException, MessagingException, IOException {
        FilesTransferListener mockListener = getMockFilesTransferListener();

        try {
            ExecutorService executor = Executors.newCachedThreadPool();
            executor.execute(mockListener);

            List<String> events = List.of(
                    "transfertask.notification",
                    "transfertask.updated",
                    "transfertask.unknown"
            );

            //push all notifications to queue
            pushAllNotifications(events);

            executor.awaitTermination(50, TimeUnit.MILLISECONDS);

            verify(mockListener, never()).updateTransferStatus(any(LogicalFile.class), any(StagingTaskStatus.class), anyString());
            verify(mockListener, never()).updateDestinationLogicalFile(anyString(), anyString(), any(), anyString());

            executor.shutdown();
        } catch (Exception e) {
            fail("Exceptions should be swallowed.");
        }
    }

    @Test(priority = 2)
    public void testMissingSourceLogicalFileRejectsMessage() throws RemoteCredentialException, MessageProcessingException, LogicalFileException, RemoteDataException, MessagingException, IOException {
        FilesTransferListener mockListener = getMockFilesTransferListener();

        try {
            ExecutorService executor = Executors.newCachedThreadPool();
            executor.execute(mockListener);

            LogicalFile missingSourceFile = new LogicalFile(SYSTEM_OWNER, storageSystem, URI.create(MISSING_FILE), destPath);

            pushNotification(getNotification(getTransferTask(missingSourceFile, "CREATED"), "transfertask.created").toString());

            executor.awaitTermination(100, TimeUnit.MILLISECONDS);

            executor.shutdown();
        } catch (Exception e) {
            fail("Exceptions should be swallowed.");
        }
    }

    @Test(priority = 3)
    public void testUpdateDestinationLogicalFile() throws RemoteCredentialException, MessageProcessingException, LogicalFileException, RemoteDataException, MessagingException, IOException {
        FilesTransferListener listener = getMockFilesTransferListener();
        doCallRealMethod().when(listener).updateDestinationLogicalFile(anyString(), anyString(), any(), anyString());

        LogicalFile expectedDestLogicalFile = new LogicalFile(SYSTEM_OWNER, storageSystem, destPath);
        expectedDestLogicalFile.setSourceUri(file.getPath());
        expectedDestLogicalFile.setStatus(FileEventType.CREATED.name());
        when(listener.lookupLogicalFileByUrl(destPath, file.getTenantId())).thenReturn(expectedDestLogicalFile);
        listener.updateDestinationLogicalFile(destPath, SYSTEM_OWNER, FilesTransferListener.TransferTaskEventType.TRANSFERTASK_FINISHED, file.getTenantId());
        verify(listener, times(1)).persistLogicalFile(expectedDestLogicalFile);

    }

}