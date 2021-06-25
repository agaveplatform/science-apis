package org.iplantc.service.io.queue.listeners;

import com.fasterxml.jackson.databind.JsonNode;
import com.surftools.BeanstalkClientImpl.ClientImpl;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.exceptions.MessageProcessingException;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.exceptions.LogicalFileException;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.mockito.InOrder;
import org.testng.annotations.*;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.iplantc.service.io.model.enumerations.StagingTaskStatus.*;
import static org.iplantc.service.io.model.enumerations.StagingTaskStatus.STAGING_FAILED;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@Test(groups = {"integration"})
public class FilesTransferListenerIT extends BaseTestCase {
    private StorageSystem storageSystem;
    private LogicalFile file;
    private LogicalFileDao logicalFileDao = new LogicalFileDao();
    URI srcURI;
    String destPath;

    @BeforeClass
    public void setUp() throws Exception {
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
    public void beforeMethod() throws Exception {
        clearQueues();
        clearLogicalFiles();

        file = new LogicalFile(SYSTEM_OWNER, storageSystem, srcURI, destPath);
        logicalFileDao.persist(file);
    }

    @AfterClass
    public void afterClass() throws Exception {
        clearQueues();
        clearLogicalFiles();
        clearSystems();
    }

    /**
     * Flushes the messaging tube of any and all existing jobs.
     */
    public void clearQueues() {
        ClientImpl client = null;

        // drain the message queue
        client = new ClientImpl(Settings.MESSAGING_SERVICE_HOST,
                Settings.MESSAGING_SERVICE_PORT);

        for (String tube : client.listTubes()) {
            try {
                client.watch(tube);
                client.useTube(tube);
                client.kick(Integer.MAX_VALUE);

                com.surftools.BeanstalkClient.Job beanstalkJob = null;
                do {
                    try {
                        beanstalkJob = client.peekReady();
                        if (beanstalkJob != null)
                            client.delete(beanstalkJob.getJobId());
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                } while (beanstalkJob != null);
                do {
                    try {
                        beanstalkJob = client.peekBuried();
                        if (beanstalkJob != null)
                            client.delete(beanstalkJob.getJobId());
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                } while (beanstalkJob != null);
                do {
                    try {
                        beanstalkJob = client.peekDelayed();

                        if (beanstalkJob != null)
                            client.delete(beanstalkJob.getJobId());
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                } while (beanstalkJob != null);

            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                try {
                    client.ignore(tube);
                } catch (Throwable e) {
                }

            }
        }
        try {
            client.close();
        } catch (Throwable e) {
        }
        client = null;
    }

    private Object[][] getMessageTypeToStagingTaskStatusArray() {
        return new Object[][]{
                {"transfertask.created", STAGING},
                {"transfertask.assigned", STAGING_QUEUED},
                {"transfer.completed", STAGING_COMPLETED},
                {"transfertask.finished", STAGING_COMPLETED},
                {"transfertask.failed", STAGING_FAILED},
                {"transfer.failed", STAGING_FAILED}
        };
    }

    private List<String> getNotificationEvents() {
        return List.of(
                "transfertask.created",
                "transfertask.assigned",
                "transfer.completed",
                "transfertask.finished",
                "transfertask.failed",
                "transfer.failed"
        );
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
    public FilesTransferListener getMockFilesTransferListener() throws MessageProcessingException, IOException, MessagingException, LogicalFileException, RemoteDataException, RemoteCredentialException {
        FilesTransferListener mockListener = mock(FilesTransferListener.class);
        doCallRealMethod().when(mockListener).run();
        doCallRealMethod().when(mockListener).getMessageClient();
        doCallRealMethod().when(mockListener).processTransferNotification(any(JsonNode.class));
        when(mockListener.lookupLogicalFileByUrl(file.getSourceUri(), file.getTenantId())).thenReturn(file);
        when(mockListener.updateDestinationLogicalFile(any(LogicalFile.class), anyString(), anyString())).thenCallRealMethod();
        when(mockListener.getSystemById(anyString(), anyString(), anyString())).thenReturn(storageSystem);
        doNothing().when(mockListener).persistLogicalFile(any(LogicalFile.class));
        doNothing().when(mockListener).updateTransferStatus(any(LogicalFile.class), any(StagingTaskStatus.class), anyString());

        return mockListener;
    }


    @Test
    public void testRunUpdatesStatus() throws MessagingException, MessageProcessingException, IOException, LogicalFileException, RemoteDataException, RemoteCredentialException {
        FilesTransferListener mockListener = getMockFilesTransferListener();

        try {
            ExecutorService executor = Executors.newCachedThreadPool();

            executor.execute(mockListener);

            //push all notifications to queue
            pushAllNotifications(getNotificationEvents());

            executor.awaitTermination(50, TimeUnit.MILLISECONDS);
            Object[][] events = getMessageTypeToStagingTaskStatusArray();

            InOrder verifyOrder = inOrder(mockListener);

            for (Object[] event : events) {
                verifyOrder.verify(mockListener).updateTransferStatus(file, StagingTaskStatus.valueOf(event[1].toString()), SYSTEM_OWNER);
                if (StagingTaskStatus.valueOf(event[1].toString()).equals(STAGING_COMPLETED.name())) {
                    //updateDestinationLogicalFile should be called for transfer.completed and transfertask.finished)
                    verify(mockListener, times(2)).updateDestinationLogicalFile(any(LogicalFile.class), anyString(), anyString());
                }
            }

            executor.shutdown();
        } catch (Exception e) {
            if (e.getClass().equals(MessagingException.class)) {
                fail("Exceptions should be swallowed.");
            }
        }
    }

    @Test(dependsOnMethods = "testRunUpdatesStatus")
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
            verify(mockListener, never()).updateDestinationLogicalFile(any(LogicalFile.class), anyString(), anyString());

            executor.shutdown();
        } catch (Exception e) {
            if (e.getClass().equals(MessagingException.class)) {
                fail("Exceptions should be swallowed.");
            }
        }
    }

    @Test
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
            fail("No exception should be thrown");
            if (e.getClass().equals(MessagingException.class)) {
                fail("Exceptions should be swallowed.");
            }
        }
    }

}