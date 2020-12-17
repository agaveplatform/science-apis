package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.enumerations.TransferTaskEventType;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemException;
import org.iplantc.service.systems.model.StorageConfig;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.transfer.*;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("URLCopy Tests")
public class URLCopyIT extends BaseTestCase {
    protected File tmpFile = null;
    protected File tmpDir = null;

    protected RemoteDataClient client;
    protected StorageConfig storageConfig;
    protected StorageSystem destSystem;
    protected String credential;
    protected String salt;
    protected String DEST_FILENAME = "test_upload.txt.copy";
    protected boolean allowRelayTransfers;

    @BeforeAll
    protected void beforeSubclass() {
//        allowRelayTransfers = Settings.ALLOW_RELAY_TRANSFERS;
//        Settings.ALLOW_RELAY_TRANSFERS = true;

        JSONObject systemJson = getSystemJson(StorageProtocolType.SFTP);
        destSystem = StorageSystem.fromJSON(systemJson);
        destSystem.setOwner(SYSTEM_USER);

        String homeDir = destSystem.getStorageConfig().getHomeDir();
        homeDir = StringUtils.isEmpty(homeDir) ? "" : homeDir;
        destSystem.getStorageConfig().setHomeDir(homeDir + "/" + getClass().getSimpleName());
        storageConfig = destSystem.getStorageConfig();
        salt = destSystem.getSystemId() + storageConfig.getHost() +
                storageConfig.getDefaultAuthConfig().getUsername();

        SystemDao dao = new SystemDao();
        if (dao.findBySystemId(destSystem.getSystemId()) == null) {
            dao.persist(destSystem);
        }
    }

    @BeforeEach
    protected void beforeMethod() {
        FileUtils.deleteQuietly(new File(getLocalDownloadDir()));
        FileUtils.deleteQuietly(tmpFile);
    }

    @AfterAll
    protected void afterClass() throws Exception {
        try {
            FileUtils.deleteQuietly(new File(getLocalDownloadDir()));
            FileUtils.deleteQuietly(tmpFile);
            clearSystems();
        } finally {
            try {
                getClient().disconnect();
            } catch (Exception e) {
            }
        }

        try {
            if (destSystem != null) {
                getClient().authenticate();
                // remove test directory
                if (getClient().doesExist("")) {
                    getClient().delete("..");
                }
                assertFalse(getClient().doesExist(""), "Failed to clean up home directory after test.");
            }
        } catch (Exception e) {
            fail("Failed to clean up test home directory " + getClient().resolvePath("") + " after test method.", e);
        } finally {
            try {
                getClient().disconnect();
            } catch (Exception ignored) {}
        }

        Settings.ALLOW_RELAY_TRANSFERS = allowRelayTransfers;
    }

//    @DataProvider(name = "putFileProvider")
//    protected Object[][] putFileProvider() {
//        // expected result of copying LOCAL_BINARY_FILE to the remote system given the following dest
//        // paths
//        return new Object[][]{
//                // remote dest path,  expected result path,  exception?, message
//                {TRANSFER_DEST, TRANSFER_DEST, false, "Put file to empty (tmp) directory should result in a file with the same name on remote system."},
//                {"agave://sftp//", "agave://sftp//", false, "Put file to empty directory name should result in file with the same name on the home directory of the remote system."},
//                {"agave://sftp/", "agave://sftp/", false, "Put file to empty directory name should result in file with the same name on the home directory of the remote system."},
//                {LOCAL_TXT_FILE_NAME, LOCAL_TXT_FILE_NAME, false, "put local file to remote destination with different name should result in file with new name on remote system."},
//                {"", LOCAL_BINARY_FILE_NAME, false, "put local file to empty(home) directory name should result in file with same name on remote system"},
//        };
//    }
//
//    @DataProvider(name = "httpImportProvider")
//    protected Object[][] httpImportProvider() {
////        ArrayList<StorageProtocolType> testDestTypes = new ArrayList<>();
////        testDestTypes.add(StorageProtocolType.SFTP);
////
////        List<Object[]> testCases = new ArrayList<Object[]>();
////        for (StorageProtocolType destType : testDestTypes) {
////            testCases.add(new Object[]{URI.create("http://preview.agaveplatform.org/wp-content/themes/agave/images/favicon.ico"), destType, "Public URL copy to " + destType.name() + " should succeed", false});
////            testCases.add(new Object[]{URI.create("https://agaveplatform.org/wp-content/themes/agave/images/favicon.ico"), destType, "Public 301 redirected URL copy to " + destType.name() + " should succeed", false});
////            testCases.add(new Object[]{URI.create("https://avatars0.githubusercontent.com/u/785202"), destType, "Public HTTPS URL copy to " + destType.name() + " should succeed", false});
////            testCases.add(new Object[]{URI.create("http://docker.example.com:10080/public/" + DEST_FILENAME), destType, "Public URL copy to " + destType.name() + " on alternative port should succeed", false});
////            testCases.add(new Object[]{URI.create("https://docker.example.com:10443/public/" + DEST_FILENAME), destType, "Public HTTPS URL copy to " + destType.name() + " on alternative port should succeed", false});
////            testCases.add(new Object[]{URI.create("https://docker.example.com:10443/public/" + DEST_FILENAME + "?t=now"), destType, "Public URL copy to " + destType.name() + " should succeed", false});
////            testCases.add(new Object[]{URI.create("http://testuser:testuser@docker.example.com:10080/private/" + DEST_FILENAME), destType, "Public URL copy to " + destType.name() + " should succeed", false});
////
////            testCases.add(new Object[]{URI.create("http://docker.example.com:10080"), destType, "Public URL copy of no path to " + destType.name() + " should fail", true});
////            testCases.add(new Object[]{URI.create("http://docker.example.com:10080/"), destType, "Public URL copy of empty path to " + destType.name() + " should fail", true});
////            testCases.add(new Object[]{URI.create("http://docker.example.com:10080/" + MISSING_FILE), destType, "Missing file URL " + destType.name() + " should fail", true});
////            testCases.add(new Object[]{URI.create("http://testuser@docker.example.com:10080/private/test_upload.bin"), destType, "Protected URL missing password should fail auth and copy to " + destType.name() + " should fail", true});
////            testCases.add(new Object[]{URI.create("http://testuser:testotheruser@docker.example.com:10080/private/test_upload.bin"), destType, "Protected URL with bad credentials should fail auth and copy to " + destType.name() + " should fail", true});
////        }
//
//        StorageProtocolType destType = StorageProtocolType.SFTP;
//
//        return new Object[][]{
////                {URI.create("http://preview.agaveplatform.org/wp-content/themes/agave/images/favicon.ico"), destType, "Public URL copy to " + destType.name() + " should succeed", false},
////                {URI.create("https://agaveplatform.org/wp-content/themes/agave/images/favicon.ico"), destType, "Public 301 redirected URL copy to " + destType.name() + " should succeed", false},
//                {URI.create("https://avatars0.githubusercontent.com/u/785202"), destType, "Public HTTPS URL copy to " + destType.name() + " should succeed", false},
//                {URI.create("http://docker.example.com:10080/public/" + DEST_FILENAME), destType, "Public URL copy to " + destType.name() + " on alternative port should succeed", false},
//                {URI.create("https://docker.example.com:10443/public/" + DEST_FILENAME), destType, "Public HTTPS URL copy to " + destType.name() + " on alternative port should succeed", false},
//                {URI.create("https://docker.example.com:10443/public/" + DEST_FILENAME + "?t=now"), destType, "Public URL copy to " + destType.name() + " should succeed", false},
//                {URI.create("http://testuser:testuser@docker.example.com:10080/private/" + DEST_FILENAME), destType, "Public URL copy to " + destType.name() + " should succeed", false},
//
//                {URI.create("http://docker.example.com:10080"), destType, "Public URL copy of no path to " + destType.name() + " should fail", true},
//                {URI.create("http://docker.example.com:10080/"), destType, "Public URL copy of empty path to " + destType.name() + " should fail", true},
//                {URI.create("http://docker.example.com:10080/" + MISSING_FILE), destType, "Missing file URL " + destType.name() + " should fail", true},
//                {URI.create("http://testuser@docker.example.com:10080/private/test_upload.bin"), destType, "Protected URL missing password should fail auth and copy to " + destType.name() + " should fail", true},
//                {URI.create("http://testuser:testotheruser@docker.example.com:10080/private/test_upload.bin"), destType, "Protected URL with bad credentials should fail auth and copy to " + destType.name() + " should fail", true}
//
//        };
//    }
//
//    @DataProvider(name = "putFolderProvider", parallel = false)
//    protected Object[][] putFolderProvider() {
//        return new Object[][]{
//                // remote dest path,   expected result path,   exception?, message
//                {LOCAL_DIR_NAME, LOCAL_DIR_NAME, false, "put local file to remote home directory explicitly setting the identical name should result in folder with same name on remote system."},
//                {"somedir", "somedir", false, "put local file to remote home directory explicitly setting a new name should result in folder with new name on remote system."},
//                {"", LOCAL_DIR_NAME, false, "put local directory to empty (home) remote directory without setting a new name should result in folder with same name on remote system."},
//
//        };
//    }

    /**
     * Gets getClient() from current thread
     *
     * @return {@link RemoteDataClient} of current thread
     * @throws RemoteCredentialException if unable to retrieve the {@link RemoteDataClient} due to missing permissions
     * @throws RemoteDataException       if unable to retrieve the {@link RemoteDataClient}
     */
    protected RemoteDataClient getClient() {
        RemoteDataClient client = null;
        try {
            if (destSystem != null) {
                client = destSystem.getRemoteDataClient();
                client.updateSystemRoots(client.getRootDir(), destSystem.getStorageConfig().getHomeDir() + "/thread-" + Thread.currentThread().getId());
            }
        } catch (RemoteDataException | RemoteCredentialException e) {
            fail("Failed to get client", e);
        }
        return client;
    }

    protected String getLocalDownloadDir() {
        return LOCAL_DOWNLOAD_DIR + Thread.currentThread().getId();
    }

    RemoteDataClient getMockRemoteDataClientInstance(String path) throws IOException, RemoteDataException {
        RemoteDataClient mockRemoteDataClient = Mockito.mock(RemoteDataClient.class);
        when(mockRemoteDataClient.isThirdPartyTransferSupported()).thenReturn(false);
        when(mockRemoteDataClient.length(path)).thenReturn(new File(path).length());
        when(mockRemoteDataClient.resolvePath(path)).thenReturn(URI.create(path).getPath());
        doNothing().when(mockRemoteDataClient).get(anyString(), anyString(), any(RemoteTransferListener.class));

        return mockRemoteDataClient;
    }

    /**
     * Get the RemoteDataClient with settings specified in the corresponding StorageProtocolType json file
     *
     * @param type
     * @return
     * @throws Exception
     */
    private RemoteDataClient getRemoteDataClientFromSystemJson(StorageProtocolType type) throws Exception {
        JSONObject systemJson = getSystemJson(type);
        StorageSystem system = StorageSystem.fromJSON(systemJson);
        system.setOwner(SYSTEM_USER);

        RemoteDataClient client = system.getRemoteDataClient();
        client.updateSystemRoots(client.getRootDir(), system.getStorageConfig().getHomeDir() + "/thread-" + Thread.currentThread().getId());
        client.authenticate();
        return client;
    }

    /**
     * Reads the json defintion of a {@link StorageSystem} from disk
     * and returns as JSONObject.
     *
     * @param protocolType the protocol of the remote system. One sytem definition exists for each {@link StorageProtocolType}
     * @return the json representation of a storage system
     */
    protected JSONObject getSystemJson(StorageProtocolType protocolType) {
        try {
            String systemConfigFilename = String.format("%s/%s.example.com.json",
                    STORAGE_SYSTEM_TEMPLATE_DIR, protocolType.name().toLowerCase());

            InputStream in = null;

            File pfile = new File(systemConfigFilename);
            in = new FileInputStream(pfile.getAbsoluteFile());
            String json = IOUtils.toString(in, "UTF-8");

            return new JSONObject(json);

        } catch (JSONException | IOException e) {
            fail("Unable to read system json definition");
            return null;
        }
    }


    protected void clearSystems() {
        Session session = null;
        try {
            HibernateUtil.beginTransaction();
            session = HibernateUtil.getSession();
            session.clear();
            HibernateUtil.disableAllFilters();

            session.createQuery("delete RemoteSystem").executeUpdate();
            session.createQuery("delete BatchQueue").executeUpdate();
            session.createQuery("delete StorageConfig").executeUpdate();
            session.createQuery("delete LoginConfig").executeUpdate();
            session.createQuery("delete AuthConfig").executeUpdate();
            session.createQuery("delete SystemRole").executeUpdate();
            session.createQuery("delete CredentialServer").executeUpdate();
        } catch (HibernateException ex) {
            throw new SystemException(ex);
        } finally {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception ignored) {
            }
        }
    }

    public RemoteTransferListenerImpl getMockRemoteTransferListener(TransferTask transferTask, RetryRequestManager retryRequestManager) {
        RemoteTransferListenerImpl mockRemoteTransferListenerImpl = mock(RemoteTransferListenerImpl.class);
        when(mockRemoteTransferListenerImpl.getRetryRequestManager()).thenReturn(retryRequestManager);
        when(mockRemoteTransferListenerImpl.isCancelled()).thenReturn(false);
        when(mockRemoteTransferListenerImpl.getTransferTask()).thenReturn(transferTask);
        return mockRemoteTransferListenerImpl;
    }

    @Test
    @DisplayName("Test URLCopy relayTransfer")
    public void testUnaryCopy(Vertx vertx, VertxTestContext ctx) {
        try {
            allowRelayTransfers = Settings.ALLOW_RELAY_TRANSFERS;
            Settings.ALLOW_RELAY_TRANSFERS = true;

            Vertx mockVertx = mock(Vertx.class);
            RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);

            RemoteDataClient mockSrcRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_SRC);
            RemoteDataClient mockDestRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_DEST);

            TransferTask tt = _createTestTransferTask();
            tt.setId(1L);
            tt.setSource(TRANSFER_SRC);
            tt.setDest(TRANSFER_DEST);

            URLCopy mockCopy = mock(URLCopy.class);
            when(mockCopy.copy(any(TransferTask.class))).thenCallRealMethod();

            //remote transfer listener
            RemoteTransferListenerImpl mockRemoteTransferListenerImpl = getMockRemoteTransferListener(tt, mockRetryRequestManager);
            doReturn(mockRemoteTransferListenerImpl).when(mockCopy).getRemoteTransferListenerForTransferTask(tt);

            //first leg child transfer task
            TransferTask srcChildTransferTask = new TransferTask(
                    tt.getSource(),
                    "https://workers.prod.agaveplatform.org/" + new File(TRANSFER_SRC).getPath(),
                    tt.getOwner(),
                    tt.getUuid(),
                    tt.getRootTaskId());
            srcChildTransferTask.setTenantId(tt.getTenantId());
            srcChildTransferTask.setStatus(TransferStatusType.READ_STARTED);

            doNothing().when(mockCopy)._doPublishEvent(anyString(), any(JsonObject.class));

            //mock child remote transfer listener
            RemoteTransferListenerImpl mockChildRemoteTransferListenerImpl = getMockRemoteTransferListener(srcChildTransferTask, mockRetryRequestManager);
            when(mockCopy.getRemoteTransferListenerForTransferTask(srcChildTransferTask)).thenReturn(mockChildRemoteTransferListenerImpl);

            //second leg child transfer task
            TransferTask destChildTransferTask = new TransferTask(
                    "https://workers.prod.agaveplatform.org/" + new File(LOCAL_TXT_FILE).getPath(),
                    tt.getDest(),
                    tt.getOwner(),
                    tt.getParentTaskId(),
                    tt.getRootTaskId()
            );
            destChildTransferTask.setTenantId(tt.getTenantId());
            destChildTransferTask.setStatus(TransferStatusType.WRITE_STARTED);

            RemoteTransferListenerImpl mockDestChildRemoteTransferListenerImpl = getMockRemoteTransferListener(destChildTransferTask, mockRetryRequestManager);
            when(mockCopy.getRemoteTransferListenerForTransferTask(destChildTransferTask)).thenReturn(mockDestChildRemoteTransferListenerImpl);

            //Injecting the mocked arguments to the mocked URLCopy
            //Using InjectMocks annotation will not create a mock of URLCopy, which we need to pass in the mocked RemoteTransferListenerImpl
            PowerMockito.whenNew(URLCopy.class).withArguments(any(RemoteDataClient.class), any(RemoteDataClient.class), any(Vertx.class), any(RetryRequestManager.class)).thenReturn(mockCopy);
            TransferTask copiedTransfer = new URLCopy(mockSrcRemoteDataClient, mockDestRemoteDataClient, mockVertx, mockRetryRequestManager).copy(tt);

            ctx.verify(() -> {
                assertEquals(copiedTransfer.getStatus(), TransferStatusType.COMPLETED, "Expected successful copy to return TransferTask with COMPLETED status.");

                //check that events are published correctly using the RetryRequestManager
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.RELAY_READ_STARTED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.RELAY_READ_COMPLETED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.UPDATED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.RELAY_WRITE_STARTED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.RELAY_WRITE_COMPLETED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.COMPLETED.name()),
                        any(JsonObject.class), eq(2));

//                assertEquals(copiedTransfer.getAttempts(), tt.getAttempts() + 1, "TransferTask attempts should be incremented upon copy.");
//                assertEquals(copiedTransfer.getStatus(), TransferStatusType.COMPLETED, "TransferTask status should be set to COMPLETED upon successful transfer.");
//                assertEquals(copiedTransfer.getTotalFiles(), 1, "TransferTask total files be 1 upon successful transfer of a single file.");
//                assertEquals(copiedTransfer.getBytesTransferred(), 32768, "TransferTask total bytes transferred should be 32768, the size of the httpbin download name.");
//                assertTrue(copiedTransfer.getLastUpdated().isAfter(tt.getLastUpdated()),"TransferTask last updated should be updated after URLCopy completes.");
//                assertNotNull(copiedTransfer.getStartTime(), "TransferTask start time should be set by URLCopy.");
//                assertNotNull(copiedTransfer.getEndTime(), "TransferTask end time should be set by URLCopy.");
//                assertTrue(copiedTransfer.getStartTime().isAfter(tt.getCreated()),"TransferTask start time should be after created time.");
//                assertTrue(copiedTransfer.getEndTime().isAfter(tt.getStartTime()),"TransferTask end time should be after start time.");
//                assertEquals(copiedTransfer.getTotalSize(), copiedTransfer.getBytesTransferred(), "Total size should be same as bytes transferred upon successful completion of single file transfer.");
//                assertEquals(copiedTransfer.getTotalSkippedFiles(), 0, "TransferTask skipped files should be zero upon successful completion of a single file.");


                ctx.completeNow();
            });

        } catch (Exception e) {
            Assertions.fail("Unary copy from " + TRANSFER_SRC + " to " + TRANSFER_DEST + " should not throw exception, " + e);
        } finally {
            Settings.ALLOW_RELAY_TRANSFERS = allowRelayTransfers;
        }
    }

    @Test
    @DisplayName("Test URLCopy streamingTransfer")
    public void testStreamingCopy(Vertx vertx, VertxTestContext ctx) {
        try {
            allowRelayTransfers = Settings.ALLOW_RELAY_TRANSFERS;
            Settings.ALLOW_RELAY_TRANSFERS = false;

            Vertx mockVertx = mock(Vertx.class);
            RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);

            RemoteDataClient mockSrcRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_SRC);
            RemoteDataClient mockDestRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_DEST);

            RemoteInputStream mockRemoteInputStream = mock(RemoteInputStream.class);
            when(mockRemoteInputStream.isBuffered()).thenReturn(true);
            when(mockRemoteInputStream.read(any(), eq(0), anyInt())).thenReturn(-1);

            when(mockSrcRemoteDataClient.getInputStream(anyString(), eq(true))).thenReturn(mockRemoteInputStream);

            RemoteOutputStream mockRemoteOutputStream = mock(RemoteOutputStream.class);
            when(mockRemoteOutputStream.isBuffered()).thenReturn(true);
            when(mockDestRemoteDataClient.getOutputStream(anyString(), eq(true), eq(false))).thenReturn(mockRemoteOutputStream);

            TransferTask tt = _createTestTransferTask();
            tt.setId(1L);
            tt.setSource(TRANSFER_SRC);
            tt.setDest(TRANSFER_DEST);

            URLCopy mockCopy = mock(URLCopy.class);
            when(mockCopy.copy(any(TransferTask.class))).thenCallRealMethod();

            //remote transfer listener
            RemoteTransferListenerImpl mockRemoteTransferListenerImpl = getMockRemoteTransferListener(tt, mockRetryRequestManager);
            doReturn(mockRemoteTransferListenerImpl).when(mockCopy).getRemoteTransferListenerForTransferTask(tt);

            //Injecting the mocked arguments to the mocked URLCopy
            //Using InjectMocks annotation will not create a mock of URLCopy, which we need to pass in the mocked RemoteTransferListenerImpl
            PowerMockito.whenNew(URLCopy.class).withArguments(any(RemoteDataClient.class), any(RemoteDataClient.class), any(Vertx.class), any(RetryRequestManager.class)).thenReturn(mockCopy);
            TransferTask copiedTransfer = new URLCopy(mockSrcRemoteDataClient, mockDestRemoteDataClient, mockVertx, mockRetryRequestManager).copy(tt);

            ctx.verify(() -> {
                assertEquals(copiedTransfer.getStatus(), TransferStatusType.COMPLETED, "Expected successful copy to return TransferTask with COMPLETED status.");

                //check that events are published correctly using the RetryRequestManager
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.STREAM_COPY_STARTED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.STREAM_COPY_COMPLETED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.COMPLETED.name()),
                        any(JsonObject.class), eq(2));

                ctx.completeNow();
            });

        } catch (Exception e) {
            Assertions.fail("Streaming copy from " + TRANSFER_SRC + " to " + TRANSFER_DEST + " should not throw exception, " + e);
        } finally {
            Settings.ALLOW_RELAY_TRANSFERS = allowRelayTransfers;
        }
    }

}
