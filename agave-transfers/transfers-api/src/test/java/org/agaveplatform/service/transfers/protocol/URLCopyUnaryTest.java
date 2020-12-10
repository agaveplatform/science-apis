package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
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
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.Settings;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.*;
import org.testng.annotations.Test;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("URLCopy Unary Tests")
public class URLCopyUnaryTest extends BaseTestCase {
    protected File tmpFile = null;
    protected File tmpDir = null;

    protected RemoteDataClient client;
    protected StorageConfig storageConfig;
    protected StorageSystem destSystem;
    protected String credential;
    protected String salt;

    @BeforeClass(alwaysRun = true)
    protected void beforeSubclass() {
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

    @BeforeMethod(alwaysRun = true)
    protected void beforeMethod()  {
        FileUtils.deleteQuietly(new File(getLocalDownloadDir()));
        FileUtils.deleteQuietly(tmpFile);
    }

    @AfterClass(alwaysRun = true)
    protected void afterClass() throws Exception {
        try {
            FileUtils.deleteQuietly(new File(getLocalDownloadDir()));
            FileUtils.deleteQuietly(tmpFile);
            FileUtils.deleteQuietly(tmpFile);
            clearSystems();
        } finally {
            try {
                getClient().disconnect();
            } catch (Exception e) {
            }
        }

        try {
            if (destSystem!= null) {
                getClient().authenticate();
                // remove test directory
                getClient().delete("..");
                Assert.assertFalse(getClient().doesExist(""), "Failed to clean up home directory after test.");
            }
            } catch (Exception e) {
            Assert.fail("Failed to clean up test home directory " + getClient().resolvePath("") + " after test method.", e);
        } finally {
            try {
                getClient().disconnect();
            } catch (Exception e) {
            }
        }
    }

    @DataProvider(name = "putFileProvider")
    protected Object[][] putFileProvider() {
        // expected result of copying LOCAL_BINARY_FILE to the remote system given the following dest
        // paths
        return new Object[][]{
                // remote dest path,  expected result path,  exception?, message
                {LOCAL_TXT_FILE_NAME, LOCAL_TXT_FILE_NAME, false, "put local file to remote destination with different name should result in file with new name on remote system."},
                {"", LOCAL_BINARY_FILE_NAME, false, "put local file to empty(home) directory name should result in file with same name on remote system"},
        };
    }

    @DataProvider(name = "httpImportProvider")
    protected Object[][] httpImportProvider()
    {
        String DEST_FILENAME = "test_upload.bin.copy";

        ArrayList<StorageProtocolType> testDestTypes = new ArrayList<>();
        testDestTypes.add(StorageProtocolType.SFTP);

        List<Object[]> testCases = new ArrayList<Object[]>();
        for(StorageProtocolType destType: testDestTypes) {
            testCases.add(new Object[] { URI.create("http://preview.agaveplatform.org/wp-content/themes/agave/images/favicon.ico"), destType, "Public URL copy to " + destType.name() + " should succeed", false });
            testCases.add(new Object[] { URI.create("https://agaveplatform.org/wp-content/themes/agave/images/favicon.ico"), destType, "Public 301 redirected URL copy to " + destType.name() + " should succeed", false });
            testCases.add(new Object[] { URI.create("https://avatars0.githubusercontent.com/u/785202"), destType, "Public HTTPS URL copy to " + destType.name() + " should succeed", false });
            testCases.add(new Object[] { URI.create("http://docker.example.com:10080/public/" + DEST_FILENAME), destType, "Public URL copy to " + destType.name() + " on alternative port should succeed", false });
            testCases.add(new Object[] { URI.create("https://docker.example.com:10443/public/" + DEST_FILENAME), destType, "Public HTTPS URL copy to " + destType.name() + " on alternative port should succeed", false });
            testCases.add(new Object[] { URI.create("https://docker.example.com:10443/public/" + DEST_FILENAME + "?t=now"), destType, "Public URL copy to " + destType.name() + " should succeed", false });
            testCases.add(new Object[] { URI.create("http://testuser:testuser@docker.example.com:10080/private/" + DEST_FILENAME), destType, "Public URL copy to " + destType.name() + " should succeed", false });

            testCases.add(new Object[] { URI.create("http://docker.example.com:10080"), destType, "Public URL copy of no path to " + destType.name() + " should fail", true });
            testCases.add(new Object[] { URI.create("http://docker.example.com:10080/"), destType, "Public URL copy of empty path to " + destType.name() + " should fail", true });
            testCases.add(new Object[] { URI.create("http://docker.example.com:10080/" + MISSING_FILE), destType, "Missing file URL " + destType.name() + " should fail", true });
            testCases.add(new Object[] { URI.create("http://testuser@docker.example.com:10080/private/test_upload.bin"), destType, "Protected URL missing password should fail auth and copy to " + destType.name() + " should fail", true });
            testCases.add(new Object[] { URI.create("http://testuser:testotheruser@docker.example.com:10080/private/test_upload.bin"), destType, "Protected URL with bad credentials should fail auth and copy to " + destType.name() + " should fail", true });
        }

        return new Object[][] {

        };
    }

    /**
     * Gets getClient() from current thread
     *
     * @return {@link RemoteDataClient} of current thread
     * @throws RemoteCredentialException if unable to retrieve the {@link RemoteDataClient} due to missing permissions
     * @throws RemoteDataException if unable to retrieve the {@link RemoteDataClient}
     */
    protected RemoteDataClient getClient() {
        RemoteDataClient client = null;
        try {
            if (destSystem!= null) {
                client = destSystem.getRemoteDataClient();
                client.updateSystemRoots(client.getRootDir(), destSystem.getStorageConfig().getHomeDir() + "/thread-" + Thread.currentThread().getId());
            }
        } catch (RemoteDataException | RemoteCredentialException e) {
            Assert.fail("Failed to get client", e);
        }
        return client;
    }

    protected String getLocalDownloadDir() {
        return LOCAL_DOWNLOAD_DIR + Thread.currentThread().getId();
    }

    RemoteDataClient getMockRemoteDataClientInstance() {
        return Mockito.mock(RemoteDataClient.class);
    }

    /**
     * Get the RemoteDataClient with settings specified in the corresponding StorageProtocolType json file
     * @param type
     * @return
     * @throws Exception
     */
    private RemoteDataClient getRemoteDataClientFromSystemJson(StorageProtocolType type) throws Exception {
        JSONObject systemJson = getSystemJson(type);
        StorageSystem system = (StorageSystem) StorageSystem.fromJSON(systemJson);
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

    @Test(dataProvider = "putFileProvider")
    public void putFileTest(String remoteDest, String expectedPath, Boolean shouldThrowException, String message) {
        boolean allowRelayTransfers = Settings.ALLOW_RELAY_TRANSFERS;
        try {
            Vertx mockVertx = mock(Vertx.class);
            RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);

            RemoteDataClient srcRemoteDataClient = new RemoteDataClientFactory().getInstance(SYSTEM_USER, null, URI.create(TRANSFER_SRC));
            RemoteDataClient destRemoteDataClient = getRemoteDataClientFromSystemJson(StorageProtocolType.SFTP);

            Settings.ALLOW_RELAY_TRANSFERS = true;

            TransferTask tt = _createTestTransferTask();
            tt.setId(1L);
            tt.setSource(TRANSFER_SRC);
            tt.setDest(TRANSFER_DEST);

            URLCopy copy = new URLCopy(srcRemoteDataClient, destRemoteDataClient,
                    mockVertx, mockRetryRequestManager);
            TransferTask copiedTransfer = copy.copy(tt);

            assertEquals(copiedTransfer.getStatus(), TransferStatusType.COMPLETED, "Expected successful copy to return TransferTask with COMPLETED status.");

        } catch (Exception e) {
            Assert.fail("Copy from " + TRANSFER_SRC  + " to " + TRANSFER_DEST + " should not throw exception, " + e);
        } finally {
            Settings.ALLOW_RELAY_TRANSFERS = allowRelayTransfers;
        }
    }

}
