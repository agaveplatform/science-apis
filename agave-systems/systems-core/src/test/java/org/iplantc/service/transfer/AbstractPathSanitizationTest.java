package org.iplantc.service.transfer;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.json.JSONException;
import org.json.JSONObject;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class AbstractPathSanitizationTest extends BaseTransferTestCase {
    
    private static final Logger log = Logger.getLogger(AbstractPathSanitizationTest.class);
    
    protected ThreadLocal<RemoteDataClient> threadClient = new ThreadLocal<RemoteDataClient>();
    
    /**
     * Returns a {@link JSONObject} representing the system to test.
     * 
     * @return 
     * @throws JSONException
     * @throws IOException
     */
    protected abstract JSONObject getSystemJson() throws JSONException, IOException;
    
    @BeforeClass(alwaysRun=true)
    protected void beforeSubclass() throws Exception {
        super.beforeClass();
        
        JSONObject json = getSystemJson();
        json.remove("id");
        json.put("id", this.getClass().getSimpleName());
        system = StorageSystem.fromJSON(json);
        system.setOwner(SYSTEM_USER);
        String homeDir = system.getStorageConfig().getHomeDir();
        homeDir = StringUtils.isEmpty(homeDir) ? "" : homeDir;
        system.getStorageConfig().setHomeDir( homeDir + "/" + getClass().getSimpleName());
        storageConfig = system.getStorageConfig();
        salt = system.getSystemId() + storageConfig.getHost() + 
                storageConfig.getDefaultAuthConfig().getUsername();
        
        SystemDao dao = Mockito.mock(SystemDao.class);
        Mockito.when(dao.findBySystemId(Mockito.anyString()))
            .thenReturn(system);
        
//        getClient().authenticate();
//        if (getClient().doesExist("")) {
//            try {
//                getClient().delete("..");
//            } catch (FileNotFoundException ignore) {
//                // ignore if already gone
//            }
//        }
    }
    
    @AfterClass(alwaysRun=true)
    protected void afterClass() throws Exception {
        try {
            getClient().authenticate();
        } catch (RemoteDataException e) {
            log.error(e.getMessage());
        }

        try {
            // remove test directory
            getClient().delete("..");
            Assert.assertFalse(getClient().doesExist(""), "Failed to clean up home directory " + getClient().resolvePath("") + "after test.");
        } catch (FileNotFoundException ignore) {
            // ignore if already gone
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            Assert.fail("Failed to clean up test home directory " + getClient().resolvePath("") + " after test method.", e);
        } finally {
            try { getClient().disconnect(); } catch (Exception ignored) {}
        }
    }

    @BeforeMethod(alwaysRun=true)
    protected void beforeMethod(Method m) throws Exception
    {
        String resolvedHomeDir = "";
        try {
            // auth client and ensure test directory is present
            getClient().authenticate();
        } catch (RemoteDataException e) {
            log.error(e.getMessage());
        }

        try {
            resolvedHomeDir = getClient().resolvePath("");

            if (!getClient().mkdirs("")) {
                if (!getClient().isDirectory("")) {
                    Assert.fail("System home directory " + resolvedHomeDir + " exists, but is not a directory.");
                }
            }
        } catch (IOException | RemoteDataException | AssertionError e) {
            throw e;
        } catch (Exception e) {
            Assert.fail("Failed to create home directory " + resolvedHomeDir + " before test method.", e);
        }
    }
    
    /**
     * Gets getClient() from current thread
     * @return
     * @throws RemoteCredentialException 
     * @throws RemoteDataException 
     */
    protected RemoteDataClient getClient() 
    {
        RemoteDataClient client;
        try {
            if (threadClient.get() == null) {
                client = system.getRemoteDataClient();
                String threadHomeDir = String.format("%s/thread-%s-%d",
                        system.getStorageConfig().getHomeDir(),
                        UUID.randomUUID(),
                        Thread.currentThread().getId());
                client.updateSystemRoots(client.getRootDir(),  threadHomeDir);
                threadClient.set(client);
            } 
        } catch (RemoteDataException | RemoteCredentialException e) {
            Assert.fail("Failed to get client", e);
        }
        
        return threadClient.get();
    }

    /**
     * Returns the special characters used to test. This may be overridden by subclasses
     * to prune based on naming restrictions of the underlying storage solution.
     * @return
     */
    protected char[] getSpecialCharArray() {
        String specialChars = " _-!@#$%^*()+[]{}:."; // excluding "&" due to a bug in irods
        return specialChars.toCharArray();
    }

    @DataProvider(parallel=true)
    protected Object[][] mkDirSanitizesSingleSpecialCharacterNameProvider() throws Exception {
        List<Object[]> tests = new ArrayList<Object[]>();
        for (char c: getSpecialCharArray()) {
            String sc = String.valueOf(c);
            if (c == ' ' || c == '.') {
                continue;
            } else {
                tests.add(new Object[] { sc, true, "Directory name with single special character '" + sc + "' should be created" });
            }
        }

        return tests.toArray(new Object[][] {});
    }

    @DataProvider(parallel=true)
    protected Object[][] mkDirSanitizesSingleSpecialCharacterProvider() throws Exception {
        List<Object[]> tests = new ArrayList<Object[]>();
        for (char c: getSpecialCharArray()) {
            String sc = String.valueOf(c);

            String dirName = sc + UUID.randomUUID();
            tests.add(new Object[] { dirName, true, "Directory name," + dirName + ", with leading single special character '" + sc + "' should be created" });

            dirName = UUID.randomUUID() + sc;
            tests.add(new Object[] { dirName, true, "Directory name," + dirName + ",with trailing single special character '" + sc + "' should be created" });

            dirName = sc + UUID.randomUUID() + sc;
            tests.add(new Object[] { dirName, true, "Directory name," + dirName + ", with leading and trailing single special character '" + sc + "' should be created" });

            dirName = UUID.randomUUID() + sc + "suffix";
            tests.add(new Object[] { dirName, true, "Directory name," + dirName + ", with singleinternal special character '" + sc + "' should be created" });
        }

        return tests.toArray(new Object[][] {});
    }
    
    @DataProvider(parallel=false)
    protected Object[][] mkDirSanitizesRepeatedSpecialCharacterProvider() throws Exception {
        List<Object[]> tests = new ArrayList<Object[]>();
        for (char c: getSpecialCharArray()) {
            String chars = String.valueOf(c) + c;
            if (c == ' ' || c == '.') {
                continue;
            } else {
                tests.add(new Object[] { chars, true, "Directory name," + chars + ", with only repeated special character '" + chars + "' should be created" });
            }
            String dirName = chars + UUID.randomUUID();
            tests.add(new Object[] { dirName, true, "Directory name," + dirName + ", with repeated repeated special character '" + chars + "' should be created" });

            dirName = UUID.randomUUID() + chars;
            tests.add(new Object[] { dirName, true, "Directory name," + dirName + ", with trailing repeated special character '" + chars + "' should be created" });

            dirName = chars + UUID.randomUUID() + chars;
            tests.add(new Object[] { dirName, true, "Directory name," + dirName + ", with leading and trailing repeated special character '" + chars + "' should be created" });

            dirName = UUID.randomUUID() + chars + "suffix";
            tests.add(new Object[] { dirName, true, "Directory name," + dirName + ", with repeated internal special character '" + chars + "' should be created" });
        }
        
        return tests.toArray(new Object[][] {});
    }
    
    @DataProvider(parallel=false)
    protected Object[][] mkDirSanitizesWhitespaceProvider() throws Exception {
        return new Object[][] {
                { " ", "Directory name with single whitespace character will resolve to an empty name and should throw exception" },
                { "  ", "Directory name with double whitespace character will resolve to an empty name and should throw exception" },
                { "   ", "Directory name with triple whitespace character will resolve to an empty name and should throw exception" },
        };
    }

    protected void _mkDirSanitizationTest(String filename, boolean shouldSucceed, String message) throws FileNotFoundException
    {
        try {
            Assert.assertEquals(getClient().mkdir(filename), shouldSucceed,
                    "Creating directory " + filename + " should return " + shouldSucceed);
            Assert.assertTrue(getClient().doesExist(filename), message);
        } 
        catch (Exception e) 
        {
            if (shouldSucceed) {
                Assert.fail(message, e);
            }
        }
        finally {
            try {
                if (!StringUtils.isBlank(filename)) {
                    getClient().delete(filename);
                }
            } catch(FileNotFoundException ignore) {
            }
            catch (Exception e) {
                log.error("Unable to delete directory " + getClient().resolvePath(filename), e);
            }
        }
    }
    
    protected void _mkDirsSanitizationTest(String filename, boolean shouldSucceed, String message) throws FileNotFoundException
    {
        String relativePath = UUID.randomUUID() + "/";

        try {
            Assert.assertEquals(getClient().mkdirs(relativePath + filename), shouldSucceed, message);
            Assert.assertTrue(getClient().doesExist(relativePath + filename), message);
        } 
        catch (Exception e) 
        {
            if (shouldSucceed)
                Assert.fail(message, e);
        }
        finally {
            try {
                getClient().delete(relativePath);
            } catch(FileNotFoundException ignore) {
            } catch (Exception e) {
                log.error("Unable to delete directory " + getClient().resolvePath(relativePath), e);
            }
        }
     
    }

    protected void _mkDirsAbsolutePathSanitizationTest(String filename, boolean shouldSucceed, String message) throws FileNotFoundException
    {
        String homeDir = getClient().getHomeDir();
        String rootDir = getClient().getRootDir();

        String baseTestPath = homeDir.substring(rootDir.length());
        baseTestPath = StringUtils.strip(baseTestPath, "/");

        String basePath = String.format("/%s/%s/", baseTestPath, UUID.randomUUID());

        try {
            Assert.assertEquals(getClient().mkdirs(basePath + filename), shouldSucceed, message);
            Assert.assertTrue(getClient().doesExist(basePath + filename), message);
        }
        catch (Exception e)
        {
            if (shouldSucceed)
                Assert.fail(message, e);
        }
        finally {
            try {
                getClient().delete(basePath);
            } catch(FileNotFoundException ignore) {
            } catch (Exception e) {
                log.error("Unable to delete directory " + basePath, e);
            }
        }

    }
}
