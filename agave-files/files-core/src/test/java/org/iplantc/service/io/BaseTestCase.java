/**
 *
 */
package org.iplantc.service.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.surftools.BeanstalkClientImpl.ClientImpl;
import org.apache.commons.lang.StringUtils;
import org.hibernate.CacheMode;
import org.hibernate.Session;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.io.model.JSONTestDataUtil;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.notification.queue.messaging.NotificationMessageBody;
import org.iplantc.service.notification.queue.messaging.NotificationMessageContext;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.manager.SystemManager;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.enumerations.LoginProtocolType;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.Instant;

import static org.iplantc.service.systems.model.enumerations.StorageProtocolType.*;
import static org.testng.Assert.fail;

/**
 * @author dooley
 *
 */
public class BaseTestCase {

    public static final BatchQueue shortQueue = new BatchQueue("short", (long)1000, (long)10, (long)1, 16.0, (long)16, "01:00:00", null, true);
    public static final BatchQueue mediumQueue = new BatchQueue("medium", (long)100, (long)10, (long)1, 16.0, (long)16, "12:00:00", null, false);
    public static final BatchQueue longQueue = new BatchQueue("long", (long)10, (long)4, (long)1, 16.0, (long)16, "48:00:00", null, false);
    public static final BatchQueue dedicatedQueue = new BatchQueue("dedicated", (long)1,        (long)1,        (long)1,        16.0,   (long)16,       "144:00:00", null, false);
    public static final BatchQueue unlimitedQueue = new BatchQueue("unlimited", (long)10000000, (long)10000000, (long)10000000, 2048.0, (long)10000000, "999:00:00", null, false);

    public static final String ADMIN_USER = "testadmin";
    public static final String TENANT_ADMIN = "testtenantadmin";
    public static final String SYSTEM_OWNER = "testuser";
	public static final String SYSTEM_SHARE_USER = "testshareuser";
	public static final String SYSTEM_PUBLIC_USER = "public";
	public static final String SYSTEM_UNSHARED_USER = "testotheruser";
	public static final String SYSTEM_INTERNAL_USERNAME = "test_internal_user";
    public static final String TENANT_ID = "agave.dev";


    public static final String SHARED_SYSTEM_USER = "testshareuser";
	public static String EXECUTION_SYSTEM_TEMPLATE_DIR = "systems/execution";
	public static String STORAGE_SYSTEM_TEMPLATE_DIR = "systems/storage";
	public static String SOFTWARE_SYSTEM_TEMPLATE_DIR = "software";

	// standard directories and files for io tests
    public static String MISSING_DIRECTORY = "I/Do/Not/Exist/unless/some/evil/person/has/this/test";
    public static String MISSING_FILE = "I/Do/Not/Exist/unless/some/evil/person/this/test.txt";
    public static String LOCAL_DIR = "target/test-classes/transfer";
    public static String LOCAL_DOWNLOAD_DIR = "target/test-classes/download";
    public static String LOCAL_TXT_FILE = "target/test-classes/transfer/test_upload.txt";
    public static String LOCAL_BINARY_FILE = "target/test-classes/transfer/test_upload.bin";

    public static String LOCAL_DIR_NAME = "transfer";
    public static String LOCAL_TXT_FILE_NAME = "test_upload.txt";
    public static String LOCAL_BINARY_FILE_NAME = "test_upload.bin";

	protected JSONTestDataUtil jtd;
	protected JSONObject jsonTree;
	protected SystemDao systemDao = new SystemDao();

	protected static ObjectMapper objectMapper = new ObjectMapper();

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@BeforeClass
	protected void beforeClass() throws Exception
	{
		jtd = JSONTestDataUtil.getInstance();

        clearSystems();
        clearLogicalFiles();
	}

	@AfterClass
	protected void afterClass() throws Exception
	{
		clearSystems();
		clearLogicalFiles();
	}


    protected JSONObject getSystemJson() throws JSONException, IOException {
        return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/"
                + "sftp.example.com.json");
    }

    /**
     * Clears all systems and related entity tables from db
     * @throws Exception if something goes wrong
     */
    protected void clearSystems() throws Exception {
        Session session = null;
        try {
            HibernateUtil.beginTransaction();
            HibernateUtil.disableAllFilters();
            session = HibernateUtil.getSession();
            session.clear();

            session.createQuery("delete RemoteSystem").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
            session.createQuery("delete BatchQueue").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
            session.createQuery("delete StorageConfig").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
            session.createQuery("delete LoginConfig").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
            session.createQuery("delete AuthConfig").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
            session.createQuery("delete SystemRole").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
            session.createQuery("delete CredentialServer").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
            session.flush();
        } catch (Exception e) {
            fail("Failed to clear test system tables", e);
        } finally {
            try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
        }
    }

    /**
     * Clears all logical files and related entity tables from db
     */
	protected void clearLogicalFiles() {
		Session session = null;
		try
		{
			HibernateUtil.beginTransaction();
            HibernateUtil.disableAllFilters();
			session = HibernateUtil.getSession();
			session.clear();
            session.createQuery("DELETE LogicalFile").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
			session.createQuery("DELETE StagingTask").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
            session.createQuery("DELETE TransferTaskImpl").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
            session.createSQLQuery("DELETE FROM TransferApiTasks").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
			session.createQuery("DELETE RemoteFilePermission").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
			session.createQuery("DELETE TransferTaskPermission").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
            session.createQuery("DELETE FileEvent").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
			session.createQuery("DELETE Notification").setCacheable(false).setCacheMode(CacheMode.IGNORE).executeUpdate();
		}
		catch (Exception e) {
            fail("Failed to clear test logical file tables", e);
		}
		finally
		{
			try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception ignored) {}
		}
	}


    /**
     * Create a single {@link StorageSystem} and {@link ExecutionSystem} for
     * basic testing. Subclasses will want to override this class to build
     * a larger permutation matrix of test cases.
     *
     * Templates used for these systems are taken from the
     * {@code src/test/resources/systems/execution/execute.example.com.json} and
     * {@code src/test/resources/systems/storage/storage.example.com.json} files.
     *
     * @throws Exception
     */
    protected void initSystems() throws Exception {
        StorageSystem storageSystem = (StorageSystem) getNewInstanceOfRemoteSystem(RemoteSystemType.STORAGE, "storage");
        storageSystem.setOwner(SYSTEM_OWNER);
        storageSystem.setPubliclyAvailable(true);
        storageSystem.setGlobalDefault(true);
        storageSystem.getUsersUsingAsDefault().add(SYSTEM_OWNER);
        systemDao.persist(storageSystem);

        ExecutionSystem executionSystem =
                (ExecutionSystem) getNewInstanceOfRemoteSystem(RemoteSystemType.EXECUTION, "execute");
        executionSystem.setOwner(SYSTEM_OWNER);
        executionSystem.getBatchQueues().clear();
        executionSystem.addBatchQueue(dedicatedQueue.clone());
        executionSystem.addBatchQueue(longQueue.clone());
        executionSystem.addBatchQueue(mediumQueue.clone());
        executionSystem.addBatchQueue(shortQueue.clone());
        executionSystem.setPubliclyAvailable(true);
        executionSystem.setType(RemoteSystemType.EXECUTION);
        systemDao.persist(executionSystem);
    }

    /**
     * Creates and persists an {@link StorageSystem} for every template
     * with file name matching {@link StorageProtocolType}.example.com.json
     * in the {@code src/test/resources/systems/storage} folder.
     * @throws Exception
     */
    protected void initAllStorageSystems() throws Exception {
        for (StorageProtocolType protocol: StorageProtocolType.values())
        {
            if (protocol == AZURE || protocol == SWIFT) continue;
            StorageSystem storageSystem = (StorageSystem)
                    getNewInstanceOfRemoteSystem(RemoteSystemType.STORAGE, protocol.name());
            storageSystem.setOwner(SYSTEM_OWNER);
            storageSystem.setPubliclyAvailable(true);
            storageSystem.setGlobalDefault(true);
            storageSystem.getUsersUsingAsDefault().add(SYSTEM_OWNER);
            systemDao.persist(storageSystem);
        }
    }

    /**
     * Creates and persists an {@link ExecutionSystem} for every template
     * with file name matching {@link LoginProtocolType}.example.com.json
     * in the {@code src/test/resources/systems/execution} folder.
     * @throws Exception
     */
    protected void initAllExecutionSystems() throws Exception
    {
        for (LoginProtocolType protocol: LoginProtocolType.values())
        {
            ExecutionSystem executionSystem = (ExecutionSystem)
                    getNewInstanceOfRemoteSystem(RemoteSystemType.EXECUTION, protocol.name());
            executionSystem.setOwner(SYSTEM_OWNER);
            executionSystem.getBatchQueues().clear();
            executionSystem.addBatchQueue(dedicatedQueue.clone());
            executionSystem.addBatchQueue(longQueue.clone());
            executionSystem.addBatchQueue(mediumQueue.clone());
            executionSystem.addBatchQueue(shortQueue.clone());
            executionSystem.setPubliclyAvailable(true);
            executionSystem.setType(RemoteSystemType.EXECUTION);
            systemDao.persist(executionSystem);
        }
    }

    /**
     * Reads a new {@link RemoteSystem} of the given {@code type} and {@code protocol}
     * from one of the test templates. The returned {@link RemoteSystem} is unsaved and
     * unaltered from the original template.
     *
     * @param type the system type (storage or execution).
     * @param protocol valid {@link StorageProtocolType} or {@link LoginProtocolType} value
     * @return unaltered test template {@link RemoteSystem}
     * @throws Exception
     */
    protected RemoteSystem getNewInstanceOfRemoteSystem(RemoteSystemType type, String protocol)
    throws Exception
    {
        return getNewInstanceOfRemoteSystem(type, protocol, null);
    }

    /**
     * Reads a new {@link RemoteSystem} of the given {@code type} and {@code protocol}
     * from one of the test templates. The returned {@link RemoteSystem} is unsaved and
     * unaltered from the original template.
     *
     * @param type the system type (storage or execution).
     * @param protocol valid {@link StorageProtocolType} or {@link LoginProtocolType} value
     * @param systemId custom systemid to assign to the new system
     * @return unaltered test template {@link RemoteSystem}
     * @throws Exception
     */
    protected RemoteSystem getNewInstanceOfRemoteSystem(RemoteSystemType type, String protocol, String systemId)
    throws Exception
    {
        SystemManager systemManager = new SystemManager();
        JSONObject json = null;
        if (type == RemoteSystemType.STORAGE) {
            String filename = protocol.equalsIgnoreCase("irods") ? "irods3" : protocol.toLowerCase() ;
            json = jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + File.separator +
                    filename + ".example.com.json");
        }
        else {
            json = jtd.getTestDataObject(EXECUTION_SYSTEM_TEMPLATE_DIR +
                    File.separator + protocol.toLowerCase() + ".example.com.json");
        }

        if (StringUtils.isNotEmpty(systemId)) {
            json.put("id", systemId);
        }

        return systemManager.parseSystem(json, SYSTEM_OWNER, null);
    }

    /**
     * Returns a new, unsaved {@link StorageProtocolType#SFTP} {@link StorageSystem}.
     * @return a new default sftp storage system
     * @throws Exception
     */
    protected StorageSystem getNewInstanceOfStorageSystem()
    throws Exception
    {
        return (StorageSystem)getNewInstanceOfRemoteSystem(RemoteSystemType.STORAGE, StorageProtocolType.SFTP.name());
    }

    /**
     * Returns a new, unsaved {@link StorageProtocolType#SFTP} {@link StorageSystem}.
     * @param systemId the custom systemId to assign to the new system
     * @return a new persisted storage system with the systemId
     * @throws Exception
     */
    protected StorageSystem getNewInstanceOfStorageSystem(String systemId)
    throws Exception
    {
        return (StorageSystem)getNewInstanceOfRemoteSystem(RemoteSystemType.STORAGE, StorageProtocolType.SFTP.name(), systemId);
    }

    /**
     * Returns a new, unsaved {@link StorageProtocolType#SFTP} {@link ExecutionSystem}.
     * @return a new default ssh execution system
     * @throws Exception
     */
    protected ExecutionSystem getNewInstanceOfExecutionSystem()
    throws Exception
    {
        return (ExecutionSystem)getNewInstanceOfRemoteSystem(RemoteSystemType.EXECUTION, LoginProtocolType.SSH.name());
    }

    /**
     * Returns a new, unsaved {@link StorageProtocolType#SFTP} {@link ExecutionSystem}.
     * @param systemId the custom systemId to assign to the new system
     * @return a new persisted execution system with the systemId
     * @throws Exception
     */
    protected ExecutionSystem getNewInstanceOfExecutionSystem(String systemId)
    throws Exception
    {
        return (ExecutionSystem)getNewInstanceOfRemoteSystem(RemoteSystemType.EXECUTION, LoginProtocolType.SSH.name(), systemId);
    }


	protected String getMacAddress() throws Exception
	{
		InetAddress ip = InetAddress.getLocalHost();
		System.out.println("Current IP address : " + ip.getHostAddress());

        // For some reason this returns null for me on the VM. Rather than worry about debugging a test,
        // I am just returning the ip address. While there are just 2 of us testing this, we should be safe
        // for now
        // Todo Investigate this more thoroughly and get it working
		NetworkInterface network = NetworkInterface.getByInetAddress(ip);

        if (network != null) {
            byte[] mac = network.getHardwareAddress();

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < mac.length; i++) {
                sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
            }
            return sb.toString();
        } else {
             return ip.toString();
        }
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

    /**
     * Flushes the messaging tube of any and all existing jobs.
     */
    public void clearQueues() {
        ClientImpl client = null;

        // drain the message queue
        client = new ClientImpl(org.iplantc.service.common.Settings.MESSAGING_SERVICE_HOST,
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


}
