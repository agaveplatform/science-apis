/**
 * 
 */
package org.iplantc.service.apps.dao;

import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_EXECUTION_SYSTEM_FILE;
import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_OWNER;
import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_SOFTWARE_SYSTEM_FILE;
import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_STORAGE_SYSTEM_FILE;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.iplantc.service.apps.model.JSONTestDataUtil;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareParameterEnumeratedValue;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.iplantc.service.systems.model.CredentialServer;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.SystemRole;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

/**
 * @author dooley
 *
 */
public class AbstractDaoTest 
{
	private static final Logger log = Logger.getLogger(AbstractDaoTest.class);
	protected ObjectMapper mapper = new ObjectMapper();
	
	public static final String ADMIN_USER = "testadmin";
	public static final String TENANT_ADMIN = "testtenantadmin";
	public static final String SYSTEM_OWNER = "testuser";
	public static final String SYSTEM_SHARE_USER = "testshare";
	public static final String SYSTEM_PUBLIC_USER = "public";
	public static final String SYSTEM_UNSHARED_USER = "testother";
	public static final String SYSTEM_INTERNAL_USERNAME = "test_user";
	public static final String EXECUTION_SYSTEM_TEMPLATE_DIR = "src/test/resources/systems/execution";
	public static final String STORAGE_SYSTEM_TEMPLATE_DIR = "target/test-classes/systems/storage";
	public static final String SOFTWARE_SYSTEM_TEMPLATE_DIR = "src/test/resources/software";
	public static final String FORK_SOFTWARE_TEMPLATE_FILE = SOFTWARE_SYSTEM_TEMPLATE_DIR + "/fork-1.0.0/app.json";
	public static final String INTERNAL_USER_TEMPLATE_DIR = "src/test/resources/internal_users";
	public static final String CREDENTIALS_TEMPLATE_DIR = "src/test/resources/credentials";

	protected JSONTestDataUtil jtd;
	protected SystemDao systemDao;
	
	@BeforeClass
	protected void beforeClass() throws Exception
	{
		systemDao = new SystemDao();
		
		jtd = JSONTestDataUtil.getInstance();

		clearSystems();
        clearSoftware();
	}

	/**
	 * Clears {@link Software} and {@link org.iplantc.service.systems.model.RemoteSystem} from the db.
	 * @throws Exception if the records cannot be cleaned out
	 */
	@AfterClass
	public void afterClass() throws Exception
	{
		clearSoftware();
		clearSystems();
	}

	/**
	 * Creates private execution system with a random uuid as the id.
	 * Asserts that the system is created and valid prior to returning.
	 * @return a persisted {@link ExecutionSystem}
	 */
	protected ExecutionSystem createExecutionSystem() {
		ExecutionSystem system = null;
		try {
			JSONObject json = JSONTestDataUtil.getInstance().getTestDataObject(TEST_EXECUTION_SYSTEM_FILE);
			json.put("id", UUID.randomUUID().toString());
			system = ExecutionSystem.fromJSON(json);
			system.setOwner(SYSTEM_OWNER);
			new SystemDao().persist(system);
		} catch (IOException|JSONException|SystemArgumentException e) {
			log.error("Unable create execution system", e);
			Assert.fail("Unable create execution system", e);
		}

		return system;
	}

	/**
	 * Creates private execution system with a random uuid as the id.
	 * Asserts that the system is created and valid prior to returning.
	 * @return a persisted {@link StorageSystem}
	 */
	protected StorageSystem createStorageSystem() {
		StorageSystem system = null;
		try {
			JSONObject json = JSONTestDataUtil.getInstance().getTestDataObject(TEST_STORAGE_SYSTEM_FILE);
			json.put("id", UUID.randomUUID().toString());
			system = StorageSystem.fromJSON(json);
			system.setOwner(SYSTEM_OWNER);
			system.getUsersUsingAsDefault().add(TEST_OWNER);
			new SystemDao().persist(system);
		} catch (IOException|JSONException e) {
			log.error("Unable create storage system", e);
			Assert.fail("Unable create storage system", e);
		}

		return system;
	}

	/**
	 * Removes all storage and execution systems from the db
	 * @throws Exception
	 */
	protected void clearSystems()
	{
	    Session session = null;
        try
        {
            HibernateUtil.beginTransaction();
            session = HibernateUtil.getSession();
            HibernateUtil.disableAllFilters();

            session.createQuery("DELETE ExecutionSystem").executeUpdate();
			session.createQuery("DELETE BatchQueue").executeUpdate();
			session.createQuery("DELETE AuthConfig").executeUpdate();
			session.createQuery("DELETE LoginConfig").executeUpdate();
			session.createQuery("DELETE CredentialServer").executeUpdate();
			session.createQuery("DELETE TransferTask").executeUpdate();
			session.createQuery("DELETE RemoteConfig").executeUpdate();
            session.createQuery("DELETE StorageSystem").executeUpdate();
			session.createQuery("DELETE StorageConfig").executeUpdate();
			session.createQuery("DELETE SystemRole").executeUpdate();
			session.createQuery("DELETE SystemPermission").executeUpdate();
			session.createSQLQuery("delete from userdefaultsystems").executeUpdate();
			session.flush();
        }
        finally
        {
            try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
        }
	}

	/**
	 * Creates a new persisted {@link Software} resource and assigns the {@link ExecutionSystem} and
	 * {@link StorageSystem} to it.
	 * @param executionSystem the system to which the new {@link Software} resource will be assigned
	 * @return a persisted {@link Software} resource
	 */
	protected Software createSoftware(ExecutionSystem executionSystem, StorageSystem storageSystem) throws Exception
	{
		JSONObject jsonSoftware = JSONTestDataUtil.getInstance().getTestDataObject(FORK_SOFTWARE_TEMPLATE_FILE);
		jsonSoftware.put("executionSystem", executionSystem.getSystemId());
		jsonSoftware.put("deploymentSystem", storageSystem.getSystemId());
		jsonSoftware.put("name", UUID.randomUUID().toString());
		Software software = Software.fromJSON(jsonSoftware, SYSTEM_OWNER);
		software.setExecutionSystem(executionSystem);
		software.setOwner(SYSTEM_OWNER);
		software.setName(software.getUuid());

		SoftwareDao.persist(software);

		return software;
	}

	/**
	 * Creates a new persisted {@link Software} resource and assigns it to a new {@link ExecutionSystem}
	 * and {@link StorageSystem}.
	 * @return a persisted {@link Software} resource
	 * @see #createSoftware(ExecutionSystem, StorageSystem)
	 */
	protected Software createSoftware() throws Exception {
		ExecutionSystem executionSystem = createExecutionSystem();
		StorageSystem storageSystem = createStorageSystem();
		return createSoftware(executionSystem, storageSystem);
	}

	/**
	 * Removes all software records from the db
	 * @throws Exception
	 */
	protected void clearSoftware() throws Exception
	{
	    Session session = null;
        try
        {
            HibernateUtil.beginTransaction();
            session = HibernateUtil.getSession();
            HibernateUtil.disableAllFilters();

			session.createQuery("DELETE SoftwareParameter").executeUpdate();
			session.createQuery("DELETE SoftwareInput").executeUpdate();
			session.createQuery("DELETE SoftwareOutput").executeUpdate();
			session.createSQLQuery("delete from softwares_parameters").executeUpdate();
			session.createSQLQuery("delete from softwares_inputs").executeUpdate();
			session.createSQLQuery("delete from softwares_outputs").executeUpdate();
			session.createQuery("DELETE SoftwarePermission").executeUpdate();
			session.createQuery("DELETE SoftwareParameterEnumeratedValue").executeUpdate();
			session.createQuery("DELETE SoftwareEvent").executeUpdate();
			session.createQuery("DELETE Software").executeUpdate();
			session.flush();
		}
        finally
        {
            try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
        }
	}

	/**
	 * Creates a nonce for use as the token by generating an md5 hash of the
	 * salt, current timestamp, and a random number.
	 *
	 * @param salt the salt prepended to semi-random data when calculating the nonce digest
	 * @return md5 hash of the adjusted salt
	 */
	public String createNonce(String salt) {
		String digestMessage = salt + System.currentTimeMillis() + new Random().nextInt();
		return DigestUtils.md5Hex(digestMessage);
	}

	/**
	 * Creates a nonce for use as the token by generating an md5 hash of the
	 * a random uuid, current timestamp, and a random number.
	 *
	 * @return md5 hash of the adjusted salt
	 * @see #createNonce(String)
	 */
	public String createNonce() {
		return createNonce(UUID.randomUUID().toString());
	}
}
