/**
 * 
 */
package org.iplantc.service.apps.dao;

import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_EXECUTION_SYSTEM_FILE;
import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_OWNER;
import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_SOFTWARE_SYSTEM_FILE;
import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_STORAGE_SYSTEM_FILE;

import org.apache.commons.codec.digest.DigestUtils;
import org.hibernate.Session;
import org.iplantc.service.apps.model.JSONTestDataUtil;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareParameterEnumeratedValue;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.CredentialServer;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.SystemRole;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Random;
import java.util.UUID;

/**
 * @author dooley
 *
 */
public class AbstractDaoTest 
{	
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
	protected StorageSystem privateStorageSystem;
	protected ExecutionSystem privateExecutionSystem;
	protected Software software;
	
	@BeforeClass
	protected void beforeClass() throws Exception
	{
		systemDao = new SystemDao();
		
		jtd = JSONTestDataUtil.getInstance();

        initSystems();
        clearSoftware();
	}
	
	protected void initSystems() throws Exception
	{
		clearSystems();
		
		privateExecutionSystem = ExecutionSystem.fromJSON(jtd.getTestDataObject(TEST_EXECUTION_SYSTEM_FILE));
		privateExecutionSystem.setOwner(TEST_OWNER);
		systemDao.persist(privateExecutionSystem);
		
		privateStorageSystem = StorageSystem.fromJSON(jtd.getTestDataObject(TEST_STORAGE_SYSTEM_FILE));
		privateStorageSystem.setOwner(TEST_OWNER);
		privateStorageSystem.getUsersUsingAsDefault().add(TEST_OWNER);
        systemDao.persist(privateStorageSystem);

	}
	
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
	
	protected Software createSoftware() throws Exception
	{
		JSONObject json = jtd.getTestDataObject(FORK_SOFTWARE_TEMPLATE_FILE);
		Software software = Software.fromJSON(json, TEST_OWNER);
		software.setExecutionSystem(privateExecutionSystem);
		software.setOwner(SYSTEM_OWNER);
		software.setName(software.getUuid());
		
		return software;
	}
	
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
	
	
	@AfterClass
	public void afterClass() throws Exception
	{
		clearSoftware();
		clearSystems();
	}

	/**
	 * Creates a nonce for use as the token by generating an md5 hash of the
	 * salt, current timestamp, and a random number.
	 *
	 * @param salt
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
	 */
	public String createNonce() {
		String digestMessage = UUID.randomUUID().toString() + System.currentTimeMillis() + new Random().nextInt();
		return DigestUtils.md5Hex(digestMessage);
	}
}
