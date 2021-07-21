package org.iplantc.service.io.dao;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.HibernateException;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.StorageConfig;
import org.iplantc.service.systems.model.StorageSystem;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.*;

import java.net.URI;
import java.util.List;
import java.util.UUID;

@Test(singleThreaded = true, groups={"integration"})
public class LogicalFileDaoCaseSensitivityIT extends BaseTestCase
{
	private LogicalFile file;
	private StorageSystem system;
	private String basePath;
	private String otherPath;
	private String destPath;
	private URI httpUri;
	
	@BeforeClass
	protected void setUp() throws Exception
	{
		super.beforeClass();
		clearSystems();
		clearLogicalFiles();

		JSONObject json = getSystemJson();
		json.remove("id");
		json.put("id", this.getClass().getSimpleName());
		system = (StorageSystem) StorageSystem.fromJSON(json);
		system.setOwner(SYSTEM_OWNER);
		String homeDir = system.getStorageConfig().getHomeDir();
		homeDir = org.apache.commons.lang.StringUtils.isEmpty(homeDir) ? "" : homeDir;
		system.getStorageConfig().setHomeDir(
				homeDir + "/" + getClass().getSimpleName() + "/" + UUID.randomUUID());
		StorageConfig storageConfig = system.getStorageConfig();
		SystemDao dao = new SystemDao();
		if (dao.findBySystemId(system.getSystemId()) == null) {
			dao.persist(system);
		}

		basePath = "/var/home/" + SYSTEM_OWNER + "/some";
		otherPath = "/var/home/" + SYSTEM_OWNER + "/other";
		destPath = String.format("/home/%s/%s/%s", SYSTEM_OWNER, UUID.randomUUID(), LOCAL_TXT_FILE_NAME);
		httpUri = new URI("http://example.com/foo/bar/baz");

		file = new LogicalFile(SYSTEM_OWNER, system, httpUri.toString().toLowerCase(), destPath.toLowerCase(), "originalFilename");
	}
	
	@BeforeMethod
	protected void setUpBeforeMethod() throws Exception
	{
		clearLogicalFiles();
		file = new LogicalFile(SYSTEM_OWNER, system, httpUri.toString().toLowerCase(), destPath.toLowerCase(), "originalFilename");
//		LogicalFileDao.persist(file);
	}

	@DataProvider
	protected Object[][] caseInsensitiveProvider() {
		return new Object[][] {
				{ file.getPath(), StringUtils.upperCase(file.getPath()) },
				{ file.getPath(), StringUtils.swapCase(file.getPath()) },
				{ StringUtils.upperCase(file.getPath()), file.getPath() },
				{ StringUtils.swapCase(file.getPath()), file.getPath() },
		};
	}

	@Test(dataProvider="caseInsensitiveProvider",priority = 0)
	public void testFindBySystemCaseInsensitivePath(String originalPath, String caseInsensitivePath) {
		try {
			file.setPath(originalPath);
			LogicalFileDao.save(file);
			
			LogicalFile caseInsensitiveFile = new LogicalFile(SYSTEM_OWNER, system, file.getSourceUri(), caseInsensitivePath, "originalFilename");
			LogicalFileDao.save(caseInsensitiveFile);
			
			LogicalFile f = LogicalFileDao.findBySystemAndPath(file.getSystem(), originalPath);
			Assert.assertNotNull(f, "Failed to retrieve file by system and path");
			Assert.assertEquals(f.getPath(), originalPath, "case senstivity should be honored in logical file lookups by system and path");
			
			LogicalFile f2 = LogicalFileDao.findBySystemAndPath(caseInsensitiveFile.getSystem(), caseInsensitivePath);
			Assert.assertNotNull(f2, "Failed to retrieve file by by system and path");
			Assert.assertEquals(f2.getPath(), caseInsensitivePath, "case senstivity should be honored in logical file lookups by system and path");
			
			Assert.assertNotEquals(f.getUuid(), f2.getUuid(), "files with the system and same path, but mismatched case should not result in the same rsponse from a query");
		} catch (HibernateException e) {
			Assert.fail("Retrieving file by valid source and path should not throw an exception", e);
		}
	}
	
	@DataProvider
	protected Object[][] caseInsensitiveSourceUriProvider() {
		return new Object[][] {
				{ file.getSourceUri(), StringUtils.upperCase(file.getSourceUri()) },
				{ file.getSourceUri(), StringUtils.swapCase(file.getSourceUri()) },
		};
	}

	@Test(dataProvider="caseInsensitiveSourceUriProvider", priority = 1)
	public void testFindByCaseInsensitiveSourceUri(String originalUri, String caseInsensitiveUri) {
		try {

			file.setSourceUri(originalUri);
			LogicalFileDao.save(file);

			LogicalFile caseInsensitiveFile = new LogicalFile(SYSTEM_OWNER, system, caseInsensitiveUri, file.getPath(), "originalFilename");
			LogicalFileDao.save(caseInsensitiveFile);
			
			LogicalFile f = LogicalFileDao.findBySourceUrl(originalUri);
			Assert.assertNotNull(f, "Failed to retrieve file by sourceUrl");
			Assert.assertEquals(f.getSourceUri(), originalUri, "case senstivity should be honored in logical file lookups by sourceUrl");
			
			LogicalFile f2 = LogicalFileDao.findBySourceUrl(caseInsensitiveUri);
			Assert.assertNotNull(f2, "Failed to retrieve file by getSourceUri");
			Assert.assertEquals(f2.getSourceUri(), caseInsensitiveUri, "case senstivity should be honored in logical file lookups by getSourceUri");
			
			Assert.assertNotEquals(f.getUuid(), f2.getUuid(), "files with the same getSourceUri, but mismatched case should not result in the same rsponse from a query");
		} catch (HibernateException e) {
			Assert.fail("Retrieving file by valid getSourceUri should not throw an exception", e);
		}
	}
	
	@DataProvider
	protected Object[][] caseInsensitiveOwnerProvider() {
		return new Object[][] {
				{ file.getOwner(), StringUtils.upperCase(file.getOwner()) },
				{ file.getOwner(), StringUtils.swapCase(file.getOwner()) },
		};
	}

	@Test(dataProvider="caseInsensitiveOwnerProvider", priority = 2)
	public void testFindByCaseInsensitiveOwner(String originalOwner, String caseInsensitiveOwner) {
		try {
			file.setOwner(originalOwner);
			LogicalFileDao.save(file);
			
			LogicalFile caseInsensitiveFile = new LogicalFile(caseInsensitiveOwner, system, file.getSourceUri(), file.getPath(), "originalFilename");
			LogicalFileDao.save(caseInsensitiveFile);
			
			List<LogicalFile> f = LogicalFileDao.findByOwner(originalOwner);
			Assert.assertNotNull(f, "Failed to retrieve files by owner");
			Assert.assertEquals(f.size(), 1, "Failed to retrieve files by owner");
			Assert.assertEquals(f.get(0).getOwner(), originalOwner, "case senstivity should be honored in logical file lookups by owner");
			
			List<LogicalFile> f2 = LogicalFileDao.findByOwner(caseInsensitiveOwner);
			Assert.assertNotNull(f2, "Failed to retrieve file by caseInsensitiveOwner");
			Assert.assertEquals(f2.size(), 1, "Failed to retrieve files by caseInsensitiveOwner");
			Assert.assertEquals(f2.get(0).getOwner(), caseInsensitiveOwner, "case senstivity should be honored in logical file lookups by caseInsensitiveOwner");
			
			Assert.assertNotEquals(f.get(0).getUuid(), f2.get(0).getUuid(), "files with the same owner, but mismatched case should not result in the same rsponse from a query");
		} catch (HibernateException e) {
			Assert.fail("Retrieving file by valid owner should not throw an exception", e);
		}
	}
	
	
}
