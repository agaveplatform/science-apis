package org.iplantc.service.io.dao;

import org.apache.commons.io.FilenameUtils;
import org.hibernate.HibernateException;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.model.JSONTestDataUtil;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.StorageSystem;
import org.testng.Assert;
import org.testng.annotations.*;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.testng.Assert.*;

@Test(singleThreaded = true, groups={"integration"})
public class LogicalFileDaoIT extends BaseTestCase
{
	//	private LogicalFile file;
//	private LogicalFile sibling;
//	private LogicalFile cousin;
//	private LogicalFile parent;
//	private LogicalFile uncle;
//	private LogicalFile parentParent;
//	private LogicalFile parentParentParent;
//	private LogicalFile rootParent;
	private final SystemDao systemDao = new SystemDao();
	private StorageSystem system;
	private String destPath;
	private String basePath;
	private String otherPath;
	private URI httpUri;

	@BeforeClass
	protected void beforeClass() throws Exception
	{
		super.beforeClass();
		clearSystems();
		clearLogicalFiles();

		system = StorageSystem.fromJSON(jtd.getTestDataObject(JSONTestDataUtil.TEST_STORAGE_SYSTEM_FILE));
		system.setOwner(SYSTEM_OWNER);
		system.setPubliclyAvailable(true);
		system.setGlobalDefault(true);
		system.setAvailable(true);

		systemDao.persist(system);

		basePath = "/var/home/" + SYSTEM_OWNER + "/some";
		otherPath = "/var/home/" + SYSTEM_OWNER + "/other";
	}

	@BeforeMethod
	protected void setUp() throws Exception
	{
		clearLogicalFiles();
		destPath = String.format("/%s/%s/%s/%s", UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
	}

	@AfterMethod
	protected void tearDown() throws Exception
	{
		clearLogicalFiles();
	}

	@AfterClass
	protected void afterClass() throws Exception
	{
		clearSystems();
		clearLogicalFiles();
	}

	@Test
	public void testPersistNull() {
		try {
			LogicalFileDao.save(null);
			fail("null file should throw an exception");
		} catch (HibernateException e) {
			// null file should throw an exception
		}
	}

	@Test
	public void testPersist() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.persist(file);
			assertNotNull(file.getId(), "Failed to persist logical file");
		} catch (HibernateException e) {
			fail("Persisting logical file should not thorw exception", e);
		}
	}

	@Test(dependsOnMethods={"testPersist"})
	public void testFindByIdInvalid() {
		try {
			LogicalFile f = LogicalFileDao.findById(-1);
			assertNull(f,"Invalid id should return null");
		} catch (HibernateException e) {
			fail("Retrieving file by invalid id should not throw an exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindByIdInvalid"})
	public void testFindById() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);
			assertNotNull(file.getId(), "Failed to save the file");

			LogicalFile f = LogicalFileDao.findById(file.getId());
			assertNotNull(f, "Failed to retrieve file by id");
		} catch (HibernateException e) {
			fail("Retrieving file by id should not throw an exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindById"})
	public void testFindBySystemPath() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);
			LogicalFile f = LogicalFileDao.findBySystemAndPath(file.getSystem(), file.getPath());
			assertNotNull(f,"Failed to retrieve file by url");
		} catch (HibernateException e) {
			fail("Retrieving file by valid url should not throw an exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindBySystemPath"})
	public void testFindBySystemAndNullPath() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);
			LogicalFileDao.findBySystemAndPath(file.getSystem(), null);
			fail("Null path should throw exception");
		} catch (HibernateException e) {
			assertTrue(true);
		}
	}

	@Test(dependsOnMethods={"testFindBySystemAndNullPath"})
	public void testFindByNullSystemAndPath() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);
			LogicalFileDao.findBySystemAndPath(null, file.getPath());
			fail("Null system should throw exception");
		} catch (HibernateException e) {
			assertTrue(true);
		}
	}

	@Test(dependsOnMethods={"testFindByNullSystemAndPath"})
	public void testFindByNullSystemAndNullPath() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);
			LogicalFileDao.findBySystemAndPath(null, null);
			fail("Null system and path should throw exception");
		} catch (HibernateException e) {
			assertTrue(true);
		}
	}

	@Test(dependsOnMethods={"testFindByNullSystemAndNullPath"})
	public void testFindParent() {
		try {
			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(destPath));
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile foundParent = LogicalFileDao.findParent(file);
			assertEquals(parent, foundParent, "Parent of file not found");
		} catch (HibernateException e) {
			fail("Looking for existing parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindParent"})
	public void testFindParentMissing() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile foundParent = LogicalFileDao.findParent(file);
			assertNull(foundParent, "No parent should return null parent value");
		} catch (HibernateException e) {
			fail("Looking for existing parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindParentMissing"})
	public void testFindParentReturnsFirstParent() {
		try {

			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(destPath));
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile foundParent = LogicalFileDao.findParent(file);
			assertEquals(parent, foundParent, "findParent should return first parent");
		} catch (HibernateException e) {
			fail("Looking for existing parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindParentReturnsFirstParent"})
	public void testFindClosestParentNullSystem() {
		try {
			LogicalFileDao.findClosestParent(null, "/");
			fail("Null system and path should throw exception");
		} catch (HibernateException e) {
			assertTrue(true);
		}
	}

	@Test(dependsOnMethods={"testFindClosestParentNullSystem"})
	public void testFindClosestParentNullPath() {
		try {
			LogicalFileDao.findClosestParent(system, null);
			fail("Null path should throw exception");
		} catch (HibernateException e) {
			assertTrue(true);
		}
	}

	@Test(dependsOnMethods={"testFindClosestParentNullPath"})
	public void testFindClosestParentNullSystemNullPath() {
		try {
			LogicalFileDao.findClosestParent(null, null);
			fail("Null system and path should throw exception");
		} catch (HibernateException e) {
			assertTrue(true);
		}
	}

	@Test(dependsOnMethods={"testFindClosestParentNullSystemNullPath"})
	public void testFindClosestParentReturnsImmediateParent() {
		try {
			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(destPath));
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile foundParent = LogicalFileDao.findClosestParent(file.getSystem(), file.getPath());
			assertEquals(parent, foundParent, "findClosestParent "
					+ "should return immediate parent " + parent.getPath() + " when present");
		} catch (HibernateException e) {
			fail("Find closest parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindClosestParentReturnsImmediateParent"})
	public void testFindClosestParentReturnsRootParentWhenNoIntermediate() {
		try {

			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile foundParent = LogicalFileDao.findClosestParent(file.getSystem(), file.getPath());
			assertEquals(rootParent, foundParent, "findClosestParent "
					+ "should return root parent " + rootParent.getPath() +
					" when no other parents are present");
		} catch (HibernateException e) {
			fail("Find closest parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindClosestParentReturnsRootParentWhenNoIntermediate"})
	public void testFindClosestParentReturnsNullWhenNoIntermediateAndNoRootParent() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile foundParent = LogicalFileDao.findClosestParent(file.getSystem(), file.getPath());
			assertNull(foundParent, "findClosestParent "
					+ "should return null when no parent or root known.");
		} catch (HibernateException e) {
			fail("Find closest parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindClosestParentReturnsNullWhenNoIntermediateAndNoRootParent"})
	public void testFindClosestParentReturnsImmediateParentWhenImmediateAndMultipleParentsExist() {
		try {
			Path parentParentParentPath = Paths.get("/")
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString());
			LogicalFile parentParentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentParentParentPath.toString());
			parentParentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParentParent);

			Path parentParentPath = parentParentParentPath
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString());

			LogicalFile parentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentParentPath.toString());
			parentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParent);

			Path parentPath = parentParentPath
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString());
			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentPath.toString());
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			Path filePath = parentPath
					.resolve(UUID.randomUUID().toString());
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, filePath.toString());
			LogicalFileDao.save(file);

			LogicalFile foundParent = LogicalFileDao.findClosestParent(file.getSystem(), file.getPath());
			assertEquals(foundParent, parent, "findClosestParent "
					+ "should return immediate parent when more than one parent is known.");
		} catch (HibernateException e) {
			fail("Find closest parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindClosestParentReturnsImmediateParentWhenImmediateAndMultipleParentsExist"})
	public void testFindClosestParentReturnsFirstParentWhenMultipleParentsExist() {
		try {
			Path parentParentParentPath = Paths.get("/")
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString());
			LogicalFile parentParentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentParentParentPath.toString());
			parentParentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParentParent);

			Path parentParentPath = parentParentParentPath
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString());

			LogicalFile parentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentParentPath.toString());
			parentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParent);


			Path filePath = parentParentPath
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString());
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, filePath.toString());
			LogicalFileDao.save(file);

			LogicalFile foundParent = LogicalFileDao.findClosestParent(file.getSystem(), file.getPath());
			assertEquals(foundParent, parentParent, "findClosestParent "
					+ "should return first parent when more than one parent is known.");
		} catch (HibernateException e) {
			fail("Find closest parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindClosestParentReturnsFirstParentWhenMultipleParentsExist"})
	public void testFindClosestParentReturnsFirstParentWhenMultipleParentsAndRootExist() {
		try {
			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			Path parentPath = Paths.get("/")
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString());
			LogicalFile parentParentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentPath.toString());
			parentParentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParentParent);

			Path filePath = parentPath
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString())
					.resolve(UUID.randomUUID().toString());

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, filePath.toString());
			LogicalFileDao.save(file);

			LogicalFile foundParent = LogicalFileDao.findClosestParent(file.getSystem(), file.getPath());
			assertEquals(foundParent, parentParentParent, "findClosestParent "
					+ "should return first parent when more than one parent is known.");
		} catch (HibernateException e) {
			fail("Find closest parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindClosestParentReturnsFirstParentWhenMultipleParentsAndRootExist"})
	public void testFindClosestParentReturnsNullWithoutParents() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile foundParent = LogicalFileDao.findClosestParent(file.getSystem(), file.getPath());
			assertNull(foundParent, "findClosestParent "
					+ "should return null when no parents are present");
		} catch (HibernateException e) {
			fail("Find closest parent should throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindParentReturnsFirstParent"})
	public void testFindChildrenOfFolder()
	{
		try
		{
			List<LogicalFile> srcFiles = initTestFiles(basePath);

			LogicalFile srcFile = null;
			for (int i=0; i<srcFiles.size(); i++) {
				if (!srcFiles.get(i).getPath().equals(basePath)) continue;
				srcFile = srcFiles.get(i);
				break;
			}

			List<LogicalFile> children = LogicalFileDao.findChildren(basePath, system.getId());
			assertFalse(children.contains(srcFile), "Parent folder should not be returned when looking for children");
			assertEquals(children.size(), srcFiles.size()-1, "Number of children returned for " + basePath + " is incorrect");

			for (LogicalFile file: srcFiles.subList(1, srcFiles.size() -1)) {
				assertTrue(children.contains(file), "Logical file for " + file.getPath() + " was not returned as a child of " + basePath);
			}

		} catch (HibernateException e) {
			fail("Finding children of " + basePath, e);
		}
	}

	@Test(dependsOnMethods={"testFindChildrenOfFolder"})
	public void testFindChildrenOfFile()
	{
		try
		{
			List<LogicalFile> srcFiles = initTestFiles(basePath);

			LogicalFile srcFile = null;
			for (int i=0; i<srcFiles.size(); i++) {
				if (srcFiles.get(i).isDirectory()) continue;
				srcFile = srcFiles.get(i);
				break;
			}

			List<LogicalFile> children = LogicalFileDao.findChildren(srcFile.getPath(), system.getId());
			assertTrue(children.isEmpty(), "No children should be returned for a file " + srcFiles.get(1).getPath());

		} catch (HibernateException e) {
			fail("Finding children of " + basePath, e);
		}
	}

	@Test(dependsOnMethods={"testFindChildrenOfFile"})
	public void testFindChildrenOfEmptyFolder()
	{
		try
		{
			List<LogicalFile> srcFiles = initTestFiles(basePath);

			String emptyfoldername = "emptyfolder" + System.currentTimeMillis();
			LogicalFile emptyFolder = new LogicalFile(SYSTEM_OWNER, system, null, basePath + "/" + emptyfoldername, emptyfoldername, "PROCESSING", LogicalFile.DIRECTORY);
			LogicalFileDao.persist(emptyFolder);

			List<LogicalFile> children = LogicalFileDao.findChildren(emptyFolder.getPath(), system.getId());
			assertTrue(children.isEmpty(),
					"No children should be returned for an empty folder " + srcFiles.get(srcFiles.size() - 1).getPath());

		} catch (HibernateException e) {
			fail("Finding children of " + basePath, e);
		}
	}



	@Test(dependsOnMethods={"testFindChildrenOfEmptyFolder"})
	public void testFindNonOverlappingChildrenEmptyOnIdenticalFolder()
	{
		try {
			List<LogicalFile> srcFiles = initTestFiles(basePath);
			List<LogicalFile> destFiles = initTestFiles(otherPath);

			LogicalFile srcFile = null;
			LogicalFile destFile = null;
			for (int i=0; i<srcFiles.size(); i++) {
				if (!srcFiles.get(i).isDirectory()) continue;
				srcFile = srcFiles.get(i);
				destFile = destFiles.get(i);
				break;
			}
			List<LogicalFile> overlappingChildren =
					LogicalFileDao.findNonOverlappingChildren(srcFile.getPath(),
							srcFile.getSystem().getId(),
							destFile.getPath(),
							destFile.getSystem().getId());

			assertTrue(overlappingChildren.isEmpty(),
					"No children should be returned when copying a folder with an identical tree");

//			assertEquals(overlappingChildren.size(), srcFiles.size(), "Copying an identical tree should return all children of " + basePath);
//
//			for (LogicalFile file: srcFiles) {
//				assertTrue(overlappingChildren.contains(file), "Logical files for child " + file.getPath() + " was not returned as an overlapping child of " + basePath);
//			}

		} catch (HibernateException e) {
			fail("Finding non-overlapping children should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindNonOverlappingChildrenEmptyOnIdenticalFolder"})
	public void testFindNonOverlappingChildrenEmptyOnIdenticalFfile()
	{
		try {
			List<LogicalFile> srcFiles = initTestFiles(basePath);
			List<LogicalFile> destFiles = initTestFiles(otherPath);

			LogicalFile srcFile = null;
			LogicalFile destFile = null;
			for (int i=0; i<srcFiles.size(); i++) {
				if (srcFiles.get(i).isDirectory()) continue;
				srcFile = srcFiles.get(i);
				destFile = destFiles.get(i);
				break;
			}
			List<LogicalFile> overlappingChildren =
					LogicalFileDao.findNonOverlappingChildren(srcFile.getPath(),
							srcFile.getSystem().getId(),
							destFile.getPath(),
							destFile.getSystem().getId());

			assertTrue(overlappingChildren.isEmpty(),
					"No children should be returned when copying a file");

		} catch (HibernateException e) {
			fail("Finding non-overlapping children should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindNonOverlappingChildrenEmptyOnIdenticalFfile"})
	public void testFindNonOverlappingChildrenReturnsAllChildrenOnFullCopy()
	{
		try {
			List<LogicalFile> srcFiles = initTestFiles(basePath);
			List<LogicalFile> destFiles = initTestFiles(otherPath);

			List<LogicalFile> overlappingChildren =
					LogicalFileDao.findNonOverlappingChildren(basePath,
							system.getId(),
							otherPath + "/bingo",
							system.getId());

			for (int i=0; i<srcFiles.size(); i++) {
				if (srcFiles.get(i).getPath().equals(basePath)) {
					continue;
				}
				assertTrue(overlappingChildren.contains(srcFiles.get(i)), "Logical files for child " +
						srcFiles.get(i).getPath() + " was not returned as a non overlapping child of " + basePath);
			}

		} catch (HibernateException e) {
			fail("Finding non-overlapping children should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindNonOverlappingChildrenEmptyOnIdenticalFfile"})
	public void testFindNonOverlappingChildrenReturnsOnlyOverlappingChildrenOnFullCopy()
	{
		try {
			List<LogicalFile> srcFiles = initTestFiles(basePath);
			List<LogicalFile> destFiles = initTestFiles(otherPath);
			LogicalFile overlappingChild = new LogicalFile(SYSTEM_OWNER,
					system, null, otherPath + "/folder/subfolder/foo.dat", "foo.dat", "PROCESSING", LogicalFile.RAW);
			LogicalFileDao.persist(overlappingChild);

			List<LogicalFile> overlappingChildren =
					LogicalFileDao.findNonOverlappingChildren(basePath,
							system.getId(),
							otherPath + "/folder/subfolder",
							system.getId());

			assertFalse(overlappingChildren.contains(overlappingChild),
					"Overlapping file " + overlappingChild.getPath() + " should not be returned when copying from " +
							basePath + " to " + otherPath + "/folder/subfolder");

			for (LogicalFile file: srcFiles)
			{
				if (file.getName().equals(overlappingChild.getName())) continue;
				if (file.getPath().equals(basePath)) continue;

				assertTrue(overlappingChildren.contains(file), "Logical files for child " +
						file.getPath() + " was not returned as a non overlapping child of " + basePath);
			}

		} catch (HibernateException e) {
			fail("Moving logical file tree should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindNonOverlappingChildrenReturnsOnlyOverlappingChildrenOnFullCopy"})
	public void testFindParentReturnsClosestParent() {
		try {
			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile foundParent = LogicalFileDao.findParent(file);
			assertNull(foundParent, "No parent should be returned if the direct parent folder is not known");
		} catch (HibernateException e) {
			fail("Looking for existing parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindParentReturnsClosestParent"})
	public void testFindParentNullArgumentThrowsException() {
		try {
			LogicalFileDao.findParent(null);
			fail("Null argument to findParent should throw exception");
		} catch (HibernateException e) {

		}
	}

	@Test(dependsOnMethods={"testFindParentNullArgumentThrowsException"})
	public void testFindByOwnerNull() {
		try {
			LogicalFileDao.findByOwner(null);
			Assert.fail("null username should throw an exception");
		} catch (HibernateException e) {
			// null username should throw an exception
		}
	}

	@Test(dependsOnMethods={"testFindByOwnerNull"})
	public void testFindByOwnerEmpty() {
		try {
			LogicalFileDao.findByOwner("");
			fail("Empty username should throw an exception");
		} catch (HibernateException e) {
			// empty username should throw an exception
		}
	}

	@Test(dependsOnMethods={"testFindByOwnerEmpty"})
	public void testFindByOwner() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			List<LogicalFile> files = LogicalFileDao.findByOwner(file.getOwner());
			assertNotNull(files,"Failed to retrieve inputs for " + file.getOwner());
			assertFalse(files.isEmpty(), "No files retrieved for " + file.getOwner());
		} catch (HibernateException e) {
			fail("Retrieving files for " + SYSTEM_OWNER + " should not throw an exception", e);
		}
	}

	@Test(dependsOnMethods={"testFindByOwner"})
	public void testUpdateTransferStatusLogicalFileNullString() {
		try {
			LogicalFileDao.updateTransferStatus(null, StagingTaskStatus.STAGING_COMPLETED, SYSTEM_OWNER);
			fail("Empty file should throw an exception");
		} catch (HibernateException e) {
			// empty file should throw an exception
		}
	}

//	@Test(dependsOnMethods={"testPersist", "testFindById"})
//	public void testUpdateTransferStatusLogicalFileStringNull() {
//		try {
//			LogicalFileDao.updateTransferStatus(file, (String)null);
//			fail("null status should throw an exception");
//		} catch (HibernateException e) {
//			// null status should throw an exception
//		}
//	}
//
//	@Test(dependsOnMethods={"testPersist", "testFindById"})
//	public void testUpdateTransferStatusLogicalFileStringEmpty() {
//		try {
//			LogicalFileDao.updateTransferStatus(file,"");
//			fail("Empty status should throw an exception");
//		} catch (HibernateException e) {
//			// empty status should throw an exception
//		}
//	}

	@Test(dependsOnMethods={"testUpdateTransferStatusLogicalFileNullString"})
	public void testUpdateTransferStatusLogicalFileString() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.updateTransferStatus(file, StagingTaskStatus.STAGING_COMPLETED, file.getOwner());
			LogicalFile f = LogicalFileDao.findById(file.getId());
			assertNotNull(f, "Test file should be returned from search by id");
			assertEquals(f.getStatus(),StagingTaskStatus.STAGING_COMPLETED.name(), "Status failed to updated");
		} catch (HibernateException e) {
			fail("Failed to update status", e);
		}
	}

	@Test(dependsOnMethods={"testUpdateTransferStatusLogicalFileString"})
	public void testUpdateTransferStatusStringNullString() {
		try {
			LogicalFileDao.updateTransferStatus(null, StagingTaskStatus.STAGING_FAILED, SYSTEM_OWNER);
			fail("null url should throw an exception");
		} catch (HibernateException e) {
			// null url should throw an exception
		}
	}

	@Test(dependsOnMethods={"testUpdateTransferStatusStringNullString"})
	public void testUpdateTransferStatusStringEmptyString() {
		try {
			LogicalFileDao.updateTransferStatus(null, "", StagingTaskStatus.STAGING_FAILED.name());
			fail("Empty url should throw an exception");
		} catch (HibernateException e) {
			// empty url should throw an exception
		}
	}

	@Test(dependsOnMethods={"testUpdateTransferStatusStringEmptyString"})
	public void testUpdateTransferStatusStringStringNull() {
		try {
			LogicalFileDao.updateTransferStatus(system, destPath, null);
			fail("null status should throw an exception");
		} catch (HibernateException e) {
			// null status should throw an exception
		}
	}

	@Test(dependsOnMethods={"testUpdateTransferStatusStringStringNull"})
	public void testUpdateTransferStatusStringStringEmpty() {
		try {
			LogicalFileDao.updateTransferStatus(system, destPath,"");
			fail("Empty status should throw an exception");
		} catch (HibernateException e) {
			// empty status should throw an exception
		}
	}

	@Test(dependsOnMethods={"testUpdateTransferStatusStringStringEmpty"})
	public void testUpdateTransferStatusStringString() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFileDao.updateTransferStatus(file.getSystem(), file.getPath(),StagingTaskStatus.STAGING_FAILED.name());
			LogicalFile f = LogicalFileDao.findById(file.getId());
			assertNotNull(f, "Test file should be returned from search by id");
			assertEquals(f.getStatus(), StagingTaskStatus.STAGING_FAILED.name(), "Status failed to updated");
		} catch (HibernateException e) {
			fail("Failed to update status", e);
		}
	}

	@Test(dependsOnMethods={"testUpdateTransferStatusStringString"})
	public void testRemoveNull() {
		try {
			LogicalFileDao.remove(null);
			fail("null file should throw an exception");
		} catch (HibernateException e) {
			// null file should throw an exception
		}
	}

	@Test(dependsOnMethods={"testRemoveNull"})
	public void testRemove() {
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			assertNotNull(file.getId(), "Failed to save logical file");
			Long id = file.getId();

			LogicalFileDao.remove(file);
			assertNull(LogicalFileDao.findById(id), "Failed to delete logical file");
		} catch (HibernateException e) {
			fail("File deletion should not throw an exception", e);
		}
	}

	@Test(dependsOnMethods={"testRemove"})
	public void testDeleteSubtreePathDoesNotDeleteFile()
	{
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(destPath));
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			LogicalFile parentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parent.getPath()));
			parentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParent);

			LogicalFile parentParentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parentParent.getPath()));
			parentParentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParentParent);

			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			LogicalFileDao.deleteSubtreePath(file.getPath(), file.getSystem().getId());

			LogicalFile foundFile = LogicalFileDao.findById(file.getId());

			assertEquals(foundFile, file,"deleteSubtreePath "
					+ "should not delete the logical file at hte given path.");
		} catch (HibernateException e) {
			fail("Find closest parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testRemove"})
	public void testDeleteSubtreePathDoesDeletesChildFiles()
	{
		try {
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(destPath));
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			LogicalFile parentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parent.getPath()));
			parentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParent);

			LogicalFile parentParentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parentParent.getPath()));
			parentParentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParentParent);

			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			LogicalFileDao.deleteSubtreePath(parent.getPath(), parent.getSystem().getId());

			LogicalFile foundFile = LogicalFileDao.findById(file.getId());

			assertNull(foundFile,"deleteSubtreePath "
					+ "should delete files under the given path.");
		} catch (HibernateException e) {
			fail("Find closest parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testRemove"})
	public void testDeleteSubtreePathDoesDeletesChildDirectories()
	{
		try {
			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(destPath));
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			LogicalFile parentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parent.getPath()));
			parentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParent);

			LogicalFile parentParentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parentParent.getPath()));
			parentParentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParentParent);

			LogicalFile uncle = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentParent.getPath() + "/sibling");
			uncle.setStatus(StagingTaskStatus.STAGING_QUEUED);
			uncle.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(uncle);

			LogicalFile cousin = new LogicalFile(SYSTEM_OWNER, system, httpUri, uncle.getPath() + "/cousin.dat");
			cousin.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(cousin);

			LogicalFile sibling = new LogicalFile(SYSTEM_OWNER, system, httpUri, parent.getPath() + "/sibling.dat");
			sibling.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(sibling);

			LogicalFileDao.deleteSubtreePath(parentParent.getPath(), parentParent.getSystem().getId());

			assertNull(LogicalFileDao.findById(sibling.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies and files under the given path.");

			assertNull(LogicalFileDao.findById(cousin.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies and files under the given path.");

			assertNull(LogicalFileDao.findById(uncle.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies and files under the given path.");

			assertNull(LogicalFileDao.findById(parent.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies and files under the given path.");

			assertNull(LogicalFileDao.findById(file.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies and files under the given path.");

		} catch (HibernateException e) {
			fail("Find closest parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testRemove"})
	public void testDeleteSubtreePathDoesNotDeletesSiblingDirectories()
	{
		try {
			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(destPath));
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			LogicalFile parentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parent.getPath()));
			parentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParent);

			LogicalFile parentParentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parentParent.getPath()));
			parentParentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParentParent);

			LogicalFile uncle = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentParent.getPath() + "/sibling");
			uncle.setStatus(StagingTaskStatus.STAGING_QUEUED);
			uncle.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(uncle);

			LogicalFile cousin = new LogicalFile(SYSTEM_OWNER, system, httpUri, uncle.getPath() + "/cousin.dat");
			cousin.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(cousin);

			LogicalFile sibling = new LogicalFile(SYSTEM_OWNER, system, httpUri, parent.getPath() + "/sibling.dat");
			sibling.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(sibling);

			LogicalFileDao.deleteSubtreePath(parent.getPath(), parent.getSystem().getId());

			assertNotNull(LogicalFileDao.findById(parent.getId()),
					"deleteSubtreePath should not delete the given path.");


			assertNull(LogicalFileDao.findById(sibling.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies and files under the given path.");

			assertNull(LogicalFileDao.findById(file.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies and files under the given path.");


			assertNotNull(LogicalFileDao.findById(uncle.getId()),
					"deleteSubtreePath should not delete sibling to the given path.");

			assertNotNull(LogicalFileDao.findById(cousin.getId()),
					"deleteSubtreePath should not delete children of sibling to the given path.");


		} catch (HibernateException e) {
			fail("Find closest parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testRemove"})
	public void testDeleteSubtreePathDoesDeletesEntireTree()
	{
		try {
			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(destPath));
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			LogicalFile parentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parent.getPath()));
			parentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParent);

			LogicalFile parentParentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parentParent.getPath()));
			parentParentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParentParent);

			LogicalFile uncle = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentParent.getPath() + "/sibling");
			uncle.setStatus(StagingTaskStatus.STAGING_QUEUED);
			uncle.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(uncle);

			LogicalFile cousin = new LogicalFile(SYSTEM_OWNER, system, httpUri, uncle.getPath() + "/cousin.dat");
			cousin.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(cousin);

			LogicalFile sibling = new LogicalFile(SYSTEM_OWNER, system, httpUri, parent.getPath() + "/sibling.dat");
			sibling.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(sibling);

			LogicalFileDao.deleteSubtreePath(rootParent.getPath(), rootParent.getSystem().getId());

			assertEquals(LogicalFileDao.findById(rootParent.getId()), rootParent,
					"deleteSubtreePath should not delete the given path.");

			assertNull(LogicalFileDao.findById(parentParentParent.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies under the given path.");

			assertNull(LogicalFileDao.findById(parentParent.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies under the given path.");

			assertNull(LogicalFileDao.findById(parent.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies under the given path.");

			assertNull(LogicalFileDao.findById(file.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies and files under the given path.");

			assertNull(LogicalFileDao.findById(sibling.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies and files under the given path.");

			assertNull(LogicalFileDao.findById(cousin.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies and files under the given path.");

			assertNull(LogicalFileDao.findById(uncle.getId()),
					"deleteSubtreePath should delete multiple directory hierarchies and files under the given path.");

		} catch (HibernateException e) {
			fail("Find closest parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testDeleteSubtreePathDoesDeletesEntireTree"})
	public void testFindParentsAbsoluteRootReturnsNoParents()
	{
		try
		{
			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(destPath));
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			LogicalFile parentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parent.getPath()));
			parentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParent);

			LogicalFile parentParentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parentParent.getPath()));
			parentParentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParentParent);

			LogicalFile uncle = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentParent.getPath() + "/sibling");
			uncle.setStatus(StagingTaskStatus.STAGING_QUEUED);
			uncle.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(uncle);

			LogicalFile cousin = new LogicalFile(SYSTEM_OWNER, system, httpUri, uncle.getPath() + "/cousin.dat");
			cousin.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(cousin);

			LogicalFile sibling = new LogicalFile(SYSTEM_OWNER, system, httpUri, parent.getPath() + "/sibling.dat");
			sibling.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(sibling);



			List<LogicalFile> parents = LogicalFileDao.findParents(rootParent.getSystem(), rootParent.getPath());

			assertTrue(parents.isEmpty(), "No parents should be returned for /");
		} catch (HibernateException e) {
			fail("Looking for existing parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testDeleteSubtreePathDoesDeletesEntireTree"})
	public void testFindParentsRootDirectoryReturnsNoParents()
	{
		try
		{
			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(destPath));
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			LogicalFile parentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parent.getPath()));
			parentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParent);

			LogicalFile parentParentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parentParent.getPath()));
			parentParentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParentParent);

			LogicalFile uncle = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentParent.getPath() + "/sibling");
			uncle.setStatus(StagingTaskStatus.STAGING_QUEUED);
			uncle.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(uncle);

			LogicalFile cousin = new LogicalFile(SYSTEM_OWNER, system, httpUri, uncle.getPath() + "/cousin.dat");
			cousin.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(cousin);

			LogicalFile sibling = new LogicalFile(SYSTEM_OWNER, system, httpUri, parent.getPath() + "/sibling.dat");
			sibling.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(sibling);


			List<LogicalFile> parents = LogicalFileDao.findParents(rootParent.getSystem(), rootParent.getPath());

			assertTrue(parents.isEmpty(), "No parents should be returned for root parent");
		} catch (HibernateException e) {
			fail("Looking for existing parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testDeleteSubtreePathDoesDeletesEntireTree"})
	public void testFindParents() {
		try {
			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(destPath));
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			LogicalFile parentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parent.getPath()));
			parentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParent);

			LogicalFile parentParentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parentParent.getPath()));
			parentParentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParentParent);

			LogicalFile uncle = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentParent.getPath() + "/sibling");
			uncle.setStatus(StagingTaskStatus.STAGING_QUEUED);
			uncle.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(uncle);

			LogicalFile cousin = new LogicalFile(SYSTEM_OWNER, system, httpUri, uncle.getPath() + "/cousin.dat");
			cousin.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(cousin);

			LogicalFile sibling = new LogicalFile(SYSTEM_OWNER, system, httpUri, parent.getPath() + "/sibling.dat");
			sibling.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(sibling);


			List<LogicalFile> parents = LogicalFileDao.findParents(file.getSystem(), file.getPath());

			assertFalse(parents.contains(file), "findParents should not be included the given path");

			assertTrue(parents.contains(parent), "findParents should included parent of the given path");
			assertTrue(parents.contains(parentParent), "findParents should included all parents of the given path");
			assertTrue(parents.contains(parentParentParent), "findParents should included all parents of the given path");
			assertTrue(parents.contains(rootParent), "findParents should included the root parent of the given path");

			assertFalse(parents.contains(sibling), "findParents should not included sibling");
			assertFalse(parents.contains(cousin), "findParents should not included children of parent siblings");
			assertFalse(parents.contains(uncle), "findParents should not included siblings of parents");

			assertTrue(parents.contains(parent), "findParents should included parent of the given path");

		} catch (HibernateException e) {
			fail("Looking for existing parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testDeleteSubtreePathDoesDeletesEntireTree"})
	public void testFindParentsNoSiblingResults()
	{
		try
		{
			LogicalFile rootParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, "/");
			rootParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			rootParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(rootParent);

			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			LogicalFile parent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(destPath));
			parent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parent);

			LogicalFile parentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parent.getPath()));
			parentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParent);

			LogicalFile parentParentParent = new LogicalFile(SYSTEM_OWNER, system, httpUri, FilenameUtils.getFullPathNoEndSeparator(parentParent.getPath()));
			parentParentParent.setStatus(StagingTaskStatus.STAGING_COMPLETED);
			parentParentParent.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(parentParentParent);

			LogicalFile uncle = new LogicalFile(SYSTEM_OWNER, system, httpUri, parentParent.getPath() + "/sibling");
			uncle.setStatus(StagingTaskStatus.STAGING_QUEUED);
			uncle.setNativeFormat(LogicalFile.DIRECTORY);
			LogicalFileDao.save(uncle);

			LogicalFile cousin = new LogicalFile(SYSTEM_OWNER, system, httpUri, uncle.getPath() + "/cousin.dat");
			cousin.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(cousin);

			LogicalFile sibling = new LogicalFile(SYSTEM_OWNER, system, httpUri, parent.getPath() + "/sibling.dat");
			sibling.setStatus(StagingTaskStatus.STAGING_QUEUED);
			LogicalFileDao.save(sibling);


			List<LogicalFile> parents = LogicalFileDao.findParents(cousin.getSystem(), cousin.getPath());

			assertFalse(parents.contains(cousin), "findParents should not be included the given path");

			assertTrue(parents.contains(uncle), "findParents should included parent of the given path");
			assertTrue(parents.contains(parentParent), "findParents should included all parents of the given path");
			assertTrue(parents.contains(parentParentParent), "findParents should included all parents of the given path");
			assertTrue(parents.contains(rootParent), "findParents should included the root parent of the given path");

			assertFalse(parents.contains(file), "findParents should not included children of parent siblings");
			assertFalse(parents.contains(parent), "findParents should not included parent siblings of the given path");

		} catch (HibernateException e) {
			fail("Looking for existing parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testDeleteSubtreePathDoesDeletesEntireTree"})
	public void testFindParentsEmptyResultWithNoParents()
	{
		try
		{
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			LogicalFileDao.save(file);

			List<LogicalFile> parents = LogicalFileDao.findParents(file.getSystem(), file.getPath());

			assertTrue(parents.isEmpty(), "findParents should not return results when there are no parents");

		} catch (HibernateException e) {
			fail("Looking for existing parent should not throw exception", e);
		}
	}

	@Test(dependsOnMethods={"testDeleteSubtreePathDoesDeletesEntireTree"})
	public void testFindParentsEmptyResultWithNoEntires()
	{
		try
		{
			LogicalFile file = new LogicalFile(SYSTEM_OWNER, system, httpUri, destPath);
			List<LogicalFile> parents = LogicalFileDao.findParents(file.getSystem(), file.getPath());

			assertTrue(parents.isEmpty(), "findParents should not return results when there are no no entries");

		} catch (HibernateException e) {
			fail("Looking for existing parent should not throw exception", e);
		}
	}

	private List<LogicalFile> initTestFiles(String path) {

		List<LogicalFile> srcFiles = new ArrayList<LogicalFile>();
		srcFiles.add(new LogicalFile(SYSTEM_OWNER, system, null, path, "folder", "PROCESSING", LogicalFile.DIRECTORY));
		srcFiles.add(new LogicalFile(SYSTEM_OWNER, system, null, path + "/folder", "folder", "PROCESSING", LogicalFile.DIRECTORY));
		srcFiles.add(new LogicalFile(SYSTEM_OWNER, system, null, path + "/folder/foo.dat", "foo.dat", "PROCESSING", LogicalFile.RAW));
		srcFiles.add(new LogicalFile(SYSTEM_OWNER, system, null, path + "/folder/bar.dat", "bar.dat", "PROCESSING", LogicalFile.RAW));
		srcFiles.add(new LogicalFile(SYSTEM_OWNER, system, null, path + "/folder/subfolder", "subfolder", "PROCESSING", LogicalFile.DIRECTORY));
		srcFiles.add(new LogicalFile(SYSTEM_OWNER, system, null, path + "/folder/subfolder/alpha.txt", "alpha.txt", "PROCESSING", LogicalFile.RAW));
		srcFiles.add(new LogicalFile(SYSTEM_OWNER, system, null, path + "/file.dat", "file.dat", "PROCESSING", LogicalFile.RAW));
		srcFiles.add(new LogicalFile(SYSTEM_OWNER, system, null, path + "/emptyfolder", "emptyfolder", "PROCESSING", LogicalFile.DIRECTORY));

		for (LogicalFile file: srcFiles) {
			file.setStatus(StagingTaskStatus.STAGING_QUEUED);
			file.setUuid(file.getPath());
			LogicalFileDao.save(file);
		}

		return srcFiles;
	}
}
