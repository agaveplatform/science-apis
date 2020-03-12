package org.iplantc.service.io.permissions;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.io.Settings;
import org.iplantc.service.io.model.JSONTestDataUtil;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.transfer.model.RemoteFilePermission;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups={"integration"})
public class PermissionManagerStaticUriReadTest extends AbstractPermissionManagerTest 
{
	@BeforeClass
	protected void beforeClass() throws Exception 
	{
		TenancyHelper.setCurrentTenantId("agave.dev");
		TenancyHelper.setCurrentEndUser(ADMIN_USER);

		super.beforeClass();
	}
	
	protected RemoteSystem getTestSystemDescription(RemoteSystemType type) throws Exception 
	{
		if (type.equals(RemoteSystemType.EXECUTION)) {
			return ExecutionSystem.fromJSON(jtd.getTestDataObject(JSONTestDataUtil.TEST_EXECUTION_SYSTEM_FILE));
		} else if (type.equals(RemoteSystemType.STORAGE)) {
			return StorageSystem.fromJSON(jtd.getTestDataObject(JSONTestDataUtil.TEST_STORAGE_SYSTEM_FILE));
		} else {
			throw new SystemException("RemoteSystem type " + type + " is not supported.");
		}
	}


	/**
	 * Generates test data arrays for the list of {@link RemoteSystem}s. The given {@link PermissionType} will be
	 * granted to the sharedUsed on the publicTestPath and privateTestPath based on whether the system is public
	 * or private. The test case will be generated for the sharedUser,
	 * @param systems the systems for which test cases will be generated
	 * @param permissionType the permission to grant the sharedUser on the testPath
	 * @param publicTestPath the to which permissions will be granted on a public systems. This is the exact path that will be tested.
	 * @param privateTestPath the to which permissions will be granted on a public systems. This is the exact path that will be tested.
	 * @param fileType whether the logical file should be a {@link LogicalFile#FILE} or {@link LogicalFile#DIRECTORY}
	 * @param sharedUser the user for whom the permission check will be granted
	 * @param testUser the user for whom the permission check will be made
	 * @param internalUsername the name of the internal user to check
	 * @param expectedResult true if the testUser should have the expected permission
	 * @param shouldThrowException true if an exception should be thrown
	 * @return list of object arrays representing a test case for each of the systems given the test criteria
	 * @throws Exception if there was an issue creating the logical file
	 */
	private Collection<? extends Object[]> _generateSameExpectationTestsForRecursiveExplicitGrant(RemoteSystem[] systems,
																								  PermissionType permissionType,
																								  String publicTestPath,
																								  String privateTestPath,
																								  String fileType,
																								  String sharedUser,
																								  String testUser,
																								  String internalUsername,
																								  boolean expectedResult,
																								  boolean shouldThrowException)
			throws Exception {

		List<Object[]> testList = new ArrayList<Object[]>();

		for (RemoteSystem system: systems) {
			String testPath = system.isPubliclyAvailable() ? publicTestPath : privateTestPath;

			createSharedLogicalFile(SYSTEM_OWNER,
					permissionType,
					sharedUser,
					system,
					testPath,
					fileType,
					true);

			testList.add(new Object[]{ system, testPath, testUser, internalUsername, expectedResult, shouldThrowException});
		}

		return testList;
	}

	/**
	 * Generates test data arrays for the list of {@link RemoteSystem}s. The given {@link PermissionType} will be
	 * granted to the sharedUsed on the publicTestPath and privateTestPath based on whether the system is public
	 * or private. The test case will be generated for the sharedUser,
	 * @param systems the systems for which test cases will be generated
	 * @param permissionType the permission to grant the sharedUser on the testPath
	 * @param publicTestPath the to which permissions will be granted on a public systems. This is the exact path that will be tested.
	 * @param privateTestPath the to which permissions will be granted on a public systems. This is the exact path that will be tested.
	 * @param fileType whether the logical file should be a {@link LogicalFile#FILE} or {@link LogicalFile#DIRECTORY}
	 * @param sharedUser the user for whom the permission check will be granted
	 * @param testUser the user for whom the permission check will be made
	 * @param internalUsername the name of the internal user to check
	 * @param expectedResult true if the testUser should have the expected permission
	 * @param shouldThrowException true if an exception should be thrown
	 * @return list of object arrays representing a test case for each of the systems given the test criteria
	 * @throws Exception if there was an issue creating the logical file
	 */
	private Collection<? extends Object[]> _generateSameExpectationTestsForExplicitGrant(RemoteSystem[] systems,
																						 PermissionType permissionType,
																						 String publicTestPath,
																						 String privateTestPath,
																						 String fileType,
																						 String sharedUser,
																						 String testUser,
																						 String internalUsername,
																						 boolean expectedResult,
																						 boolean shouldThrowException)
			throws Exception {

		List<Object[]> testList = new ArrayList<Object[]>();

		for (RemoteSystem system: systems) {
			String testPath = system.isPubliclyAvailable() ? publicTestPath : privateTestPath;

			createSharedLogicalFile(SYSTEM_OWNER,
					permissionType,
					sharedUser,
					system,
					testPath,
					fileType,
					false);

			testList.add(new Object[]{ system, testPath, testUser, internalUsername, expectedResult, shouldThrowException});
		}

		return testList;
	}

	/**
	 * Generates test data arrays for the list of {@link RemoteSystem}s. The given {@link PermissionType} will be
	 * granted to the sharedUsed on the publicTestPath and privateTestPath based on whether the system is public
	 * or private. The test case will be generated for the sharedUser,
	 * @param systems the systems for which test cases will be generated
	 * @param permissionType the permission to grant the sharedUser on the testPath
	 * @param publicBasePath the base path of the parent directory to which permissions will be granted on a public systems. The public system testPath will be relative to this directory.
	 * @param privateBasePath the base path of the parent directory to which permissions will be granted on a private systems. The public system testPath will be relative to this directory.
	 * @param testPath the path being tested relative to the publicTestPath and privateTestPath.
	 * @param fileType whether the logical file should be a {@link LogicalFile#FILE} or {@link LogicalFile#DIRECTORY}
	 * @param sharedUser the user for whom the permission check will be granted
	 * @param testUser the user for whom the permission check will be made
	 * @param internalUsername the name of the internal user to check
	 * @param expectedResult true if the testUser should have the expected permission
	 * @param shouldThrowException true if an exception should be thrown
	 * @return list of object arrays representing a test case for each of the systems given the test criteria
	 * @throws Exception if there was an issue creating the logical file
	 */
	private Collection<? extends Object[]> _generateSameExpectationTestsForImplicitGrant(RemoteSystem[] systems,
																						 PermissionType permissionType,
																						 String publicBasePath,
																						 String privateBasePath,
																						 String testPath,
																						 String fileType,
																						 String sharedUser,
																						 String testUser,
																						 String internalUsername,
																						 boolean expectedResult,
																						 boolean shouldThrowException)
			throws Exception {

		List<Object[]> testList = new ArrayList<Object[]>();

		for (RemoteSystem system: systems) {
			String systemSpecificBaseTestPath = system.isPubliclyAvailable() ? publicBasePath : privateBasePath;

			createSharedLogicalFile(SYSTEM_OWNER,
					permissionType,
					sharedUser,
					system,
					systemSpecificBaseTestPath,
					fileType,
					false);

			testList.add(new Object[]{
					system,
					String.format("%s/%s", systemSpecificBaseTestPath, testPath),
					testUser,
					internalUsername,
					expectedResult,
					shouldThrowException
			});
		}

		return testList;
	}

	/**
	 * Generates test data arrays for the list of {@link RemoteSystem}s. The given {@link PermissionType} will be
	 * checked for the <code>testUser</code>. No {@link RemoteFilePermission} or {@link LogicalFile} will be created.
	 *
	 * @param systems the systems for which test cases will be generated
	 * @param testPath the path being tested relative to the publicTestPath and privateTestPath.
	 * @param testUser the user for whom the permission check will be made
	 * @param internalUsername the name of the internal user to check
	 * @param expectedResult true if the testUser should have the expected permission
	 * @param shouldThrowException true if an exception should be thrown
	 * @return list of object arrays representing a test case for each of the systems given the test criteria
	 * @throws Exception if there was an issue creating the logical file
	 */
	private Collection<? extends Object[]> _generateSameExpectationTestsWithoutGrant(RemoteSystem[] systems,
																						 String testPath,
																						 String testUser,
																						 String internalUsername,
																						 boolean expectedResult,
																						 boolean shouldThrowException) {

		List<Object[]> testList = new ArrayList<Object[]>();

		for (RemoteSystem system: systems) {
			testList.add(new Object[]{
					system,
					testPath,
					testUser,
					internalUsername,
					expectedResult,
					shouldThrowException
			});
		}

		return testList;
	}

	/**
	 * Generates test cases to test read permissions for combinations of paths and directories outside of a home directory
	 * on the test system. Permissions will be granted to the sharedUser and tests will check access for the testUser.
	 * The system owner in all tests will be the {@link #SYSTEM_OWNER}.
	 *
	 * @param basePath the base path to check for on each system.
	 * @param sharedUser the user to whom the test permissions will be granted
	 * @param testUser the user for whom to test access
	 * @return full suite of tests for all system types
	 * @throws Exception if system parsing or persistence fails
	 */
	protected Object[][] _testCanReadInOutsideHomeProvider(String basePath, String sharedUser, String testUser) throws Exception
	{
		beforeTestData();
		List<Object[]> testList = new ArrayList<Object[]>();

		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);

		RemoteSystem[] systems = {
				publicStorageSystem,
				publicMirroredStorageSystem,
				publicGuestStorageSystem,
				privateStorageSystem,
				privateSharedGuestStorageSystem,
				privateSharedUserStorageSystem,
				privateSharedPublisherStorageSystem,
				privateSharedAdminStorageSystem
		};

		/* Users access on these systems is dependent on the actual permission
		 * grants {@link SystemRole} entitlements.
		 */
		RemoteSystem[] systemsWithoutRoles = {
				privateStorageSystem,
		};

		/* Users should always have read access to these systems due to their system
		 * {@link SystemRole} entitlements.
		 */
		RemoteSystem[] systemsWithSharedUserRole = {
				publicStorageSystem,
				publicMirroredStorageSystem,
				publicGuestStorageSystem,
				privateSharedGuestStorageSystem,
				privateSharedUserStorageSystem,
				privateSharedPublisherStorageSystem,
				privateSharedAdminStorageSystem
		};

		String testPath = String.format("%s/unsharedfolderoutsidehome", basePath);

		testList.addAll(_generateSameExpectationTestsForRecursiveExplicitGrant(
				systems,
				PermissionType.READ,
				testPath,
				testPath,
				LogicalFile.DIRECTORY,
				sharedUser,
				testUser,
				null,
				true,
				false));

		String publicTestPath = String.format("%s/%sfoldernotrecursiveexplicit", basePath, sharedUser);
		String privateTestPath = String.format("%s/%sfoldernotrecursiveexplicit", basePath, sharedUser);

		testList.addAll(_generateSameExpectationTestsForImplicitGrant(
				systemsWithoutRoles,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				"sharedfile.dat",
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				true,
				false));

		testList.addAll(_generateSameExpectationTestsForImplicitGrant(
				systemsWithSharedUserRole,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				"sharedfile.dat",
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				true,
				false));


		publicTestPath = String.format("%s/%sfoldernotrecursiveimplicit", basePath, sharedUser);
		privateTestPath = String.format("%s/%sfoldernotrecursiveimplicit", basePath, sharedUser);

		testList.addAll(_generateSameExpectationTestsForImplicitGrant(
				systemsWithoutRoles,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				"unsharedfile.dat",
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				false,
				false));

		testList.addAll(_generateSameExpectationTestsForImplicitGrant(
				systemsWithSharedUserRole,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				"unsharedfile.dat",
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				true,
				false));


		publicTestPath = String.format("%s/%sfolderrecursiveexplicit", basePath, sharedUser);
		privateTestPath = String.format("%s/%sfolderrecursiveexplicit", basePath, sharedUser);

		testList.addAll(_generateSameExpectationTestsForRecursiveExplicitGrant(
				systems,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				LogicalFile.DIRECTORY,
				sharedUser,
				testUser,
				null,
				true,
				false));

		publicTestPath = String.format("%s/%sfolderrecursiveimplicit", basePath, sharedUser);
		privateTestPath = String.format("%s/%sfolderrecursiveimplicit", basePath, sharedUser);

		testList.addAll(_generateSameExpectationTestsForImplicitGrant(
				systemsWithoutRoles,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				"unsharedfile.dat",
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				true,
				false));

		testList.addAll(_generateSameExpectationTestsForImplicitGrant(
				systemsWithSharedUserRole,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				"unsharedfile.dat",
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				true,
				false));



		publicTestPath = String.format("%s/%sfolderrecursiveexplicit/sharedfile.dat", basePath, sharedUser);
		privateTestPath = String.format("%s/%sfolderrecursiveexplicit/sharedfile.dat", basePath, sharedUser);

		testList.addAll(_generateSameExpectationTestsForRecursiveExplicitGrant(
				systems,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				LogicalFile.DIRECTORY,
				sharedUser,
				testUser,
				null,
				true,
				false));

		publicTestPath = String.format("%s/%ssharedfileexplicit.dat", basePath, sharedUser);
		privateTestPath = String.format("%s/%ssharedfileexplicit.dat", basePath, sharedUser);

		testList.addAll(_generateSameExpectationTestsForRecursiveExplicitGrant(
				systems,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				true,
				false));


		return testList.toArray(new Object[][]{});
	}

	/**
	 * Generates test cases to test read permissions for combinations of paths and directories within a home directory
	 * on the test system. Permissions will be granted to the sharedUser and tests will check access for the testUser.
	 * The system owner in all tests will be the {@link #SYSTEM_OWNER}.
	 *
	 * @param sharedUser the user to whom the test permissions will be granted
	 * @param testUser the user for whom to test access
	 * @return full suite of tests for all system types
	 * @throws Exception if system parsing or persistence fails
	 */
	protected Object[][] _testCanReadInUserHomeProvider(String sharedUser, String testUser) throws Exception
	{
		beforeTestData();
		List<Object[]> testList = new ArrayList<Object[]>();

		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);

		RemoteSystem[] systems = {
				publicStorageSystem,
				publicMirroredStorageSystem,
				publicGuestStorageSystem,
				privateStorageSystem,
				privateSharedGuestStorageSystem,
				privateSharedUserStorageSystem,
				privateSharedPublisherStorageSystem,
				privateSharedAdminStorageSystem
		};

		/* Users access on these systems is dependent on the actual permission
		 * grants {@link SystemRole} entitlements.
		 */
		RemoteSystem[] systemsWithoutRoles = {
				publicStorageSystem,
				publicMirroredStorageSystem,
				privateStorageSystem,
		};

		/* Users should always have read access to these systems due to their system
		 * {@link SystemRole} entitlements.
		 */
		RemoteSystem[] systemsWithSharedUserRole = {
				publicGuestStorageSystem,
				privateSharedGuestStorageSystem,
				privateSharedUserStorageSystem,
				privateSharedPublisherStorageSystem,
				privateSharedAdminStorageSystem
		};

		String privateHomeDir = privateStorageSystem.getStorageConfig().getHomeDir();
		String publicHomeDir = String.format("%s/%s", publicStorageSystem.getStorageConfig().getHomeDir(), SYSTEM_OWNER);

		String publicTestPath = String.format("%s/%sfoldernotrecursiveexplicit", publicHomeDir, sharedUser);
		String privateTestPath = String.format("%s/%sfoldernotrecursiveexplicit", privateHomeDir, sharedUser);

		testList.addAll(_generateSameExpectationTestsForExplicitGrant(
				systems,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				LogicalFile.DIRECTORY,
				sharedUser,
				testUser,
				null,
				true,
				false));


		publicTestPath = String.format("%s/%sfoldernotrecursiveexplicit", publicHomeDir, sharedUser);
		privateTestPath = String.format("%s/%sfoldernotrecursiveexplicit", privateHomeDir, sharedUser);

		testList.addAll(_generateSameExpectationTestsForImplicitGrant(
				systemsWithoutRoles,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				"sharedfile.dat",
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				false,
				false));

		testList.addAll(_generateSameExpectationTestsForImplicitGrant(
				systemsWithSharedUserRole,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				"sharedfile.dat",
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				true,
				false));


		publicTestPath = String.format("%s/%sfoldernotrecursiveimplicit", publicHomeDir, sharedUser);
		privateTestPath = String.format("%s/%sfoldernotrecursiveimplicit", privateHomeDir, sharedUser);

		testList.addAll(_generateSameExpectationTestsForImplicitGrant(
				systemsWithoutRoles,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				"unsharedfile.dat",
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				false,
				false));

		testList.addAll(_generateSameExpectationTestsForImplicitGrant(
				systemsWithSharedUserRole,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				"unsharedfile.dat",
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				true,
				false));


		publicTestPath = String.format("%s/%sfolderrecursiveexplicit", publicHomeDir, sharedUser);
		privateTestPath = String.format("%s/%sfolderrecursiveexplicit", privateHomeDir, sharedUser);

		testList.addAll(_generateSameExpectationTestsForRecursiveExplicitGrant(
				systems,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				LogicalFile.DIRECTORY,
				sharedUser,
				testUser,
				null,
				true,
				false));

		publicTestPath = String.format("%s/%sfolderrecursiveimplicit", publicHomeDir, sharedUser);
		privateTestPath = String.format("%s/%sfolderrecursiveimplicit", privateHomeDir, sharedUser);

		testList.addAll(_generateSameExpectationTestsForImplicitGrant(
				systemsWithoutRoles,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				"unsharedfile.dat",
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				false,
				false));

		testList.addAll(_generateSameExpectationTestsForImplicitGrant(
				systemsWithSharedUserRole,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				"unsharedfile.dat",
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				true,
				false));



		publicTestPath = String.format("%s/%sfolderrecursiveexplicit/sharedfile.dat", publicHomeDir, sharedUser);
		privateTestPath = String.format("%s/%sfolderrecursiveexplicit/sharedfile.dat", privateHomeDir, sharedUser);

		testList.addAll(_generateSameExpectationTestsForRecursiveExplicitGrant(
				systems,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				LogicalFile.DIRECTORY,
				sharedUser,
				testUser,
				null,
				true,
				false));

		publicTestPath = String.format("%s/%ssharedfileexplicit.dat", publicHomeDir, sharedUser);
		privateTestPath = String.format("%s/%ssharedfileexplicit.dat", privateHomeDir, sharedUser);

		testList.addAll(_generateSameExpectationTestsForRecursiveExplicitGrant(
				systems,
				PermissionType.READ,
				publicTestPath,
				privateTestPath,
				LogicalFile.FILE,
				sharedUser,
				testUser,
				null,
				true,
				false));


		return testList.toArray(new Object[][]{});
	}

	/**
	 * Generic test whether the given user can read the given uri
	 * 
	 * @param system
	 * @param path
	 * @param owner
	 * @param internalUsername
	 * @param expectedResult
	 * @param shouldThrowException
	 */
	@Override
	protected void _testCanRead(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		try 
		{
			// create a semantically correct uri to test from the system and path
			URI uri = new URI("agave://" + system.getSystemId() + "/" + path);

			boolean actualResult = PermissionManager.canUserReadUri(owner, internalUsername, uri);
			
			String errorMessage = String.format("User %s %s have permission to read %s on a %s%s%s", 
					owner,
					expectedResult ? "should have" : "should not have",
					uri.toString(),
					system.isPubliclyAvailable() ? "public" : "private",
					system.getUserRole(Settings.WORLD_USER_USERNAME).canRead() ? " readonly" : "",
					system.getType().name().toLowerCase() + " system");

			Assert.assertEquals( actualResult, expectedResult, errorMessage );
		
		}
		catch (Exception e)
		{
			if (!shouldThrowException) Assert.fail(e.getMessage(), e);
		}
	}

	/**
	 * Internal method to run the permission check for the various test permutations in this test class.
	 * @param uri
	 * @param owner
	 * @param internalUsername
	 * @param expectedResult
	 * @param shouldThrowException
	 */
	protected void _testCanReadUri(String uri, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		try 
		{
			// create a semantically correct uri to test from the system and path
			URI testUri = new URI(uri);
			
			boolean actualResult = PermissionManager.canUserReadUri(owner, internalUsername, testUri);
			
			String errorMessage = String.format("User %s %s have permission to read %s", 
					owner,
					expectedResult ? "should have" : "should not have",
					uri);

			Assert.assertEquals( actualResult, expectedResult, errorMessage );
		}
		catch (Exception e)
		{
			if (!shouldThrowException) Assert.fail(e.getMessage(), e);
		}
	}
	
	/************************************************************************
	/*							 SCHEMA TESTS      							*
	/************************************************************************/
	
	@DataProvider
	protected Object[][] testCanUserReadUriSchemaProvider() throws Exception
	{
		beforeTestData();
		RemoteSystem system = getPrivateSystem(RemoteSystemType.STORAGE);
		
		return new Object[][] {
				{ "agave://" + system.getSystemId() + "//", SYSTEM_OWNER, null, true, false },
				{ "http://storage.example.com//", SYSTEM_OWNER, null, true, false },
				{ "https://storage.example.com//", SYSTEM_OWNER, null, true, false },
				{ Settings.IPLANT_IO_SERVICE, SYSTEM_OWNER, null, true, false },
				{ Settings.IPLANT_IO_SERVICE + "/listings", SYSTEM_OWNER, null, true, false },
				{ Settings.IPLANT_IO_SERVICE + "listings", SYSTEM_OWNER, null, true, false },
				{ Settings.IPLANT_IO_SERVICE + "listings/", SYSTEM_OWNER, null, true, false },
				{ Settings.IPLANT_IO_SERVICE + "/media", SYSTEM_OWNER, null, true, false },
				{ Settings.IPLANT_IO_SERVICE + "media", SYSTEM_OWNER, null, true, false },
				{ Settings.IPLANT_IO_SERVICE + "media/", SYSTEM_OWNER, null, true, false },
				{ Settings.IPLANT_IO_SERVICE + "media//", SYSTEM_OWNER, null, true, false },
				{ Settings.IPLANT_IO_SERVICE + "media/system", SYSTEM_OWNER, null, false, true },
				{ Settings.IPLANT_IO_SERVICE + "media/system/", SYSTEM_OWNER, null, false, true },
				{ Settings.IPLANT_IO_SERVICE + "media/system//", SYSTEM_OWNER, null, false, true },
				{ "ftp://storage.example.com//", SYSTEM_OWNER, null, false, false },
				{ "gsissh://storage.example.com//", SYSTEM_OWNER, null, false, false },
				{ "s3://storage.example.com//", SYSTEM_OWNER, null, false, false },
				{ "irods://storage.example.com//", SYSTEM_OWNER, null, false, false },
				{ "sftp://storage.example.com//", SYSTEM_OWNER, null, false, false },
				{ "azure://storage.example.com//", SYSTEM_OWNER, null, false, false },
				{ "gsiftp://storage.example.com//", SYSTEM_OWNER, null, false, false },
				{ "booya://storage.example.com//", SYSTEM_OWNER, null, false, false },
				{ "http://storage.example.com", SYSTEM_OWNER, null, true, false },
				{ "://storage.example.com//", SYSTEM_OWNER, null, false, true },
				{ "//storage.example.com//", SYSTEM_OWNER, null, false, true },
				{ "C:\\storage.example.com\\", SYSTEM_OWNER, null, false, true },
				{ "file://storage.example.com//", SYSTEM_OWNER, null, false, false },
				{ "/", SYSTEM_OWNER, null, true, true },
				{ "", SYSTEM_OWNER, null, false, true },
				{ null, SYSTEM_OWNER, null, false, true },
		};
	}
	
	@Test(dataProvider="testCanUserReadUriSchemaProvider")
	public void testCanUserReadUriSchema(String uri, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanReadUri(uri, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanUserReadUriPathProvider() throws Exception
	{
		beforeTestData();
		RemoteSystem system = getPrivateSystem(RemoteSystemType.STORAGE);
		
		return new Object[][] {
				{ "agave://" + system.getSystemId() + "/", SYSTEM_OWNER, null, true, false },
				{ "agave://" + system.getSystemId() + "/" + SYSTEM_OWNER, SYSTEM_OWNER, null, true, false },
				{ "agave://" + system.getSystemId() + "//", SYSTEM_OWNER, null, true, false },
		};
	}
	
	@Test(dataProvider="testCanUserReadUriPathProvider")
	public void testCanUserReadUriPath(String uri, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanReadUri(uri, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanUserReadUriHostProvider() throws Exception
	{
		beforeTestData();
		RemoteSystem system = getPrivateSystem(RemoteSystemType.STORAGE);
		
		return new Object[][] {
				{ "agave://" + system.getSystemId() + "//", SYSTEM_OWNER, null, true, false },
				{ "agave:////", SYSTEM_OWNER, null, false, true }, // should find default storage system
				{ "agave://asmldkjfapsodufapojk//", SYSTEM_OWNER, null, false, true },
				{ "agave://a..b//", SYSTEM_OWNER, null, false, true },
				{ "agave://g^739//", SYSTEM_OWNER, null, false, true },
				{ "agave://127.0.0.1//", SYSTEM_OWNER, null, false, true },
				{ "agave://some-new-system" + System.currentTimeMillis() + "//", SYSTEM_OWNER, null, false, true },
		};
	}
	
	@Test(dataProvider="testCanUserReadUriHostProvider")
	public void testCanUserReadUriHost(String uri, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanReadUri(uri, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	public void testCanUserReadUriNullHostNoDefaultFails(String uri, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	throws Exception
	{
		beforeTestData();
		RemoteSystem privateSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		
		// no hostname in an internal url and no default system should throw exception
		_testCanReadUri("agave:////", SYSTEM_OWNER, null, false, true);
		_testCanReadUri("agave:////", SYSTEM_SHARE_USER, null, false, true);
		_testCanReadUri("agave:////", SYSTEM_UNSHARED_USER, null, false, true);
		
	}
	
	public void testCanUserReadUriNullHostPrivateDefault(String uri, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	throws Exception
	{
		beforeTestData();
		RemoteSystem privateSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		privateSystem.addUserUsingAsDefault(SYSTEM_OWNER);
		SystemDao dao = new SystemDao();
		dao.persist(privateSystem);
		
		// no hostname in an internal url and no default system should throw exception
		_testCanReadUri("agave:////", SYSTEM_OWNER, null, true, false);
		_testCanReadUri("agave:////", SYSTEM_SHARE_USER, null, false, true);
		_testCanReadUri("agave:////", SYSTEM_UNSHARED_USER, null, false, true);
	}
	
	public void testCanUserReadUriNullHostPrivateSharedDefault(String uri, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	throws Exception
	{
		beforeTestData();
		RemoteSystem privateSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		privateSystem.addUserUsingAsDefault(SYSTEM_OWNER);
		privateSystem.addUserUsingAsDefault(SYSTEM_SHARE_USER);
		SystemDao dao = new SystemDao();
		dao.persist(privateSystem);
		
		// no hostname in an internal url and no default system should throw exception
		_testCanReadUri("agave:////", SYSTEM_OWNER, null, true, false);
		_testCanReadUri("agave:////", SYSTEM_SHARE_USER, null, true, false);
		_testCanReadUri("agave:////", SYSTEM_UNSHARED_USER, null, false, true);
	}
	
	public void testCanUserReadUriNullHostPublicNonDefault(String uri, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	throws Exception
	{
		beforeTestData();
		RemoteSystem system = getPublicSystem(RemoteSystemType.STORAGE);
		system.setGlobalDefault(false);
		system.getUsersUsingAsDefault().clear();
		SystemDao dao = new SystemDao();
		dao.persist(system);
		
		// no hostname in an internal url and no default system should throw exception
		_testCanReadUri("agave:////", SYSTEM_OWNER, null, false, true);
		_testCanReadUri("agave:////", SYSTEM_SHARE_USER, null, false, true);
		_testCanReadUri("agave:////", SYSTEM_UNSHARED_USER, null, false, true);
	}
	
	public void testCanUserReadUriNullHostPublicDefault(String uri, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	throws Exception
	{
		beforeTestData();
		RemoteSystem system = getPublicSystem(RemoteSystemType.STORAGE);
		system.addUserUsingAsDefault(SYSTEM_OWNER);
		system.addUserUsingAsDefault(SYSTEM_SHARE_USER);
		SystemDao dao = new SystemDao();
		dao.persist(system);
		
		// no hostname in an internal url and no default system should throw exception
		_testCanReadUri("agave:////", SYSTEM_OWNER, null, true, false);
		_testCanReadUri("agave:///" + SYSTEM_SHARE_USER, SYSTEM_SHARE_USER, null, true, false);
		_testCanReadUri("agave:////", SYSTEM_UNSHARED_USER, null, false, true);
	}
	
	public void testCanUserReadUriNullHostPublicGlobalDefault(String uri, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	throws Exception
	{
		beforeTestData();
		RemoteSystem system = getPublicSystem(RemoteSystemType.STORAGE);
		system.setGlobalDefault(true);
		SystemDao dao = new SystemDao();
		dao.persist(system);
		
		// no hostname in an internal url and no default system should throw exception
		_testCanReadUri("agave:////", SYSTEM_OWNER, null, true, false);
		_testCanReadUri("agave:///" + SYSTEM_SHARE_USER, SYSTEM_SHARE_USER, null, true, false);
		_testCanReadUri("agave:///" + SYSTEM_UNSHARED_USER, SYSTEM_UNSHARED_USER, null, true, false);
	}
	
	
	/************************************************************************
	/*							 READ TESTS      							*
	/************************************************************************/
	
	@DataProvider
	protected Object[][] testCanReadRootProvider() throws Exception
	{
		beforeTestData();
		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);
		
		return new Object[][] {
				{ publicStorageSystem, "/", SYSTEM_OWNER, null, true, false },
				{ publicMirroredStorageSystem, "/", SYSTEM_OWNER, null, true, false },
				{ publicGuestStorageSystem, "/", SYSTEM_OWNER, null, true, false },
				{ privateStorageSystem, "/", SYSTEM_OWNER, null, true, false },
				{ privateSharedGuestStorageSystem, "/", SYSTEM_OWNER, null, true, false },
				{ privateSharedUserStorageSystem, "/", SYSTEM_OWNER, null, true, false },
				{ privateSharedPublisherStorageSystem, "/", SYSTEM_OWNER, null, true, false },
				{ privateSharedAdminStorageSystem, "/", SYSTEM_OWNER, null, true, false },
				
				{ publicStorageSystem, "/", SYSTEM_SHARE_USER, null, false, false },
				{ publicMirroredStorageSystem, "/", SYSTEM_SHARE_USER, null, false, false },
				{ publicGuestStorageSystem, "/", SYSTEM_SHARE_USER, null, true, false },
				{ privateStorageSystem, "/", SYSTEM_SHARE_USER, null, false, false },
				{ privateSharedGuestStorageSystem, "/", SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedUserStorageSystem, "/", SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedPublisherStorageSystem, "/", SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedAdminStorageSystem, "/", SYSTEM_SHARE_USER, null, true, false },
				
				{ publicStorageSystem, "/", SYSTEM_UNSHARED_USER, null, false, false },
				{ publicMirroredStorageSystem, "/", SYSTEM_UNSHARED_USER, null, false, false },
				{ publicGuestStorageSystem, "/", SYSTEM_UNSHARED_USER, null, true, false },
				{ privateStorageSystem, "/", SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedGuestStorageSystem, "/", SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedUserStorageSystem, "/", SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedPublisherStorageSystem, "/", SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedAdminStorageSystem, "/", SYSTEM_UNSHARED_USER, null, false, false },
		};
	}
	
	@Test(dataProvider="testCanReadRootProvider")
	public void testCanReadRoot(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadSystemHomeProvider() throws Exception
	{
		beforeTestData();
		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);
		
		return new Object[][] {
				{ publicStorageSystem, "", SYSTEM_OWNER, null, true, false },
				{ publicMirroredStorageSystem, "", SYSTEM_OWNER, null, true, false },
				{ publicGuestStorageSystem, "", SYSTEM_OWNER, null, true, false },
				{ privateStorageSystem, "", SYSTEM_OWNER, null, true, false },
				{ privateSharedGuestStorageSystem, "", SYSTEM_OWNER, null, true, false },
				{ privateSharedUserStorageSystem, "", SYSTEM_OWNER, null, true, false },
				{ privateSharedPublisherStorageSystem, "", SYSTEM_OWNER, null, true, false },
				{ privateSharedAdminStorageSystem, "", SYSTEM_OWNER, null, true, false },
				
				{ publicStorageSystem, "", SYSTEM_SHARE_USER, null, false, false },
				{ publicMirroredStorageSystem, "", SYSTEM_SHARE_USER, null, false, false },
				{ publicGuestStorageSystem, "", SYSTEM_SHARE_USER, null, true, false },
				{ privateStorageSystem, "", SYSTEM_SHARE_USER, null, false, false },
				{ privateSharedGuestStorageSystem, "", SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedUserStorageSystem, "", SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedPublisherStorageSystem, "", SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedAdminStorageSystem, "", SYSTEM_SHARE_USER, null, true, false },
				
				{ publicStorageSystem, "", SYSTEM_UNSHARED_USER, null, false, false },
				{ publicMirroredStorageSystem, "", SYSTEM_UNSHARED_USER, null, false, false },
				{ publicGuestStorageSystem, "", SYSTEM_UNSHARED_USER, null, true, false },
				{ privateStorageSystem, "", SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedGuestStorageSystem, "", SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedUserStorageSystem, "", SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedPublisherStorageSystem, "", SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedAdminStorageSystem, "", SYSTEM_UNSHARED_USER, null, false, false },
		};
	}
	
	@Test(dataProvider="testCanReadSystemHomeProvider")
	public void testCanReadImplicitSystemHome(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@Test(dataProvider="testCanReadSystemHomeProvider")
	public void testCanReadExplicitSystemHome(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, system.getStorageConfig().getHomeDir(), owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadUserHomeProvider() throws Exception
	{
		beforeTestData();
		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);
		
		return new Object[][] {
				{ publicStorageSystem, SYSTEM_OWNER, SYSTEM_OWNER, null, true, false },
				{ publicMirroredStorageSystem, SYSTEM_OWNER, SYSTEM_OWNER, null, true, false },
				{ publicGuestStorageSystem, SYSTEM_OWNER, SYSTEM_OWNER, null, true, false },
				{ privateStorageSystem, SYSTEM_OWNER, SYSTEM_OWNER, null, true, false },
				{ privateSharedGuestStorageSystem, SYSTEM_OWNER, SYSTEM_OWNER, null, true, false },
				{ privateSharedUserStorageSystem, SYSTEM_OWNER, SYSTEM_OWNER, null, true, false },
				{ privateSharedPublisherStorageSystem, SYSTEM_OWNER, SYSTEM_OWNER, null, true, false },
				{ privateSharedAdminStorageSystem, SYSTEM_OWNER, SYSTEM_OWNER, null, true, false },
				
				{ publicStorageSystem, SYSTEM_OWNER, SYSTEM_SHARE_USER, null, false, false },
				{ publicMirroredStorageSystem, SYSTEM_OWNER, SYSTEM_SHARE_USER, null, false, false },
				{ publicGuestStorageSystem, SYSTEM_OWNER, SYSTEM_SHARE_USER, null, true, false },
				{ privateStorageSystem, SYSTEM_OWNER, SYSTEM_SHARE_USER, null, false, false },
				{ privateSharedGuestStorageSystem, SYSTEM_OWNER, SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedUserStorageSystem, SYSTEM_OWNER, SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedPublisherStorageSystem, SYSTEM_OWNER, SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedAdminStorageSystem, SYSTEM_OWNER, SYSTEM_SHARE_USER, null, true, false },
				
				{ publicStorageSystem, SYSTEM_SHARE_USER, SYSTEM_SHARE_USER, null, true, false },
				{ publicMirroredStorageSystem, SYSTEM_SHARE_USER, SYSTEM_SHARE_USER, null, true, false },
				{ publicGuestStorageSystem, SYSTEM_SHARE_USER, SYSTEM_SHARE_USER, null, true, false },
				{ privateStorageSystem, SYSTEM_SHARE_USER, SYSTEM_SHARE_USER, null, false, false },
				{ privateSharedGuestStorageSystem, SYSTEM_SHARE_USER, SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedUserStorageSystem, SYSTEM_SHARE_USER, SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedPublisherStorageSystem, SYSTEM_SHARE_USER, SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedAdminStorageSystem, SYSTEM_SHARE_USER, SYSTEM_SHARE_USER, null, true, false },
				
				{ publicStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_SHARE_USER, null, false, false },
				{ publicMirroredStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_SHARE_USER, null, false, false },
				{ publicGuestStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_SHARE_USER, null, true, false },
				{ privateStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_SHARE_USER, null, false, false },
				{ privateSharedGuestStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedUserStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedPublisherStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_SHARE_USER, null, true, false },
				{ privateSharedAdminStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_SHARE_USER, null, true, false },
				
				{ publicStorageSystem, SYSTEM_OWNER, SYSTEM_UNSHARED_USER, null, false, false },
				{ publicMirroredStorageSystem, SYSTEM_OWNER, SYSTEM_UNSHARED_USER, null, false, false },
				{ publicGuestStorageSystem, SYSTEM_OWNER, SYSTEM_UNSHARED_USER, null, true, false },
				{ privateStorageSystem, SYSTEM_OWNER, SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedGuestStorageSystem, SYSTEM_OWNER, SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedUserStorageSystem, SYSTEM_OWNER, SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedPublisherStorageSystem, SYSTEM_OWNER, SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedAdminStorageSystem, SYSTEM_OWNER, SYSTEM_UNSHARED_USER, null, false, false },
				
				{ publicStorageSystem, SYSTEM_SHARE_USER, SYSTEM_UNSHARED_USER, null, false, false },
				{ publicMirroredStorageSystem, SYSTEM_SHARE_USER, SYSTEM_UNSHARED_USER, null, false, false },
				{ publicGuestStorageSystem, SYSTEM_SHARE_USER, SYSTEM_UNSHARED_USER, null, true, false },
				{ privateStorageSystem, SYSTEM_SHARE_USER, SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedGuestStorageSystem, SYSTEM_SHARE_USER, SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedUserStorageSystem, SYSTEM_SHARE_USER, SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedPublisherStorageSystem, SYSTEM_SHARE_USER, SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedAdminStorageSystem, SYSTEM_SHARE_USER, SYSTEM_UNSHARED_USER, null, false, false },
				
				{ publicStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_UNSHARED_USER, null, true, false },
				{ publicMirroredStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_UNSHARED_USER, null, true, false },
				{ publicGuestStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_UNSHARED_USER, null, true, false },
				{ privateStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedGuestStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedUserStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedPublisherStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_UNSHARED_USER, null, false, false },
				{ privateSharedAdminStorageSystem, SYSTEM_UNSHARED_USER, SYSTEM_UNSHARED_USER, null, false, false },
		};
	}
	
	@Test(dataProvider="testCanReadUserHomeProvider")
	public void testCanReadImplicitUserHome(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@Test(dataProvider="testCanReadUserHomeProvider")
	public void testCanReadExplicitUserHome(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, system.getStorageConfig().getHomeDir() + "/" + path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadUnSharedDataProvider() throws Exception
	{
		beforeTestData();
		List<Object[]> testList = new ArrayList<Object[]>();
		
		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);
		
		RemoteSystem[] systems = { 
				publicStorageSystem, 
				publicMirroredStorageSystem,
				publicGuestStorageSystem,
				privateStorageSystem,
				privateSharedGuestStorageSystem,
				privateSharedUserStorageSystem,
				privateSharedPublisherStorageSystem,
				privateSharedAdminStorageSystem
		};
		
		// system owner shares with self only
		String path = "/unknownfolder";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = "/unknownfolder/shelfshared.dat";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = "/unknownfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		return testList.toArray(new Object[][]{});
	}
	
	@Test(dataProvider="testCanReadUnSharedDataProvider")
	public void testCanReadUnSharedDirectory(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadUnSharedDataInUserHomeProvider() throws Exception
	{
		beforeTestData();
		List<Object[]> testList = new ArrayList<Object[]>();
		
		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);
		
		RemoteSystem[] systems = { 
				publicStorageSystem, 
				publicMirroredStorageSystem,
				publicGuestStorageSystem,
				privateStorageSystem,
				privateSharedGuestStorageSystem,
				privateSharedUserStorageSystem,
				privateSharedPublisherStorageSystem,
				privateSharedAdminStorageSystem
		};
		
		// system owner shares with self only
		String path = SYSTEM_OWNER + "/unknownfolder";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = SYSTEM_OWNER + "/unknownfolder/shelfshared.dat";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = SYSTEM_OWNER + "/unknownfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
	
		path = SYSTEM_OWNER + "/some/deep/path/to/unknownfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		return testList.toArray(new Object[][]{});
	}
	
	@Test(dataProvider="testCanReadUnSharedDataInUserHomeProvider")
	public void testCanReadUnSharedDataInUserHome(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadUnSharedDataInOwnHomeProvider() throws Exception
	{
		beforeTestData();
		List<Object[]> testList = new ArrayList<Object[]>();
		
		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);
		
		RemoteSystem[] systems = { 
				publicStorageSystem, 
				publicMirroredStorageSystem,
				publicGuestStorageSystem,
				privateStorageSystem,
				privateSharedGuestStorageSystem,
				privateSharedUserStorageSystem,
				privateSharedPublisherStorageSystem,
				privateSharedAdminStorageSystem
		};
		
		// system owner shares with self only
		String path = SYSTEM_SHARE_USER + "/unknownfolder";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = SYSTEM_SHARE_USER + "/unknownfolder/shelfshared.dat";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = SYSTEM_SHARE_USER + "/unknownfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = SYSTEM_SHARE_USER + "/some/deep/path/to/unknownfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		return testList.toArray(new Object[][]{});
	}
	
	@Test(dataProvider="testCanReadUnSharedDataInOwnHomeProvider")
	public void testCanReadUnSharedDataInOwnHome(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadDataSharedWithSelfProvider() throws Exception
	{
		beforeTestData();
		List<Object[]> testList = new ArrayList<Object[]>();
		
		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);
		
		RemoteSystem[] systems = { 
				publicStorageSystem, 
				publicMirroredStorageSystem,
				publicGuestStorageSystem,
				privateStorageSystem,
				privateSharedGuestStorageSystem,
				privateSharedUserStorageSystem,
				privateSharedPublisherStorageSystem,
				privateSharedAdminStorageSystem
		};
		
		String rootDir = StringUtils.substring(getSystemRoot(systems[0]), 0, -1);
		
		// system owner shares with self only
		String path = "/systemownerselfsharednotrecursive";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, SYSTEM_OWNER, system, rootDir + path, LogicalFile.DIRECTORY, false); 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
		path = "/systemownerselfsharednotrecursive/systemownershelfshared.dat";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, SYSTEM_OWNER, system, rootDir + path, LogicalFile.DIRECTORY, false); 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
		path = rootDir + "systemownershelfshared.dat";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, SYSTEM_OWNER, system, rootDir + path, LogicalFile.DIRECTORY, false); 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
		return testList.toArray(new Object[][]{});
	}
	
	@Test(dataProvider="testCanReadDataSharedWithSelfProvider")
	public void testCanReadDataSharedWithSelf(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadDataSharedWithUserProvider() throws Exception
	{
		beforeTestData();
		List<Object[]> testList = new ArrayList<Object[]>();
		
		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);
		
		RemoteSystem[] systems = { 
				publicStorageSystem, 
				publicMirroredStorageSystem,
				publicGuestStorageSystem,
				privateStorageSystem,
				privateSharedGuestStorageSystem,
				privateSharedUserStorageSystem,
				privateSharedPublisherStorageSystem,
				privateSharedAdminStorageSystem
		};
		
		String rootDir = StringUtils.substring(getSystemRoot(systems[0]), 0, -1);
		
		// system owner shares with the SYSTEM_SHARE_USER
		String path = "/sharedfoldernotrecursive";
		for (RemoteSystem system: systems)
		{
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, SYSTEM_SHARE_USER, system, rootDir + path, LogicalFile.DIRECTORY, false);
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = "/sharedfoldernotrecursive/sharedfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, SYSTEM_SHARE_USER, system, rootDir + path, LogicalFile.DIRECTORY, false); 
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = "/sharedfoldernotrecursive/unsharedfile.dat";
//		for (RemoteSystem system: systems)
//		{
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
//		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });

		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = "/sharedfolderrecursive";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, SYSTEM_SHARE_USER, system, rootDir + path, LogicalFile.DIRECTORY, true); 
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = "/sharedfolderrecursive/unsharedfile.dat";
		for (RemoteSystem system: systems) 
		{ 
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		
		path = "/sharedfolderrecursive/sharedfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, SYSTEM_SHARE_USER, system, rootDir + path, LogicalFile.DIRECTORY, false); 
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		
		path = "/unsharedfolder";
		for (RemoteSystem system: systems) 
		{ 
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = "/unsharedfolder/unsharedfile.dat";
		for (RemoteSystem system: systems) 
		{ 
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		
		path = "/unsharedfolder/sharedfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, SYSTEM_SHARE_USER, system, rootDir + path, LogicalFile.DIRECTORY, false); 
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		
		path = "/sharedfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, SYSTEM_SHARE_USER, system, rootDir + path, LogicalFile.FILE, false);
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });

		path = "/unsharedfile.dat";
//		for (RemoteSystem system: systems)
//		{
////			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, SYSTEM_SHARE_USER, system, rootDir + path, LogicalFile.FILE, false);
////			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
//		}

		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });

		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		return testList.toArray(new Object[][]{});
	}
	
	@Test(dataProvider="testCanReadDataSharedWithUserProvider")
	public void testCanReadDataSharedWithUser(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadHomeDirectorySharedWithUserProvider() throws Exception
	{
		beforeTestData();
		List<Object[]> testList = new ArrayList<Object[]>();
		
		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);
		
		RemoteSystem[] systems = { 
				publicStorageSystem, 
				publicMirroredStorageSystem,
				publicGuestStorageSystem,
				privateStorageSystem,
				privateSharedGuestStorageSystem,
				privateSharedUserStorageSystem,
				privateSharedPublisherStorageSystem,
				privateSharedAdminStorageSystem
		};

		String privateHomeDir = getSystemHome(systems[0]);
		String publicHomeDir = String.format("%s/%s", privateHomeDir, SYSTEM_OWNER);

		for (RemoteSystem system: systems) 
		{
			String testHomeDir = system.isPubliclyAvailable() ? publicHomeDir : privateHomeDir;

			createSharedLogicalFile(SYSTEM_OWNER,
					PermissionType.READ,
					SYSTEM_SHARE_USER,
					system,
					testHomeDir,
					LogicalFile.DIRECTORY,
					false);
			testList.add(new Object[]{ system, 	testHomeDir, SYSTEM_SHARE_USER, null, true,	false });
		}

		String publicHomeDirFile = String.format("%s/someunsharedfile.txt", publicHomeDir);
		String privateHomeDirFile = String.format("%s/someunsharedfile.txt", privateHomeDir);

		testList.add(new Object[]{ publicStorageSystem, 				publicHomeDirFile, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			publicHomeDirFile, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				privateHomeDirFile, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	privateHomeDirFile, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		privateHomeDirFile, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, privateHomeDirFile, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			publicHomeDirFile, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	privateHomeDirFile, SYSTEM_SHARE_USER, null, true, 	false });
		
		return testList.toArray(new Object[][]{});
	}
	
	@Test(dataProvider="testCanReadHomeDirectorySharedWithUserProvider")
	public void testCanReadHomeDirectorySharedWithUser(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadHomeDirectoryDataSharedWithUserProvider() throws Exception
	{
		beforeTestData();
		List<Object[]> testList = new ArrayList<Object[]>();
		
		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);
		
		RemoteSystem[] systems = { 
				publicStorageSystem, 
				publicMirroredStorageSystem,
				publicGuestStorageSystem,
				privateStorageSystem,
				privateSharedGuestStorageSystem,
				privateSharedUserStorageSystem,
				privateSharedPublisherStorageSystem,
				privateSharedAdminStorageSystem
		};

		String privateHomeDir = getSystemHome(systems[0]);
		String publicHomeDir = String.format("%s/%s", privateHomeDir, SYSTEM_OWNER);

		String publicTestPath = String.format("%s/sharedfoldernotrecursive", publicHomeDir);
		String privateTestPath = String.format("%s/sharedfoldernotrecursive", privateHomeDir);

		for (RemoteSystem system: systems)
		{
			String testDir = system.isPubliclyAvailable() ? publicTestPath : privateTestPath;

			createSharedLogicalFile(SYSTEM_OWNER,
					PermissionType.READ,
					SYSTEM_SHARE_USER,
					system,
					testDir,
					LogicalFile.DIRECTORY,
					false);

			testList.add(new Object[]{ system, 	testDir, SYSTEM_SHARE_USER, null, true,	false });
		}

		testList.add(new Object[]{ publicStorageSystem, 				publicTestPath, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });


		publicTestPath = String.format("%s/sharedfoldernotrecursive/sharedfile.dat", publicHomeDir);
		privateTestPath = String.format("%s/sharedfoldernotrecursive/sharedfile.dat", privateHomeDir);

		for (RemoteSystem system: systems)
		{
			String testPath = system.isPubliclyAvailable() ? publicTestPath : privateTestPath;

			createSharedLogicalFile(SYSTEM_OWNER,
					PermissionType.READ,
					SYSTEM_SHARE_USER,
					system,
					testPath,
					LogicalFile.FILE,
					false);
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				publicTestPath, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });


		publicTestPath = String.format("%s/sharedfoldernotrecursive/unsharedfile.dat", publicHomeDir);
		privateTestPath = String.format("%s/sharedfoldernotrecursive/unsharedfile.dat", privateHomeDir);

		testList.add(new Object[]{ publicStorageSystem, 				publicTestPath, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			publicTestPath, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				privateTestPath, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });


		publicTestPath = String.format("%s/sharedfolderrecursive", publicHomeDir);
		privateTestPath = String.format("%s/sharedfolderrecursive", privateHomeDir);

		for (RemoteSystem system: systems)
		{
			String testPath = system.isPubliclyAvailable() ? publicTestPath : privateTestPath;

			createSharedLogicalFile(SYSTEM_OWNER,
					PermissionType.READ,
					SYSTEM_SHARE_USER,
					system,
					testPath,
					LogicalFile.DIRECTORY,
					true);

			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, SYSTEM_SHARE_USER, system, testPath, LogicalFile.DIRECTORY, true);
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				publicTestPath, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });



		publicTestPath = String.format("%s/sharedfolderrecursive/unsharedfile.dat", publicHomeDir);
		privateTestPath = String.format("%s/sharedfolderrecursive/unsharedfile.dat", privateHomeDir);

		testList.add(new Object[]{ publicStorageSystem, 				publicTestPath, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });

		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });


		publicTestPath = String.format("%s/sharedfolderrecursive/sharedfile.dat", publicHomeDir);
		privateTestPath = String.format("%s/sharedfolderrecursive/sharedfile.dat", privateHomeDir);

		for (RemoteSystem system: systems) 
		{
			String testPath = system.isPubliclyAvailable() ? publicTestPath : privateTestPath;

			createSharedLogicalFile(SYSTEM_OWNER,
					PermissionType.READ,
					SYSTEM_SHARE_USER,
					system,
					testPath,
					LogicalFile.FILE,
					false);
		}

		testList.add(new Object[]{ publicStorageSystem, 				publicTestPath, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });

		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });


		publicTestPath = String.format("%s/unsharedfolder", publicHomeDir);
		privateTestPath = String.format("%s/unsharedfolder", privateHomeDir);

		testList.add(new Object[]{ publicStorageSystem, 				publicTestPath, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			publicTestPath, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				privateTestPath, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });

		publicTestPath = String.format("%s/unsharedfolder/unsharedfile.dat", publicHomeDir);
		privateTestPath = String.format("%s/unsharedfolder/unsharedfile.dat", privateHomeDir);

		testList.add(new Object[]{ publicStorageSystem, 				publicTestPath, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			publicTestPath, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				privateTestPath, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });

		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });


		publicTestPath = String.format("%s/unsharedfolder/sharedfile.dat", publicHomeDir);
		privateTestPath = String.format("%s/unsharedfolder/sharedfile.dat", privateHomeDir);

		for (RemoteSystem system: systems)
		{
			String testPath = system.isPubliclyAvailable() ? publicTestPath : privateTestPath;

			createSharedLogicalFile(SYSTEM_OWNER,
					PermissionType.READ,
					SYSTEM_SHARE_USER,
					system,
					testPath,
					LogicalFile.FILE,
					false);
		}

		testList.add(new Object[]{ publicStorageSystem, 				publicTestPath, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });

		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });

		publicTestPath = String.format("%s/systemownerselfshared.dat", publicHomeDir);
		privateTestPath = String.format("%s/systemownerselfshared.dat", privateHomeDir);

		for (RemoteSystem system: systems)
		{
			String testPath = system.isPubliclyAvailable() ? publicTestPath : privateTestPath;

			createSharedLogicalFile(SYSTEM_OWNER,
					PermissionType.READ,
					SYSTEM_SHARE_USER,
					system,
					testPath,
					LogicalFile.FILE,
					false);
		}

		testList.add(new Object[]{ publicStorageSystem, 				publicTestPath, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });

		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			publicTestPath, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	privateTestPath, SYSTEM_SHARE_USER, null, true, 	false });


		return testList.toArray(new Object[][]{});
	}
	
	@Test(dataProvider="testCanReadHomeDirectoryDataSharedWithUserProvider")
	public void testCanReadHomeDirectoryDataSharedWithUser(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadPublicDirectoryInRootDirectoryProvider() throws Exception
	{
		beforeTestData();
		List<Object[]> testList = new ArrayList<Object[]>();
		
		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);
		
		RemoteSystem[] systems = { 
				publicStorageSystem, 
				publicMirroredStorageSystem,
				publicGuestStorageSystem,
				privateStorageSystem,
				privateSharedGuestStorageSystem,
				privateSharedUserStorageSystem,
				privateSharedPublisherStorageSystem,
				privateSharedAdminStorageSystem
		};
		
		String rootDir = getSystemRoot(systems[0]);
		String sharedUser = Settings.PUBLIC_USER_USERNAME;
		
		// system owner shares with self only
		String path = "/publicfoldernotrecursive";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, sharedUser, system, rootDir + path, LogicalFile.DIRECTORY, false); 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = "/publicfoldernotrecursive/sharedfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, sharedUser, system, rootDir + path, LogicalFile.DIRECTORY, false); 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = "/publicfoldernotrecursive/unsharedfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = "/sharedfolderrecursive";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, sharedUser, system, rootDir + path, LogicalFile.DIRECTORY, true); 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = "/sharedfolderrecursive/unsharedfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		
		path = "/sharedfolderrecursive/sharedfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, sharedUser, system, rootDir + path, LogicalFile.DIRECTORY, false); 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		
		path = "/unsharedfolder";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		path = "/unsharedfolder/unsharedfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		
		path = "/unsharedfolder/sharedfile.dat";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, sharedUser, system, rootDir + path, LogicalFile.DIRECTORY, false); 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		
		path = "/systemownerpublicshared.dat";
		for (RemoteSystem system: systems) 
		{ 
			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, sharedUser, system, rootDir + path, LogicalFile.DIRECTORY, false); 
			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
		}
		
		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
		
		// public readonly systems and systems where the user is admin should always be readable
		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
		
		return testList.toArray(new Object[][]{});
	}
	
	@Test(dataProvider="testCanReadPublicDirectoryInRootDirectoryProvider")
	public void testCanReadPublicDirectoryInRootDirectory(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadWorldDirectoryInRootDirectoryProvider() throws Exception
	{
		return _testCanReadInOutsideHomeProvider("", Settings.WORLD_USER_USERNAME, SHARED_SYSTEM_USER);
//
//		beforeTestData();
//		List<Object[]> testList = new ArrayList<Object[]>();
//
//		RemoteSystem publicStorageSystem = getPublicSystem(RemoteSystemType.STORAGE);
//		RemoteSystem publicMirroredStorageSystem = getPublicMirroredSystem(RemoteSystemType.STORAGE);
//		RemoteSystem publicGuestStorageSystem = getPublicGuestSystem(RemoteSystemType.STORAGE);
//		RemoteSystem privateStorageSystem = getPrivateSystem(RemoteSystemType.STORAGE);
//		RemoteSystem privateSharedGuestStorageSystem = getPrivateSharedGuestSystem(RemoteSystemType.STORAGE);
//		RemoteSystem privateSharedUserStorageSystem = getPrivateSharedUserSystem(RemoteSystemType.STORAGE);
//		RemoteSystem privateSharedPublisherStorageSystem = getPrivateSharedPublisherSystem(RemoteSystemType.STORAGE);
//		RemoteSystem privateSharedAdminStorageSystem = getPrivateSharedAdminSystem(RemoteSystemType.STORAGE);
//
//		RemoteSystem[] systems = {
//				publicStorageSystem,
//				publicMirroredStorageSystem,
//				publicGuestStorageSystem,
//				privateStorageSystem,
//				privateSharedGuestStorageSystem,
//				privateSharedUserStorageSystem,
//				privateSharedPublisherStorageSystem,
//				privateSharedAdminStorageSystem
//		};
//
//		String rootDir = getSystemRoot(systems[0]);
//		String sharedUser = Settings.WORLD_USER_USERNAME;
//
//		// system owner shares with self only
//		String path = "/publicfoldernotrecursive";
//		for (RemoteSystem system: systems)
//		{
//			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, sharedUser, system, rootDir + path, LogicalFile.DIRECTORY, false);
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
//		}
//
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		path = "/publicfoldernotrecursive/sharedfile.dat";
//		for (RemoteSystem system: systems)
//		{
//			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, sharedUser, system, rootDir + path, LogicalFile.DIRECTORY, false);
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
//		}
//
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		path = "/publicfoldernotrecursive/unsharedfile.dat";
//		for (RemoteSystem system: systems)
//		{
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
//		}
//
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		path = "/sharedfolderrecursive";
//		for (RemoteSystem system: systems)
//		{
//			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, sharedUser, system, rootDir + path, LogicalFile.DIRECTORY, true);
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
//		}
//
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		path = "/sharedfolderrecursive/unsharedfile.dat";
//		for (RemoteSystem system: systems)
//		{
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
//		}
//
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
//
//		path = "/sharedfolderrecursive/sharedfile.dat";
//		for (RemoteSystem system: systems)
//		{
//			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, sharedUser, system, rootDir + path, LogicalFile.DIRECTORY, false);
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
//		}
//
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
//
//		path = "/unsharedfolder";
//		for (RemoteSystem system: systems)
//		{
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
//		}
//
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		path = "/unsharedfolder/unsharedfile.dat";
//		for (RemoteSystem system: systems)
//		{
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
//		}
//
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, false,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
//
//		path = "/unsharedfolder/sharedfile.dat";
//		for (RemoteSystem system: systems)
//		{
//			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, sharedUser, system, rootDir + path, LogicalFile.DIRECTORY, false);
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
//		}
//
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
//
//		path = "/systemownerpublicshared.dat";
//		for (RemoteSystem system: systems)
//		{
//			createSharedLogicalFile(SYSTEM_OWNER, PermissionType.READ, sharedUser, system, rootDir + path, LogicalFile.DIRECTORY, false);
//			testList.add(new Object[]{ system, path, SYSTEM_OWNER, 		null, true, 	false });
//		}
//
//		testList.add(new Object[]{ publicStorageSystem, 				path, SYSTEM_SHARE_USER, null, true,	false });
//		testList.add(new Object[]{ publicMirroredStorageSystem,			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateStorageSystem, 				path, SYSTEM_SHARE_USER, null, false, 	false });
//		testList.add(new Object[]{ privateSharedGuestStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedUserStorageSystem, 		path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedPublisherStorageSystem, path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		// public readonly systems and systems where the user is admin should always be readable
//		testList.add(new Object[]{ publicGuestStorageSystem, 			path, SYSTEM_SHARE_USER, null, true, 	false });
//		testList.add(new Object[]{ privateSharedAdminStorageSystem, 	path, SYSTEM_SHARE_USER, null, true, 	false });
//
//		return testList.toArray(new Object[][]{});
	}
	
	@Test(dataProvider="testCanReadWorldDirectoryInRootDirectoryProvider")
	public void testCanReadWorldDirectoryInRootDirectory(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadPublicDirectoryInUserHomeProvider() throws Exception
	{
		return _testCanReadInUserHomeProvider(Settings.PUBLIC_USER_USERNAME, SYSTEM_SHARE_USER);
	}
	
	@Test(dataProvider="testCanReadPublicDirectoryInUserHomeProvider")
	public void testCanReadPublicDirectoryInUserHome(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
	
	@DataProvider
	protected Object[][] testCanReadWorldDirectoryInUserHomeProvider() throws Exception {
		return _testCanReadInUserHomeProvider(Settings.WORLD_USER_USERNAME, SYSTEM_SHARE_USER);
	}

	@Test(dataProvider="testCanReadWorldDirectoryInUserHomeProvider")
	public void testCanReadWorldDirectoryInUserHome(RemoteSystem system, String path, String owner, String internalUsername, boolean expectedResult, boolean shouldThrowException)
	{
		_testCanRead(system, path, owner, internalUsername, expectedResult, shouldThrowException);
	}
}
