package org.iplantc.service.io.model;

import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageConfig;
import org.iplantc.service.systems.model.StorageSystem;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test(groups={"unit"})
public class LogicalFileTest extends BaseTestCase {

	@BeforeClass
	@Override
	protected void beforeClass() throws Exception {

	}

	@AfterClass
	@Override
	protected void afterClass() throws Exception {

	}

	@DataProvider
	public Object[][] getAgaveRelativePathFromAbsolutePathProvider() {
		return new Object[][] {
//				rootDir			absoluteFileItemPath	expectedAgaveRelativePath
				{ "/", 			"/", 				"/" },
				{ "/", 			"/.", 				"/" },
				{ "/", 			"/./", 				"/" },
				{ "/", 			"/./.",				"/" },

				{ "/", 			"/..", 				"/" },
				{ "/", 			"/../", 			"/" },
				{ "/", 			"/../.", 			"/" },
				{ "/", 			"/.././", 			"/" },
				{ "/", 			"/../..", 			"/" },
				{ "/", 			"/../../", 			"/" },

				{ "/", 			"/foo", 			"/foo" },
				{ "/", 			"/foo/////", 		"/foo/" },
				{ "/", 			"/foo/.", 			"/foo/" },
				{ "/", 			"/foo/./.", 		"/foo/" },
				{ "/", 			"/foo/..",	 		"/" },
				{ "/", 			"/foo/../.", 		"/" },

				{ "/", 			"/./foo", 			"/foo" },
				{ "/", 			"/./foo/.", 		"/foo/" },
				{ "/", 			"/./foo/./.", 		"/foo/" },
				{ "/", 			"/./foo/..",	 	"/" },
				{ "/", 			"/./foo/../.", 		"/" },

				{ "/", 			"/../foo/../.", 	"/" },
				{ "/", 			"/../foo/..", 		"/" },
				{ "/", 			"/../foo/../", 		"/" },

				{ "/boo", 		"/boo", 				"/" },
				{ "/boo", 		"/boo/.", 				"/" },
				{ "/boo", 		"/boo/./", 				"/" },
				{ "/boo", 		"/boo/./.",				"/" },

				{ "/boo", 		"/boo/boo/..", 				"/" },
				{ "/boo", 		"/boo/boo/../", 			"/" },
				{ "/boo", 		"/boo/boo/../.", 			"/" },
				{ "/boo", 		"/boo/boo/.././", 			"/" },
				{ "/boo", 		"/boo/boo/boo/../..", 			"/" },
				{ "/boo", 		"/boo/boo/boo/../../", 			"/" },

				{ "/boo", 		"/boo/foo", 			"/foo" },
				{ "/boo", 		"/boo/foo/////", 		"/foo/" },
				{ "/boo", 		"/boo/foo/.", 			"/foo/" },
				{ "/boo", 		"/boo/foo/./.", 		"/foo/" },
				{ "/boo", 		"/boo/foo/..",	 		"/" },
				{ "/boo", 		"/boo/foo/../.", 		"/" },

				{ "/boo", 		"/boo/./foo", 			"/foo" },
				{ "/boo", 		"/boo/./foo/.", 		"/foo/" },
				{ "/boo", 		"/boo/./foo/./.", 		"/foo/" },
				{ "/boo", 		"/boo/./foo/..",	 	"/" },
				{ "/boo", 		"/boo/./foo/../.", 		"/" },

				{ "/boo", 		"/boo/boo/../foo/../.", 	"/" },
				{ "/boo", 		"/boo/boo/../foo/..", 		"/" },
				{ "/boo", 		"/boo/boo/../foo/../", 		"/" },
		};
	}

	@Test(dataProvider = "getAgaveRelativePathFromAbsolutePathProvider")
	public void getAgaveRelativePathFromAbsolutePath(String rootDir, String absoluteFileItemPath, String expectedPath) {
		
		StorageConfig config = new StorageConfig();
		config.setRootDir(rootDir);
		config.setHomeDir("/");

		RemoteSystem system = mock(StorageSystem.class);
		when(system.getSystemId()).thenReturn(UUID.randomUUID().toString());
		when(system.getStorageConfig()).thenReturn(config);

		LogicalFile logicalFile = new LogicalFile(SYSTEM_OWNER, system, absoluteFileItemPath);
		String actualPath = logicalFile.getAgaveRelativePathFromAbsolutePath();
		Assert.assertEquals(actualPath, expectedPath,
		"Logical file should resolve its remote absolute file path to a path relative to the system rootDir.");
	}

//	@Test(groups={"notReady"})
//	public void getMetadataLink() {
//		throw new RuntimeException("Test not implemented");
//	}
//
//	@Test(groups={"notReady"})
//	public void getOwnerLink() {
//		throw new RuntimeException("Test not implemented");
//	}
//
//	@Test(groups={"notReady"})
//	public void getPublicLink() {
//		throw new RuntimeException("Test not implemented");
//	}
//
//	@Test(groups={"notReady"})
//	public void getSourceUri() {
//		throw new RuntimeException("Test not implemented");
//	}
}
