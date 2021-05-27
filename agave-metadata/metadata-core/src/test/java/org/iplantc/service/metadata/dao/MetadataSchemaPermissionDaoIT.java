package org.iplantc.service.metadata.dao;

import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataSchemaPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.*;

@Test(groups={"integration"})
public class MetadataSchemaPermissionDaoIT extends AbstractMetadataPermissionDaoIT{

	@Override
	public String getResourceUuid() {
		return new AgaveUUID(UUIDType.SCHEMA).toString();
	}

	@Test
	public void persistTest() throws MetadataException, MetadataStoreException {
			String schemaId = getResourceUuid();
			MetadataSchemaPermission pem = new MetadataSchemaPermission(schemaId, TEST_OWNER, PermissionType.READ);
			MetadataSchemaPermission addedPem = MetadataSchemaPermissionDao.persist(pem);
			assertNotNull(addedPem.getId(), "Schema permission did not persist.");
	}

	@Test(dependsOnMethods={"persistTest"})
	public void getByUuidTest() throws MetadataException, MetadataStoreException {
		String schemaId = getResourceUuid();

		MetadataSchemaPermission pem = new MetadataSchemaPermission(schemaId, TEST_OWNER, PermissionType.READ);
		MetadataSchemaPermission addedPem = MetadataSchemaPermissionDao.persist(pem);
		assertNotNull(addedPem.getId(), "Schema permission did not persist.");

		List<MetadataSchemaPermission> pems = MetadataSchemaPermissionDao.getBySchemaId(schemaId);
		assertNotNull(pems, "getBySchemaId did not return any permissions.");
		assertEquals(pems.size(), 1,
				"getBySchemaId should only return permissions for the given schema Id.");
		assertEquals(schemaId, pems.get(0).getSchemaId(),
				"getBySchemaId should not return a permission for another schemaId.");
	}

	@Test(dependsOnMethods={"getByUuidTest"})
	public void getByUsernameAndUuidTest() throws MetadataException, MetadataStoreException {
		String schemaId = getResourceUuid();

		MetadataSchemaPermission pem1 = new MetadataSchemaPermission(schemaId, TEST_OWNER, PermissionType.READ);
		MetadataSchemaPermission addedPem1 = MetadataSchemaPermissionDao.persist(pem1);
		assertNotNull(addedPem1.getId(), "Metadata Schema permission 1 did not persist.");

		MetadataSchemaPermission pem2 = new MetadataSchemaPermission(schemaId, TEST_SHARED_OWNER, PermissionType.READ);
		MetadataSchemaPermissionDao.persist(pem2);
		MetadataSchemaPermission addedPem2 = MetadataSchemaPermissionDao.persist(pem1);
		assertNotNull(addedPem2.getId(), "Job permission 2 did not persist.");

		MetadataSchemaPermission userPem = MetadataSchemaPermissionDao.getByUsernameAndSchemaId(TEST_OWNER, schemaId);
		assertNotNull(userPem, "getByUsernameAndSchemaId did not return the user permission.");
		assertEquals(userPem, pem1, "getBySchemaId did not return the correct metadata schema permission for the user.");
	}

	@Test(dependsOnMethods={"getByUsernameAndUuidTest"})
	public void deleteTest() throws MetadataException, MetadataStoreException {
		String schemaId = getResourceUuid();

		MetadataSchemaPermission pem = new MetadataSchemaPermission(schemaId, TEST_OWNER, PermissionType.READ);
		MetadataSchemaPermission addedPem = MetadataSchemaPermissionDao.persist(pem);
		assertNotNull(addedPem.getId(), "Schema permission did not persist.");

		MetadataSchemaPermissionDao.delete(pem);
		List<MetadataSchemaPermission> pems = MetadataSchemaPermissionDao.getBySchemaId(schemaId);
		assertFalse(pems.contains(addedPem), "Schema permission did not delete.");
	}

	@Test(dependsOnMethods={"deleteTest"})
	public void getUuidOfAllSharedMetadataSchemaItemReadableByUserTest() throws MetadataException, MetadataStoreException {
		String schemaId = getResourceUuid();

		MetadataSchemaPermission pem = new MetadataSchemaPermission(schemaId, TEST_OWNER, PermissionType.READ);
		MetadataSchemaPermission addedPem = MetadataSchemaPermissionDao.persist(pem);
		assertNotNull(addedPem.getId(), "Schema permission did not persist.");

		List<String> uuids = MetadataSchemaPermissionDao.getUuidOfAllSharedMetataSchemaItemReadableByUser(TEST_OWNER, 0, 100);
		assertTrue(uuids.contains(schemaId), "Test schema uuid should be returned from shared schema list");

		for (PermissionType permissionType : PermissionType.values()){
			pem.setPermission(permissionType);
			MetadataSchemaPermission updatedPem = MetadataSchemaPermissionDao.persist(pem);
			uuids = MetadataSchemaPermissionDao.getUuidOfAllSharedMetataSchemaItemReadableByUser(TEST_OWNER, 0, 100);

			if (permissionType.canRead()){
				assertTrue(uuids.contains(schemaId), "Test schema uuid should be returned from shared schema list");
			} else {
				assertFalse(uuids.contains(schemaId), "Test schema uuid should not be returned from shared schema list");
			}
		}


	}

	@Test(dependsOnMethods={"getUuidOfAllSharedMetadataSchemaItemReadableByUserTest"})
	public void insertAndUpdateTest() throws MetadataException, MetadataStoreException {
		clearMongoPermissions();

		String schemaId = getResourceUuid();
		MetadataSchemaPermission pem = new MetadataSchemaPermission(schemaId, TEST_OWNER, PermissionType.READ);
		MetadataSchemaPermission addedPem = MetadataSchemaPermissionDao.persist(pem);
		assertNotNull(addedPem.getId(), "Schema permission did not persist.");
		pem.setPermission(PermissionType.ALL);
		MetadataSchemaPermission updatedPem = MetadataSchemaPermissionDao.persist(pem);
		assertNotNull(updatedPem.getId(), "Schema permission did not update.");
		assertEquals(updatedPem.getPermission(), PermissionType.ALL, "Schema permission should be updated to ALL.");

		List<String> pems = MetadataSchemaPermissionDao.getUuidOfAllSharedMetataSchemaItemReadableByUser(TEST_OWNER);

		assertEquals(pems.size(), 1, "Expected single permission to be updated.");

	}

}
