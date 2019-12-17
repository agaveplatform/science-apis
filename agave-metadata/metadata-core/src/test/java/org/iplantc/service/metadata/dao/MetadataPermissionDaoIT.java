package org.iplantc.service.metadata.dao;

import java.util.List;

import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups={"integration"})
public class MetadataPermissionDaoIT extends AbstractMetadataPermissionDaoIT {

	@Override
	public String getResourceUuid() {
		return new AgaveUUID(UUIDType.METADATA).toString();
	}

	@Test
	public void persistTest() throws MetadataException
	{
		String uuid = getResourceUuid();
		MetadataPermission pem = new MetadataPermission(uuid, TEST_OWNER, PermissionType.READ);
		MetadataPermissionDao.persist(pem);
		Assert.assertNotNull(pem.getId(), "Metadata permission did not persist.");
	}

	@Test(dependsOnMethods={"persistTest"})
	public void getByUuidTest() throws MetadataException
	{
		String uuid = getResourceUuid();
		MetadataPermission pem = new MetadataPermission(uuid, TEST_OWNER, PermissionType.READ);
		MetadataPermissionDao.persist(pem);
		Assert.assertNotNull(pem.getId(), "Metadata permission did not persist.");

		List<MetadataPermission> pems = MetadataPermissionDao.getByUuid(uuid);
		Assert.assertNotNull(pems, "getByUuid did not return any permissions.");
		Assert.assertEquals(pems.size(), 1, "getByUuid did not return the correct number of permissions.");
		Assert.assertEquals(pems.get(0).getUuid(), uuid, "getByUuid should only return permissions for the given uuid.");
	}

	@Test(dependsOnMethods={"getByUuidTest"})
	public void getByUsernameAndUuidTest() throws MetadataException
	{
		String uuid = getResourceUuid();
		MetadataPermission pem1 = new MetadataPermission(uuid, TEST_OWNER, PermissionType.READ);
		MetadataPermissionDao.persist(pem1);
		Assert.assertNotNull(pem1.getId(), "Metadata permission 1 did not persist.");
		
		MetadataPermission pem2 = new MetadataPermission(uuid, TEST_SHARED_OWNER, PermissionType.READ);
		MetadataPermissionDao.persist(pem2);
		Assert.assertNotNull(pem2.getId(), "Metadata permission 2 did not persist.");
		
		MetadataPermission userPem = MetadataPermissionDao.getByUsernameAndUuid(TEST_OWNER, uuid);
		Assert.assertNotNull(userPem, "getByUsernameAndUuid did not return the user permission.");
		Assert.assertEquals(userPem, pem1, "getByUsernameAndUuid did not return the correct metadata permission for the user.");
	}

	@Test(dependsOnMethods={"getByUsernameAndUuidTest"})
	public void deleteTest() throws MetadataException
	{
		String uuid = getResourceUuid();
		MetadataPermission pem = new MetadataPermission(uuid, TEST_OWNER, PermissionType.READ);
		MetadataPermissionDao.persist(pem);
		Assert.assertNotNull(pem.getId(), "Metadata permission did not persist.");

		MetadataPermissionDao.delete(pem);
		List<MetadataPermission> pems = MetadataPermissionDao.getByUuid(uuid);
		Assert.assertFalse(pems.contains(pem), "Metadata permission did not delete.");
	}
}
