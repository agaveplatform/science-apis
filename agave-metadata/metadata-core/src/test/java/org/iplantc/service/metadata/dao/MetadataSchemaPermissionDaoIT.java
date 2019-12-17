package org.iplantc.service.metadata.dao;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.model.MetadataSchemaItem;
import org.iplantc.service.metadata.model.MetadataSchemaPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

@Test(groups={"integration"})
public class MetadataSchemaPermissionDaoIT extends AbstractMetadataPermissionDaoIT{

	@Override
	public String getResourceUuid() {
		return new AgaveUUID(UUIDType.SCHEMA).toString();
	}

	@Test
	public void persistTest() throws MetadataException
	{
			String schemaId = getResourceUuid();
			MetadataSchemaPermission pem = new MetadataSchemaPermission(schemaId, TEST_OWNER, PermissionType.READ);
			MetadataSchemaPermissionDao.persist(pem);
			Assert.assertNotNull(pem.getId(), "Schema permission did not persist.");
	}

	@Test(dependsOnMethods={"persistTest"})
	public void getByUuidTest() throws MetadataException
	{
		String schemaId = getResourceUuid();

		MetadataSchemaPermission pem = new MetadataSchemaPermission(schemaId, TEST_OWNER, PermissionType.READ);
		MetadataSchemaPermissionDao.persist(pem);
		Assert.assertNotNull(pem.getId(), "Schema permission did not persist.");

		List<MetadataSchemaPermission> pems = MetadataSchemaPermissionDao.getBySchemaId(schemaId);
		Assert.assertNotNull(pems, "getBySchemaId did not return any permissions.");
		Assert.assertEquals(pems.size(), 1,
				"getBySchemaId should only return permissions for the given schema Id.");
		Assert.assertEquals(schemaId, pems.get(0).getSchemaId(),
				"getBySchemaId should not return a permission for another schemaId.");
	}

	@Test(dependsOnMethods={"getByUuidTest"})
	public void getByUsernameAndUuidTest() throws MetadataException
	{
		String schemaId = getResourceUuid();

		MetadataSchemaPermission pem1 = new MetadataSchemaPermission(schemaId, TEST_OWNER, PermissionType.READ);
		MetadataSchemaPermissionDao.persist(pem1);
		Assert.assertNotNull(pem1.getId(), "Metadata Schema permission 1 did not persist.");

		MetadataSchemaPermission pem2 = new MetadataSchemaPermission(schemaId, TEST_SHARED_OWNER, PermissionType.READ);
		MetadataSchemaPermissionDao.persist(pem2);
		Assert.assertNotNull(pem2.getId(), "Job permission 2 did not persist.");

		MetadataSchemaPermission userPem = MetadataSchemaPermissionDao.getByUsernameAndSchemaId(TEST_OWNER, schemaId);
		Assert.assertNotNull(userPem, "getByUsernameAndSchemaId did not return the user permission.");
		Assert.assertEquals(userPem, pem1, "getBySchemaId did not return the correct metadata schema permission for the user.");
	}

	@Test(dependsOnMethods={"getByUsernameAndUuidTest"})
	public void deleteTest() throws MetadataException
	{
		String schemaId = getResourceUuid();

		MetadataSchemaPermission pem = new MetadataSchemaPermission(schemaId, TEST_OWNER, PermissionType.READ);
		MetadataSchemaPermissionDao.persist(pem);
		Assert.assertNotNull(pem.getId(), "Schema permission did not persist.");

		MetadataSchemaPermissionDao.delete(pem);
		List<MetadataSchemaPermission> pems = MetadataSchemaPermissionDao.getBySchemaId(schemaId);
		Assert.assertFalse(pems.contains(pem), "Schema permission did not delete.");
	}


}
