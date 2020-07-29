package org.iplantc.service.metadata.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.search.MetadataSearch;
import org.json.JSONException;
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

	@Test
	public void findAllPermissionsForUserTest() throws MetadataException, MetadataQueryException, MetadataStoreException, PermissionException, IOException, JSONException {
		String username = "testUser";
		String sharedUser = "testSharedUser";
		String metadataQueryMustard =
				"  {" +
						"    \"name\": \"mustard plant\"," +
						"    \"value\": {" +
						"      \"type\": \"a plant\"," +
						"        \"profile\": {" +
						"        \"status\": \"active\"" +
						"           }," +
						"        \"description\": \"The seed of the mustard plant is used as a spice...\"" +
						"       }" +
						"   }";

		String metadataQueryCactus =
				"  {" +
						"    \"name\": \"cactus (cactaeceae)\"," +
						"    \"value\": {" +
						"      \"type\": \"a plant\"," +
						"      \"order\": \"Caryophyllales\", " +
						"        \"profile\": {" +
						"        \"status\": \"inactive\"" +
						"           }," +
						"        \"description\": \"It could take a century for a cactus to produce its first arm. /n" +
						"                           A type of succulent and monocots. .\"" +
						"       }" +
						"   }";

		String metadataQueryAgavoideae =
				"  {" +
						"    \"name\": \"Agavoideae\"," +
						"    \"value\": {" +
						"      \"type\": \"a flowering plant\"," +
						"      \"order\": \" Asparagales\", " +
						"        \"profile\": {" +
						"        \"status\": \"paused\"" +
						"           }," +
						"        \"description\": \"Includes desert and dry-zone types such as the agaves and yuucas.\"" +
						"       }" +
						"   }";




		//create all entities under username
		List<String> queryList = Arrays.asList(metadataQueryMustard, metadataQueryCactus, metadataQueryAgavoideae);
		List<String> uuidList = new ArrayList<>();

		MetadataSearch search = new MetadataSearch(false, username);
		search.setOwner(username);

		for (String query : queryList) {
			JsonFactory factory = new ObjectMapper().getFactory();
			JsonNode jsonMetadataNode = factory.createParser(query).readValueAsTree();
			search.parseJsonMetadata(jsonMetadataNode);
			search.setOwner(username);
			search.setUuid(new AgaveUUID(UUIDType.METADATA).toString());
			MetadataItem metadataItem = search.updateMetadataItem();
			uuidList.add(metadataItem.getUuid());
		}

		//add permissions for shared user for the first 2 uuids in the list
		search.setUuid(uuidList.get(0));
		search.updatePermissions(sharedUser, "", PermissionType.READ);

		search.setUuid(uuidList.get(1));
		search.updatePermissions(sharedUser, "", PermissionType.READ_WRITE);
		search.updatePermissions("newuser", "", PermissionType.READ);

		List<MetadataPermission> permissionList = MetadataPermissionDao.getByUuid_mongo(uuidList.get(0), MetadataDao.getInstance().getDefaultMetadataItemCollection());
		StringBuilder jPems = new StringBuilder(new MetadataPermission(uuidList.get(0), username, PermissionType.ALL).toJSON());

		for (MetadataPermission permission: permissionList)
		{
			if (!StringUtils.equals(permission.getUsername(), username)) {
				jPems.append(",").append(permission.toJSON());
			}
		}
		Assert.assertTrue(StringUtils.isNotEmpty(String.valueOf(jPems)), "Permission json should have permissions after they were added.");

		MetadataPermission permission;

		permission = MetadataPermissionDao.getByUsernameAndUuid_mongo(sharedUser, uuidList.get(0), MetadataDao.getInstance().getDefaultMetadataItemCollection());
		Assert.assertEquals(permission.getPermission(), PermissionType.READ);

		permission = MetadataPermissionDao.getByUsernameAndUuid_mongo(sharedUser, uuidList.get(1), MetadataDao.getInstance().getDefaultMetadataItemCollection());
		Assert.assertEquals(permission.getPermission(), PermissionType.READ_WRITE);

		permission = MetadataPermissionDao.getByUsernameAndUuid_mongo("newuser", uuidList.get(1), MetadataDao.getInstance().getDefaultMetadataItemCollection());
		Assert.assertEquals(permission.getPermission(), PermissionType.READ);

		permission = MetadataPermissionDao.getByUsernameAndUuid_mongo(sharedUser, uuidList.get(2), MetadataDao.getInstance().getDefaultMetadataItemCollection());
		Assert.assertNull(permission, "User was not granted any permissions for this metadata item.");
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
