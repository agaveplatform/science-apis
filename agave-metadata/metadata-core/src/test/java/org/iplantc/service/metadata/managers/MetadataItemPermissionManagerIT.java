package org.iplantc.service.metadata.managers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.iplantc.service.metadata.search.MetadataSearch;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Test(groups={"integration"})
public class MetadataItemPermissionManagerIT {
    private ObjectMapper mapper = new ObjectMapper();
    String TEST_USER = "TEST_USER";
    String SHARED_USER = "SHARE_USER";

    @Mock
    private MetadataDao metadataDao;

    @InjectMocks
    private MetadataItemPermissionManager permissionManager;


    @BeforeTest
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @AfterTest
    public void cleanup(){
        MongoCollection collection = metadataDao.getInstance().getDefaultCollection();
        collection.deleteMany(new Document());
    }

    //add new permission
    @Test
    public void addNewPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        //create metadata item
        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);

        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.insertMetadataItem();

        //create and add permission
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
        pemManager.setAccessibleOwnersExplicit();
        MetadataPermission permissionToUpdate = new MetadataPermission(addedMetadataItem.getUuid(), SHARED_USER, PermissionType.READ);
        MetadataPermission updatedResult = pemManager.updatePermissions(permissionToUpdate);

        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
        updatedSearch.setUuid(metadataItem.getUuid());
        MetadataItem updatedMetadataItem = updatedSearch.findOne();

        Assert.assertNotNull(updatedMetadataItem);
        Assert.assertEquals(updatedMetadataItem.getPermissions_User(SHARED_USER), permissionToUpdate, "READ user permission for 'shareuser' should be added.");

    }

    @Test
    public void addInvalidPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);

        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.insertMetadataItem();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
        pemManager.setAccessibleOwnersExplicit();


        MetadataPermission permissionToUpdate = new MetadataPermission(addedMetadataItem.getUuid(), SHARED_USER, PermissionType.getIfPresent("read"));
        Assert.assertEquals(permissionToUpdate.getPermission(), PermissionType.UNKNOWN);

        try {
            pemManager.updatePermissions(permissionToUpdate);
            Assert.fail("Updating with unknown permission should throw Metadata Exception");
        } catch (MetadataException e) {
            Assert.assertEquals(e.getMessage(), "Unknown metadata permission.");
        }
    }


    //update existing permission
    @Test
    public void updateExistingPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);
        MetadataPermission permissionToUpdate = new MetadataPermission(metadataItem.getUuid(), SHARED_USER, PermissionType.READ);
        metadataItem.setPermissions(Arrays.asList(permissionToUpdate));

        //add item
        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.insertMetadataItem();

        //update
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
        pemManager.setAccessibleOwnersExplicit();
        MetadataPermission updatedPermission = new MetadataPermission(addedMetadataItem.getUuid(), SHARED_USER, PermissionType.READ_WRITE);
        pemManager.updatePermissions(updatedPermission);

        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
        updatedSearch.setUuid(metadataItem.getUuid());
        MetadataItem updatedMetadataItem = updatedSearch.findOne();
        Assert.assertEquals(updatedMetadataItem.getPermissions_User(SHARED_USER).getPermission(), PermissionType.READ_WRITE, "User permission should be updated from READ to READ_WRITE.");

    }

    @Test
    public void updatePermissionWithoutWritePermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);

        //add item
        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.insertMetadataItem();

        //update
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(SHARED_USER, metadataItem.getUuid());
        pemManager.setAccessibleOwnersExplicit();
        MetadataPermission updatedPermission = new MetadataPermission(addedMetadataItem.getUuid(), "NEW_USER", PermissionType.READ_WRITE);
        Assert.assertThrows(PermissionException.class, ()->pemManager.updatePermissions(updatedPermission));

        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
        updatedSearch.setUuid(metadataItem.getUuid());
        MetadataItem updatedMetadataItem = updatedSearch.findOne();
        Assert.assertNull(updatedMetadataItem.getPermissions_User(SHARED_USER), "User with insufficient permissions should not be able to update Metadata Item permissions.");

    }

    //delete permission
    @Test
    public void deletePermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);
        MetadataPermission permissionToUpdate = new MetadataPermission(metadataItem.getUuid(), SHARED_USER, PermissionType.READ);
        List<MetadataPermission> permissionList = new ArrayList(Arrays.asList(permissionToUpdate));
        metadataItem.setPermissions(permissionList);

        //add item
        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.insertMetadataItem();

        //delete
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
        pemManager.setAccessibleOwnersExplicit();

        MetadataPermission updatedPermission = new MetadataPermission(addedMetadataItem.getUuid(), SHARED_USER, PermissionType.NONE);
        pemManager.updatePermissions(updatedPermission);

        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
        updatedSearch.setUuid(metadataItem.getUuid());
        MetadataItem updatedMetadataItem = updatedSearch.findOne();
        Assert.assertNull(updatedMetadataItem.getPermissions_User(SHARED_USER), "Removed user permission should return null. ");
    }

    @Test
    public void deletePermissionWithReadPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);
        MetadataPermission permissionToUpdate = new MetadataPermission(metadataItem.getUuid(), SHARED_USER, PermissionType.READ);
        List<MetadataPermission> permissionList = new ArrayList(Arrays.asList(permissionToUpdate));
        metadataItem.setPermissions(permissionList);

        //add item
        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.insertMetadataItem();

        //delete
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(SHARED_USER, metadataItem.getUuid());
        pemManager.setAccessibleOwnersExplicit();

        MetadataPermission updatedPermission = new MetadataPermission(addedMetadataItem.getUuid(), SHARED_USER, PermissionType.NONE);
        Assert.assertThrows(PermissionException.class,() -> pemManager.updatePermissions(updatedPermission));

        MetadataSearch updatedSearch = new MetadataSearch(SHARED_USER);
        updatedSearch.setUuid(metadataItem.getUuid());
        MetadataItem updatedMetadataItem = updatedSearch.findOne();
        Assert.assertNotNull(updatedMetadataItem.getPermissions_User(SHARED_USER), "User with insufficient permissions should not be able to delete Metadata Item permissions.");
    }

    @Test
    public void deleteNonExistentPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);

        //add item
        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.insertMetadataItem();

        //delete permission
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
        MetadataPermission updatedPermission = new MetadataPermission(addedMetadataItem.getUuid(), "invalidUser", PermissionType.NONE);
        pemManager.deletePermission(addedMetadataItem, updatedPermission);

        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
        updatedSearch.setAccessibleOwnersExplicit();
        updatedSearch.setUuid(addedMetadataItem.getUuid());
        MetadataItem updatedMetadataItem = updatedSearch.findOne();
        Assert.assertNull(updatedMetadataItem.getPermissions_User("invalidUser"));
    }

    //find user permission
    @Test
    public void findUserPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        String TEST_USER = "testuser";
        String SHARED_USER = "shareuser";

        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);
        MetadataPermission permissionToUpdate = new MetadataPermission(metadataItem.getUuid(), SHARED_USER, PermissionType.READ_WRITE);
        List<MetadataPermission> permissionList = new ArrayList(Arrays.asList(permissionToUpdate));
        metadataItem.setPermissions(permissionList);

        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersImplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.insertMetadataItem();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
        pemManager.setAccessibleOwnersExplicit();
        MetadataPermission foundMetadataItem = pemManager.findPermission_User(SHARED_USER);

        Assert.assertEquals(foundMetadataItem.getPermission(), PermissionType.READ_WRITE);
    }

    @Test
    public void findOwnerPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        String TEST_USER = "testuser";
        String SHARED_USER = "shareuser";

        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);

        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.insertMetadataItem();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
        pemManager.setAccessibleOwnersExplicit();
        pemManager.setAccessibleOwnersExplicit();
        MetadataPermission foundMetadataItem = pemManager.findPermission_User(TEST_USER);

        Assert.assertEquals(foundMetadataItem.getPermission(), PermissionType.ALL);

    }

    //find invalid user permission
    @Test
    public void findNonExistentUserPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        String TEST_USER = "testuser";
        String SHARED_USER = "shareuser";

        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);

        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.insertMetadataItem();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
        pemManager.setAccessibleOwnersExplicit();
        MetadataPermission foundMetadataItem = pemManager.findPermission_User(SHARED_USER);

        Assert.assertNull(foundMetadataItem, "User without permissions should not return anything.");
    }

    //find metadata permission
    @Test
    public void findPermissionForMetadataItemTest() throws MetadataException, PermissionException, MetadataStoreException {
        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);
        MetadataPermission permissionToUpdate = new MetadataPermission(metadataItem.getUuid(), SHARED_USER, PermissionType.READ_WRITE);
        List<MetadataPermission> permissionList = new ArrayList(Arrays.asList(permissionToUpdate));
        metadataItem.setPermissions(permissionList);

        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.insertMetadataItem();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
        pemManager.setAccessibleOwnersExplicit();
        List<MetadataPermission> foundMetadataItem = pemManager.findPermission_Uuid();
        Assert.assertEquals(foundMetadataItem, addedMetadataItem.getPermissions(), "Metadata item found should match the Metadata item added.");
    }

    //find invalid metadata permission
    @Test
    public void findPermissionForNonExistentMetadataItemTest() throws PermissionException {
        String uuid = new AgaveUUID(UUIDType.JOB).toString();
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, uuid);
        pemManager.setAccessibleOwnersExplicit();
        List<MetadataPermission> foundMetadataItem = pemManager.findPermission_Uuid();
        Assert.assertNull(foundMetadataItem, "Invalid uuid should return null.");
    }
}
