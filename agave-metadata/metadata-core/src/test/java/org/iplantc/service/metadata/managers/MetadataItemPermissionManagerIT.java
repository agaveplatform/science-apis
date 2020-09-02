package org.iplantc.service.metadata.managers;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Test(groups={"integration"})
public class MetadataItemPermissionManagerIT {
    private ObjectMapper mapper = new ObjectMapper();

    @Mock
    private MetadataDao metadataDao;

    @InjectMocks
    private MetadataItemPermissionManager permissionManager;


    @BeforeTest
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    //add new permission
    @Test
    public void addNewPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        String TEST_USER = "testuser";
        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);

        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.updateMetadataItem();


        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER);
        String SHARED_USER = "shareuser";
        MetadataPermission permissionToUpdate = new MetadataPermission(addedMetadataItem.getUuid(), SHARED_USER, PermissionType.READ);

        List<MetadataPermission> updatedResult = pemManager.updatePermissions(SHARED_USER, permissionToUpdate, addedMetadataItem);

        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
        updatedSearch.setAccessibleOwnersExplicit();

        addedMetadataItem.updatePermissions(permissionToUpdate);
        updatedSearch.setMetadataItem(addedMetadataItem);
        MetadataItem updatedMetadataItem = updatedSearch.findOne(new String[0]);

        Assert.assertNotNull(updatedMetadataItem);
        Assert.assertEquals(updatedMetadataItem.getPermissions_User(SHARED_USER), permissionToUpdate, "READ user permission for 'shareuser' should be added.");

    }

    @Test
    public void addInvalidPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        String TEST_USER = "testuser";
        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);

        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.updateMetadataItem();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER);
        String SHARED_USER = "shareuser";

        MetadataPermission permissionToUpdate = new MetadataPermission(addedMetadataItem.getUuid(), SHARED_USER, PermissionType.getIfPresent("read"));
        Assert.assertEquals(permissionToUpdate.getPermission(), PermissionType.UNKNOWN);

        try {
            pemManager.updatePermissions(SHARED_USER, permissionToUpdate, addedMetadataItem);
            Assert.fail("Updating with unknown permission should throw Metadata Exception");
        } catch (MetadataException e) {
            Assert.assertEquals(e.getMessage(), "Unknown metadata permission.");
        }
    }


    //update existing permission
    @Test
    public void updateExistingPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        String TEST_USER = "testuser";
        String SHARED_USER = "shareuser";

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
        MetadataItem addedMetadataItem = search.updateMetadataItem();

        //verify it was added
        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
        updatedSearch.setAccessibleOwnersExplicit();
        updatedSearch.setUuid(addedMetadataItem.getUuid());

        MetadataItem initialMetadataItem = updatedSearch.findOne(new String[0]);
        Assert.assertEquals(initialMetadataItem.getPermissions_User(SHARED_USER).getPermission(), PermissionType.READ);

        //update
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER);
        MetadataPermission updatedPermission = new MetadataPermission(addedMetadataItem.getUuid(), SHARED_USER, PermissionType.READ_WRITE);
        pemManager.updatePermissions(SHARED_USER, updatedPermission, addedMetadataItem);

        MetadataItem updatedMetadataItem = updatedSearch.findOne(new String[0]);
        Assert.assertEquals(updatedMetadataItem.getPermissions_User(SHARED_USER).getPermission(), PermissionType.READ_WRITE, "User permission should be updated from READ to READ_WRITE.");

    }

    //delete permission
    @Test
    public void deletePermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        String TEST_USER = "testuser";
        String SHARED_USER = "shareuser";

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
        MetadataItem addedMetadataItem = search.updateMetadataItem();

        //verify it was added
        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
        updatedSearch.setAccessibleOwnersExplicit();
        updatedSearch.setUuid(addedMetadataItem.getUuid());

        MetadataItem initialMetadataItem = updatedSearch.findOne(new String[0]);
        Assert.assertEquals(initialMetadataItem.getPermissions_User(SHARED_USER).getPermission(), PermissionType.READ);

        //delete
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER);
        MetadataPermission updatedPermission = new MetadataPermission(addedMetadataItem.getUuid(), SHARED_USER, PermissionType.NONE);
        pemManager.updatePermissions(SHARED_USER, updatedPermission, addedMetadataItem);

        MetadataItem updatedMetadataItem = updatedSearch.findOne(new String[0]);
        Assert.assertNull(updatedMetadataItem.getPermissions_User(SHARED_USER), "Removed user permission should return null. ");
    }

    @Test
    public void deleteNonExistentPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
        String TEST_USER = "testuser";
        String SHARED_USER = "shareuser";

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
        MetadataItem addedMetadataItem = search.updateMetadataItem();

        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
        updatedSearch.setAccessibleOwnersExplicit();
        updatedSearch.setUuid(addedMetadataItem.getUuid());

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER);
        MetadataPermission updatedPermission = new MetadataPermission(addedMetadataItem.getUuid(), "invalidUser", PermissionType.NONE);
        Assert.assertNull(pemManager.updatePermissions("invalidUser", updatedPermission, addedMetadataItem));

        MetadataItem updatedMetadataItem = updatedSearch.findOne(new String[0]);
        Assert.assertNull(updatedMetadataItem.getPermissions_User("invalidUser"));
    }

    //find user permission
    @Test
    public void findUserPermissionTest() throws MetadataException, PermissionException {
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
        MetadataItem addedMetadataItem = search.updateMetadataItem();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER);
        pemManager.setAccessibleOwnersImplicit();
        List<MetadataItem> foundMetadataItem = pemManager.findPermission_User(SHARED_USER, addedMetadataItem.getUuid());

        Assert.assertEquals(foundMetadataItem.get(0).getPermissions_User(SHARED_USER).getPermission(), PermissionType.READ_WRITE);
    }

    @Test
    public void findOwnerPermissionTest() throws MetadataException, PermissionException {
        String TEST_USER = "testuser";
        String SHARED_USER = "shareuser";

        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);

        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.updateMetadataItem();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER);
        pemManager.setAccessibleOwnersImplicit();
        List<MetadataItem> foundMetadataItem = pemManager.findPermission_User(TEST_USER, addedMetadataItem.getUuid());

        Assert.assertEquals(foundMetadataItem.get(0).getPermissions_User(TEST_USER).getPermission(), PermissionType.ALL);

    }

    //find invalid user permission
    @Test
    public void findNonExistentUserPermissionTest() throws MetadataException, PermissionException {
        String TEST_USER = "testuser";
        String SHARED_USER = "shareuser";

        MetadataItem metadataItem = new MetadataItem();
        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItem.setOwner(TEST_USER);

        MetadataSearch search = new MetadataSearch(TEST_USER);
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.updateMetadataItem();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER);
        pemManager.setAccessibleOwnersImplicit();
        List<MetadataItem> foundMetadataItem = pemManager.findPermission_User(SHARED_USER, addedMetadataItem.getUuid());

        Assert.assertEquals(foundMetadataItem.size(), 0, "User without permissions should not return anything.");
    }

    //find metadata permission
    @Test
    public void findPermissionForMetadataItemTest() throws MetadataException, PermissionException {
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
        search.setAccessibleOwnersExplicit();
        search.setMetadataItem(metadataItem);
        MetadataItem addedMetadataItem = search.updateMetadataItem();

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER);
        pemManager.setAccessibleOwnersImplicit();
        MetadataItem foundMetadataItem = pemManager.findPermission_Uuid(addedMetadataItem.getUuid());
        Assert.assertEquals(foundMetadataItem, addedMetadataItem, "Metadata item found should match the Metadata item added.");
    }

    //find invalid metadata permission
    @Test
    public void findPermissionForNonExistentMetadataItemTest() throws MetadataException, PermissionException {
        String TEST_USER = "testuser";
        String uuid = new AgaveUUID(UUIDType.JOB).toString();
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER);
        pemManager.setAccessibleOwnersImplicit();
        MetadataItem foundMetadataItem = pemManager.findPermission_Uuid(uuid);
        Assert.assertNull(foundMetadataItem, "Invalid uuid should return null.");
    }
}
