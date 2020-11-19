package org.iplantc.service.metadata.managers;

import org.testng.annotations.Test;

@Test(enabled = false,groups={"integration"})
@Deprecated
public class MetadataItemPermissionManagerIT {
//    private ObjectMapper mapper = new ObjectMapper();
//    String TEST_USER = "TEST_USER";
//    String SHARED_USER = "SHARE_USER";
//
//    @Mock
//    private MetadataDao metadataDao;
//
//    @BeforeTest
//    public void setup() {
//        MockitoAnnotations.initMocks(this);
//    }
//
//    @AfterTest
//    public void cleanup(){
//        MongoCollection collection = metadataDao.getInstance().getDefaultCollection();
//        collection.deleteMany(new Document());
//    }
//
//    //add new permission
//    @Test
//    public void addNewPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
//        //create metadata item
//        MetadataItem metadataItem = new MetadataItem();
//        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
//        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        metadataItem.setOwner(TEST_USER);
//
//        MetadataSearch search = new MetadataSearch(TEST_USER);
//        search.setAccessibleOwnersExplicit();
//        search.setMetadataItem(metadataItem);
//        MetadataItem addedMetadataItem = search.insertCurrentMetadataItem();
//
//        //create and add permission
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
//        pemManager.setAccessibleOwnersExplicit();
//        MetadataPermission permissionToUpdate = new MetadataPermission(SHARED_USER, PermissionType.READ);
//        MetadataPermission updatedResult = pemManager.updateCurrentMetadataItemWithPermission(permissionToUpdate);
//
//        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
//        updatedSearch.setUuid(metadataItem.getUuid());
//        MetadataItem updatedMetadataItem = updatedSearch.findOne();
//
//        Assert.assertNotNull(updatedMetadataItem);
//        assertEquals(updatedMetadataItem.getPermissionForUsername(SHARED_USER), permissionToUpdate, "READ user permission for 'shareuser' should be added.");
//
//    }
//
//    @Test
//    public void addInvalidPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
//        MetadataItem metadataItem = new MetadataItem();
//        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
//        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        metadataItem.setOwner(TEST_USER);
//
//        MetadataSearch search = new MetadataSearch(TEST_USER);
//        search.setAccessibleOwnersExplicit();
//        search.setMetadataItem(metadataItem);
//        MetadataItem addedMetadataItem = search.insertCurrentMetadataItem();
//
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
//        pemManager.setAccessibleOwnersExplicit();
//
//
//        MetadataPermission permissionToUpdate = new MetadataPermission(SHARED_USER, PermissionType.getIfPresent("read"));
//        assertEquals(permissionToUpdate.getPermission(), PermissionType.UNKNOWN);
//
//        try {
//            pemManager.updateCurrentMetadataItemWithPermission(permissionToUpdate);
//            Assert.fail("Updating with unknown permission should throw Metadata Exception");
//        } catch (MetadataException e) {
//            assertEquals(e.getMessage(), "Unknown metadata permission.");
//        }
//    }
//
//
//    //update existing permission
//    @Test
//    public void updateExistingPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
//        MetadataItem metadataItem = new MetadataItem();
//        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
//        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        metadataItem.setOwner(TEST_USER);
//        MetadataPermission permissionToUpdate = new MetadataPermission(SHARED_USER, PermissionType.READ);
//        metadataItem.setPermissions(Arrays.asList(permissionToUpdate));
//
//        //add item
//        MetadataSearch search = new MetadataSearch(TEST_USER);
//        search.setAccessibleOwnersExplicit();
//        search.setMetadataItem(metadataItem);
//        MetadataItem addedMetadataItem = search.insertCurrentMetadataItem();
//
//        //update
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
//        pemManager.setAccessibleOwnersExplicit();
//        MetadataPermission updatedPermission = new MetadataPermission(SHARED_USER, PermissionType.READ_WRITE);
//        pemManager.updateCurrentMetadataItemWithPermission(updatedPermission);
//
//        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
//        updatedSearch.setUuid(metadataItem.getUuid());
//        MetadataItem updatedMetadataItem = updatedSearch.findOne();
//        assertEquals(updatedMetadataItem.getPermissionForUsername(SHARED_USER).getPermission(), PermissionType.READ_WRITE, "User permission should be updated from READ to READ_WRITE.");
//
//    }
//
//    @Test
//    public void updatePermissionWithoutWritePermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
//        MetadataItem metadataItem = new MetadataItem();
//        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
//        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        metadataItem.setOwner(TEST_USER);
//
//        //add item
//        MetadataSearch search = new MetadataSearch(TEST_USER);
//        search.setAccessibleOwnersExplicit();
//        search.setMetadataItem(metadataItem);
//        MetadataItem addedMetadataItem = search.insertCurrentMetadataItem();
//
//        //update
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(SHARED_USER, metadataItem.getUuid());
//        pemManager.setAccessibleOwnersExplicit();
//        MetadataPermission updatedPermission = new MetadataPermission("NEW_USER", PermissionType.READ_WRITE);
//        Assert.assertThrows(PermissionException.class, ()->pemManager.updateCurrentMetadataItemWithPermission(updatedPermission));
//
//        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
//        updatedSearch.setUuid(metadataItem.getUuid());
//        MetadataItem updatedMetadataItem = updatedSearch.findOne();
//        Assert.assertNull(updatedMetadataItem.getPermissionForUsername(SHARED_USER), "User with insufficient permissions should not be able to update Metadata Item permissions.");
//
//    }
//
//    //delete permission
//    @Test
//    public void deletePermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
//        MetadataItem metadataItem = new MetadataItem();
//        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
//        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        metadataItem.setOwner(TEST_USER);
//        MetadataPermission permissionToUpdate = new MetadataPermission(SHARED_USER, PermissionType.READ);
//        List<MetadataPermission> permissionList = new ArrayList(Arrays.asList(permissionToUpdate));
//        metadataItem.setPermissions(permissionList);
//
//        //add item
//        MetadataSearch search = new MetadataSearch(TEST_USER);
//        search.setAccessibleOwnersExplicit();
//        search.setMetadataItem(metadataItem);
//        MetadataItem addedMetadataItem = search.insertCurrentMetadataItem();
//
//        assertEquals(addedMetadataItem.getUuid(), metadataItem.getUuid(), "UUID should be the same after inserting to the db.");
//
//        //delete
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, addedMetadataItem.getUuid());
//        pemManager.setAccessibleOwnersExplicit();
//
//        MetadataPermission updatedPermission = new MetadataPermission(SHARED_USER, PermissionType.NONE);
//        pemManager.updateCurrentMetadataItemWithPermission().updatePermission(updatedPermission);
//
//        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
//        updatedSearch.setUuid(metadataItem.getUuid());
//        MetadataItem updatedMetadataItem = updatedSearch.findOne();
//        Assert.assertNull(updatedMetadataItem.getPermissionForUsername(SHARED_USER), "Removed user permission should return null. ");
//    }
//
//    @Test
//    public void deletePermissionWithReadPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
//        MetadataItem metadataItem = new MetadataItem();
//        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
//        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        metadataItem.setOwner(TEST_USER);
//        MetadataPermission permissionToUpdate = new MetadataPermission(SHARED_USER, PermissionType.READ);
//        List<MetadataPermission> permissionList = new ArrayList(Arrays.asList(permissionToUpdate));
//        metadataItem.setPermissions(permissionList);
//
//        //add item
//        MetadataSearch search = new MetadataSearch(TEST_USER);
//        search.setAccessibleOwnersExplicit();
//        search.setMetadataItem(metadataItem);
//        MetadataItem addedMetadataItem = search.insertCurrentMetadataItem();
//
//        //delete
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(SHARED_USER, metadataItem.getUuid());
//        pemManager.setAccessibleOwnersExplicit();
//
//        MetadataPermission updatedPermission = new MetadataPermission(SHARED_USER, PermissionType.NONE);
//        Assert.assertThrows(PermissionException.class,() -> pemManager.updateCurrentMetadataItemWithPermission(updatedPermission));
//
//        MetadataSearch updatedSearch = new MetadataSearch(SHARED_USER);
//        updatedSearch.setUuid(metadataItem.getUuid());
//        MetadataItem updatedMetadataItem = updatedSearch.findOne();
//        Assert.assertNotNull(updatedMetadataItem.getPermissionForUsername(SHARED_USER), "User with insufficient permissions should not be able to delete Metadata Item permissions.");
//    }
//
//    @Test
//    public void deleteNonExistentPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
//        MetadataItem metadataItem = new MetadataItem();
//        metadataItem.setName(UUID.randomUUID().toString());
//        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        metadataItem.setOwner(TEST_USER);
//
//        //add item
//        MetadataSearch search = new MetadataSearch(TEST_USER);
//        search.setAccessibleOwnersExplicit();
//        search.setMetadataItem(metadataItem);
//        MetadataItem addedMetadataItem = search.insertCurrentMetadataItem();
//
//        //delete permission
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
//        MetadataPermission updatedPermission = new MetadataPermission("invalidUser", PermissionType.NONE);
//        pemManager.deletePermission(addedMetadataItem, updatedPermission);
//
//        MetadataSearch updatedSearch = new MetadataSearch(TEST_USER);
//        updatedSearch.setAccessibleOwnersExplicit();
//        updatedSearch.setUuid(addedMetadataItem.getUuid());
//        MetadataItem updatedMetadataItem = updatedSearch.findOne();
//        Assert.assertNull(updatedMetadataItem.getPermissionForUsername("invalidUser"));
//    }
//
//    //find user permission
//    @Test
//    public void findUserPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
//        String TEST_USER = "testuser";
//        String SHARED_USER = "shareuser";
//
//        MetadataItem metadataItem = new MetadataItem();
//        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
//        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        metadataItem.setOwner(TEST_USER);
//        MetadataPermission permissionToUpdate = new MetadataPermission(SHARED_USER, PermissionType.READ_WRITE);
//        List<MetadataPermission> permissionList = new ArrayList(Arrays.asList(permissionToUpdate));
//        metadataItem.setPermissions(permissionList);
//
//        MetadataSearch search = new MetadataSearch(TEST_USER);
//        search.setAccessibleOwnersImplicit();
//        search.setMetadataItem(metadataItem);
//        MetadataItem addedMetadataItem = search.insertCurrentMetadataItem();
//
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
//        pemManager.setAccessibleOwnersExplicit();
//        MetadataPermission foundMetadataItem = pemManager.findCurrentMetadataItemPermissionForUsername(SHARED_USER);
//
//        assertEquals(foundMetadataItem.getPermission(), PermissionType.READ_WRITE);
//    }
//
//    @Test
//    public void findOwnerPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
//        String TEST_USER = "testuser";
//        String SHARED_USER = "shareuser";
//
//        MetadataItem metadataItem = new MetadataItem();
//        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
//        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        metadataItem.setOwner(TEST_USER);
//
//        MetadataSearch search = new MetadataSearch(TEST_USER);
//        search.setAccessibleOwnersExplicit();
//        search.setMetadataItem(metadataItem);
//        MetadataItem addedMetadataItem = search.insertCurrentMetadataItem();
//
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
//        pemManager.setAccessibleOwnersExplicit();
//        pemManager.setAccessibleOwnersExplicit();
//        MetadataPermission foundMetadataItem = pemManager.findCurrentMetadataItemPermissionForUsername(TEST_USER);
//
//        assertEquals(foundMetadataItem.getPermission(), PermissionType.ALL);
//
//    }
//
//    //find invalid user permission
//    @Test
//    public void findNonExistentUserPermissionTest() throws MetadataException, PermissionException, MetadataStoreException {
//        String TEST_USER = "testuser";
//        String SHARED_USER = "shareuser";
//
//        MetadataItem metadataItem = new MetadataItem();
//        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
//        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        metadataItem.setOwner(TEST_USER);
//
//        MetadataSearch search = new MetadataSearch(TEST_USER);
//        search.setAccessibleOwnersExplicit();
//        search.setMetadataItem(metadataItem);
//        MetadataItem addedMetadataItem = search.insertCurrentMetadataItem();
//
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
//        pemManager.setAccessibleOwnersExplicit();
//        MetadataPermission foundMetadataItem = pemManager.findCurrentMetadataItemPermissionForUsername(SHARED_USER);
//
//        Assert.assertNull(foundMetadataItem, "User without permissions should not return anything.");
//    }
//
//    //find metadata permission
//    @Test
//    public void findPermissionForMetadataItemTest() throws MetadataException, PermissionException, MetadataStoreException {
//        MetadataItem metadataItem = new MetadataItem();
//        metadataItem.setName(MetadataItemPermissionManagerIT.class.getName());
//        metadataItem.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        metadataItem.setOwner(TEST_USER);
//        MetadataPermission permissionToUpdate = new MetadataPermission(SHARED_USER, PermissionType.READ_WRITE);
//        List<MetadataPermission> permissionList = new ArrayList(Arrays.asList(permissionToUpdate));
//        metadataItem.setPermissions(permissionList);
//
//        MetadataSearch search = new MetadataSearch(TEST_USER);
//        search.setAccessibleOwnersExplicit();
//        search.setMetadataItem(metadataItem);
//        MetadataItem addedMetadataItem = search.insertCurrentMetadataItem();
//
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, metadataItem.getUuid());
//        pemManager.setAccessibleOwnersExplicit();
//        List<MetadataPermission> foundMetadataItem = pemManager.findCurrentMetadataItemPermission();
//        assertEquals(foundMetadataItem, addedMetadataItem.getPermissions(), "Metadata item found should match the Metadata item added.");
//    }
//
//    //find invalid metadata permission
//    @Test
//    public void findPermissionForNonExistentMetadataItemTest() throws PermissionException {
//        String uuid = new AgaveUUID(UUIDType.JOB).toString();
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(TEST_USER, uuid);
//        pemManager.setAccessibleOwnersExplicit();
//        List<MetadataPermission> foundMetadataItem = pemManager.findCurrentMetadataItemPermission();
//        Assert.assertNull(foundMetadataItem, "Invalid uuid should return null.");
//    }
}
