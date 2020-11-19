package org.iplantc.service.metadata.managers;

import org.testng.annotations.Test;

@Test(enabled = false, groups={"integration"})
@Deprecated
public class MetadataItemPermissionManagerTest {
//    private ObjectMapper mapper = new ObjectMapper();
//
//    public MetadataItem createSingleMetadataItem() {
//        MetadataItem metadataItemToUpdate = new MetadataItem();
//        metadataItemToUpdate.setName(MetadataItemPermissionManagerTest.class.getName());
//        metadataItemToUpdate.setValue(mapper.createObjectNode().put("testKey", "testValue"));
//        metadataItemToUpdate.setOwner("owner");
//        return metadataItemToUpdate;
//    }
//
//    //update permission - with permission
//    @Test
//    public void updatePermissionTest() throws MetadataException, MetadataStoreException, PermissionException {
//        String userToUpdate = "testUser";
//        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
//
//        MetadataPermission metadataPermission = new MetadataPermission(userToUpdate, PermissionType.READ);
//        metadataItemToUpdate.updatePermissions(metadataPermission);
//
//        MetadataDao mockMetadataDao = mock(MetadataDao.class);
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("owner", metadataItemToUpdate.getUuid());
//        pemManager.setMetadataDao(mockMetadataDao);
//        when(mockMetadataDao.hasWrite(any(), any())).thenReturn(true);
//        when(mockMetadataDao.findSingleMetadataItem(any())).thenReturn(metadataItemToUpdate);
//        when(mockMetadataDao.updatePermission(any())).thenReturn(metadataItemToUpdate.getPermissions());
//
//        MetadataPermission updatedPem = pemManager.updateCurrentMetadataItemWithPermission(metadataPermission);
//        Assert.assertEquals(updatedPem, metadataPermission, "Permission should be updated for " + userToUpdate + " to " + PermissionType.READ);
//
//    }
//
//    //update permission - with permission - unknown permission type
//    @Test
//    public void updatePermissionUnknownPermissionTypeTest() throws MetadataException, PermissionException, MetadataStoreException {
//        String userToUpdate = "testUser";
//        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
//
//        MetadataPermission metadataPermission = new MetadataPermission(userToUpdate, PermissionType.UNKNOWN);
//
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("owner", metadataItemToUpdate.getUuid());
//        Assert.assertThrows(MetadataException.class, () -> pemManager.updateCurrentMetadataItemWithPermission(metadataPermission));
//    }
//
//    //update permission - without permission
//    @Test
//    public void updatePermissionWithoutValidPermissionTest() throws MetadataException {
//        String userToUpdate = "testUser";
//        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
//
//        MetadataPermission metadataPermission = new MetadataPermission(userToUpdate, PermissionType.READ);
//
//        MetadataDao mockMetadataDao = mock(MetadataDao.class);
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("owner", metadataItemToUpdate.getUuid());
//        pemManager.setMetadataDao(mockMetadataDao);
//        when(mockMetadataDao.hasWrite(any(), any())).thenReturn(false);
//        Assert.assertThrows(PermissionException.class, () -> pemManager.updateCurrentMetadataItemWithPermission(metadataPermission));
//    }
//
//    //find permission for user - with authorization - implicit - have ownership or is admin
//    @Test
//    public void findPermissionImplicitTest() throws MetadataException, PermissionException {
//        String userToUpdate = "testUser";
//        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
//        MetadataPermission metadataPermission = new MetadataPermission(userToUpdate, PermissionType.READ);
//        metadataItemToUpdate.updatePermissions(metadataPermission);
//
//        MetadataDao mockMetadataDao = mock(MetadataDao.class);
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("owner", metadataItemToUpdate.getUuid());
//        pemManager.setAccessibleOwnersImplicit();
//        pemManager.setMetadataDao(mockMetadataDao);
//        when(mockMetadataDao.hasRead(any(), any())).thenReturn(true);
//        when(mockMetadataDao.find("owner", any())).thenReturn(new ArrayList<>(Arrays.asList(metadataItemToUpdate)));
//
//        Assert.assertEquals(pemManager.findCurrentMetadataItemPermissionForUsername("owner"), metadataItemToUpdate, "Permission for user " + "owner" + "should be found.");
//    }
//
//    //find permission for user - without authorization - implicit - have ownership or is admin
//    @Test
//    public void findPermissionImplicitWithoutValidPermissionTest() throws MetadataException, PermissionException {
//        String userToUpdate = "testUser";
//        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
//        MetadataPermission metadataPermission = new MetadataPermission(userToUpdate, PermissionType.READ);
//        metadataItemToUpdate.updatePermissions(metadataPermission);
//
//        MetadataDao mockMetadataDao = mock(MetadataDao.class);
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("sharedUser", metadataItemToUpdate.getUuid());
//        pemManager.setAccessibleOwnersImplicit();
//        pemManager.setMetadataDao(mockMetadataDao);
//        when(mockMetadataDao.hasRead(any(), any())).thenReturn(false);
//
//        Assert.assertThrows(PermissionException.class, () -> pemManager.findCurrentMetadataItemPermissionForUsername("testUser"));
//    }
//
//    //find permission for user - with authorization - explicit - have ownership or granted explicit access
//    @Test
//    public void findPermissionExplicitTest() {
//        //TODO
//    }
//
//    //find permission for user - without authorization - explicit - have ownership or granted explicit access
//    @Test
//    public void findPermissionExplicitWithoutValidPermissionTest() {
//        //TODO
//    }
//
//    //find metadata item permission - with authorization - implicit - have ownership or is admin
//    @Test
//    public void findUuidPermissionImplicitTest() throws MetadataException, PermissionException {
//        String testUser = "testUser";
//        String shareUser = "shareUser";
//        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
//        MetadataPermission testUserPermission = new MetadataPermission(testUser, PermissionType.READ);
//        MetadataPermission shareUserPermission = new MetadataPermission(shareUser, PermissionType.READ);
//        metadataItemToUpdate.setPermissions(new ArrayList<>(Arrays.asList(testUserPermission, shareUserPermission)));
//
//        MetadataDao mockMetadataDao = mock(MetadataDao.class);
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("owner", metadataItemToUpdate.getUuid());
//        pemManager.setAccessibleOwnersImplicit();
//        pemManager.setMetadataDao(mockMetadataDao);
//        when(mockMetadataDao.hasRead(any(), any())).thenReturn(true);
//        when(mockMetadataDao.find("owner", any())).thenReturn(new ArrayList<>(Arrays.asList(metadataItemToUpdate)));
//
//        Assert.assertEquals(pemManager.findCurrentMetadataItemPermission(), metadataItemToUpdate.getPermissions(), "List of permissions for given MetadataItem should be returned.");
//    }
//
//    @Test
//    public void findUuidPermissionImplicitInvalidUuidTest() throws MetadataException, PermissionException {
//        String userToUpdate = "testUser";
//        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
//        MetadataPermission metadataPermission = new MetadataPermission(userToUpdate, PermissionType.READ);
//        metadataItemToUpdate.updatePermissions(metadataPermission);
//
//        MetadataDao mockMetadataDao = mock(MetadataDao.class);
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("owner", metadataItemToUpdate.getUuid());
//        pemManager.setAccessibleOwnersImplicit();
//        pemManager.setMetadataDao(mockMetadataDao);
//        when(mockMetadataDao.hasRead(any(), any())).thenReturn(true);
//        when(mockMetadataDao.find("owner", any())).thenReturn(new ArrayList<>());
//
//        Assert.assertNull(pemManager.findCurrentMetadataItemPermission(), "Nothing should be returned for invalid/nonexistent uuids.");
//    }
//
//    //find metadata item permission - without authorization - implicit - have ownership or is admin
//    @Test
//    public void findUuidPermissionImplicitWithoutValidPermissionTest() throws MetadataException, PermissionException {
//        String userToUpdate = "testUser";
//        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
//        MetadataPermission metadataPermission = new MetadataPermission(userToUpdate, PermissionType.READ);
//        metadataItemToUpdate.updatePermissions(metadataPermission);
//
//        MetadataDao mockMetadataDao = mock(MetadataDao.class);
//        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("sharedUser", metadataItemToUpdate.getUuid());
//        pemManager.setAccessibleOwnersImplicit();
//        pemManager.setMetadataDao(mockMetadataDao);
//        when(mockMetadataDao.hasRead(any(), any())).thenReturn(false);
//
//        Assert.assertThrows(PermissionException.class, () -> pemManager.findCurrentMetadataItemPermission());
//    }
//
//    //find metadata item permission - with authorization - explicit - have ownership or granted explicit access
//    @Test
//    public void findUuidPermissionExplicitTest() {
//        //TODO
//    }
//
//    //find metadata item permission - without authorization - explicit - have ownership or granted explicit access
//    @Test
//    public void findUuidPermissionExplicitWithoutValidPermissionTest() {
//        //TODO
//    }


}
