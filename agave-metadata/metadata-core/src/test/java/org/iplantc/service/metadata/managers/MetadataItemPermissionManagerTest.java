package org.iplantc.service.metadata.managers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

//@Test(groups={"unit"})
public class MetadataItemPermissionManagerTest {
    private ObjectMapper mapper = new ObjectMapper();

    public MetadataItem createSingleMetadataItem() {
        MetadataItem metadataItemToUpdate = new MetadataItem();
        metadataItemToUpdate.setName(MetadataItemPermissionManagerTest.class.getName());
        metadataItemToUpdate.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItemToUpdate.setOwner("owner");
        return metadataItemToUpdate;
    }

    //update permission - with permission
    @Test
    public void updatePermissionTest() throws MetadataException, MetadataStoreException, PermissionException {
        String userToUpdate = "testUser";
        MetadataItem metadataItemToUpdate = createSingleMetadataItem();

        MetadataPermission metadataPermission = new MetadataPermission(metadataItemToUpdate.getUuid(), userToUpdate, PermissionType.READ);
        metadataItemToUpdate.updatePermissions(metadataPermission);

        MetadataDao mockMetadataDao = mock(MetadataDao.class);
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("owner", metadataItemToUpdate.getUuid());
        pemManager.setMetadataDao(mockMetadataDao);
        when(mockMetadataDao.hasWrite(any(), any())).thenReturn(true);
        when(mockMetadataDao.findSingleMetadataItem(any())).thenReturn(metadataItemToUpdate);
        when(mockMetadataDao.updatePermission(any())).thenReturn(metadataItemToUpdate.getPermissions());

        MetadataPermission updatedPem = pemManager.updatePermissions(metadataPermission);
        Assert.assertEquals(updatedPem, metadataPermission, "Permission should be updated for " + userToUpdate + " to " + PermissionType.READ);

    }

    //update permission - with permission - unknown permission type
    @Test
    public void updatePermissionUnknownPermissionTypeTest() throws MetadataException, PermissionException, MetadataStoreException {
        String userToUpdate = "testUser";
        MetadataItem metadataItemToUpdate = createSingleMetadataItem();

        MetadataPermission metadataPermission = new MetadataPermission(metadataItemToUpdate.getUuid(), userToUpdate, PermissionType.UNKNOWN);

        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("owner", metadataItemToUpdate.getUuid());
        Assert.assertThrows(MetadataException.class, () -> pemManager.updatePermissions(metadataPermission));
    }

    //update permission - without permission
    @Test
    public void updatePermissionWithoutValidPermissionTest() throws MetadataException {
        String userToUpdate = "testUser";
        MetadataItem metadataItemToUpdate = createSingleMetadataItem();

        MetadataPermission metadataPermission = new MetadataPermission(metadataItemToUpdate.getUuid(), userToUpdate, PermissionType.READ);

        MetadataDao mockMetadataDao = mock(MetadataDao.class);
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("owner", metadataItemToUpdate.getUuid());
        pemManager.setMetadataDao(mockMetadataDao);
        when(mockMetadataDao.hasWrite(any(), any())).thenReturn(false);
        Assert.assertThrows(PermissionException.class, () -> pemManager.updatePermissions(metadataPermission));
    }

    //find permission for user - with authorization - implicit - have ownership or is admin
    @Test
    public void findPermissionImplicitTest() throws MetadataException, PermissionException {
        String userToUpdate = "testUser";
        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
        MetadataPermission metadataPermission = new MetadataPermission(metadataItemToUpdate.getUuid(), userToUpdate, PermissionType.READ);
        metadataItemToUpdate.updatePermissions(metadataPermission);

        MetadataDao mockMetadataDao = mock(MetadataDao.class);
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("owner", metadataItemToUpdate.getUuid());
        pemManager.setAccessibleOwnersImplicit();
        pemManager.setMetadataDao(mockMetadataDao);
        when(mockMetadataDao.hasRead(any(), any())).thenReturn(true);
        when(mockMetadataDao.find("owner", any())).thenReturn(new ArrayList<>(Arrays.asList(metadataItemToUpdate)));

        Assert.assertEquals(pemManager.findPermission_User("owner"), metadataItemToUpdate, "Permission for user " + "owner" + "should be found.");
    }

    //find permission for user - without authorization - implicit - have ownership or is admin
    @Test
    public void findPermissionImplicitWithoutValidPermissionTest() throws MetadataException, PermissionException {
        String userToUpdate = "testUser";
        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
        MetadataPermission metadataPermission = new MetadataPermission(metadataItemToUpdate.getUuid(), userToUpdate, PermissionType.READ);
        metadataItemToUpdate.updatePermissions(metadataPermission);

        MetadataDao mockMetadataDao = mock(MetadataDao.class);
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("sharedUser", metadataItemToUpdate.getUuid());
        pemManager.setAccessibleOwnersImplicit();
        pemManager.setMetadataDao(mockMetadataDao);
        when(mockMetadataDao.hasRead(any(), any())).thenReturn(false);

        Assert.assertThrows(PermissionException.class, () -> pemManager.findPermission_User("testUser"));
    }

    //find permission for user - with authorization - explicit - have ownership or granted explicit access
    @Test
    public void findPermissionExplicitTest() {
        //TODO
    }

    //find permission for user - without authorization - explicit - have ownership or granted explicit access
    @Test
    public void findPermissionExplicitWithoutValidPermissionTest() {
        //TODO
    }

    //find metadata item permission - with authorization - implicit - have ownership or is admin
    @Test
    public void findUuidPermissionImplicitTest() throws MetadataException, PermissionException {
        String testUser = "testUser";
        String shareUser = "shareUser";
        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
        MetadataPermission testUserPermission = new MetadataPermission(metadataItemToUpdate.getUuid(), testUser, PermissionType.READ);
        MetadataPermission shareUserPermission = new MetadataPermission(metadataItemToUpdate.getUuid(), shareUser, PermissionType.READ);
        metadataItemToUpdate.setPermissions(new ArrayList<>(Arrays.asList(testUserPermission, shareUserPermission)));

        MetadataDao mockMetadataDao = mock(MetadataDao.class);
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("owner", metadataItemToUpdate.getUuid());
        pemManager.setAccessibleOwnersImplicit();
        pemManager.setMetadataDao(mockMetadataDao);
        when(mockMetadataDao.hasRead(any(), any())).thenReturn(true);
        when(mockMetadataDao.find("owner", any())).thenReturn(new ArrayList<>(Arrays.asList(metadataItemToUpdate)));

        Assert.assertEquals(pemManager.findPermission_Uuid(), metadataItemToUpdate.getPermissions(), "List of permissions for given MetadataItem should be returned.");
    }

    @Test
    public void findUuidPermissionImplicitInvalidUuidTest() throws MetadataException, PermissionException {
        String userToUpdate = "testUser";
        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
        MetadataPermission metadataPermission = new MetadataPermission(metadataItemToUpdate.getUuid(), userToUpdate, PermissionType.READ);
        metadataItemToUpdate.updatePermissions(metadataPermission);

        MetadataDao mockMetadataDao = mock(MetadataDao.class);
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("owner", metadataItemToUpdate.getUuid());
        pemManager.setAccessibleOwnersImplicit();
        pemManager.setMetadataDao(mockMetadataDao);
        when(mockMetadataDao.hasRead(any(), any())).thenReturn(true);
        when(mockMetadataDao.find("owner", any())).thenReturn(new ArrayList<>());

        Assert.assertNull(pemManager.findPermission_Uuid(), "Nothing should be returned for invalid/nonexistent uuids.");
    }

    //find metadata item permission - without authorization - implicit - have ownership or is admin
    @Test
    public void findUuidPermissionImplicitWithoutValidPermissionTest() throws MetadataException, PermissionException {
        String userToUpdate = "testUser";
        MetadataItem metadataItemToUpdate = createSingleMetadataItem();
        MetadataPermission metadataPermission = new MetadataPermission(metadataItemToUpdate.getUuid(), userToUpdate, PermissionType.READ);
        metadataItemToUpdate.updatePermissions(metadataPermission);

        MetadataDao mockMetadataDao = mock(MetadataDao.class);
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager("sharedUser", metadataItemToUpdate.getUuid());
        pemManager.setAccessibleOwnersImplicit();
        pemManager.setMetadataDao(mockMetadataDao);
        when(mockMetadataDao.hasRead(any(), any())).thenReturn(false);

        Assert.assertThrows(PermissionException.class, () -> pemManager.findPermission_Uuid());
    }

    //find metadata item permission - with authorization - explicit - have ownership or granted explicit access
    @Test
    public void findUuidPermissionExplicitTest() {
        //TODO
    }

    //find metadata item permission - without authorization - explicit - have ownership or granted explicit access
    @Test
    public void findUuidPermissionExplicitWithoutValidPermissionTest() {
        //TODO
    }


}
