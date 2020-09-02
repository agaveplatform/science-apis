package org.iplantc.service.metadata.managers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.metadata.dao.MetadataDao;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;
import org.iplantc.service.metadata.model.enumerations.PermissionType;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doReturn;

//@Test(groups={"unit"})
public class MetadataItemPermissionManagerTest {
    private ObjectMapper mapper = new ObjectMapper();

    @Mock
    private MetadataDao metadataDao;

    @InjectMocks
    private MetadataItemPermissionManager permissionManager;


    @BeforeTest
    public void setup(){
        MockitoAnnotations.initMocks(this);
    }

    //check permission - no user specified
    public void checkPermissionNullUserTest()  {
        PermissionType allPem = PermissionType.valueOf("ALL");
        String username = null;
        boolean bolWrite = false;
        MetadataItemPermissionManager pemManager = new MetadataItemPermissionManager(username);
        Assert.assertThrows(MetadataException.class, ()->pemManager.checkPermission(allPem, username, bolWrite));
    }

    //check permission - read
    @Test
    public void checkPermissionReadTest(){
        PermissionType readPem = PermissionType.valueOf("READ_WRITE");
    }

    //check permission - write

    //check permission - unknown permission type

    //update permission - with permission
    @Test
    public void updatePermissionTest() throws MetadataException, MetadataStoreException, PermissionException {
        String userToUpdate = "testUser";
        MetadataItem metadataItemToUpdate = new MetadataItem();
        metadataItemToUpdate.setName(MetadataItemPermissionManagerTest.class.getName());
        metadataItemToUpdate.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItemToUpdate.setOwner("owner");

        MetadataPermission metadataPermission = new MetadataPermission(metadataItemToUpdate.getUuid(), userToUpdate, PermissionType.READ);

        MetadataItemPermissionManager pemManager = Mockito.mock(MetadataItemPermissionManager.class);
        when(pemManager.canWrite(anyString())).thenReturn(true);

        List<MetadataPermission> returnPemList = new ArrayList<>();
        returnPemList.add(metadataPermission);
        metadataItemToUpdate.updatePermissions(metadataPermission);
        doReturn(returnPemList).when(metadataDao).updatePermission(any(), any());

        List<MetadataPermission> updatedPem = pemManager.updatePermissions(userToUpdate, metadataPermission, metadataItemToUpdate);

        Assert.assertEquals(updatedPem.get(0), metadataPermission);

    }

    //update permission - with permission - unknown permission type
    @Test
    public void updatePermissionUnknownPermissionTypeTest() throws MetadataException {
        //TODO
        String userToUpdate = "testUser";
        MetadataItem metadataItemToUpdate = new MetadataItem();

        Assert.assertThrows(MetadataException.class, () -> new MetadataPermission(metadataItemToUpdate.getUuid(), userToUpdate, PermissionType.valueOf("unknown")));
    }

    //update permission - without permission
    @Test
    public void updatePermissionWithoutValidPermissionTest() throws MetadataException {
        String userToUpdate = "testUser";
        MetadataItem metadataItemToUpdate = new MetadataItem();
        metadataItemToUpdate.setName(MetadataItemPermissionManagerTest.class.getName());
        metadataItemToUpdate.setValue(mapper.createObjectNode().put("testKey", "testValue"));
        metadataItemToUpdate.setOwner("owner");

        MetadataPermission metadataPermission = new MetadataPermission(metadataItemToUpdate.getUuid(), userToUpdate, PermissionType.READ);

        MetadataItemPermissionManager pemManager = Mockito.mock(MetadataItemPermissionManager.class);
        when(pemManager.canWrite(anyString())).thenReturn(false);

        Assert.assertThrows(PermissionException.class, ()-> pemManager.updatePermissions("updateUser", metadataPermission, metadataItemToUpdate));
    }

    //find permission for user - with authorization - implicit - have ownership or is admin

    //find permission for user - without authorization - implicit - have ownership or is admin

    //find permission for user - with authorization - explicit - have ownership or granted explicit access

    //find permission for user - without authorization - explicit - have ownership or granted explicit access

    //find metadata item permission - with authorization - implicit - have ownership or is admin

    //find metadata item permission - without authorization - implicit - have ownership or is admin

    //find metadata item permission - with authorization - explicit - have ownership or granted explicit access

    //find metadata item permission - without authorization - explicit - have ownership or granted explicit access




}
