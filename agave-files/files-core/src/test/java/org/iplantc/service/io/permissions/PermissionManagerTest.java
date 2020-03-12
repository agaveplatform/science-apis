package org.iplantc.service.io.permissions;

import org.iplantc.service.io.model.JSONTestDataUtil;
import org.iplantc.service.systems.exceptions.SystemException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

@Test(enabled=false)
public class PermissionManagerTest extends AbstractPermissionManagerTest {

    @Test
    public void testGetAllPermissions() {
    }

    @Test
    public void testGetUserPermission() {
    }

    @Test
    public void testIsUserHomeDirOnPublicSystem() {
    }

    @Test
    public void testIsUnderSystemRootDir() {
    }

    @Test
    public void testIsUnderUserHomeDirOnPublicSystem() {
    }

    @Test
    public void testCanRead() {
    }

    @Test
    public void testCanReadWrite() {
    }

    @Test
    public void testCanReadExecute() {
    }

    @Test
    public void testCanWriteExecute() {
    }

    @Test
    public void testRemoveReadPermission() {
    }

    @Test
    public void testTestRemoveReadPermission() {
    }

    @Test
    public void testRemoveWritePermission() {
    }

    @Test
    public void testTestRemoveWritePermission() {
    }

    @Test
    public void testRemoveExecutePermission() {
    }

    @Test
    public void testTestRemoveExecutePermission() {
    }

    @Test
    public void testClearPermissions() {
    }

    @Test
    public void testCanUserReadUri() {
    }

    @Test
    public void testGetImpliedOwnerForFileItemPath() {
    }

    @Override
    protected RemoteSystem getTestSystemDescription(RemoteSystemType type) throws Exception {
        if (type.equals(RemoteSystemType.EXECUTION)) {
            return ExecutionSystem.fromJSON(jtd.getTestDataObject(JSONTestDataUtil.TEST_EXECUTION_SYSTEM_FILE));
        } else if (type.equals(RemoteSystemType.STORAGE)) {
            return StorageSystem.fromJSON(jtd.getTestDataObject(JSONTestDataUtil.TEST_STORAGE_SYSTEM_FILE));
        } else {
            throw new SystemException("RemoteSystem type " + type + " is not supported.");
        }
    }
}