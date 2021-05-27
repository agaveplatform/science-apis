package org.iplantc.service.transfer;

import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.model.enumerations.PermissionType;
import org.testng.annotations.Test;

import java.io.IOException;

public interface IRemoteDataClientPermissionProviderTest {
    @Test(groups= {"permissions", "mirroring"})
    void isPermissionMirroringRequired();

    @Test(groups= {"permissions", "all", "get"}, dataProvider="getAllPermissionsProvider", dependsOnGroups= {"mirroring"})
    void getAllPermissions(String path, String errorMessage);

    @Test(groups= {"permissions", "all", "get"}, dataProvider="getAllPermissionsProvider", dependsOnMethods={"getAllPermissions"})
    void getAllPermissionsWithUserFirst(String path, String errorMessage);

    @Test(groups= {"permissions", "get"}, dataProvider="getPermissionForUserProvider", dependsOnGroups={"all"})
    void getPermissionForUser(String username, String path, boolean shouldHavePermission, String errorMessage);

    @Test(groups= {"permissions", "get"}, dataProvider="getPermissionForUserProvider", dependsOnMethods={"getPermissionForUser"})
    void hasExecutePermission(String username, String path, boolean shouldHavePermission, String errorMessage);

    @Test(groups= {"permissions", "get"}, dataProvider="getPermissionForUserProvider", dependsOnMethods={"hasExecutePermission"})
    void hasReadPermission(String username, String path, boolean shouldHavePermission, String errorMessage);

    @Test(groups= {"permissions", "get"}, dataProvider="getPermissionForUserProvider", dependsOnMethods={"hasReadPermission"})
    void hasWritePermission(String username, String path, boolean shouldHavePermission, String errorMessage);

    @Test(groups= {"permissions", "set"}, dataProvider="setPermissionForSharedUserProvider", dependsOnGroups={"get"})
    void setPermissionForSharedUser(String username, String path, PermissionType type, boolean recursive, boolean shouldSetPermission, boolean shouldThrowException, String errorMessage);

    @Test(groups= {"permissions", "set"}, dataProvider="setPermissionForUserProvider", dependsOnMethods={"setPermissionForSharedUser"})
    void setPermissionForUser(String username, String path, PermissionType type, boolean recursive, boolean shouldSetPermission, boolean shouldThrowException, String errorMessage);

    @Test(groups= {"permissions", "set"}, dataProvider="setExecutePermissionProvider", dependsOnMethods={"setPermissionForUser"})
    void setExecutePermission(String username, String path, boolean recursive, boolean shouldSetPermission, String errorMessage)
    throws RemoteDataException, IOException;

    @Test(groups= {"permissions", "set"}, dataProvider="setOwnerPermissionProvider", dependsOnMethods={"setExecutePermission"})
    void setOwnerPermission(String username, String path, boolean recursive, boolean shouldSetPermission, String errorMessage)
    throws Exception;

    @Test(groups= {"permissions", "set"}, dataProvider="setReadPermissionProvider")
    void setReadPermission(String username, String path, boolean recursive, boolean shouldSetPermission, String errorMessage)
    throws Exception;

    @Test(groups= {"permissions", "set"}, dataProvider="setWritePermissionProvider")
    void setWritePermission(String username, String path, boolean recursive, boolean shouldSetPermission, String errorMessage)
    throws Exception;

    @Test(groups= {"permissions", "delete"}, dataProvider="removeExecutePermissionProvider", dependsOnGroups={"set"})
    void removeExecutePermission(String username, String path, boolean recursive, boolean shouldRemovePermission, String errorMessage) throws RemoteDataException;

    @Test(groups= {"permissions", "delete"}, dataProvider="removeReadPermissionProvider", dependsOnMethods={"removeExecutePermission"})
    void removeReadPermission(String username, String path, boolean recursive, boolean shouldRemovePermission, String errorMessage);

    @Test(groups= {"permissions", "delete"}, dataProvider="removeWritePermissionProvider", dependsOnMethods={"removeReadPermission"})
    void removeWritePermission(String username, String path, boolean recursive, boolean shouldRemovePermission, String errorMessage);

    @Test(groups= {"permissions", "delete"}, dataProvider="clearPermissionProvider", dependsOnMethods={"removeWritePermission"})
    void clearPermissions(String username, String path, PermissionType initialPermission, boolean recursive, boolean shouldClearPermission, String errorMessage)
    throws Exception;
}
