/**
 * 
 */
package org.iplantc.service.transfer.irods4;

import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.irods.AbstractIrodsRemoteDataClientPermissionProviderTest;
import org.iplantc.service.transfer.model.enumerations.PermissionType;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Test for all IRODS4 client permission implementations. This inherits nearly
 * all it's functionality from the parent class.
 * 
 * @author dooley
 *
 */
@Test(singleThreaded = true, groups={"irods4","irods4.permissions"})
public class Irods4RemoteDataClientPermissionProviderIT extends AbstractIrodsRemoteDataClientPermissionProviderTest
{
    @BeforeClass
    protected void beforeClass() throws Exception
    {
        super.beforeClass();
    }

    @AfterClass
    protected void afterClass() throws Exception
    {
        super.afterClass();
    }

    @BeforeMethod
    protected void beforeMethod(Method m)
    {
        super.beforeMethod(m);
    }

    @AfterMethod
    protected void afterMethod(Method m) throws IOException
    {
        super.afterMethod(m);
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.transfer.RemoteDataClientPermissionProviderTest#getSystemJson()
     */
    @Override
    protected JSONObject getSystemJson() throws JSONException, IOException {
        return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "irods4-password.example.com.json");
    }

    @Test(groups= {"mirroring"})
    public void isPermissionMirroringRequired() {
        _isPermissionMirroringRequired();
    }

    @Test(groups= {"permissions", "all", "get"}, dataProvider="getAllPermissionsProvider", dependsOnGroups= {"mirroring"})
    public void getAllPermissions(String path, String errorMessage) {
        _getAllPermissions(path, errorMessage);
    }

    @Test(groups= {"permissions", "all", "get"}, dataProvider="getAllPermissionsProvider", dependsOnMethods={"getAllPermissions"})
    public void getAllPermissionsWithUserFirst(String path, String errorMessage){
        _getAllPermissionsWithUserFirst(path, errorMessage);
    }

    @Test(groups= {"permissions", "get"}, dataProvider="getPermissionForUserProvider", dependsOnGroups={"all"})
    public void getPermissionForUser(String username, String path, boolean shouldHavePermission, String errorMessage) {
        _getPermissionForUser(username, path, shouldHavePermission, errorMessage);
    }

    @Test(groups= {"permissions", "get"}, dataProvider="getPermissionForUserProvider", dependsOnMethods={"getPermissionForUser"})
    public void hasExecutePermission(String username, String path, boolean shouldHavePermission, String errorMessage) {
        _hasExecutePermission(username, path, shouldHavePermission, errorMessage);
    }

    @Test(groups= {"permissions", "get"}, dataProvider="getPermissionForUserProvider", dependsOnMethods={"hasExecutePermission"})
    public void hasReadPermission(String username, String path, boolean shouldHavePermission, String errorMessage) {
        _hasReadPermission(username, path, shouldHavePermission, errorMessage);
    }

    @Test(groups= {"permissions", "get"}, dataProvider="getPermissionForUserProvider", dependsOnMethods={"hasReadPermission"})
    public void hasWritePermission(String username, String path, boolean shouldHavePermission, String errorMessage) {
        _hasWritePermission(username, path, shouldHavePermission, errorMessage);
    }

    @Test(groups= {"permissions", "set"}, dataProvider="setPermissionForSharedUserProvider", dependsOnGroups={"get"})
    public void setPermissionForSharedUser(String username, String path, PermissionType type, boolean recursive, boolean shouldSetPermission, boolean shouldThrowException, String errorMessage) {
        _setPermissionForSharedUser(username, path, type, recursive, shouldSetPermission, shouldThrowException, errorMessage);
    }

    @Test(groups= {"permissions", "set"}, dataProvider="setPermissionForUserProvider", dependsOnMethods={"setPermissionForSharedUser"})
    public void setPermissionForUser(String username, String path, PermissionType type, boolean recursive, boolean shouldSetPermission, boolean shouldThrowException, String errorMessage) {
        _setPermissionForUser(username, path, type, recursive, shouldSetPermission, shouldThrowException, errorMessage);
    }

    @Test(groups= {"permissions", "set"}, dataProvider="setExecutePermissionProvider", dependsOnMethods={"setPermissionForUser"},
            expectedExceptions = RemoteDataException.class,
            expectedExceptionsMessageRegExp = "Execute permissions are not supported on IRODS systems.")
    public void setExecutePermission(String username, String path, boolean recursive, boolean shouldSetPermission, String errorMessage)
            throws RemoteDataException, IOException {
        getClient().setExecutePermission(username, path, recursive);
    }

    @Test(groups= {"permissions", "set"}, dataProvider="setOwnerPermissionProvider", dependsOnMethods={"setExecutePermission"})
    public void setOwnerPermission(String username, String path, boolean recursive, boolean shouldSetPermission, String errorMessage)
            throws Exception {
        _setOwnerPermission(username, path, recursive, shouldSetPermission, errorMessage);
    }

    @Test(groups= {"permissions", "set"}, dataProvider="setReadPermissionProvider", dependsOnMethods={"setOwnerPermission"})
    public void setReadPermission(String username, String path, boolean recursive, boolean shouldSetPermission, String errorMessage)
            throws Exception {
        _setReadPermission(username, path, recursive, shouldSetPermission, errorMessage);
    }

    @Test(groups= {"permissions", "set"}, dataProvider="setWritePermissionProvider", dependsOnMethods={"setReadPermission"})
    public void setWritePermission(String username, String path, boolean recursive, boolean shouldSetPermission, String errorMessage)
            throws Exception {
        _setWritePermission(username, path, recursive, shouldSetPermission, errorMessage);
    }

    @Test(groups= {"permissions", "delete"}, dataProvider="removeExecutePermissionProvider", dependsOnGroups={"set"},
            expectedExceptions = RemoteDataException.class,
            expectedExceptionsMessageRegExp = "Execute permissions are not supported on IRODS systems.")
    public void removeExecutePermission(String username, String path, boolean recursive, boolean shouldRemovePermission, String errorMessage) throws RemoteDataException {
        try {
            getClient().removeExecutePermission(username, path, recursive);
        } catch (IOException e) {
            Assert.fail("Removing execution permissions from an irods system should throw a RemoteDataException", e);
        }
    }

    @Test(groups= {"permissions", "delete"}, dataProvider="removeReadPermissionProvider", dependsOnMethods={"removeExecutePermission"})
    public void removeReadPermission(String username, String path, boolean recursive, boolean shouldRemovePermission, String errorMessage) {
        _removeReadPermission(username, path, recursive, shouldRemovePermission, errorMessage);
    }

    @Test(groups= {"permissions", "delete"}, dataProvider="removeWritePermissionProvider", dependsOnMethods={"removeReadPermission"})
    public void removeWritePermission(String username, String path, boolean recursive, boolean shouldRemovePermission, String errorMessage) {
        _removeWritePermission(username, path, recursive, shouldRemovePermission, errorMessage);
    }

    @Test(groups= {"permissions", "delete"}, dataProvider="clearPermissionProvider", dependsOnMethods={"removeWritePermission"})
    public void clearPermissions(String username, String path, PermissionType initialPermission, boolean recursive, boolean shouldClearPermission, String errorMessage)
            throws Exception {
        _clearPermissions(username, path, initialPermission, recursive, shouldClearPermission, errorMessage);
    }
}