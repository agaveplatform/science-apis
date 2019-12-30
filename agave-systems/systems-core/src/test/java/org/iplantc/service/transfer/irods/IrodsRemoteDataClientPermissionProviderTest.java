/**
 * 
 */
package org.iplantc.service.transfer.irods;

import java.io.IOException;
import java.lang.reflect.Method;

import org.iplantc.service.transfer.IRemoteDataClientPermissionProviderTest;
import org.iplantc.service.transfer.RemoteDataClientPermissionProviderTest;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.model.enumerations.PermissionType;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test for all IRODS3 client permission implementations. This inherits nearly
 * all it's functionality from the parent class.
 * 
 * @author dooley
 *
 */
@Test(groups={"irods3","irods3.permissions"})
public class IrodsRemoteDataClientPermissionProviderTest extends RemoteDataClientPermissionProviderTest implements IRemoteDataClientPermissionProviderTest {

    /* (non-Javadoc)
     * @see org.iplantc.service.transfer.RemoteDataClientPermissionProviderTest#getSystemJson()
     */
    @Override
    protected JSONObject getSystemJson() throws JSONException, IOException {
        return jtd.getTestDataObject(STORAGE_SYSTEM_TEMPLATE_DIR + "/" + "irods3.example.com.json");
    }
	
	@DataProvider(name="setPermissionForSharedUserProvider")
	public Object[][] setPermissionForSharedUserProvider(Method m)
	{
		return new Object[][] {
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.ALL, false, true, false, "Failed to set delegated ALL permission on folder root"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.EXECUTE, false, true, true, "Failed to set delegated EXECUTE permission on folder root"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ, false, true, false, "Failed to set delegated READ permission on folder root"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.WRITE, false, true, false, "Failed to set delegated WRITE permission on folder root"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ_EXECUTE, false, true, true, "Failed to set delegated READ_EXECUTE permission on folder root"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ_WRITE, false, true, false, "Failed to set delegated READ_WRITE permission on folder root"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.WRITE_EXECUTE, false, true, true, "Failed to set delegated WRITE_EXECUTE permission on folder root"},
			
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.ALL, true, true, false, "Failed to set delegated ALL permission on folder recursively"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.EXECUTE, true, true, true, "Failed to set delegated EXECUTE permission on folder recursively"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ, true, true, false, "Failed to set delegated READ permission on folder recursively"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.WRITE, true, true, false, "Failed to set delegated WRITE permission on folder recursively"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ_EXECUTE, true, true, true, "Failed to set delegated READ_EXECUTE permission on folder recursively"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ_WRITE, true, true, false, "Failed to set delegated READ_WRITE permission on folder recursively"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.WRITE_EXECUTE, true, true, true, "Failed to set delegated WRITE_EXECUTE permission on folder recursively"},
			
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.ALL, false, true, false, "Failed to set delegated ALL permission on file"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.EXECUTE, false, true, true, "Failed to set delegated EXECUTE permission on file"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.READ, false, true, false, "Failed to set delegated READ permission on file"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.WRITE, false, true, false, "Failed to set delegated WRITE permission on file"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.READ_EXECUTE, false, false, true, "Failed to set delegated READ_EXECUTE permission on file"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.READ_WRITE, false, true, false, "Failed to set delegated READ_WRITE permission on file"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.WRITE_EXECUTE, false, true, true, "Failed to set delegated WRITE_EXECUTE permission on file"},
		};
	}
	
	@DataProvider(name="setPermissionForUserProvider")
	public Object[][] setPermissionForUserProvider(Method m)
	{
		return new Object[][] {
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.ALL, false, true, false, "Owner should not be able to change their ownership to ALL permission on folder root"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.EXECUTE, false, false, true, "Owner should not be able to change their ownership toEXECUTE permission on folder root"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ, false, false, false, "Owner should not be able to change their ownership toREAD permission on folder root"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.WRITE, false, false, false, "Owner should not be able to change their ownership toWRITE permission on folder root"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ_EXECUTE, false, false, true, "Owner should not be able to change their ownership toREAD_EXECUTE permission on folder root"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ_WRITE, false, false, false, "Owner should not be able to change their ownership toREAD_WRITE permission on folder root"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.WRITE_EXECUTE, false, false, true, "Owner should not be able to change their ownership toWRITE_EXECUTE permission on folder root"},
			
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.ALL, false, true, false, "Owner should not be able to change their ownership toALL permission on file"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.EXECUTE, false, false, true, "Owner should not be able to change their ownership to EXECUTE permission on file"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.READ, false, false, false, "Owner should not be able to change their ownership to READ permission on file"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.WRITE, false, false, false, "Owner should not be able to change their ownership to WRITE permission on file"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.READ_EXECUTE, false, false, true, "Owner should not be able to change their ownership to READ_EXECUTE permission on file"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.READ_WRITE, false, false, false, "Owner should not be able to change their ownership to READ_WRITE permission on file"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.WRITE_EXECUTE, false, false, true, "Owner should not be able to change their ownership to WRITE_EXECUTE permission on file"},
			
		};
	}
	
	
	@Override
	@DataProvider(name="basePermissionProvider")
	public Object[][] basePermissionProvider(Method m)
	{
		return new Object[][] {
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, false, true, "Failed to set delegated permission on folder root"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, true, true, "Failed to set delegated permission on folder recursively"},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, false, true, "Failed to set delegated permission on file"},
			
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, false, false, "Owner should not be able to change their ownership on folder root"},
			// Weird behavior. All recursive permissions get "modify" pem, which translates to writeonly, so this fails every time.
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, true, false, "Owner should not be able to change their ownership on folder recursively"},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, false, false, "Owner should not be able to change their ownership on file"},
		};
	}
	
	@DataProvider(name="setExecutePermissionProvider")
	public Object[][] setExecutePermissionProvider(Method m)
	{
		return new Object[][] {
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, false, false, "Delegated execute permissions should not supported on IRODS."},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, true, false, "Delegated execute permissions should not supported on IRODS."},
			{SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, false, false, "Delegated execute permissions should not supported on IRODS."},
			
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, false, false, "Execute permissions for owner should not supported on IRODS."},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, true, false, "Execute permissions for owner should not supported on IRODS."},
			{SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, false, false, "Execute permissions for owner should not supported on IRODS."},
			
		};
	}

	@DataProvider(name="removeReadPermissionProvider")
    public Object[][] removeReadPermissionProvider(Method m)
    {
        return new Object[][] {
            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, false, true, "Failed to remove delegated permission on folder root"},
            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, true, true, "Failed to remove delegated permission on folder recursively"},
            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, false, true, "Failed to remove delegated permission on file"},
            
            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, false, true, "Owner should be able to remove their own permissions on folder root"},
            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, true, true, "Owner should be able to remove their ownpermission on folder recursively"},
            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, false, true, "Owner should be able to remove their own permission on file"},
        };
    }
	
	@DataProvider(name="removeWritePermissionProvider")
    public Object[][] removeWritePermissionProvider(Method m)
    {
        return removeReadPermissionProvider(m);
    }
	
	@DataProvider(name="clearPermissionProvider")
    public Object[][] clearPermissionProvider(Method m)
    {
        return new Object[][] {
//            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.ALL, false, true, "Failed to delete delegated ALL permission on folder root"},
////            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.EXECUTE, false, true, "Failed to delete delegated EXECUTE permission on folder root"},
//            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ, false, true, "Failed to delete delegated READ permission on folder root"},
//            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.WRITE, false, true, "Failed to delete delegated WRITE permission on folder root"},
////            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ_EXECUTE, false, true, "Failed to delete delegated READ_EXECUTE permission on folder root"},
//            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ_WRITE, false, true, "Failed to delete delegated READ_WRITE permission on folder root"},
////            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.WRITE_EXECUTE, false, true, "Failed to delete delegated WRITE_EXECUTE permission on folder root"},
//            
//            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.ALL, true, true, "Failed to delete delegated ALL permission on folder recursively"},
////            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.EXECUTE, true, true, "Failed to delete delegated EXECUTE permission on folder recursively"},
//            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ, true, true, "Failed to delete delegated READ permission on folder recursively"},
//            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.WRITE, true, true, "Failed to delete delegated WRITE permission on folder recursively"},
////            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ_EXECUTE, true, true, "Failed to delete delegated READ_EXECUTE permission on folder recursively"},
//            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ_WRITE, true, true, "Failed to delete delegated READ_WRITE permission on folder recursively"},
////            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.WRITE_EXECUTE, true, true, "Failed to delete delegated WRITE_EXECUTE permission on folder recursively"},
//            
//            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.ALL, false, true, "Failed to delete delegated ALL permission on file"},
////            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.EXECUTE, false, true, "Failed to delete delegated EXECUTE permission on file"},
//            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.READ, false, true, "Failed to delete delegated READ permission on file"},
//            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.WRITE, false, true, "Failed to delete delegated WRITE permission on file"},
////            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.READ_EXECUTE, false, true, "Failed to delete delegated READ_EXECUTE permission on file"},
//            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.READ_WRITE, false, true, "Failed to delete delegated READ_WRITE permission on file"},
////            {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.WRITE_EXECUTE, false, true, "Failed to delete delegated WRITE_EXECUTE permission on file"},
//            
            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.ALL, false, true, "Owner should be able to clear their ownership after adding ALL permission on folder root"},
//            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.EXECUTE, false, true, "Owner should not be able to clear their ownership after adding EXECUTE permission on folder root"},
            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ, false, true, "Owner should be able to clear their ownership after adding READ permission on folder root"},
            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.WRITE, false, true, "Owner should be able to clear their ownership after adding WRITE permission on folder root"},
//            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ_EXECUTE, false, true, "Owner should not be able to clear their ownership after adding READ_EXECUTE permission on folder root"},
            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.READ_WRITE, false, true, "Owner should be able to clear their ownership after adding READ_WRITE permission on folder root"},
//            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, PermissionType.WRITE_EXECUTE, false, true, "Owner should not be able to clear their ownership after adding WRITE_EXECUTE permission on folder root"},
            
            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.ALL, false, true, "Owner should be able to clear their ownership after adding ALL permission on file"},
//            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.EXECUTE, false, false, "Owner should not be able to clear their ownership after adding EXECUTE permission on file"},
            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.READ, false, true, "Owner should be able to clear their ownership after adding READ permission on file"},
            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.WRITE, false, true, "Owner should be able to clear their ownership after adding WRITE permission on file"},
//            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.READ_EXECUTE, false, false, "Owner should not be able to clear their ownership after adding READ_EXECUTE permission on file"},
            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.READ_WRITE, false, true, "Owner should be able to clear their ownership after adding READ_WRITE permission on file"},
//            {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, PermissionType.WRITE_EXECUTE, false, false, "Owner should not be able to clear their ownership after adding WRITE_EXECUTE permission on file"},

        };
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

	@Test(groups= {"permissions", "set"}, dataProvider="setReadPermissionProvider")
	public void setReadPermission(String username, String path, boolean recursive, boolean shouldSetPermission, String errorMessage)
			throws Exception {
		_setReadPermission(username, path, recursive, shouldSetPermission, errorMessage);
	}

	@Test(groups= {"permissions", "set"}, dataProvider="setWritePermissionProvider")
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
