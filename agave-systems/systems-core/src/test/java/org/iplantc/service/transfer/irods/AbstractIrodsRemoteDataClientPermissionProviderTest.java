package org.iplantc.service.transfer.irods;

import org.iplantc.service.transfer.IRemoteDataClientPermissionProviderTest;
import org.iplantc.service.transfer.RemoteDataClientPermissionProviderTest;
import org.iplantc.service.transfer.model.enumerations.PermissionType;
import org.testng.annotations.DataProvider;

import java.lang.reflect.Method;

public abstract class AbstractIrodsRemoteDataClientPermissionProviderTest extends RemoteDataClientPermissionProviderTest implements IRemoteDataClientPermissionProviderTest {
    @DataProvider(name = "setPermissionForSharedUserProvider")
    public Object[][] setPermissionForSharedUserProvider(Method m) {
        return new Object[][]{
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

    @DataProvider(name = "setPermissionForUserProvider")
    public Object[][] setPermissionForUserProvider(Method m) {
        return new Object[][]{
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
    @DataProvider(name = "basePermissionProvider")
    public Object[][] basePermissionProvider(Method m) {
        return new Object[][]{
                {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, false, true, "Failed to set delegated permission on folder root"},
                {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, true, true, "Failed to set delegated permission on folder recursively"},
                {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, false, true, "Failed to set delegated permission on file"},

                {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, false, false, "Owner should not be able to change their ownership on folder root"},
                // Weird behavior. All recursive permissions get "modify" pem, which translates to writeonly, so this fails every time.
                {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, true, false, "Owner should not be able to change their ownership on folder recursively"},
                {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, false, false, "Owner should not be able to change their ownership on file"},
        };
    }

    @DataProvider(name = "setExecutePermissionProvider")
    public Object[][] setExecutePermissionProvider(Method m) {
        return new Object[][]{
                {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, false, false, "Delegated execute permissions should not supported on IRODS."},
                {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, true, false, "Delegated execute permissions should not supported on IRODS."},
                {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, false, false, "Delegated execute permissions should not supported on IRODS."},

                {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, false, false, "Execute permissions for owner should not supported on IRODS."},
                {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, true, false, "Execute permissions for owner should not supported on IRODS."},
                {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, false, false, "Execute permissions for owner should not supported on IRODS."},

        };
    }

    @DataProvider(name = "removeReadPermissionProvider")
    public Object[][] removeReadPermissionProvider(Method m) {
        return new Object[][]{
                {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, false, true, "Failed to remove delegated permission on folder root"},
                {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, true, true, "Failed to remove delegated permission on folder recursively"},
                {SHARED_SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, false, true, "Failed to remove delegated permission on file"},

                {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, false, true, "Owner should be able to remove their own permissions on folder root"},
                {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME, true, true, "Owner should be able to remove their ownpermission on folder recursively"},
                {SYSTEM_USER, m.getName() + "/" + LOCAL_DIR_NAME + "/" + LOCAL_BINARY_FILE_NAME, false, true, "Owner should be able to remove their own permission on file"},
        };
    }

    @DataProvider(name = "removeWritePermissionProvider")
    public Object[][] removeWritePermissionProvider(Method m) {
        return removeReadPermissionProvider(m);
    }

    @DataProvider(name = "clearPermissionProvider")
    public Object[][] clearPermissionProvider(Method m) {
        return new Object[][]{
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
}
