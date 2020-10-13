package org.iplantc.service.systems.manager;

import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.uuid.UniqueId;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.dao.SystemRoleDao;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.iplantc.service.systems.model.*;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Test(groups={"integration"})
public class SystemRoleManagerIT extends PersistedSystemsModelTestCommon {
    private SystemDao dao = new SystemDao();
    
    @Override
    @BeforeClass
    public void beforeClass() throws Exception {
        jtd = JSONTestDataUtil.getInstance();
    }
    
    @BeforeMethod
    public void beforeMethod() throws Exception {
        clearSystems();
    }
    
    @AfterMethod
    public void afterMethod() throws Exception {
        clearSystems();
    }
    
    private StorageSystem getStorageSystem(boolean publik)
    throws SystemArgumentException, JSONException, IOException 
    {
        StorageSystem system = StorageSystem.fromJSON( jtd.getTestDataObject(
                JSONTestDataUtil.TEST_STORAGE_SYSTEM_FILE));
        system.setOwner(SYSTEM_OWNER);
        system.setPubliclyAvailable(true);
        system.setGlobalDefault(true);
        system.setSystemId(new UniqueId().getStringId());
        return system;
//        return getStorageSystem(publik, null);
    }
    
//    private StorageSystem getStorageSystem(boolean publik, SystemRole[] initialRoles)
//    throws SystemArgumentException, JSONException, IOException
//    {
//        StorageSystem system = StorageSystem.fromJSON( jtd.getTestDataObject(
//                JSONTestDataUtil.TEST_STORAGE_SYSTEM_FILE));
//        system.setOwner(SYSTEM_OWNER);
//        system.setPubliclyAvailable(true);
//        system.setGlobalDefault(true);
//        system.setSystemId(new UniqueId().getStringId());
//
//        if (initialRoles != null) {
//            for (SystemRole role: initialRoles) {
//                system.addRole(role.clone());
//            }
//        }
//
//        return system;
//    }
    
    private ExecutionSystem getExecutionSystem(boolean publik)
    throws SystemArgumentException, JSONException, IOException 
    {
        ExecutionSystem system = ExecutionSystem.fromJSON( jtd.getTestDataObject(
                JSONTestDataUtil.TEST_EXECUTION_SYSTEM_FILE).put("id",new UniqueId().getStringId()));
        system.setOwner(SYSTEM_OWNER);
        system.setPubliclyAvailable(true);
        system.setGlobalDefault(true);
        return system;
//        return getExecutionSystem(publik, null);
    }
    
//    private ExecutionSystem getExecutionSystem(boolean publik, SystemRole[] initialRoles)
//    throws SystemArgumentException, JSONException, IOException
//    {
//        ExecutionSystem system = ExecutionSystem.fromJSON( jtd.getTestDataObject(
//            JSONTestDataUtil.TEST_EXECUTION_SYSTEM_FILE).put("id",new UniqueId().getStringId()));
//        system.setOwner(SYSTEM_OWNER);
//        system.setPubliclyAvailable(true);
//        system.setGlobalDefault(true);
//
//        if (initialRoles != null) {
//            for (SystemRole role: initialRoles) {
//                system.addRole(role);
//            }
//        }
//
//        return system;
//    }
    
    @DataProvider
    public Object[][] clearRolesProvider() throws Exception {

        return new Object[][] {
                {getStorageSystem(true), new SystemRole[]{ new SystemRole(SYSTEM_SHARE_USER, RoleType.USER), new SystemRole(SYSTEM_UNSHARED_USER, RoleType.USER) }, false, "System roles were not cleared on public storage system"},
                {getStorageSystem(false), new SystemRole[]{ new SystemRole(SYSTEM_SHARE_USER, RoleType.USER), new SystemRole(SYSTEM_UNSHARED_USER, RoleType.USER) }, false, "System roles were not cleared on private storage system"},
                {getExecutionSystem(true), new SystemRole[]{ new SystemRole(SYSTEM_SHARE_USER, RoleType.USER), new SystemRole(SYSTEM_UNSHARED_USER, RoleType.USER) }, false, "System roles were not cleared on public execution system"},
                {getExecutionSystem(false), new SystemRole[]{ new SystemRole(SYSTEM_SHARE_USER, RoleType.USER), new SystemRole(SYSTEM_UNSHARED_USER, RoleType.USER) }, false, "System roles were not cleared on private execution system"},
        };
    }

    @Test(dataProvider="clearRolesProvider")
    public void clearRoles(RemoteSystem system, SystemRole[] roles, boolean shouldThrowException, String message) {
        
        try {
            dao.persist(system);
            for (SystemRole role: roles) {
                system.addRole(role);
            }
            dao.persist(system);

            SystemRoleManager manager = new SystemRoleManager(system);
            manager.clearRoles(system.getOwner());
            
            RemoteSystem clearedSystem = new SystemDao().findActiveAndInactiveSystemBySystemId(system.getSystemId());
            Assert.assertNotNull(clearedSystem, "Unable to fetch system by id after clearing roles.");
            Assert.assertTrue(clearedSystem.getRoles().isEmpty(), "System roles should be empty after clearing.");

            // Confirm the same is true when querying from the manager
            manager = new SystemRoleManager(clearedSystem);
            List<SystemRole> updatedRolesFromManager = manager.getRoles(9999, 0);
            Assert.assertNotNull(updatedRolesFromManager, "Null should not be returned from SystemRoleManager#getRoles(int, int).");
            Assert.assertEquals(updatedRolesFromManager.size(), 1, "SystemRoleManager#getRoles(int, int) should only return one role role after clearing roles.");
            Assert.assertEquals(updatedRolesFromManager.get(0).getRole(), RoleType.OWNER, "SystemRoleManager#getRoles(int, int) should only return the owner role after clearing roles");
            Assert.assertEquals(updatedRolesFromManager.get(0).getUsername(), system.getOwner(), "SystemRoleManager#getRoles(int, int) should only return a role belonging to the system owner after clearing roles");
        } 
        catch (Exception e) {
            if (!shouldThrowException) {
                Assert.fail("Clearing permissions on system should not throw exceptions",e);
            }
        }
        finally {
            try { dao.remove(system); } catch (Exception ignored) {}
        }
    }
    
    @DataProvider
    public Object[][] setRoleFailsOnPublicSystemsProvider() throws Exception {
        return new Object[][] {
//            { getStorageSystem(true), TENANT_ADMIN, false, "Setting role on public storage system should not throw exception for a tenant admin." },
            { getStorageSystem(true), SYSTEM_OWNER, true, "Setting role on public storage system should throw exception for the system owner." },
            { getStorageSystem(true), SYSTEM_SHARE_USER, true, "Setting role on public storage system should throw exception for a system shared user." },
            { getStorageSystem(true), SYSTEM_PUBLIC_USER, true, "Setting role on public storage system should throw exception for a public user." },
            { getStorageSystem(true), SYSTEM_UNSHARED_USER, true, "Setting role on public storage system should throw exception for a random user." },

//            { getExecutionSystem(true), TENANT_ADMIN, false, "Setting role on public execution system should not throw exception a tenant admin." },
            { getExecutionSystem(true), SYSTEM_OWNER, true, "Setting role on public execution system should throw exception for the system owner." },
            { getExecutionSystem(true), SYSTEM_SHARE_USER, true, "Setting role on public execution system should throw exception for a system shared user." },
            { getExecutionSystem(true), SYSTEM_PUBLIC_USER, true, "Setting role on public execution system should throw exception for a public user." },
            { getExecutionSystem(true), SYSTEM_UNSHARED_USER, true, "Setting role on public execution system should throw exception for a random user." },
        };
    }

    @Test(dataProvider="setRoleFailsOnPublicSystemsProvider")
    public void setRoleFailsOnPublicSystems(RemoteSystem system, String requestingUser, boolean shouldThrowException, String message) {
        try {
            dao.persist(system);
            TenancyHelper.setCurrentEndUser(requestingUser);

            for (RoleType role: RoleType.values()) {
                SystemRoleManager manager = new SystemRoleManager(system);
                manager.setRole(SYSTEM_UNSHARED_USER, role, system.getOwner());
            }
        }
        catch (Exception e) {
            if (!shouldThrowException) {
                Assert.fail(message,e);
            }
        }
        finally {
            try { dao.remove(system); } catch (Exception ignored) {}
        }
    }
    
    @DataProvider
    public Object[][] setPublisherRoleFailsOnStorageSystemProvider() throws Exception {
        
        return new Object[][] {
            { getStorageSystem(false), null, TENANT_ADMIN, "Setting PUBLISHER role on private storage system should throw exception for a tenant admin." },
            { getStorageSystem(false), null, SYSTEM_OWNER, "Setting PUBLISHER role on private storage system should throw exception for a tenant admin." },
            { getStorageSystem(false), new SystemRole[]{ new SystemRole(SYSTEM_SHARE_USER, RoleType.PUBLISHER)}, SYSTEM_SHARE_USER, "Setting PUBLISHER role on private storage system should throw exception for a tenant admin." },
            
            { getStorageSystem(true), null, TENANT_ADMIN, "Setting PUBLISHER role on private storage system should throw exception for a tenant admin." },
            { getStorageSystem(true), null, SYSTEM_OWNER, "Setting PUBLISHER role on private storage system should throw exception for a tenant admin." },
            { getStorageSystem(true), new SystemRole[]{ new SystemRole(SYSTEM_SHARE_USER, RoleType.PUBLISHER)}, SYSTEM_SHARE_USER, "Setting PUBLISHER role on private storage system should throw exception for a tenant admin." },
        };
    }
    
    @Test(dependsOnMethods={"clearRoles"}, dataProvider="setPublisherRoleFailsOnStorageSystemProvider")
    public void setPublisherRoleFailsOnStorageSystem(RemoteSystem system, SystemRole[] roles, String requestingUser, String message) {
        try {
            dao.persist(system);
            if (roles != null) {
                for (SystemRole role : roles) {
                    system.addRole(role);
                }
                dao.persist(system);
            }

            TenancyHelper.setCurrentEndUser(requestingUser);
            
            SystemRoleManager manager = new SystemRoleManager(system);
            
            manager.setRole(SYSTEM_SHARE_USER, RoleType.PUBLISHER, requestingUser);
            
            Assert.fail(message);
        } 
        catch (Exception ignored) {
            
        }
        finally {
            try { dao.remove(system); } catch (Exception ignored) {}
        }
    }
    
    @DataProvider
    public Object[][] setRoleDoesNotChangeExistingOwnerRoleProvider() throws Exception {
        List<Object[]> testCases = new ArrayList<Object[]>();
        
        for (RoleType roleType: RoleType.values()) {
            if (roleType != RoleType.OWNER) {
                if (roleType != RoleType.PUBLISHER) {
                    testCases.add(new Object[]{getStorageSystem(true), TENANT_ADMIN, SYSTEM_OWNER, roleType, RoleType.OWNER, false, "setRole should not alter owner permission on public storage system"});
                    testCases.add(new Object[]{getStorageSystem(false), TENANT_ADMIN, SYSTEM_OWNER, roleType, RoleType.OWNER, false, "setRole should not alter owner permission on private storage system"});
                }
                testCases.add(new Object[]{getExecutionSystem(true), TENANT_ADMIN, SYSTEM_OWNER, roleType, RoleType.OWNER, false, "setRole should not alter owner permission on public execution system"});
                testCases.add(new Object[]{getExecutionSystem(false), TENANT_ADMIN, SYSTEM_OWNER, roleType, RoleType.OWNER, false, "setRole should not alter owner permission on private execution system"});
            }
        };
        
        return testCases.toArray(new Object[][] {});
    }
    
    @Test(dependsOnMethods={"setPublisherRoleFailsOnStorageSystem"}, dataProvider="setRoleDoesNotChangeExistingOwnerRoleProvider")
    public void setRoleDoesNotChangeExistingOwnerRole(RemoteSystem system, String requestingUser, String roleUsername, RoleType roleType, RoleType expectedRoleType, boolean shouldThrowException, String message) {
        try {
            system.addRole(new SystemRole(roleUsername, roleType));
            dao.persist(system);
            
            TenancyHelper.setCurrentEndUser(requestingUser);
            
            SystemRoleManager manager = new SystemRoleManager(system);
            manager.setRole(roleUsername, roleType, requestingUser);
            
            RemoteSystem updatedSystem = dao.findById(system.getId());
            SystemRole udpatedRole = updatedSystem.getUserRole(roleUsername);
            
            Assert.assertNotNull(udpatedRole, "No user role retured after adding to system.");
            Assert.assertEquals(udpatedRole.getUsername(), roleUsername, "getUserRole returned role for incorrect user after adding to system.");
            Assert.assertEquals(udpatedRole.getRole(), expectedRoleType, message);         
        } 
        catch (Exception e) {
            Assert.fail("Updating owner role should not throw exception.", e);
        }
        finally {
            try { dao.remove(system); } catch (Exception ignored) {}
        }
    }
    
    @DataProvider
    public Object[][] setRoleDoesNotChangeExistingSuperAdminRoleProvider() throws Exception {
        List<Object[]> testCases = new ArrayList<Object[]>();
        
        for (RoleType roleType: RoleType.values()) {
            if (roleType != RoleType.ADMIN && roleType != RoleType.NONE) {
                if (roleType != RoleType.PUBLISHER) {
                    testCases.add(new Object[]{getStorageSystem(true), TENANT_ADMIN, TENANT_ADMIN, roleType, RoleType.ADMIN, false, "setRole should not alter tenant admin permission on public storage system"});
                    testCases.add(new Object[]{getStorageSystem(false), TENANT_ADMIN, TENANT_ADMIN, roleType, RoleType.ADMIN, false, "setRole should not alter tenant admin permission on private storage system"});
                }
                testCases.add(new Object[]{getExecutionSystem(true), TENANT_ADMIN, TENANT_ADMIN, roleType, RoleType.ADMIN, false, "setRole should not alter tenant admin permission on public execution system"});
                testCases.add(new Object[]{getExecutionSystem(false), TENANT_ADMIN, TENANT_ADMIN, roleType, RoleType.ADMIN, false, "setRole should not alter tenant admin permission on private execution system"});
            }
        };
        
        return testCases.toArray(new Object[][] {});
    }

//    @Test(dependsOnMethods={"setRoleDoesNotChangeExistingOwnerRole"}, dataProvider="setRoleDoesNotChangeExistingSuperAdminRoleProvider")
    @Test(dataProvider="setRoleDoesNotChangeExistingSuperAdminRoleProvider")
    public void setRoleDoesNotChangeExistingSuperAdminRole(RemoteSystem system, String requestingUser, String roleUsername, RoleType roleType, RoleType expectedRoleType, boolean shouldThrowException, String message) {
        try {
            system.addRole(new SystemRole(roleUsername, roleType));
            dao.persist(system);
            
            TenancyHelper.setCurrentEndUser(requestingUser);
            
            SystemRoleManager manager = new SystemRoleManager(system);
            manager.setRole(roleUsername, roleType, requestingUser);
            
            RemoteSystem updatedSystem = dao.findById(system.getId());
            SystemRole udpatedRole = updatedSystem.getUserRole(roleUsername);
            
            Assert.assertNotNull(udpatedRole, "No user role retured after adding to system.");
            Assert.assertEquals(udpatedRole.getUsername(), roleUsername, "getUserRole returned role for incorrect user after adding to system.");
            Assert.assertEquals(udpatedRole.getRole(), expectedRoleType, message);         
        } 
        catch (Exception e) {
            Assert.fail("Updating owner role should not throw exception.", e);
        }
        finally {
            try { dao.remove(system); } catch (Exception ignored) {}
        }
    }
    
//    @Test
//    public void setRoleNoneRemovesExistingRole(RemoteSystem system, boolean shouldThrowException, String message) {
//        throw new RuntimeException("Test not implemented");
//    }
//    @Test
//    public void setRoleDoesNotChangeExistingDuplicateRole(RemoteSystem system, boolean shouldThrowException, String message) {
//        throw new RuntimeException("Test not implemented");
//    }
    
    @DataProvider
    public Object[][] getRolesSetByTenantAdminProvider() throws Exception {
        
        return new Object[][] {
                {getStorageSystem(true), TENANT_ADMIN, TENANT_ADMIN, RoleType.OWNER, RoleType.ADMIN, false, "setRole should not throw exception when an owner permission is applied"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_SHARE_USER, RoleType.OWNER, RoleType.OWNER, false, "setRole should not throw exception when an owner permission is applied"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_UNSHARED_USER , RoleType.OWNER, RoleType.OWNER, false, "setRole should not throw exception when an owner permission is applied"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_PUBLIC_USER, RoleType.OWNER, RoleType.OWNER, false, "setRole should not throw exception when an owner permission is applied"},
                
                {getStorageSystem(true), TENANT_ADMIN, TENANT_ADMIN, RoleType.ADMIN, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets an admin permission on a public system"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_SHARE_USER, RoleType.ADMIN, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets an admin permission on a public system"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_UNSHARED_USER, RoleType.ADMIN, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets an admin permission on a public system"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_PUBLIC_USER, RoleType.ADMIN, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets an admin permission on a public system"},
                
//                {getStorageSystem(true), TENANT_ADMIN, TENANT_ADMIN, RoleType.PUBLISHER, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets an publisher permission on a public system"},
//                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_OWNER, RoleType.PUBLISHER, RoleType.OWNER, false, "setRole should not throw exception when a tenant admin sets an publisher permission on a public system"},
//                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_SHARE_USER, RoleType.PUBLISHER, RoleType.PUBLISHER, false, "setRole should not throw exception when a tenant admin sets an publisher permission on a public system"},
//                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_UNSHARED_USER, RoleType.PUBLISHER, RoleType.PUBLISHER, false, "setRole should not throw exception when a tenant admin sets an publisher permission on a public system"},
//                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_PUBLIC_USER, RoleType.PUBLISHER, RoleType.PUBLISHER, false, "setRole should not throw exception when a tenant admin sets an publisher permission on a public system"},
//                
                {getStorageSystem(true), TENANT_ADMIN, TENANT_ADMIN, RoleType.USER, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets an user permission on a public system"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_SHARE_USER, RoleType.USER, RoleType.USER, false, "setRole should not throw exception when a tenant admin sets an user permission on a public system"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_UNSHARED_USER, RoleType.USER, RoleType.USER, false, "setRole should not throw exception when a tenant admin sets an user permission on a public system"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_PUBLIC_USER, RoleType.USER, RoleType.USER, false, "setRole should not throw exception when a tenant admin sets an user user on a public system"},
                
                {getStorageSystem(true), TENANT_ADMIN, TENANT_ADMIN, RoleType.GUEST, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_SHARE_USER, RoleType.GUEST, RoleType.GUEST, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_UNSHARED_USER, RoleType.GUEST, RoleType.GUEST, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_PUBLIC_USER, RoleType.GUEST, RoleType.GUEST, false, "setRole should not throw exception when a tenant admin sets a guest user on a public system"},
                
                {getStorageSystem(true), TENANT_ADMIN, TENANT_ADMIN, RoleType.NONE, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_SHARE_USER, RoleType.NONE, RoleType.USER, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_UNSHARED_USER, RoleType.NONE, RoleType.USER, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getStorageSystem(true), TENANT_ADMIN, SYSTEM_PUBLIC_USER, RoleType.NONE, RoleType.USER, false, "setRole should not throw exception when a tenant admin sets a guest user on a public system"},
                
                // execution tests
                {getExecutionSystem(true), TENANT_ADMIN, TENANT_ADMIN, RoleType.OWNER, RoleType.ADMIN, false, "setRole should not throw exception when an owner permission is applied"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_SHARE_USER, RoleType.OWNER, RoleType.OWNER, false, "setRole should not throw exception when an owner permission is applied"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_UNSHARED_USER , RoleType.OWNER, RoleType.OWNER, false, "setRole should not throw exception when an owner permission is applied"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_PUBLIC_USER, RoleType.OWNER, RoleType.OWNER, false, "setRole should not throw exception when an owner permission is applied"},
                
                {getExecutionSystem(true), TENANT_ADMIN, TENANT_ADMIN, RoleType.ADMIN, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets an admin permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_OWNER, RoleType.ADMIN, RoleType.OWNER, false, "setRole should not throw exception when a tenant admin sets an admin permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_SHARE_USER, RoleType.ADMIN, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets an admin permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_UNSHARED_USER, RoleType.ADMIN, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets an admin permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_PUBLIC_USER, RoleType.ADMIN, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets an admin permission on a public system"},
                
                {getExecutionSystem(true), TENANT_ADMIN, TENANT_ADMIN, RoleType.PUBLISHER, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets an publisher permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_OWNER, RoleType.PUBLISHER, RoleType.OWNER, false, "setRole should not throw exception when a tenant admin sets an publisher permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_SHARE_USER, RoleType.PUBLISHER, RoleType.PUBLISHER, false, "setRole should not throw exception when a tenant admin sets an publisher permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_UNSHARED_USER, RoleType.PUBLISHER, RoleType.PUBLISHER, false, "setRole should not throw exception when a tenant admin sets an publisher permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_PUBLIC_USER, RoleType.PUBLISHER, RoleType.PUBLISHER, false, "setRole should not throw exception when a tenant admin sets an publisher permission on a public system"},
                
                {getExecutionSystem(true), TENANT_ADMIN, TENANT_ADMIN, RoleType.USER, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets an user permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_OWNER, RoleType.USER, RoleType.OWNER, false, "setRole should not throw exception when a tenant admin sets an user permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_SHARE_USER, RoleType.USER, RoleType.USER, false, "setRole should not throw exception when a tenant admin sets an user permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_UNSHARED_USER, RoleType.USER, RoleType.USER, false, "setRole should not throw exception when a tenant admin sets an user permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_PUBLIC_USER, RoleType.USER, RoleType.USER, false, "setRole should not throw exception when a tenant admin sets an user user on a public system"},
                
                {getExecutionSystem(true), TENANT_ADMIN, TENANT_ADMIN, RoleType.GUEST, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_OWNER, RoleType.GUEST, RoleType.OWNER, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_SHARE_USER, RoleType.GUEST, RoleType.GUEST, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_UNSHARED_USER, RoleType.GUEST, RoleType.GUEST, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_PUBLIC_USER, RoleType.GUEST, RoleType.GUEST, false, "setRole should not throw exception when a tenant admin sets a guest user on a public system"},
                
                {getExecutionSystem(true), TENANT_ADMIN, TENANT_ADMIN, RoleType.NONE, RoleType.ADMIN, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_OWNER, RoleType.NONE, RoleType.OWNER, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_SHARE_USER, RoleType.NONE, RoleType.USER, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_UNSHARED_USER, RoleType.NONE, RoleType.USER, false, "setRole should not throw exception when a tenant admin sets a guest permission on a public system"},
                {getExecutionSystem(true), TENANT_ADMIN, SYSTEM_PUBLIC_USER, RoleType.NONE, RoleType.USER, false, "setRole should not throw exception when a tenant admin sets a guest user on a public system"},
                
        };
    }

    @Test(dependsOnMethods={"setRoleDoesNotChangeExistingSuperAdminRole"}, dataProvider="getRolesSetByTenantAdminProvider")
    public void setNewRoleForUser(RemoteSystem system, String requestingUser, String roleUsername, RoleType roleType, RoleType expectedRoleType, boolean shouldThrowException, String message) {
        
        try {
            dao.persist(system);
            TenancyHelper.setCurrentEndUser(requestingUser);
            
            SystemRoleManager manager = new SystemRoleManager(system);
            manager.setRole(roleUsername, roleType, requestingUser);

            RemoteSystem updatedSystem = dao.findById(system.getId());
            SystemRole udpatedRole = updatedSystem.getUserRole(roleUsername);
            
            Assert.assertNotNull(udpatedRole, "No user role retured after adding to system.");
            Assert.assertEquals(udpatedRole.getUsername(), roleUsername, "getUserRole returned role for incorrect user after adding to system.");
            Assert.assertEquals(udpatedRole.getRole(), expectedRoleType, "Incorrect user roleType retured after adding to system.");
        } 
        catch (Exception e) {
            if (!shouldThrowException) {
                Assert.fail(message, e);
            }
        }
        finally {
            try { dao.remove(system); } catch (Exception ignored) {}
        }
    }
    
    @Test(dependsOnMethods={"setNewRoleForUser"}, dataProvider="getRolesSetByTenantAdminProvider")
    public void setDuplicateRoleForUserReturnsSameRole(RemoteSystem system, String requestingUser, String roleUsername, RoleType roleType, RoleType expectedRoleType, boolean shouldThrowException, String message) {
        
        try {
            system.addRole(new SystemRole(roleUsername, roleType));
            dao.persist(system);
            
            TenancyHelper.setCurrentEndUser(requestingUser);
            
            SystemRoleManager manager = new SystemRoleManager(system);
            manager.setRole(roleUsername, roleType, requestingUser);
            
            RemoteSystem updatedSystem = dao.findById(system.getId());
            SystemRole udpatedRole = updatedSystem.getUserRole(roleUsername);

            Assert.assertNotNull(udpatedRole, "No user role retured after adding to system.");
            Assert.assertEquals(udpatedRole.getUsername(), roleUsername, "getUserRole returned role for incorrect user after adding to system.");
            Assert.assertEquals(udpatedRole.getRole(), expectedRoleType, "Incorrect user roleType retured after adding to system.");         
        } 
        catch (Exception e) {
            if (!shouldThrowException) {
                Assert.fail(message, e);
            }
        }
        finally {
            try { dao.remove(system); } catch (Exception ignored) {}
        }
    }

    @DataProvider
    protected Object[][] updateRoleForUserProvider() throws SystemArgumentException, JSONException, IOException {
        return new Object[][]{
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.GUEST, RoleType.GUEST, RoleType.GUEST},
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.GUEST, RoleType.USER, RoleType.USER},
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.GUEST, RoleType.ADMIN, RoleType.ADMIN},
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.GUEST, RoleType.OWNER, RoleType.OWNER},

                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.USER, RoleType.GUEST, RoleType.GUEST},
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.USER, RoleType.USER, RoleType.USER},
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.USER, RoleType.ADMIN, RoleType.ADMIN},
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.USER, RoleType.OWNER, RoleType.OWNER},

                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.ADMIN, RoleType.GUEST, RoleType.GUEST},
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.ADMIN, RoleType.USER, RoleType.USER},
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.ADMIN, RoleType.ADMIN, RoleType.ADMIN},
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.ADMIN, RoleType.OWNER, RoleType.OWNER},

                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.OWNER, RoleType.GUEST, RoleType.GUEST},
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.OWNER, RoleType.USER, RoleType.USER},
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.OWNER, RoleType.ADMIN, RoleType.ADMIN},
                {getStorageSystem(false), SYSTEM_OWNER, SYSTEM_SHARE_USER, RoleType.OWNER, RoleType.OWNER, RoleType.OWNER},
        };
    }

    @Test(dataProvider="updateRoleForUserProvider", dependsOnMethods={"setDuplicateRoleForUserReturnsSameRole"})
    public void updateRoleForUser(RemoteSystem system, String requestingUser, String roleUsername, RoleType originalRoleType, RoleType updatedRoleType, RoleType expectedRoleType) {

        try {
            system.addRole(new SystemRole(roleUsername, originalRoleType));
            dao.persist(system);

            TenancyHelper.setCurrentEndUser(requestingUser);

            // update the user role via the SystemRoleManager
            SystemRoleManager manager = new SystemRoleManager(system);
            manager.setRole(roleUsername, updatedRoleType, requestingUser);

            // Query the system to get the user role after updating by the manager
            RemoteSystem updatedSystem = dao.findById(system.getId());
            SystemRole updatedRole = updatedSystem.getUserRole(roleUsername);
            Assert.assertNotNull(updatedRole, "No user role found in system role set after manager update.");
            Assert.assertEquals(updatedRole.getUsername(), roleUsername, "getUserRole returned role for incorrect user after manager update.");
            Assert.assertEquals(updatedRole.getRole(), expectedRoleType, "Incorrect user roleType found in system role set after manager update.");

            // Confirm the same is true when querying from the manager
            manager = new SystemRoleManager(updatedSystem);
            SystemRole updatedRoleFromManager = manager.getUserRole(roleUsername);
            Assert.assertNotNull(updatedRoleFromManager, "No user role returned from SystemRoleManager#dRoleFromManager() after manager update.");
            Assert.assertEquals(updatedRoleFromManager.getUsername(), roleUsername, "SystemRoleManager#dRoleFromManager() returned role for incorrect user after manager update.");
            Assert.assertEquals(updatedRoleFromManager.getRole(), expectedRoleType, "Incorrect user roleType returned from SystemRoleManager#dRoleFromManager() after manager update.");

            // Confirm object is present in db
            SystemRole updatedRoleFromDao = SystemRoleDao.getSystemRoleForUser(roleUsername, system.getSystemId());
            Assert.assertNotNull(updatedRoleFromManager, "No user role returned from SystemRoleDao.getSystemRoleForUser(String, String) after manager update.");
            Assert.assertEquals(updatedRoleFromManager.getUsername(), roleUsername, "SystemRoleDao.getSystemRoleForUser(String, String) returned role for incorrect user after manager update.");
            Assert.assertEquals(updatedRoleFromManager.getRole(), expectedRoleType, "Incorrect user roleType returned from SystemRoleDao.getSystemRoleForUser(String, String) after manager update.");
        }
        catch (Exception e) {
            Assert.fail("System role update for user should not thorw exception.", e);
        }
        finally {
            try { dao.remove(system); } catch (Exception ignored) {}
        }
    }
}
