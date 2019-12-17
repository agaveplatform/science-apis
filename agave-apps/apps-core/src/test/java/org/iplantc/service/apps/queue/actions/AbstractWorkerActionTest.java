/**
 * 
 */
package org.iplantc.service.apps.queue.actions;

import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_EXECUTION_SYSTEM_FILE;
import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_OWNER;
import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_SOFTWARE_SYSTEM_FILE;
import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_STORAGE_SYSTEM_FILE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.apps.dao.AbstractDaoTest;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.queue.actions.WorkerAction;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.SystemRole;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Catchall setup class for {@link WorkerAction} test classes
 * @author dooley
 *
 */
public abstract class AbstractWorkerActionTest extends AbstractDaoTest {

    protected StorageSystem sharedStorageSystem;
    protected StorageSystem publicStorageSystem;
    protected ExecutionSystem sharedExecutionSystem;
    protected ExecutionSystem publicExecutionSystem;

    protected void initSystems() throws Exception
    {
        clearSystems();
        
        privateExecutionSystem = ExecutionSystem.fromJSON(jtd.getTestDataObject(TEST_EXECUTION_SYSTEM_FILE));
        privateExecutionSystem.setOwner(TEST_OWNER);
        systemDao.persist(privateExecutionSystem);
        
        privateStorageSystem = StorageSystem.fromJSON(jtd.getTestDataObject(TEST_STORAGE_SYSTEM_FILE));
        privateStorageSystem.setOwner(TEST_OWNER);
        privateStorageSystem.getUsersUsingAsDefault().add(TEST_OWNER);
        systemDao.persist(privateStorageSystem);

        publicExecutionSystem = ExecutionSystem.fromJSON( jtd.getTestDataObject(TEST_EXECUTION_SYSTEM_FILE)
                .put("id", privateExecutionSystem.getSystemId() + ".public") );
        publicExecutionSystem.setOwner(SYSTEM_OWNER);
        publicExecutionSystem.setPubliclyAvailable(true);
        publicExecutionSystem.setGlobalDefault(true);
        systemDao.persist(publicExecutionSystem);

        publicStorageSystem = StorageSystem.fromJSON(jtd.getTestDataObject(TEST_STORAGE_SYSTEM_FILE)
                .put("id", privateStorageSystem.getSystemId() + ".public") );
        publicStorageSystem.setOwner(SYSTEM_OWNER);
        publicStorageSystem.setPubliclyAvailable(true);
        publicStorageSystem.setGlobalDefault(true);
        systemDao.persist(publicStorageSystem);

        sharedExecutionSystem = ExecutionSystem.fromJSON( jtd.getTestDataObject(TEST_EXECUTION_SYSTEM_FILE)
                .put("id", privateExecutionSystem.getSystemId() + ".shared") );
        sharedExecutionSystem.setOwner(SYSTEM_OWNER);
        systemDao.persist(sharedExecutionSystem);
        sharedExecutionSystem.addRole(new SystemRole(SYSTEM_SHARE_USER, RoleType.ADMIN));
        systemDao.persist(sharedExecutionSystem);

        sharedStorageSystem = StorageSystem.fromJSON( jtd.getTestDataObject(TEST_STORAGE_SYSTEM_FILE)
                .put("id", privateStorageSystem.getSystemId() + ".shared") );
        sharedStorageSystem.setOwner(SYSTEM_OWNER);
        systemDao.persist(sharedStorageSystem);
        sharedStorageSystem.addRole(new SystemRole(SYSTEM_SHARE_USER, RoleType.ADMIN));
        systemDao.persist(sharedStorageSystem);
    }

    protected Software createSoftware() throws JSONException, IOException {
        JSONObject json = jtd.getTestDataObject(FORK_SOFTWARE_TEMPLATE_FILE);
        Software software = Software.fromJSON(json, TEST_OWNER);
        software.setExecutionSystem(privateExecutionSystem);
        software.setOwner(SYSTEM_OWNER);

        return software;
    }

    protected Software createSoftware(ExecutionSystem executionSystem, StorageSystem storageSystem, 
            String name, String version, boolean publiclyAvailable, String owner) 
    throws JSONException, IOException 
    {
        JSONObject json = jtd.getTestDataObject(FORK_SOFTWARE_TEMPLATE_FILE);
        software = Software.fromJSON(json, TEST_OWNER);
        software.setExecutionSystem(executionSystem);
        software.setStorageSystem(storageSystem);
        software.setName(name);
        software.setVersion(version);
        software.setPubliclyAvailable(publiclyAvailable);
        software.setOwner(owner);
        return software;
    }
    
    protected Software createExecutionSystem(boolean isPubliclyAvailable, boolean isGlobalDefault, 
            SystemStatusType status, boolean isAvailable, boolean owner)
    throws JSONException, IOException 
    {
        JSONObject json = jtd.getTestDataObject(TEST_SOFTWARE_SYSTEM_FILE);
        software = Software.fromJSON(json, TEST_OWNER);
        software.setExecutionSystem(privateExecutionSystem);
        software.setOwner(TEST_OWNER);
        software.setVersion("1.0.1");
        software.setChecksum("abc12345");
        return software;
    }

    /**
     * Stages {@link Software} deployment assets to the {@link StorageSystem}
     * @param software
     * @throws Exception
     */
    protected void stageRemoteSoftwareAssets(Software software) throws Exception 
    {
        RemoteSystem storageSystem = software.getStorageSystem();
        RemoteDataClient storageDataClient = null;
        try 
        {
            Path localSoftwareDeploymentDir = Paths.get(FORK_SOFTWARE_TEMPLATE_FILE).getParent();
            storageDataClient = storageSystem.getRemoteDataClient();
            storageDataClient.authenticate();

            String parentPath = FilenameUtils.getPath(software.getDeploymentPath());
            storageDataClient.mkdirs(parentPath);
            storageDataClient.put(localSoftwareDeploymentDir.toAbsolutePath().toString(), parentPath);

//            if (!storageDataClient.doesExist(software.getDeploymentPath())) {
//                String parentPath = FilenameUtils.getPath(software.getDeploymentPath());
//                storageDataClient.mkdirs(parentPath);
//                storageDataClient.put(localSoftwareDeploymentDir.toAbsolutePath().toString(), parentPath);
//            }
//            else
//            {
//                for (File localSoftwareAssetPath: localSoftwareDeploymentDir.toFile().listFiles()) {
//                    if (!storageDataClient.doesExist(software.getDeploymentPath() + File.separator + localSoftwareAssetPath.getName())) {
//                        storageDataClient.put(localSoftwareAssetPath.getAbsolutePath(), FilenameUtils.getPath(software.getDeploymentPath()) + File.separator + localSoftwareAssetPath.getName());
//                    }
//                }
//            }
        }
        finally {
            try {
                if (storageDataClient != null) {
                    storageDataClient.disconnect();
                }
            } catch (Exception ignored) {}
        }
        
    }

    /**
     * Cleans up data in the deploymentPath of the {@link StorageSystem} of the {@link Software}
     * @param software
     * @throws Exception
     */
    protected void deleteRemoteSoftwareAssets(Software software) throws Exception
    {
        RemoteSystem storageSystem = software.getStorageSystem();
        RemoteDataClient remoteDataClient = null;
        try 
        {
            // delete parent directory of deploymentPath if relative to homedir, which it should be
            String homeDir = StringUtils.trimToEmpty(storageSystem.getStorageConfig().getHomeDir());
            String deploymentDir = software.getDeploymentPath();

            remoteDataClient = storageSystem.getRemoteDataClient();
            remoteDataClient.authenticate();

            // public systems deployment dir is a file, no need to resolve paths there.
            // if an absolute path, just delete the folder
            if (software.isPubliclyAvailable() || deploymentDir.startsWith("/")) {
                remoteDataClient.delete(deploymentDir);
            }
            // delete parents of deployment path relative to home directory
            else {
                deploymentDir = deploymentDir.replace(homeDir, "");
                String[] tokens = deploymentDir.split("/");
                if (tokens.length > 0) {
                    remoteDataClient.delete(tokens[0]);
                }
            }
        }
        finally {
            try {
                if (remoteDataClient != null) {
                    remoteDataClient.disconnect();
                }
            } catch (Exception ignored) {}
        }
        
    }

}
