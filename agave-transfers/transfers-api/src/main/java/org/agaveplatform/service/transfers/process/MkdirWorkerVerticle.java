package org.agaveplatform.service.transfers.process;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.hibernate.HibernateException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

public class MkdirWorkerVerticle extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(MkdirWorkerVerticle.class);
    private String username;
    private String tenantId;
    private String logicalFileId;
    private String systemId;
    private String dirPath;
    private String path;

    public MkdirWorkerVerticle(Vertx vertx) {

    }

    @Override
    public void start() {
        EventBus bus = vertx.eventBus();
        bus.<JsonObject>consumer("mkdir", msg -> {
            JsonObject body = msg.body();
			String username = body.getString("owner");
            String tenantId = body.getString("tenant_id");
            String systemId = body.getString("system_id");
            String parentPath = body.getString("path");
            String dirPath = body.getString("dirPath");

            doMkdirOperation(systemId, parentPath, dirPath, username, tenantId, null);
        });
    }

    private void resultHandler(AsyncResult<String> ar) {
        if (ar.succeeded()) {
            log.info("Mkdir success: {}", ar.result());
        } else {
            log.error("Mkdir operation failed: {}", ar.cause());
        }
    }

    protected void doMkdirOperation(String systemId, String parentPath, String dirPath, String tenantId, String username, String internalUsername) {
        String newPath;
        RemoteSystem system;
        RemoteDataClient remoteDataClient;
        try {
            TenancyHelper.setCurrentTenantId(tenantId);
            TenancyHelper.setCurrentEndUser(username);

            // resolve the new directory path with it's parent directory path
            newPath = Paths.get(parentPath, dirPath).toString();

            // find the system for which the directory should be created
            system = new SystemDao().findBySystemId(systemId);

            if (system == null) {
                log.error("No system found with id: " + systemId);
            } else {
                // get a handle on a remote data client to create the remote directory
                remoteDataClient = new RemoteDataClientFactory().getInstance(system, internalUsername);
                remoteDataClient.mkdirs(newPath);
            }
        } catch (RemoteDataException|RemoteCredentialException|IOException e) {
            log.error(e.getMessage());
//            throw e;
        } catch (HibernateException e) {
            String msg = "An unexpected internal error occurred while processing the request. " +
                    "If this persists, please contact your tenant administrator";
            log.error(e.getMessage(), e);
//            throw new RemoteDataException(msg, e);
        }
    }
}
