package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class DefaultTransferTaskAssignedListener extends AbstractVerticle {
    private final Logger logger = LoggerFactory.getLogger(DefaultTransferTaskAssignedListener.class);
    private String eventChannel = "transfertask.assigned.*.*.*.*";
//    private String eventChannel = "transfertask.*.get";

    public DefaultTransferTaskAssignedListener(Vertx vertx) {
        this(vertx, null);
    }

    public DefaultTransferTaskAssignedListener(Vertx vertx, String eventChannel) {
        super();
        setVertx(vertx);
        setEventChannel(eventChannel);
    }

    @Override
    public void start() {
        EventBus bus = vertx.eventBus();
        bus.<JsonObject>consumer(getEventChannel(), msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("id");
            String source = body.getString("source");
            String dest = body.getString("dest");
            logger.info("Transfer task {} created: {} -> {}", uuid, source, dest);
            this.processTransferTask(body);
        })
        ;
    }

    protected String processTransferTask(JsonObject body) {
        String uuid = body.getString("id");
        String source = body.getString("source");
//		String dest =  body.getString("dest");
        String username = body.getString("owner");
        String tenantId = body.getString("tenantId");
        String protocol = null;

        try {

            URI srcUri;
            try {
                srcUri = URI.create(source);
            } catch (Exception e) {
                String msg = String.format("Unable to parse source uri %s for transfertask %s: %s",
											source, uuid, e.getMessage());
                throw new RemoteDataSyntaxException(msg, e);
            }
            // basic sanity check on uri again
            if (RemoteDataClientFactory.isSchemeSupported(srcUri)) {
                // if it's an "agave://" uri, look up the connection info, get a rdc, and process the remote
                // file item
                if (srcUri.getScheme().equalsIgnoreCase("agave")) {
                    // pull the system out of the url. system id is the hostname in an agave uri
                    RemoteSystem srcSystem = new SystemDao().findBySystemId(srcUri.getHost());
                    // get a remtoe data client for the sytem
                    RemoteDataClient client = srcSystem.getRemoteDataClient();
                    // stat the remote path to check its type
                    RemoteFileInfo fileInfo = client.getFileInfo(srcUri.getPath());

                    // if the path is a file, then we can move it directly, so we raise an event telling the protocol
                    // listener to move the file item
                    if (fileInfo.isFile()) {
                        // write to the protocol event channel. the uri is all they should need for this....
                        // might need tenant id. not sure yet.
                        if (interruptEvent(uuid, source, username, tenantId)) {
                            vertx.eventBus().publish("transfertask." + srcSystem.getType() + ".get",
                                    "agave://" + srcSystem.getSystemId() + "/" + srcUri.getPath());
                        }
                    } else {
                        if (interruptEvent(uuid, source, username, tenantId)) {
                            // path is a directory, so walk the first level of the directory
                            client.ls(srcUri.getPath())
                                    .forEach(childFileItem -> {
                                        // if it's a file, we can process this as we would if the original path were a file
                                        if (childFileItem.isFile()) {
                                            vertx.eventBus().publish("transfertask." + srcSystem.getType() + ".get",
                                                    "agave://" + srcSystem.getSystemId() + "/" + srcUri.getPath() + "/" + childFileItem.getName());
                                        }
                                        // if a directory, then create a new transfer task to repeat this process,
                                        // keep the association between this transfer task, the original, and the children
                                        // in place for traversal in queries later on.
                                        else {
                                            // build the child paths
                                            String childSource = body.getString("source") + "/" + childFileItem.getName();
                                            String childDest = body.getString("dest") + "/" + childFileItem.getName();

                                            TransferTask transferTask = new TransferTask(childSource, childDest);
                                            transferTask.setTenantId(tenantId);
                                            transferTask.setOwner(username);
                                            transferTask.setParentTaskId(uuid);
                                            if (StringUtils.isNotEmpty(body.getString("rootTask"))) {
                                                transferTask.setRootTaskId(body.getString("rootTaskId"));
                                            }
                                            vertx.eventBus().publish("transfertask.created", transferTask.toJSON());
                                        }
                                    });
                        }
                    }
                } else {
                    if (interruptEvent(uuid, source, username, tenantId)) {
                        vertx.eventBus().publish("transfertask." + srcUri.getScheme() + ".get", source);
                    }
                }
            } else {
                String msg = String.format("Unknown source schema %s for the transfertask %s",
											srcUri.getScheme(), uuid);
                throw new RemoteDataSyntaxException(msg);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            JsonObject json = new JsonObject()
                    .put("cause", e.getClass().getName())
                    .put("message", e.getMessage())
                    .mergeIn(body);

            getVertx().eventBus().publish("transfertask.error", json);
        }

        return protocol;
    }

    public boolean interruptEvent( String uuid, String source, String username, String tenantId ){
        EventBus bus = vertx.eventBus();
        bus.consumer(getEventChannel());
        if ( bus.consumer("paused." + tenantId +"." + username + "." + uuid ).isRegistered()) {
            logger.info("Transfer task paused {} created: {} -> {}", tenantId, uuid, source);
            return true;
        }
        return false;
    }

    /**
     * Sets the vertx instance for this listener
     *
     * @param vertx the current instance of vertx
     */
    private void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }

    /**
     * @return the message type to listen to
     */
    public String getEventChannel() {
        return eventChannel;
    }

    /**
     * Sets the message type for which to listen
     *
     * @param eventChannel
     */
    public void setEventChannel(String eventChannel) {
        this.eventChannel = eventChannel;
    }
}
