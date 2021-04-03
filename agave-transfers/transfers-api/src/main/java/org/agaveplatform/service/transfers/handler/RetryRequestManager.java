package org.agaveplatform.service.transfers.handler;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.listener.AbstractNatsListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang.NotImplementedException;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.TRANSFERTASK_MAX_ATTEMPTS;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_CREATED;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_ALL;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.agaveplatform.service.transfers.enumerations.ErrorMessageTypes;


public class RetryRequestManager extends AbstractNatsListener {
    private static final Logger log = LoggerFactory.getLogger(RetryRequestManager.class);
    private Vertx vertx;
    private TransferTaskDatabaseService dbService;
    public Connection nc = _connect();
    public RetryRequestManager() throws IOException, InterruptedException {
        super();
    }

    /**
     * Constructs a RetryRequest that will attempt to make a request to the event bus and, upon failure, retry the
     * messsage up to {@code maxRetries} times.
     * @param vertx instance of vertx
     */
    public RetryRequestManager(Vertx vertx) throws IOException, InterruptedException {
        super();
        log.info("RetryRequestManager starting");
        setVertx(vertx);
    }

    /**
     * Attempts to make a request to the event bus and, upon failure, retry the message up to {@code maxAttempts} times.
     * @param address the address to which the message will be sent
     * @param body the message to send
     * @param maxAttempts the maximum times to retry delivery of the message
     */
    public void request(final String address, final JsonObject body, final int maxAttempts) throws IOException, InterruptedException {
        log.debug("Got into the RetryRequestManager.request method.");

        //getVertx().eventBus().request(address, body, new DeliveryOptions(), new Handler<AsyncResult<Message<JsonObject>>>() {

        Dispatcher d = nc.createDispatcher((msg) -> {});
        Subscription s = d.subscribe(address, msg -> {
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            log.debug("response is {}", response);

            int attempts = 0;
            Handler<io.vertx.core.AsyncResult<Boolean>> event = null;
            try {
                handle( body, handler -> {
                    if (handler.succeeded()){

                    }else {
                        // failed.

                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        });
    }
            /**
             * Something has happened, so handle it.
             *
             * the event to handle
             */

        protected void handle (JsonObject body, Handler<AsyncResult<Boolean>> handler) throws IOException, InterruptedException, TimeoutException {
            log.trace("Got into the RetryReqestManager.handle method.");
            int attempts = body.getInteger("attempts", 0);
            String address = body.getString("eventId");
            int maxAttempts = Integer.valueOf(TRANSFERTASK_MAX_ATTEMPTS);

            // the event is the same as the address (ErrorMessageTypes) i.e. TRANSFER_COMPLETED
            if (ErrorMessageTypes.getError().contains(address)){
                if (attempts < maxAttempts) {
                    log.error("Unable to send {} event for transfer task {} after {} attempts. {} Max attempts...",
                            address, body.getString("uuid"), attempts, maxAttempts);
                    String tenantId = body.getString("tenantId");
                    body.put("attempts", attempts + 1);
                    TransferTask transferTask = new TransferTask(body);

                    getDbService().createOrUpdateChildTransferTask(tenantId, transferTask, result -> {

                        if (result.succeeded()) {
                            try {
                                _doPublishEvent(address, result.result());
                            } catch (IOException e) {
                                log.debug(e.getMessage());
                            } catch (InterruptedException e) {
                                log.debug(e.getMessage());
                            }
                            //promise.complete();
                        } else {
                            // update failed
                            String uuid = body.getString("uuid");
                            String msg = String.format("Error updating status of transfer task %s to ASSIGNED. %s",
                                    uuid, result.cause().getMessage());
                            log.error("Error updating status of transfer task.");
                            try {
                                doHandleFailure(result.cause(), msg, body, null);
                            } catch (IOException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            //promise.fail(childResult.cause());
                        }
                    });

                    //getVertx().eventBus().request(address, body, new DeliveryOptions(), this);
                    Dispatcher d = nc.createDispatcher((msg) -> {});
                    Subscription s = d.subscribe(address, msg -> {
                        String response = new String(msg.getData(), StandardCharsets.UTF_8);
                        log.debug("response is {}", response);
                    });
                    d.subscribe(address);
                    nc.flush(Duration.ofMillis(500));

                } else {
                    log.error("Unable to send {} event for transfer task {} after {} attempts for message {}. \".\" No further attempts will be made.",
                            address, body.getString("uuid"), attempts, body.encode());
                }
            } else {
                log.debug("Successfully sent {} event for transfer task {}", address, body.getString("uuid"));
                handler.handle(Future.succeededFuture(true));
        }
    }


    public Vertx getVertx() {
        return vertx;
    }

    public void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }

    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }

    /**
     * Obtains a new {@link RemoteDataClient} for the given {@code uri}. The schema and hostname are used to identify
     * agave {@link RemoteSystem} URI vs externally accesible URI. Tenancy is honored.
     * @param tenantId the tenant whithin which any system lookups should be made
     * @param username the user for whom the system looks should be made
     * @param target the uri from which to parse the system info
     * @return a new instance of a {@link RemoteDataClient} for the given {@code target}
     * @throws SystemUnknownException if the sytem is unknown
     * @throws AgaveNamespaceException if the URI does match any known agave uri pattern
     * @throws RemoteCredentialException if the credentials for the system represented by the URI cannot be found/refreshed/obtained
     * @throws PermissionException when the user does not have permission to access the {@code target}
     * @throws FileNotFoundException when the remote {@code target} does not exist
     * @throws RemoteDataException when a connection cannot be made to the {@link RemoteSystem}
     * @throws NotImplementedException when the schema is not supported
     */
    protected RemoteDataClient getRemoteDataClient(String tenantId, String username, URI target) throws NotImplementedException, SystemUnknownException, AgaveNamespaceException, RemoteCredentialException, PermissionException, FileNotFoundException, RemoteDataException {
        TenancyHelper.setCurrentTenantId(tenantId);
        return new RemoteDataClientFactory().getInstance(username, null, target);
    }
}
