/**
 *
 */
package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.globus.ftp.GridFTPSession;
import org.iplantc.service.transfer.*;
import org.iplantc.service.transfer.exceptions.RangeValidationException;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.gridftp.GridFTP;
import org.iplantc.service.transfer.local.Local;
import org.iplantc.service.transfer.model.Range;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.channels.ClosedByInterruptException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;
import static org.agaveplatform.service.transfers.enumerations.TransferTaskEventType.COMPLETED;
import static org.agaveplatform.service.transfers.enumerations.TransferTaskEventType.STREAM_COPY_STARTED;
import static org.agaveplatform.service.transfers.enumerations.TransferTaskEventType.*;

/**
 * Handles the copying of data between one {@link RemoteDataClient} and another. Situations where
 * server-side and third-party are handled as well as triage for relay vs streaming transfers. Transfer
 * tracking and notification listeners are managed through this class. Generally speaking, this is the
 * mechanism for which various QueueTask and AbstractWorkerActions will use to move data.
 * @author dooley
 *
 */
public class URLCopy{
    //private static Logger log = Logger.getLogger(URLCopy.class);
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(URLCopy.class);
    private final RetryRequestManager retryRequestManager;
    private RemoteDataClient sourceClient;
    private RemoteDataClient destClient;

    private final Vertx vertx;
    private AtomicBoolean killed = new AtomicBoolean(false);
    private TransferTaskDatabaseService dbService;

    public URLCopy(RemoteDataClient sourceClient, RemoteDataClient destClient, Vertx vertx, RetryRequestManager retryRequestManager) {
        this.sourceClient = sourceClient;
        this.destClient = destClient;
        this.vertx = vertx;
        this.retryRequestManager = retryRequestManager;
    }

    /**
     * @return the source client for this transfer
     */
    public RemoteDataClient getSourceClient() {
        return sourceClient;
    }

    /**
     * @return the destination client for this transfer
     */
    public RemoteDataClient getDestClient() {
        return destClient;
    }

    /**
     * @return the vertx instance for this transfer
     */
    public Vertx getVertx() {
        return vertx;
    }

    /**
     * @return the killed
     */
    public synchronized boolean isKilled() {
        return this.killed.get();
    }

    /**
     * @param killed the killed to set
     */
    public synchronized void setKilled(boolean killed) {
        this.killed.set(killed);
        if ((sourceClient instanceof GridFTP) && (destClient instanceof GridFTP)) {
            try {
                ((GridFTP) sourceClient).abort();
            } catch (Exception ignored) {
            }
            try {
                ((GridFTP) destClient).abort();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Threadsafe check for killed copy command either through the
     * thread being explicitly killed or the transfertask status
     * being set to cancelled.
     * @param listener
     * @throws ClosedByInterruptException
     */
    protected void checkCancelled(RemoteTransferListener listener)
            throws ClosedByInterruptException {
        if (isKilled() || listener.isCancelled()) {
            throw new ClosedByInterruptException();
        }
    }

    /**
     * Copies one file item to another leveraging the {@link RemoteDataClient} interface.
     * Directory copy is supported and authentication is handled automatically.The algorithm 
     * used to copy is chosen based on the 
     * protocol, file size, and locality of the data. Progress is written to the transfer task
     * via a {@link RemoteTransferListenerImpl}
     *
     * @param
     * @param
     * @param transferTask
     * @throws RemoteDataException
     * @throws IOException
     * @throws TransferException
     * @throws ClosedByInterruptException
     */
    public TransferTask copy(TransferTask transferTask)
            throws RemoteDataException, RemoteDataSyntaxException, IOException, TransferException, ClosedByInterruptException {
        return copy(transferTask, null);
    }

    /**
     * Copies one file item to another leveraging the {@link RemoteDataClient} interface.
     * Directory copy is supported and authentication is handled automatically.The algorithm
     * used to copy is chosen based on the
     * protocol, file size, and locality of the data. Progress is written to the transfer task
     * via a {@link RemoteTransferListenerImpl}
     *
     * @param transferTask the reference transfer task
     * @param exclusions blacklist of paths relative to {@code srcPath} not to copy
     * @throws RemoteDataException
     * @throws IOException
     * @throws TransferException
     * @throws ClosedByInterruptException
     */
    public TransferTask copy(TransferTask transferTask, List<String> exclusions)
            throws RemoteDataException, RemoteDataSyntaxException, IOException, TransferException, ClosedByInterruptException {
        log.debug("UrlCopy.copy method.");
        if (transferTask == null) {
            throw new TransferException("TransferTask cannot be null. Please provide"
                    + "a valid transfer task to track this operation.");
        } else if (transferTask.getId() == null) {
            throw new TransferException("TransferTask does not have a valid id. "
                    + "Please persiste the transfer taks and attempt the operation again.");
        }

        if (exclusions == null) {
            exclusions = new ArrayList<String>();
        }
        String srcPath = URI.create(transferTask.getSource()).getPath();
        String destPath = URI.create(transferTask.getDest()).getPath();

        sourceClient = getSourceClient();
        destClient = getDestClient();

        try {

            RemoteTransferListenerImpl listener = getRemoteTransferListenerForTransferTask(transferTask);

            // don't copy the redundant destPath or parent of destPath returned from unix style listings dir
            // to avoid infinite loops and full file system copies.
            if (List.of(".", "..").contains(FilenameUtils.getName(srcPath))) {
                return transferTask;
            }

            // source and dest are the same host, so do a server-side copy
            if (sourceClient.equals(destClient)) {
                // should be able to do a relay transfer here just as easily
                sourceClient.copy(srcPath, destPath, listener);
                transferTask = (TransferTask)listener.getTransferTask();
            }
            // delegate to third-party transfer if supported
            else if (sourceClient.isThirdPartyTransferSupported() &&
                    destClient.isThirdPartyTransferSupported() &&
                    sourceClient.getClass().equals(destClient.getClass())) {
                transferTask = dothirdPartyTransfer(srcPath, destPath, listener);
            }
            // otherwise, we're doing the heavy lifting ourselves
            else {

                try {
                    double srcFileLength = sourceClient.length(srcPath);
                    long availableBytes = new File("/").getUsableSpace();

                    // we have a choice of using a relay or streaming transfer. For relay transfers,
                    // we have to ensure the host has available disk space to do the inital get
                    // and cache the file on disk. Streaming transfer does not
                    if (Settings.ALLOW_RELAY_TRANSFERS
                            && srcFileLength < (Settings.MAX_RELAY_TRANSFER_SIZE * Math.pow(2, 30))) {
                        if (availableBytes > (srcFileLength + (5 * Math.pow(2, 30)))) {
                            log.debug("Local disk has " + availableBytes + " unused bytes  prior to "
                                    + "relay transfer of " + srcFileLength + " bytes for transfer task "
                                    + transferTask.getUuid() + ". Relay transfer will be allowed.");
                            transferTask = relayTransfer(srcPath, destPath, transferTask);
                        } else {
                            log.debug("Local disk has insufficient space (" + availableBytes +
                                    " < " + srcFileLength + ") for relay transfer of transfer task "
                                    + transferTask.getUuid() + ". Switching to streaming transfer instead.");
                            transferTask = streamingTransfer(srcPath, destPath, listener);
                        }
                    }
                    // only streaming transfers are supported at this point, so carry on with those.
                    else {
                        transferTask = streamingTransfer(srcPath, destPath, listener);
                    }
                } catch (ClosedByInterruptException e) {
                    throw e;
                } catch (Throwable e) {
                    log.error("Exception {}", e.getMessage(), e.getCause());
//                    throw e;
                }
            }

            return transferTask;
//        } catch (Throwable e) {
//            log.error("Exception {}", e.getMessage(), e.getCause());
//            return null;
        } finally {
            try {
                if (destClient.isPermissionMirroringRequired()) {
                    destClient.setOwnerPermission(destClient.getUsername(), destPath, true);
                    destClient.setOwnerPermission(transferTask.getOwner(), destPath, true);
                }
            } catch (Exception e) {
                log.error("Failed to set permissions on " + destClient.getHost() + " for user " + transferTask.getOwner(), e);
            }
        }
    }

    public RemoteTransferListenerImpl getRemoteTransferListenerForTransferTask(TransferTask transferTask) {
        return new RemoteTransferListenerImpl(transferTask, getVertx(), getRetryRequestManager());
    }

//    TransferTaskImpl convTransferTask(TransferTask transferTask){
//
//        TransferTaskImpl convTransferTask = new TransferTaskImpl();
//        convTransferTask.setId(transferTask.getId());
//        convTransferTask.setSource(transferTask.getSource());
//        convTransferTask.setDest(transferTask.getDest());
//        convTransferTask.setOwner(transferTask.getOwner());
//        convTransferTask.setEventId(transferTask.getEventId());
//        convTransferTask.setAttempts(transferTask.getAttempts());
//        convTransferTask.setStatus( org.iplantc.service.transfer.model.enumerations.TransferStatusType.valueOf(transferTask.getStatus().toString()));
//        convTransferTask.setTotalSize(transferTask.getTotalSize());
//        convTransferTask.setTotalFiles(transferTask.getTotalFiles());
//        convTransferTask.setTotalSkippedFiles(transferTask.getTotalSkippedFiles());
//        convTransferTask.setBytesTransferred(transferTask.getBytesTransferred());
//        convTransferTask.setTransferRate(transferTask.getTransferRate());
//        convTransferTask.setTenantId(transferTask.getTenantId());
//
//        TransferTaskImpl tParent = new TransferTaskImpl( transferTask.getSource(), transferTask.getDest() );
//        TransferTaskImpl tRoot = new TransferTaskImpl( transferTask.getSource(), transferTask.getDest() );
//        convTransferTask.setParentTask(tParent);
//        convTransferTask.setRootTask(tRoot);
//
//        if (transferTask.getStartTime() == null){
//            log.error("transferTask.getStartTime is null");
//        }else {
//            convTransferTask.setStartTime(Date.from(transferTask.getStartTime()));
//        }
//
//        if (transferTask.getEndTime() == null){
//            log.error("transferTask.getEndTime is null");
//        }else {
//            convTransferTask.setEndTime(Date.from(transferTask.getEndTime()));
//        }
//
//        if (transferTask.getCreated() == null){
//            log.error("transferTask.getEndTime is null");
//        }else {
//            convTransferTask.setCreated(Date.from(transferTask.getCreated()));
//        }
//
//        if (transferTask.getLastUpdated() == null){
//            log.error("transferTask.getEndTime is null");
//        }else {
//            convTransferTask.setLastUpdated(Date.from(transferTask.getLastUpdated()));
//        }
//
//
//        convTransferTask.setUuid(transferTask.getUuid());
//        convTransferTask.setVersion(0);
//
//        return convTransferTask;
//    }
//
//    TransferTask convToAgaveTransferTask(TransferTaskImpl txfrTsk){
//        TransferTask newTransferTask = new TransferTask(txfrTsk.getSource(), txfrTsk.getDest());
//        newTransferTask.setOwner(txfrTsk.getOwner());
//        newTransferTask.setEventId(txfrTsk.getEventId());
//        newTransferTask.setAttempts(txfrTsk.getAttempts());
//        newTransferTask.setStatus(org.agaveplatform.service.transfers.enumerations.TransferStatusType.valueOf(txfrTsk.getStatus().toString()));
//        newTransferTask.setTotalSize(txfrTsk.getTotalSize());
//        newTransferTask.setTotalFiles(txfrTsk.getTotalFiles());
//        newTransferTask.setTotalSkippedFiles(txfrTsk.getTotalSkippedFiles());
//        newTransferTask.setBytesTransferred(txfrTsk.getBytesTransferred());
//        newTransferTask.setTransferRate(txfrTsk.getTransferRate());
//        newTransferTask.setTenantId(txfrTsk.getTenantId());
//        newTransferTask.setStartTime(txfrTsk.getStartTime().toInstant());
//        newTransferTask.setEndTime(txfrTsk.getEndTime().toInstant());
//        newTransferTask.setCreated(txfrTsk.getCreated().toInstant());
//        newTransferTask.setLastUpdated(txfrTsk.getLastUpdated().toInstant());
//        newTransferTask.setUuid(txfrTsk.getUuid());
//
//        newTransferTask.setParentTaskId(txfrTsk.getParentTask().getUuid());
//        newTransferTask.setRootTaskId(txfrTsk.getRootTask().getUuid());
//
//        return newTransferTask;
//    }

    /**
     * Proxies a file/folder transfer from source to destination by using the underlying
     * {@link RemoteDataClient#(String, String, RemoteTransferListenerImpl )} and {@link RemoteDataClient#(String, String, RemoteTransferListenerImpl )}
     * methods to stage the data to the local host, then push to the destination system.
     * This can be significantly faster than the standard {@link #streamingTransfer(String, String, RemoteTransferListenerImpl)}
     * method when the underlying protocols support parallelism and/or threading. Care must
     * be taken with this approach to properly check that there is available disk space to
     * perform the copy.
     *
     * @param srcPath path on the src system to move to the destPath
     * @param destPath path on the dest system where the data will be copied.
     * @param aggregateTransferTask the top level TransferTask tracking the aggregate data movement
     * @return the updated {@code aggregatedTransferTask} after processing
     * @throws RemoteDataException if an error occurred attempting the transfer
     * @throws ClosedByInterruptException if the transfer was interrupted
     */
    protected TransferTask relayTransfer(String srcPath, String destPath, TransferTask aggregateTransferTask)
            throws RemoteDataException, ClosedByInterruptException {
        File tmpFile = null;
        File tempDir = null;
        TransferTask srcChildTransferTask = null;
        RemoteTransferListener srcChildRemoteTransferListener = null;
        TransferTask destChildTransferTask = null;
        RemoteTransferListener destChildRemoteTransferListener = null;

        try {
            if (sourceClient instanceof Local) {
                tmpFile = new File(sourceClient.resolvePath(srcPath));
                tempDir = tmpFile.getParentFile();

                log.debug(String.format(
                        "Skipping first leg of relay transfer for task %s. %s to %s . Protocol: %s => %s",
                        aggregateTransferTask.getUuid(),
                        aggregateTransferTask.getSource(),
                        "file://" + tmpFile.getPath(),
                        getProtocolForClass(destClient.getClass()),
                        "local"));
            } else {
                tempDir = new File(org.iplantc.service.common.Settings.TEMP_DIRECTORY,
                        DigestUtils.md5Hex(srcPath) + "-" + System
                                .currentTimeMillis() + ".relay.tmp");

                if (destClient instanceof Local) {
                    tmpFile = new File(destClient.resolvePath(destPath));
                    tempDir = tmpFile.getParentFile();
                } else {
                    tempDir.mkdirs();
                    tmpFile = new File(tempDir, FilenameUtils.getName(srcPath));
                }

                log.debug(String.format(
                        "Beginning first leg of relay transfer for task %s. %s to %s . Protocol: %s => %s",
                        aggregateTransferTask.getUuid(),
                        aggregateTransferTask.getSource(),
                        "file://" + tmpFile.getPath(),
                        getProtocolForClass(sourceClient.getClass()),
                        "local"));
                try {
                    // we create a subtask local to this method to track each end of the relay transfer.
                    //
                    srcChildTransferTask = new TransferTask(
                            aggregateTransferTask.getSource(),
                            "https://workers.prod.agaveplatform.org/" + tmpFile.getPath(),
                            aggregateTransferTask.getOwner(),
                            aggregateTransferTask.getUuid(),
                            aggregateTransferTask.getRootTaskId());

                    srcChildTransferTask.setTenantId(aggregateTransferTask.getTenantId());
                    srcChildTransferTask.setStatus(READ_STARTED);

                    _doPublishEvent(RELAY_READ_STARTED.name(), aggregateTransferTask.toJson());

                    srcChildRemoteTransferListener =
                            getRemoteTransferListenerForTransferTask(srcChildTransferTask);

                    // perform the get
                    sourceClient.get(srcPath, tmpFile.getPath(), srcChildRemoteTransferListener);

                    srcChildTransferTask = (TransferTask)srcChildRemoteTransferListener.getTransferTask();
                    aggregateTransferTask.setStartTime(srcChildTransferTask.getStartTime());
                    aggregateTransferTask.setStatus(srcChildTransferTask.getStatus());

                    // cancelled status will be handled in the calling function.
                    if (!isKilled()) {
//                        aggregateTransferTask.setStatus(TransferStatusType.CANCELLED);
//                    } else {
                        aggregateTransferTask.setStatus(READ_COMPLETED);
                        _doPublishEvent(RELAY_READ_COMPLETED.name(), aggregateTransferTask.toJson());
                        _doPublishEvent(UPDATED.name(), aggregateTransferTask.toJson());
                    }


                    // must be in here as the LOCAL files will not have a src transfer listener associated with them.
                    checkCancelled(srcChildRemoteTransferListener);

                } catch (RemoteDataException e) {
                    try {
                        aggregateTransferTask.setStatus(TransferStatusType.FAILED);
                        aggregateTransferTask.setEndTime(Instant.now());
                    } catch (Throwable t) {
                        log.error("Failed to set status of relay source child task to failed.", t);
                    }

                    log.debug(String.format(
                            "Failed first leg of relay transfer for task %s. %s to %s . Protocol: %s => %s",
                            aggregateTransferTask.getUuid(),
                            aggregateTransferTask.getSource(),
                            "file://" + tmpFile.getPath(),
                            getProtocolForClass(sourceClient.getClass()),
                            "local"), e);
                    throw e;
                } catch (Throwable e) {
                    try {
                        aggregateTransferTask.setStatus(TransferStatusType.FAILED);
                        aggregateTransferTask.setEndTime(Instant.now());
                    } catch (Throwable t) {
                        log.error("Failed to set status of relay source child task to failed.", t);
                    }

                    log.debug(String.format(
                            "Failed first leg of relay transfer for task %s. %s to %s . Protocol: %s => %s",
                            aggregateTransferTask.getUuid(),
                            aggregateTransferTask.getSource(),
                            "file://" + tmpFile.getPath(),
                            getProtocolForClass(sourceClient.getClass()),
                            "local"), e);
                    // stuff happens, what are you going to do.
                    throw new RemoteDataException("Transfer failed from " + sourceClient.getUriForPath(srcPath), e);
                }
            }

            if (!((sourceClient instanceof Local) && (destClient instanceof Local))) {
                try {
                    log.debug(String.format(
                            "Beginning second leg of relay transfer for task %s. %s to %s . Protocol: %s => %s",
                            aggregateTransferTask.getUuid(),
                            "file://" + tmpFile.getPath(),
                            aggregateTransferTask.getDest(),
                            "local",
                            getProtocolForClass(destClient.getClass())));

                    destChildTransferTask = new TransferTask(
                            "https://workers.prod.agaveplatform.org/" + tmpFile.getPath(),
                            aggregateTransferTask.getDest(),
                            aggregateTransferTask.getOwner(),
                            aggregateTransferTask.getParentTaskId(),
                            aggregateTransferTask.getRootTaskId());
                    destChildTransferTask.setTenantId(aggregateTransferTask.getTenantId());
                    aggregateTransferTask.setStatus(WRITE_STARTED);
                    _doPublishEvent(RELAY_WRITE_STARTED.name(), aggregateTransferTask.toJson());

                    destChildRemoteTransferListener =
                            getRemoteTransferListenerForTransferTask(destChildTransferTask);

                    destClient.put(tmpFile.getPath(), destPath, destChildRemoteTransferListener);

                    if (!isKilled()) {
                        destChildTransferTask = (TransferTask)destChildRemoteTransferListener.getTransferTask();
                        // now update the aggregate task with the info from the child task
                        aggregateTransferTask.setBytesTransferred(destChildTransferTask.getBytesTransferred());
                        aggregateTransferTask.setTotalFiles(destChildTransferTask.getTotalFiles());
                        aggregateTransferTask.setTotalSkippedFiles(destChildTransferTask.getTotalSkippedFiles());
                        aggregateTransferTask.setTotalSize(destChildTransferTask.getTotalSize());
                        aggregateTransferTask.setAttempts(destChildTransferTask.getAttempts());
                        aggregateTransferTask.setEndTime(destChildTransferTask.getEndTime());
                        aggregateTransferTask.updateTransferRate();

                        aggregateTransferTask.setStatus(WRITE_COMPLETED);
                        _doPublishEvent(RELAY_WRITE_COMPLETED.name(), aggregateTransferTask.toJson());

                        aggregateTransferTask.setStatus(TransferStatusType.COMPLETED);
                        _doPublishEvent(COMPLETED.name(), aggregateTransferTask.toJson());
                    }
                } catch (RemoteDataException e) {
                    try {
                        destChildTransferTask.setStatus(TransferStatusType.FAILED);
                        destChildTransferTask.setEndTime(Instant.now());
                    } catch (Throwable t) {
                        log.error("Failed to set status of relay dest child task to failed.", t);
                    }

                    log.debug(String.format(
                            "Failed second leg of relay transfer for task %s. %s to %s . Protocol: %s => %s",
                            aggregateTransferTask.getUuid(),
                            "file://" + tmpFile.getPath(),
                            aggregateTransferTask.getDest(),
                            "local",
                            getProtocolForClass(destClient.getClass())), e);
                    throw e;
                } catch (Throwable e) {
                    // fail the destination transfer task
                    try {
                        aggregateTransferTask.setStatus(TransferStatusType.FAILED);
                        aggregateTransferTask.setEndTime(Instant.now());
                    } catch (Throwable t) {
                        log.error("Failed to set status of relay dest child task to failed.", t);
                    }

                    log.debug(String.format(
                            "Failed second leg of relay transfer for task %s. %s to %s . Protocol: %s => %s",
                            aggregateTransferTask.getUuid(),
                            "file://" + tmpFile.getPath(),
                            aggregateTransferTask.getDest(),
                            "local",
                            getProtocolForClass(destClient.getClass())));
                    throw new RemoteDataException("Transfer failed to " + sourceClient.getUriForPath(srcPath) +
                            " using " + destClient.getClass().getSimpleName(), e);
                }
            } else {
                log.debug(String.format(
                        "Skipping second leg of relay transfer for task %s. %s to %s. Protocol: %s => %s",
                        aggregateTransferTask.getUuid(),
                        "file://" + tmpFile.getPath(),
                        aggregateTransferTask.getDest(),
                        "local",
                        getProtocolForClass(destClient.getClass())));

                destChildTransferTask = new TransferTask(
                        "https://workers.prod.agaveplatform.org/" + tmpFile.getPath(),
                        aggregateTransferTask.getDest(),
                        aggregateTransferTask.getOwner(),
                        aggregateTransferTask.getParentTaskId(),
                        aggregateTransferTask.getRootTaskId());
                destChildTransferTask.setTenantId(aggregateTransferTask.getTenantId());
                destChildTransferTask.setStartTime(aggregateTransferTask.getStartTime());

                aggregateTransferTask.setStatus(WRITE_STARTED);
                _doPublishEvent(RELAY_WRITE_STARTED.name(), aggregateTransferTask.toJson());

                destChildRemoteTransferListener =
                        getRemoteTransferListenerForTransferTask(destChildTransferTask);


                long tmpFileLength = tmpFile.length();

                // trigger the lifecycle of the transfer as if it were a remote copy. This preserves the child event
                // lifecycle
                destChildRemoteTransferListener.started(tmpFileLength, tmpFile.getPath());
                destChildRemoteTransferListener.progressed(tmpFileLength);
                destChildRemoteTransferListener.completed();

                // now update the aggregate task with the info from the child task
                aggregateTransferTask.setBytesTransferred(destChildTransferTask.getBytesTransferred());
                aggregateTransferTask.setTotalFiles(destChildTransferTask.getTotalFiles());
                aggregateTransferTask.setTotalSkippedFiles(destChildTransferTask.getTotalSkippedFiles());
                aggregateTransferTask.setTotalSize(destChildTransferTask.getTotalSize());
                aggregateTransferTask.setAttempts(destChildTransferTask.getAttempts());
                aggregateTransferTask.setEndTime(destChildTransferTask.getEndTime());
                aggregateTransferTask.updateTransferRate();

                aggregateTransferTask.setStatus(WRITE_COMPLETED);
                _doPublishEvent(RELAY_WRITE_COMPLETED.name(), aggregateTransferTask.toJson());

                aggregateTransferTask.setStatus(TransferStatusType.COMPLETED);
                _doPublishEvent(COMPLETED.name(), aggregateTransferTask.toJson());
            }
        }
        catch (ClosedByInterruptException e) {
            log.debug(String.format(
                    "Aborted relay transfer for task %s. %s to %s . Protocol: %s => %s",
                    aggregateTransferTask.getUuid(),
                    aggregateTransferTask.getSource(),
                    aggregateTransferTask.getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())), e);
            Thread.currentThread().interrupt();
            throw e;
        }
        catch (RemoteDataException e) {
//            try {
                aggregateTransferTask.setEndTime(Instant.now());
                aggregateTransferTask.setStatus(TransferStatusType.FAILED);
//            } catch (TransferException e1) {
//                log.error("Failed to update parent transfer task "
//                        + aggregateTransferTask.getUuid() + " status to FAILED", e1);
//            }

//			checkCancelled(remoteTransferListener);

            throw e;
        }
        catch (Exception e) {
            aggregateTransferTask.setEndTime(Instant.now());
            aggregateTransferTask.setStatus(TransferStatusType.FAILED);

            throw new RemoteDataException(
                    getDefaultErrorMessage(
                            srcPath,
                            aggregateTransferTask), e);
        } finally {
            log.info(String.format(
                    "Total of %s bytes transferred in task %s . Protocol %s => %s",
                    aggregateTransferTask.getBytesTransferred(),
                    aggregateTransferTask.getUuid(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())));

            if (sourceClient instanceof Local) {
                log.info("Skipping deleting relay cache file " + tempDir.getPath() + " as source originated from this host.");
            } else {
                log.info("Deleting relay cache file " + tempDir.getPath());
                FileUtils.deleteQuietly(tempDir);
            }
        }

        return aggregateTransferTask;
    }

    /**
     * Returns shortname for package containing a {@link RemoteDataClient}. This
     * allows us to determine the protocol used by that client quickly for logging
     * purposes. For example S3JCloud => s3, MaverickSFTP => sftp.
     *
     * @param clientClass class for which to get the protocol
     * @return data protocol shortname used by a client.
     */
    private Object getProtocolForClass(Class<? extends RemoteDataClient> clientClass) {
        String fullName = clientClass.getName();
        String[] tokens = fullName.split("\\.");
        return tokens[tokens.length - 2];
    }

    /**
     * Parses the hostname out of a URI. This is used to extract systemId info from
     * the TransferTask.rootTask.source and TransferTask.rootTask.dest fields and
     * create the child source and dest values.
     *
     * @param serializedUri
     * @return
     */
    private String getSystemId(String serializedUri) {
        URI uri = null;
        try {
            uri = URI.create(serializedUri);
            return uri.getHost();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Performs the transfer by streaming data from one system to another through
     * the local server.
     * <p>
     * If the destPath is a directory, then the srcPath will be copied to a file with
     * the exact same name within the destPath directory. This keeps the behavior
     * identical to that of the {@link RemoteDataClient#put(String, String)} method
     * used in the {@link #relayTransfer(String, String, TransferTask)} method.
     * </p>
     *
     * @param srcPath agave source path of the file on the remote system
     * @param destPath the destination agave path of the transfer on the remote system
     * @param listener the listener to track the transfer info
     * @return the updated {@code aggregatedTransferTask} after processing
     * @throws RemoteDataException if an error occurred attempting the transfer
     * @throws IOException if unable to open a stream
     * @throws ClosedByInterruptException if the transfer was interrupted
     */
    protected TransferTask streamingTransfer(String srcPath, String destPath, RemoteTransferListenerImpl listener)
            throws RemoteDataException, IOException, ClosedByInterruptException {
        // The "b" in the variable names means "buffered".
        RemoteInputStream<?> in = null;
        InputStream bis = null;
        RemoteOutputStream<?> out = null;
        OutputStream bos = null;

        long bytesSoFar = 0;
        try {
            log.debug(String.format(
                    "Beginning streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())));

            long totalSize = sourceClient.length(srcPath);

            listener.getTransferTask().setStatusString(STREAM_COPY_STARTED.name());
            _doPublishEvent(STREAM_COPY_STARTED.name(), ((TransferTask)listener.getTransferTask()).toJson());

            // Buffer the input stream only if it's not already buffered.
            try {
                in = getInputStream(sourceClient, srcPath);
            } catch (Exception e) {
                log.error("Unable to get input stream for " + sourceClient.getUsername() +
                        " on host " + sourceClient.getHost() + " for path " + srcPath + ".");
                throw e;
            }
            if (in.isBuffered()) bis = in;
            else bis = new BufferedInputStream(in);

            checkCancelled(listener);

            // Buffer the output stream only if it's not already buffered.
            try {
                out = getOutputStream(destClient, destPath);
            } catch (Exception e) {
                log.error("Unable to get output stream for " + destClient.getUsername() +
                        " on host " + destClient.getHost() + " for path " + destPath + ".");
                throw e;
            }
            if (out.isBuffered()) bos = out;
            else bos = new BufferedOutputStream(out);

            checkCancelled(listener);

            int length = 0;
            long callbackTime = System.currentTimeMillis();
            int bufferSize = Math.min(sourceClient.getMaxBufferSize(), destClient.getMaxBufferSize());
            byte[] b = new byte[bufferSize];

            listener.started(totalSize, srcPath);

            while ((length = bis.read(b, 0, bufferSize)) != -1) {
                bytesSoFar += length;

                bos.write(b, 0, length);


                // update the progress every 15 seconds buffer cycle. This reduced the impact
                // from the observing process while keeping the update interval at a
                // rate the user can somewhat trust
                if (System.currentTimeMillis() > (callbackTime + 10000)) {
                    // check to see if this transfer has been cancelled due to outside
                    // intervention such as updating the TransferTask record or the parent
                    // thread interrupting this one and setting the AbstractTransferTask.cancelled
                    // field to true
                    checkCancelled(listener);

                    callbackTime = System.currentTimeMillis();

                    listener.progressed(bytesSoFar);
                }
            }

            // update with the final transferred blocks and wrap the transfer.
            listener.progressed(bytesSoFar);
            listener.completed();

            // now update the aggregate task with the info from the child task
            TransferTask streamingTransferTask = (TransferTask)listener.getTransferTask();

            streamingTransferTask.setStatus(WRITE_COMPLETED);
            _doPublishEvent(TransferStatusType.STREAM_COPY_COMPLETED.name(), streamingTransferTask.toJson());

            streamingTransferTask.setStatus(TransferStatusType.COMPLETED);
            _doPublishEvent(COMPLETED.name(), streamingTransferTask.toJson());


            log.debug(String.format(
                    "Completed streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())));

            return streamingTransferTask;

        }
        catch (ClosedByInterruptException e) {
            log.debug(String.format(
                    "Aborted streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(destClient.getClass()),
                    getProtocolForClass(destClient.getClass())), e);

            log.info("Transfer task " + listener.getTransferTask().getUuid() + " killed by worker shutdown.");
            setKilled(true);

            listener.progressed(bytesSoFar);
            listener.cancel();

            Thread.currentThread().interrupt();

            throw e;
        }
        catch (RemoteDataException | IOException e) {
            log.debug(String.format(
                    "Failed streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())), e);

            // transfer failed due to connectivity issue
            listener.failed();

            throw e;
        }
        catch (Throwable e) {
            log.debug(String.format(
                    "Failed streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())), e);

            // stuff happens, what are you going to do.
            listener.failed();

            throw new RemoteDataException("Transfer task %s failed.", e);
        }
        finally {
            if (listener != null && listener.getTransferTask() != null) {
                log.info(String.format(
                        "Total of %s bytes transferred in task %s . Protocol %s => %s",
                        listener.getTransferTask().getBytesTransferred(),
                        listener.getTransferTask().getUuid(),
                        getProtocolForClass(sourceClient.getClass()),
                        getProtocolForClass(destClient.getClass())));
            }

            try { if (bis != null) bis.close(); } catch (Throwable ignored) {}
            try { if (bos != null) bos.close(); } catch (Throwable ignored) {}
            try { if (in != null) in.close(); } catch (Throwable ignored) {}
            try { if (out != null) out.close(); } catch (Throwable ignored) {}
        }
    }

    /**
     * Performs the transfer by streaming data from one system to another through
     * the local server. This option honors range requests, so only 
     *
     * @param srcPath agave source path of the file on the remote system
     * @param srcRangeOffset offset to start reading from the source file
     * @param srcRangeSize length of the range to read, -1 represents remainder of file
     * @param destPath the destination agave path of the transfer on the remote system  
     * @param destRangeOffset offset to start writing to the dest file
     * @param transferTask the transfer task tracking the transfer
     * @return transferTask updated with results of the transfer
     * @throws RemoteDataException
     * @throws IOException
     * @throws TransferException
     * @throws ClosedByInterruptException
     */
    public TransferTask copyRange(String srcPath, long srcRangeOffset, long srcRangeSize,
                                  String destPath, long destRangeOffset, TransferTask transferTask)
            throws RemoteDataException, IOException, TransferException, ClosedByInterruptException {
        if (transferTask == null) {
            throw new TransferException("TransferTask cannot be null. Please provide"
                    + "a valid transfer task to track this operation.");
        } else if (transferTask.getId() == null) {
            throw new TransferException("TransferTask does not have a valid id. "
                    + "Please persiste the transfer taks and attempt the operation again.");
        }

        try {
            // if we are transferring a directory
            if (sourceClient.isDirectory(srcPath)) {
                throw new TransferException("Range transfers are not supported on directories");
            } else {

                RemoteTransferListenerImpl listener = getRemoteTransferListenerForTransferTask(transferTask);

                if (StringUtils.equals(FilenameUtils.getName(srcPath), ".") ||
                        StringUtils.equals(FilenameUtils.getName(srcPath), "..")) {
                    // skip current directory and parent to avoid infinite loops and
                    // full file system copies.
                }
//                else if (sourceClient.isThirdPartyTransferSupported() &&
//                        destClient.isThirdPartyTransferSupported() &&
//                        sourceClient.getClass().equals(destClient.getClass()))
//                {
//                    dothirdPartyTransfer(srcPath, srcRangeOffset, srcRangeSize, destPath, destRangeOffset, listener);
//                }
                else {
                    RangeValidator sourceRangeValidator = new RangeValidator(srcRangeOffset, srcRangeSize, sourceClient.length(srcPath));
                    RangeValidator destRangeValidator = new RangeValidator(destRangeOffset, Range.SIZE_MAX, sourceClient.length(srcPath));

                    try {
                        long absoluteSourceIndex = sourceRangeValidator.getAbsoluteIndex();
                        long absoluteDestIndex = destRangeValidator.getAbsoluteIndex();

                        transferTask = proxyRangeTransfer(srcPath, absoluteSourceIndex, srcRangeSize, destPath, absoluteDestIndex, listener);
                    } catch (RangeValidationException e) {
                        throw new RemoteDataException(e.getMessage(), e);
                    }
                }

                return (TransferTask)listener.getTransferTask();
            }
        } finally {
            try {
                if (destClient.isPermissionMirroringRequired()) {
                    destClient.setOwnerPermission(destClient.getUsername(), destPath, true);
                    destClient.setOwnerPermission(transferTask.getOwner(), destPath, true);
                }
            } catch (Exception e) {
                log.error("Failed to set permissions on " + destClient.getHost() + " for user " + transferTask.getOwner(), e);
            }
        }
    }

    /**
     * Performs the transfer by streaming data from one system to another through
     * the local server. This option honors range requests, so only 
     *
     * @param srcPath agave source path of the file on the remote system
     * @param srcRangeOffset offset to start reading from the source file
     * @param srcRangeSize length of the range to read, -1 represents remainder of file
     * @param destPath the destination agave path of the transfer on the remote system
     * @param destRangeOffset offset to start writing to the dest file
     * @param listener the listener to track the transfer info
     * @return the updated {@code aggregatedTransferTask} after processing
     * @throws RemoteDataException if an error occurred attempting the transfer
     * @throws IOException if unable to open a stream
     * @throws ClosedByInterruptException if the transfer was interrupted
     */
    protected TransferTask proxyRangeTransfer(String srcPath, long srcRangeOffset, long srcRangeSize,
                                      String destPath, long destRangeOffset, RemoteTransferListenerImpl listener)
            throws RemoteDataException, IOException, ClosedByInterruptException {

        if (listener == null) {
            throw new RemoteDataException("Transfer listener cannot be null");
        }

        // The "b" in the variable names means "buffered".
        RemoteInputStream<?> in = null;
        InputStream bis = null;

        RemoteInputStream<?> originalIn = null;
        InputStream originalBis = null;

        RemoteOutputStream<?> out = null;
        OutputStream bos = null;

        long bytesSoFar = 0;
        try {
            log.debug(String.format(
                    "Beginning streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())));

            listener.getTransferTask().setStatusString(STREAM_COPY_STARTED.name());
            _doPublishEvent(STREAM_COPY_STARTED.name(), ((TransferTask)listener.getTransferTask()).toJson());

            if (sourceClient.isDirectory(srcPath)) {
                throw new RemoteDataException("Cannot perform range query on directories");
            }

            long totalSize = srcRangeSize;
            if (totalSize == Range.SIZE_MAX) {
                totalSize = sourceClient.length(srcPath) - srcRangeOffset;
            }

            // Buffer the input stream only if it's not already buffered.
            try {
                in = getInputStream(sourceClient, srcPath);
            } catch (Exception e) {
                log.error("Unable to get input stream for " + sourceClient.getUsername() +
                        " on host " + sourceClient.getHost() + " for path " + srcPath + ".");
                throw e;
            }
            in.skip(srcRangeOffset);
            if (in.isBuffered()) bis = in;
            else bis = new BufferedInputStream(in);

            // Buffer the original input stream only if it's not already buffered.
            try {
                originalIn = getInputStream(destClient, destPath);
            } catch (Exception e) {
                log.error("Unable to get input stream for " + destClient.getUsername() +
                        " on host " + destClient.getHost() + " for path " + destPath + ".");
                throw e;
            }
            if (originalIn.isBuffered()) originalBis = originalIn;
            else originalBis = new BufferedInputStream(originalIn);

            // Buffer the output stream only if it's not already buffered.
            String tmpFilename = destPath + ".tmp-" + System.currentTimeMillis();
            try {
                out = getOutputStream(destClient, tmpFilename);
            } catch (Exception e) {
                log.error("Unable to get output stream for " + destClient.getUsername() +
                        " on host " + destClient.getHost() + " for path " + tmpFilename + ".");
                throw e;
            }

            if (out.isBuffered()) bos = out;
            else bos = new BufferedOutputStream(out);

            checkCancelled(listener);

            int length = 0;
            long remainingOffset = destRangeOffset;

            long callbackTime = System.currentTimeMillis();
            int bufferSize = sourceClient.getMaxBufferSize();
            byte[] newBytes = new byte[bufferSize];
            byte[] originalBytes = new byte[bufferSize];

            listener.started(totalSize, srcPath);

            // skip ahead in the file to get to the position we want to begin overwriting.
            // this is generally not supported in most protocols, so we instead manually write
            // the first destRangeOffset bytes to a temp file, then append the input stream,
            // then write whatever is left of the file.
            while ((length = originalBis.read(originalBytes, 0, (int) Math.min(bufferSize, remainingOffset))) != -1) {
                remainingOffset -= length;
                bytesSoFar += length;

                bos.write(originalBytes);

                // update the progress every 15 seconds buffer cycle. This reduced the impact
                // from the observing process while keeping the update interval at a 
                // rate the user can somewhat trust
                if (System.currentTimeMillis() > (callbackTime + 10000)) {
                    // check to see if this transfer has been cancelled due to outside
                    // intervention such as updating the TransferTask record or the parent
                    // thread interrupting this one and setting the AbstractTransferTask.cancelled
                    // field to true
                    checkCancelled(listener);

                    callbackTime = System.currentTimeMillis();

                    listener.progressed(bytesSoFar);
                }
            }

            // check to see if this transfer has been cancelled due to outside
            // intervention such as updating the TransferTask record or the parent
            // thread interrupting this one and setting the AbstractTransferTask.cancelled
            // field to true
            checkCancelled(listener);
            callbackTime = System.currentTimeMillis();
            listener.progressed(bytesSoFar);

            long remainingBytes = totalSize;
            boolean haveReachedTheEndOfOriginalDesinationFile = false;

            // now we are at the destination, so start writing the input stream to the temp file
            while (remainingBytes > 0 &&
                    (length = bis.read(newBytes, 0, (int) Math.min(bufferSize, remainingOffset))) != -1) {
                // write the new input from the source stream to the destination. 
                // This is essentially an append operation until we run out of input.
                bos.write(newBytes);

                remainingBytes -= length;
                bytesSoFar += length;

                // we need to keep up with the copy on the original, so read along here byte for byte
                // until we hit the end of the file at which point we stop reading from the original.
                if (!haveReachedTheEndOfOriginalDesinationFile) {
                    haveReachedTheEndOfOriginalDesinationFile = (originalBis.read(originalBytes, 0, length) == -1);
                }

                // update the progress every 15 seconds buffer cycle. This reduced the impact
                // from the observing process while keeping the update interval at a 
                // rate the user can somewhat trust
                if (System.currentTimeMillis() > (callbackTime + 10000)) {
                    // check to see if this transfer has been cancelled due to outside
                    // intervention such as updating the TransferTask record or the parent
                    // thread interrupting this one and setting the AbstractTransferTask.cancelled
                    // field to true
                    checkCancelled(listener);


                    callbackTime = System.currentTimeMillis();

                    listener.progressed(bytesSoFar);
                }
            }

            // check to see if this transfer has been cancelled due to outside
            // intervention such as updating the TransferTask record or the parent
            // thread interrupting this one and setting the AbstractTransferTask.cancelled
            // field to true
            checkCancelled(listener);
            callbackTime = System.currentTimeMillis();
            listener.progressed(bytesSoFar);


            // finally, we are done reading from the input stream and we have no overwritten
            // the end of the output stream, so we finish writing the rest of the file.
            if (!haveReachedTheEndOfOriginalDesinationFile) {
                while ((length = originalBis.read(originalBytes, 0, bufferSize)) != -1) {
                    bytesSoFar += length;

                    bos.write(originalBytes);

                    // update the progress every 15 seconds buffer cycle. This reduced the impact
                    // from the observing process while keeping the update interval at a 
                    // rate the user can somewhat trust
                    if (System.currentTimeMillis() > (callbackTime + 10000)) {
                        // check to see if this transfer has been cancelled due to outside
                        // intervention such as updating the TransferTask record or the parent
                        // thread interrupting this one and setting the AbstractTransferTask.cancelled
                        // field to true
                        checkCancelled(listener);


                        callbackTime = System.currentTimeMillis();

                        listener.progressed(bytesSoFar);
                    }
                }
            }

            // update with the final transferred blocks and wrap the transfer.
            listener.progressed(bytesSoFar);

            // now replace the original with the patched temp file
            destClient.doRename(tmpFilename, destPath);

            listener.completed();

            // now update the aggregate task with the info from the child task
            TransferTask streamingTransferTask = (TransferTask)listener.getTransferTask();

            streamingTransferTask.setStatus(WRITE_COMPLETED);
            _doPublishEvent(RELAY_WRITE_COMPLETED.name(), streamingTransferTask.toJson());

            streamingTransferTask.setStatus(TransferStatusType.COMPLETED);
            _doPublishEvent(RELAY_WRITE_COMPLETED.name(), streamingTransferTask.toJson());

            // and we're spent
            log.debug(String.format(
                    "Completed streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())));



            return (TransferTask)listener.getTransferTask();
        }
        catch (ClosedByInterruptException e) {
            log.debug(String.format(
                    "Aborted streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(destClient.getClass()),
                    getProtocolForClass(destClient.getClass())), e);

            log.info("Transfer task " + listener.getTransferTask().getUuid() + " killed by worker shutdown.");
            setKilled(true);

            listener.progressed(bytesSoFar);
            listener.cancel();
            Thread.currentThread().interrupt();
            throw e;
        }
        catch (RemoteDataException | IOException e) {
            log.debug(String.format(
                    "Failed streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())), e);

            // transfer failed due to connectivity issue
            listener.failed();

            throw e;
        }
        catch (Throwable e) {
            log.debug(String.format(
                    "Failed streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())), e);

            // stuff happens, what are you going to do.
            listener.failed();

            throw new RemoteDataException("Transfer task " + listener.getTransferTask().getUuid()
                    + " failed.", e);
        }
        finally {
            if (listener.getTransferTask() != null) {
                log.info(String.format(
                        "Total of %s bytes transferred in task %s . Protocol %s => %s",
                        listener.getTransferTask().getBytesTransferred(),
                        listener.getTransferTask().getUuid(),
                        getProtocolForClass(sourceClient.getClass()),
                        getProtocolForClass(destClient.getClass())));
            }

            try {
                if (bis != null) bis.close();
            } catch (Throwable ignored) {
            }
            try {
                if (bos != null) bos.close();
            } catch (Throwable ignored) {
            }
            try {
                if (in != null) in.close();
            } catch (Throwable ignored) {
            }
            try {
                if (out != null) out.close();
            } catch (Throwable ignored) {
            }
        }
    }

    /**
     * Creates a default error message for use in exceptions saying
     * this {@link TransferTask} failed and optionally including the uuid.
     *
     * @return message
     */
    private String getDefaultErrorMessage(String srcPath, TransferTask transferTask) {
        return String.format(
                "Transfer %s cancelled while copying from source.",
                (transferTask == null ? "of " + srcPath : transferTask.getUuid()));
    }

    /**
     * Convenience method to get the output stream for the destination.
     * @param client remote data client to remote system
     * @param destPath Agave path to file on remote system
     * @return output stream to source
     * @throws IOException
     * @throws RemoteDataException
     */
    private RemoteOutputStream<?> getOutputStream(RemoteDataClient client, String destPath)
            throws IOException, RemoteDataException {
        try {
            return client.getOutputStream(destPath, true, false);
        } catch (Exception e) {
            // reauthenticate and retry in case of something weird
            client.disconnect();

            try {
                client.authenticate();
                return client.getOutputStream(destPath, true, true);
            } catch (RemoteDataException e1) {
                throw new RemoteDataException("Failed to open an output stream to " + destPath, e1);
            }
        }
    }

    /**
     * Convenience method to get the input stream for the destination.
     * @param client remote data client to remote system
     * @param srcPath Agave path to file on remote system
     * @return input stream from source
     * @throws IOException
     * @throws RemoteDataException
     */
    private RemoteInputStream<?> getInputStream(RemoteDataClient client, String srcPath) throws IOException, RemoteDataException {
        try {
            return client.getInputStream(srcPath, true);
        } catch (RemoteDataException e) {
            throw e;
        } catch (Exception e) {
            // reauthenticate and retry in case of something weird
            client.disconnect();
            client.authenticate();
            return client.getInputStream(srcPath, false);
        }
    }

    /**
     * This performs third party transfer only if source and destination urls
     * have a matching protocol that support third party transfers.
     *
     * @param srcPath agave source path of the file on the remote system
     * @param destPath the destination agave path of the transfer on the remote system
     * @param listener the listener to track the transfer info
     * @return the updated {@code aggregatedTransferTask} after processing
     * @throws RemoteDataException if an error occurred attempting the transfer
     * @throws IOException if unable to open a stream
     * @throws ClosedByInterruptException if the transfer was interrupted
     */
    protected TransferTask dothirdPartyTransfer(String srcPath, String destPath, RemoteTransferListenerImpl listener) throws RemoteDataException, IOException {
        try {
            log.debug(String.format(
                    "Beginning third party transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())));

            ((GridFTP) destClient).setProtectionBufferSize(16384);
            ((GridFTP) destClient).setType(GridFTPSession.TYPE_IMAGE);
            ((GridFTP) destClient).setMode(GridFTPSession.MODE_EBLOCK);
            ((GridFTP) destClient).setTCPBufferSize(destClient.getMaxBufferSize());

            ((GridFTP) sourceClient).setProtectionBufferSize(16384);
            ((GridFTP) sourceClient).setType(GridFTPSession.TYPE_IMAGE);
            ((GridFTP) sourceClient).setMode(GridFTPSession.MODE_EBLOCK);
            ((GridFTP) sourceClient).setTCPBufferSize(sourceClient.getMaxBufferSize());

//	        log.info("Enabling striped transfer.");
            ((GridFTP) sourceClient).setStripedActive(((GridFTP) destClient).setStripedPassive());

//	        if (task != null)
//	        {
//	            try {
//	          	   task.setTotalSize(sourceClient.length(srcPath));
//	            } catch (Exception e) {}
//
//	            task.setBytesTransferred(0);
//	            task.setAttempts(task.getAttempts() + 1);
//	            task.setStatus(TransferStatusType.TRANSFERRING);
//	            task.setStartTime(new Date());
//	            TransferTaskDao.persist(task);
//	        }

            if (((GridFTP) sourceClient).getHost().equals(((GridFTP) destClient).getHost())) {
                ((GridFTP) sourceClient).extendedTransfer(srcPath,
                        (GridFTP) destClient,
                        ((GridFTP) destClient).resolvePath(destPath),
                        listener);
            } else {
                ((GridFTP) sourceClient).extendedTransfer(srcPath,
                        (GridFTP) destClient,
                        ((GridFTP) destClient).resolvePath(destPath),
                        listener);
            }

            log.debug(String.format(
                    "Completed third party transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())));

            return (TransferTask)listener.getTransferTask();
        }
        catch (ClosedByInterruptException e) {
            log.debug(String.format(
                    "Aborted third party transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())));

            log.info("Transfer task " + listener.getTransferTask().getUuid() + " killed by worker shutdown.");
            setKilled(true);

            try {
                ((GridFTP) sourceClient).abort();
            } catch (Exception ignored) {
            }
            try {
                ((GridFTP) destClient).abort();
            } catch (Exception ignored) {
            }
            Thread.currentThread().interrupt();
            throw e;
        }
        catch (IOException e) {
            log.debug(String.format(
                    "Failed third party transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())));

            // transfer failed due to connectivity issue
            listener.failed();

            throw e;
        }
        catch (Throwable e) {
            log.debug(String.format(
                    "Failed third party transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())));

            // stuff happens, what are you going to do.
            listener.failed();

            throw new RemoteDataException("Transfer task " + listener.getTransferTask().getUuid()
                    + " failed.", e);
        }
        finally {
            if (listener != null && listener.getTransferTask() != null) {
                log.info(String.format(
                        "Total of %s bytes transferred in task %s . Protocol %s => %s",
                        listener.getTransferTask().getBytesTransferred(),
                        listener.getTransferTask().getUuid(),
                        getProtocolForClass(sourceClient.getClass()),
                        getProtocolForClass(destClient.getClass())));
            }
        }
    }

    /**
     * Returns a {@link RetryRequestManager} to ensure messages are retried before failure.
     * @return a retry request manager.
     */
    protected RetryRequestManager getRetryRequestManager() {
        return retryRequestManager;
    }

    /**
     * Handles event creation and delivery across the existing event bus. Retry is handled by the
     * {@link RetryRequestManager} up to 3 times. The call will be made asynchronously, so this method
     * will return immediately.
     *
     * @param eventName the name of the event. This doubles as the address in the request invocation.
     * @param body the message of the body. Currently only {@link JsonObject} are supported.
     */
    public void _doPublishEvent(String eventName, JsonObject body) {
        log.debug("_doPublishEvent({}, {})", eventName, body);
        getRetryRequestManager().request(eventName, body, 2);
    }
}
