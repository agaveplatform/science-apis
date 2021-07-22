/**
 *
 */
package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.channels.ClosedByInterruptException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;
import static org.agaveplatform.service.transfers.enumerations.TransferTaskEventType.STREAM_COPY_STARTED;

/**
 * Handles the copying of data between one {@link RemoteDataClient} and another. Situations where
 * server-side and third-party are handled as well as triage for relay vs streaming transfers. Transfer
 * tracking and notification listeners are managed through this class. Generally speaking, this is the
 * mechanism for which various QueueTask and AbstractWorkerActions will use to move data.
 * @author dooley
 *
 */
public class URLCopy{
    private static final Logger log = LoggerFactory.getLogger(URLCopy.class);
    private final RetryRequestManager retryRequestManager;
    private final RemoteDataClient sourceClient;
    private final RemoteDataClient destClient;

    private final Vertx vertx;
    private final AtomicBoolean killed = new AtomicBoolean(false);

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
     * @return the atomic killed variable
     */
    protected synchronized AtomicBoolean getKilled() {
        return this.killed;
    }


    /**
     * @return the killed boolean value
     */
    public synchronized boolean isKilled() {
        return getKilled().get();
    }

    /**
     * @param shouldBeKilled the killed to set
     */
    public synchronized void setKilled(boolean shouldBeKilled) {
        getKilled().set(shouldBeKilled);

        if ((getSourceClient() instanceof GridFTP) && (getDestClient() instanceof GridFTP) && !shouldBeKilled) {
            try {
                ((GridFTP) getSourceClient()).abort();
            } catch (Exception ignored) {}

            try {
                ((GridFTP) getDestClient()).abort();
            } catch (Exception ignored) {}

            Thread.currentThread().interrupt();
        }
    }

    /**
     * Threadsafe check for killed copy command either through the thread being explicitly killed or the transfertask
     * status being set to cancelled.
     * @param listener the {@link RemoteTransferListener} observing the transfer
     * @throws ClosedByInterruptException if the transfer has been transferred
     */
    protected void checkCancelled(RemoteTransferListener listener) throws ClosedByInterruptException {
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
     * @param transferTask the transferTask to copy
     * @return the updated transferTask
     * @throws RemoteDataException
     * @throws IOException
     * @throws TransferException
     * @throws InterruptedException
     */
    public TransferTask copy(TransferTask transferTask)
            throws RemoteDataException, RemoteDataSyntaxException, IOException, TransferException, InterruptedException {
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
     * @throws InterruptedException
     */
    public TransferTask copy(TransferTask transferTask, List<String> exclusions)
            throws RemoteDataException, RemoteDataSyntaxException, IOException, TransferException, InterruptedException {
        log.debug("UrlCopy.copy method. {}", transferTask.getUuid());
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

        try {
            RemoteTransferListenerImpl listener = null;

            // don't copy the redundant destPath or parent of destPath returned from unix style listings dir
            // to avoid infinite loops and full file system copies.
            if (List.of(".", "..").contains(FilenameUtils.getName(srcPath))) {
                return transferTask;
            }

            // source and dest are the same host, so do a server-side copy
            if (getSourceClient().equals(getDestClient())) {
                listener = getRemoteUnaryTransferListenerForTransferTask(transferTask);
                // should be able to do a relay transfer here just as easily
                File destFile = new File(destPath);
                File parentPath = destFile.getParentFile();
                if (!parentPath.exists()){
                    parentPath.mkdirs();
                }

                getSourceClient().copy(srcPath, destPath, listener);
                transferTask = (TransferTask)listener.getTransferTask();
            }
            // delegate to third-party transfer if supported
            else if (getSourceClient().isThirdPartyTransferSupported() &&
                    getDestClient().isThirdPartyTransferSupported() &&
                    getSourceClient().getClass().equals(getDestClient().getClass())) {

                listener = getRemoteTransferListenerForTransferTask(transferTask);
                transferTask = dothirdPartyTransfer(srcPath, destPath, listener);
            }
            // otherwise, we're doing the heavy lifting ourselves
            else {

                try {
                    double srcFileLength = getSourceClient().length(srcPath);
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
                            transferTask = streamingTransfer(srcPath, destPath, getRemoteStreamingTransferListenerForTransferTask(transferTask));
                        }
                    }
                    // only streaming transfers are supported at this point, so carry on with those.
                    else {
                        transferTask = streamingTransfer(srcPath, destPath, getRemoteStreamingTransferListenerForTransferTask(transferTask));
                    }
                } catch (ClosedByInterruptException e) {
                    throw e;
                } catch (Throwable e) {
                    log.error("Exception Throwable {}", e.getMessage(), e.getCause());
                    throw e;
                }
            }

            return transferTask;
        } finally {
            try {
                if (getDestClient().isPermissionMirroringRequired()) {
                    getDestClient().setOwnerPermission(getDestClient().getUsername(), destPath, true);
                    getDestClient().setOwnerPermission(transferTask.getOwner(), destPath, true);
                }
            } catch (Exception e) {
                log.error("Failed to set permissions on " + getDestClient().getHost() + " for user " + transferTask.getOwner(), e);
            }
        }
    }

    /**
     * Creates a new {@link RemoteTransferListenerImpl} for the {@code transferTask}. This is used when tracking both
     * sides of a two-step transfer with a single listener.
     * @param transferTask the task whose used to create a new streaming listener
     * @return a streaming listener for the {@code transferTask}
     */
    public RemoteTransferListenerImpl getRemoteTransferListenerForTransferTask(TransferTask transferTask) {
        return new RemoteTransferListenerImpl(transferTask, getVertx(), getRetryRequestManager());
    }

    /**
     * Creates a new {@link RemoteStreamingTransferListenerImpl} for the {@code transferTask}.
     * @param transferTask the task used to create a new streaming listener
     * @return a streaming listener for the {@code transferTask}
     */
    public RemoteStreamingTransferListenerImpl getRemoteStreamingTransferListenerForTransferTask(TransferTask transferTask) {
        return new RemoteStreamingTransferListenerImpl(transferTask, getVertx(), getRetryRequestManager());
    }

    /**
     * Creates a new {@link RemoteUnaryTransferListenerImpl} for the {@link TransferTask#getDest()}. This is used
     * when only one half of a transfer is being tracked. ie. fetching a remote file locally. Another, distinct,
     * listener would then be created to track the destination transfer.
     * @param transferTask the task used to create a new listener
     * @return a unary listener for the {@code transferTask} destination
     */
    public RemoteUnaryTransferListenerImpl getRemoteUnaryTransferListenerForTransferTask(TransferTask transferTask) {
        return new RemoteUnaryTransferListenerImpl(transferTask, getVertx(), getRetryRequestManager());
    }

    /**
     * Proxies a file/folder transfer from source to destination by using the underlying
     * {@link RemoteDataClient#(String, String, RemoteTransferListenerImpl )} and {@link RemoteDataClient#(String, String, RemoteTransferListenerImpl )}
     * methods to stage the data to the local host, then push to the destination system.
     * This can be significantly faster than the standard {@link #streamingTransfer(String, String, RemoteStreamingTransferListenerImpl)}
     * method when the underlying protocols support parallelism and/or threading. Care must
     * be taken with this approach to properly check that there is available disk space to
     * perform the copy.
     *
     * @param srcPath path on the src system to move to the destPath
     * @param destPath path on the dest system where the data will be copied.
     * @param aggregateTransferTask the top level TransferTask tracking the aggregate data movement
     * @return the updated {@code aggregatedTransferTask} after processing
     * @throws RemoteDataException if an error occurred attempting the transfer
     * @throws InterruptedException if the transfer was interrupted
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
            if (getSourceClient() instanceof Local) {
                tmpFile = new File(getSourceClient().resolvePath(srcPath));
                tempDir = tmpFile.getParentFile();

                log.debug(String.format(
                        "Skipping first leg of relay transfer for task %s. %s to %s . Protocol: %s => %s",
                        aggregateTransferTask.getUuid(),
                        aggregateTransferTask.getSource(),
                        "file://" + tmpFile.getPath(),
                        getProtocolForClass(getDestClient().getClass()),
                        "local"));
            }
            else {
                tempDir = new File(org.iplantc.service.common.Settings.TEMP_DIRECTORY,
                        DigestUtils.md5Hex(srcPath) + "-" + System
                                .currentTimeMillis() + ".relay.tmp");

                if (getDestClient() instanceof Local) {
                    tmpFile = new File(getDestClient().resolvePath(destPath));
                    tempDir = tmpFile.getParentFile();
                } else {
                    tempDir.mkdirs();
                    tmpFile = new File(tempDir, FilenameUtils.getName(srcPath));
                }

                log.debug(String.format(
                        "Beginning first leg of relay transfer for task %s. %s to %s . Protocol: %s => %s",
                        aggregateTransferTask.getUuid(),
                        aggregateTransferTask.getSource(),
                        tmpFile.toURI(),
                        getProtocolForClass(getSourceClient().getClass()),
                        "local"));
                try {
                    // we create a subtask local to this method to track each end of the relay transfer.
                    //
                    srcChildTransferTask = new TransferTask(
                            aggregateTransferTask.getSource(),
                            tmpFile.toURI().toString(),
                            aggregateTransferTask.getOwner(),
                            aggregateTransferTask.getUuid(),
                            aggregateTransferTask.getRootTaskId());

                    srcChildTransferTask.setTenantId(aggregateTransferTask.getTenantId());
                    srcChildTransferTask.setStatus(READ_STARTED);

                    srcChildRemoteTransferListener =
                            getRemoteUnaryTransferListenerForTransferTask(srcChildTransferTask);

                    // perform the get
                    getSourceClient().get(srcPath, tmpFile.getPath(), srcChildRemoteTransferListener);

                    srcChildTransferTask = (TransferTask)srcChildRemoteTransferListener.getTransferTask();
                    aggregateTransferTask.setStartTime(srcChildTransferTask.getStartTime());
                    aggregateTransferTask.setStatus(srcChildTransferTask.getStatus());

                    // cancelled status will be handled in the calling function.
                    if (!isKilled()) {
                        aggregateTransferTask.setStatus(READ_COMPLETED);
                    }

                    // must be in here as the LOCAL files will not have a src transfer listener associated with them.
                    checkCancelled(srcChildRemoteTransferListener);

                }
                catch (RemoteDataException e) {
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
                            getProtocolForClass(getSourceClient().getClass()),
                            "local"), e);
                    throw e;
                } catch (ClosedByInterruptException e){
                    log.debug(String.format(
                            "Aborted relay transfer for task %s. %s to %s . Protocol: %s => %s",
                            aggregateTransferTask.getUuid(),
                            aggregateTransferTask.getSource(),
                            aggregateTransferTask.getDest(),
                            getProtocolForClass(getSourceClient().getClass()),
                            getProtocolForClass(getDestClient().getClass())), e);

                    srcChildRemoteTransferListener.cancel();
                    srcChildTransferTask = (TransferTask)srcChildRemoteTransferListener.getTransferTask();
                    aggregateTransferTask = updateAggregateTaskFromChildTask(aggregateTransferTask, srcChildTransferTask);
                    setKilled(true);
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
                            getProtocolForClass(getSourceClient().getClass()),
                            "local"), e);
                    // stuff happens, what are you going to do.
                    throw new RemoteDataException("Transfer failed from " + getSourceClient().getUriForPath(srcPath), e);
                }
            }

            if (!((getSourceClient() instanceof Local) && (getDestClient() instanceof Local))) {
                try {
                    log.debug(String.format(
                            "Beginning second leg of relay transfer for task %s. %s to %s . Protocol: %s => %s",
                            aggregateTransferTask.getUuid(),
                            tmpFile.toURI(),
                            aggregateTransferTask.getDest(),
                            "local",
                            getProtocolForClass(getDestClient().getClass())));

                    destChildTransferTask = new TransferTask(
                            tmpFile.toURI().toString(),
                            aggregateTransferTask.getDest(),
                            aggregateTransferTask.getOwner(),
                            aggregateTransferTask.getParentTaskId(),
                            aggregateTransferTask.getRootTaskId());
                    destChildTransferTask.setTenantId(aggregateTransferTask.getTenantId());
                    destChildTransferTask.setStatus(WRITE_STARTED);

                    destChildRemoteTransferListener =
                            getRemoteUnaryTransferListenerForTransferTask(destChildTransferTask);

                    getDestClient().put(tmpFile.getPath(), destPath, destChildRemoteTransferListener);

                    if (!isKilled()) {
                        destChildTransferTask = (TransferTask)destChildRemoteTransferListener.getTransferTask();
                        // now update the aggregate task with the info from the child task
                        aggregateTransferTask = updateAggregateTaskFromChildTask(aggregateTransferTask, destChildTransferTask);

                        //transfer updates are handled through the listener
                    }

                    checkCancelled(destChildRemoteTransferListener);
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
                            getProtocolForClass(getDestClient().getClass())), e);
                    throw e;
                } catch (ClosedByInterruptException e){
                    log.debug(String.format(
                            "Aborted relay transfer for task %s. %s to %s . Protocol: %s => %s",
                            aggregateTransferTask.getUuid(),
                            aggregateTransferTask.getSource(),
                            aggregateTransferTask.getDest(),
                            getProtocolForClass(getSourceClient().getClass()),
                            getProtocolForClass(getDestClient().getClass())), e);

                    destChildRemoteTransferListener.cancel();
                    destChildTransferTask = (TransferTask)destChildRemoteTransferListener.getTransferTask();
                    aggregateTransferTask = updateAggregateTaskFromChildTask(aggregateTransferTask, destChildTransferTask);
                    setKilled(true);
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
                            getProtocolForClass(getDestClient().getClass())));
                    throw new RemoteDataException("Transfer failed to " + getSourceClient().getUriForPath(srcPath) +
                            " using " + getDestClient().getClass().getSimpleName(), e);
                }
            } else {
                log.debug(String.format(
                        "Skipping second leg of relay transfer for task %s. %s to %s. Protocol: %s => %s",
                        aggregateTransferTask.getUuid(),
                        "file://" + tmpFile.getPath(),
                        aggregateTransferTask.getDest(),
                        "local",
                        getProtocolForClass(getDestClient().getClass())));

                destChildTransferTask = new TransferTask(
                        tmpFile.toURI().toString(),
                        aggregateTransferTask.getDest(),
                        aggregateTransferTask.getOwner(),
                        aggregateTransferTask.getParentTaskId(),
                        aggregateTransferTask.getRootTaskId());
                destChildTransferTask.setTenantId(aggregateTransferTask.getTenantId());
                destChildTransferTask.setStartTime(aggregateTransferTask.getStartTime());
                destChildTransferTask.setStatus(WRITE_STARTED);

                destChildRemoteTransferListener =
                        getRemoteTransferListenerForTransferTask(destChildTransferTask);

                long tmpFileLength = tmpFile.length();

                // trigger the lifecycle of the transfer as if it were a remote copy. This preserves the child event
                // lifecycle
                destChildRemoteTransferListener.started(tmpFileLength, tmpFile.getPath());
                destChildRemoteTransferListener.progressed(tmpFileLength);
                destChildRemoteTransferListener.completed();

                // now update the aggregate task with the info from the child task
                destChildTransferTask = (TransferTask) destChildRemoteTransferListener.getTransferTask();
                aggregateTransferTask = updateAggregateTaskFromChildTask(aggregateTransferTask, destChildTransferTask);

                //transfer updates are handled through the listener
            }
        }
        catch (ClosedByInterruptException e) {
            log.debug(String.format(
                    "Aborted relay transfer for task %s. %s to %s . Protocol: %s => %s",
                    aggregateTransferTask.getUuid(),
                    aggregateTransferTask.getSource(),
                    aggregateTransferTask.getDest(),
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())), e);
            killCopyTask();
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
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())));

            if (getSourceClient() instanceof Local) {
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
     * @param serializedUri the url from which to get the system id
     * @return the hostname of the URI
     * @deprecated
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
     * @throws InterruptedException if the transfer was interrupted
     */
    protected TransferTask streamingTransfer(String srcPath, String destPath, RemoteStreamingTransferListenerImpl listener)
            throws RemoteDataException, IOException, InterruptedException {
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
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())));

            long totalSize = getSourceClient().length(srcPath);

            // Buffer the input stream only if it's not already buffered.
            try {
                in = getInputStream(getSourceClient(), srcPath);
            } catch (Exception e) {
                log.error("Unable to get input stream for " + getSourceClient().getUsername() +
                        " on host " + getSourceClient().getHost() + " for path " + srcPath + ".");
                throw e;
            }
            if (in.isBuffered()) bis = in;
            else bis = new BufferedInputStream(in);

            checkCancelled(listener);

            // Buffer the output stream only if it's not already buffered.
            try {
                out = getOutputStream(getDestClient(), destPath);
            } catch (Exception e) {
                log.error("Unable to get output stream for " + getDestClient().getUsername() +
                        " on host " + getDestClient().getHost() + " for path " + destPath + ".");
                throw e;
            }
            if (out.isBuffered()) bos = out;
            else bos = new BufferedOutputStream(out);

            checkCancelled(listener);

            int length = 0;
            long callbackTime = System.currentTimeMillis();
            int bufferSize = Math.min(getSourceClient().getMaxBufferSize(), getDestClient().getMaxBufferSize());
            byte[] b = new byte[bufferSize];

            listener.started(totalSize, srcPath);

            while ((length = bis.read(b, 0, bufferSize)) != -1) {
                bytesSoFar += length;

                bos.write(b, 0, length);


                // update the progress every 10 seconds buffer cycle. This reduced the impact
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

            log.debug(String.format(
                    "Completed streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())));

            return (TransferTask)listener.getTransferTask();

        }
        catch (ClosedByInterruptException e) {
            log.debug(String.format(
                    "Aborted streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(getDestClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())), e);

            log.info("Transfer task " + listener.getTransferTask().getUuid() + " killed by worker shutdown.");

            listener.progressed(bytesSoFar);
            listener.cancel();

            throw e;
        }
        catch (RemoteDataException | IOException e) {
            log.debug(String.format(
                    "Failed streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())), e);

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
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())), e);

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
                        getProtocolForClass(getSourceClient().getClass()),
                        getProtocolForClass(getDestClient().getClass())));
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
     * @throws InterruptedException
     */
    public TransferTask copyRange(String srcPath, long srcRangeOffset, long srcRangeSize,
                                  String destPath, long destRangeOffset, TransferTask transferTask)
            throws RemoteDataException, IOException, TransferException, InterruptedException {
        if (transferTask == null) {
            throw new TransferException("TransferTask cannot be null. Please provide"
                    + "a valid transfer task to track this operation.");
        } else if (transferTask.getId() == null) {
            throw new TransferException("TransferTask does not have a valid id. "
                    + "Please persiste the transfer taks and attempt the operation again.");
        }

        try {
            // if we are transferring a directory
            if (getSourceClient().isDirectory(srcPath)) {
                throw new TransferException("Range transfers are not supported on directories");
            } else {

                RemoteTransferListenerImpl listener = getRemoteTransferListenerForTransferTask(transferTask);

                if (StringUtils.equals(FilenameUtils.getName(srcPath), ".") ||
                        StringUtils.equals(FilenameUtils.getName(srcPath), "..")) {
                    // skip current directory and parent to avoid infinite loops and
                    // full file system copies.
                }
                else {
                    RangeValidator sourceRangeValidator = new RangeValidator(srcRangeOffset, srcRangeSize, getSourceClient().length(srcPath));
                    RangeValidator destRangeValidator = new RangeValidator(destRangeOffset, Range.SIZE_MAX, getSourceClient().length(srcPath));

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
                if (getDestClient().isPermissionMirroringRequired()) {
                    getDestClient().setOwnerPermission(getDestClient().getUsername(), destPath, true);
                    getDestClient().setOwnerPermission(transferTask.getOwner(), destPath, true);
                }
            } catch (Exception e) {
                log.error("Failed to set permissions on " + getDestClient().getHost() + " for user " + transferTask.getOwner(), e);
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
     * @throws InterruptedException if the transfer was interrupted
     */
    protected TransferTask proxyRangeTransfer(String srcPath, long srcRangeOffset, long srcRangeSize,
                                      String destPath, long destRangeOffset, RemoteTransferListenerImpl listener)
            throws RemoteDataException, IOException, InterruptedException {

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
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())));

            listener.getTransferTask().setStatusString(STREAM_COPY_STARTED.name());
            _doPublishEvent(TRANSFER_STREAMING, ((TransferTask)listener.getTransferTask()).toJson());

            if (getSourceClient().isDirectory(srcPath)) {
                throw new RemoteDataException("Cannot perform range query on directories");
            }

            long totalSize = srcRangeSize;
            if (totalSize == Range.SIZE_MAX) {
                totalSize = getSourceClient().length(srcPath) - srcRangeOffset;
            }

            // Buffer the input stream only if it's not already buffered.
            try {
                in = getInputStream(getSourceClient(), srcPath);
            } catch (Exception e) {
                log.error("Unable to get input stream for " + getSourceClient().getUsername() +
                        " on host " + getSourceClient().getHost() + " for path " + srcPath + ".");
                throw e;
            }
            in.skip(srcRangeOffset);
            if (in.isBuffered()) bis = in;
            else bis = new BufferedInputStream(in);

            // Buffer the original input stream only if it's not already buffered.
            try {
                originalIn = getInputStream(getDestClient(), destPath);
            } catch (Exception e) {
                log.error("Unable to get input stream for " + getDestClient().getUsername() +
                        " on host " + getDestClient().getHost() + " for path " + destPath + ".");
                throw e;
            }
            if (originalIn.isBuffered()) originalBis = originalIn;
            else originalBis = new BufferedInputStream(originalIn);

            // Buffer the output stream only if it's not already buffered.
            String tmpFilename = destPath + ".tmp-" + System.currentTimeMillis();
            try {
                out = getOutputStream(getDestClient(), tmpFilename);
            } catch (Exception e) {
                log.error("Unable to get output stream for " + getDestClient().getUsername() +
                        " on host " + getDestClient().getHost() + " for path " + tmpFilename + ".");
                throw e;
            }

            if (out.isBuffered()) bos = out;
            else bos = new BufferedOutputStream(out);

            checkCancelled(listener);

            int length = 0;
            long remainingOffset = destRangeOffset;

            long callbackTime = System.currentTimeMillis();
            int bufferSize = getSourceClient().getMaxBufferSize();
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
            getDestClient().doRename(tmpFilename, destPath);

            listener.completed();

            // now update the aggregate task with the info from the child task
            TransferTask streamingTransferTask = (TransferTask)listener.getTransferTask();

            streamingTransferTask.setStatus(WRITE_COMPLETED);
            _doPublishEvent(TRANSFER_COMPLETED, streamingTransferTask.toJson());

            streamingTransferTask.setStatus(TransferStatusType.COMPLETED);
            _doPublishEvent(TRANSFERTASK_FINISHED, streamingTransferTask.toJson());

            // and we're spent
            log.debug(String.format(
                    "Completed streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())));



            return (TransferTask)listener.getTransferTask();
        }
        catch (InterruptedException e) {
            log.debug(String.format(
                    "Aborted streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(getDestClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())), e);

            log.info("Transfer task " + listener.getTransferTask().getUuid() + " killed by worker shutdown.");
            setKilled(true);

            listener.progressed(bytesSoFar);
            listener.cancel();
            //Thread.currentThread().interrupt();
            throw e;
        }
        catch (RemoteDataException | IOException e) {
            log.debug(String.format(
                    "Failed streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())), e);

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
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())), e);

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
                        getProtocolForClass(getSourceClient().getClass()),
                        getProtocolForClass(getDestClient().getClass())));
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
     * @throws InterruptedException if the transfer was interrupted
     */
    protected TransferTask dothirdPartyTransfer(String srcPath, String destPath, RemoteTransferListenerImpl listener) throws RemoteDataException, IOException, InterruptedException{
        try {
            log.debug(String.format(
                    "Beginning third party transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())));

            ((GridFTP) getDestClient()).setProtectionBufferSize(16384);
            ((GridFTP) getDestClient()).setType(GridFTPSession.TYPE_IMAGE);
            ((GridFTP) getDestClient()).setMode(GridFTPSession.MODE_EBLOCK);
            ((GridFTP) getDestClient()).setTCPBufferSize(getDestClient().getMaxBufferSize());

            ((GridFTP) getSourceClient()).setProtectionBufferSize(16384);
            ((GridFTP) getSourceClient()).setType(GridFTPSession.TYPE_IMAGE);
            ((GridFTP) getSourceClient()).setMode(GridFTPSession.MODE_EBLOCK);
            ((GridFTP) getSourceClient()).setTCPBufferSize(getSourceClient().getMaxBufferSize());

	        log.debug("Starting third-party striped gridftp transfer.");
	        // no way to gracefully interrupt this if the copy operation is cancelled. The
            ((GridFTP) getSourceClient()).setStripedActive(((GridFTP) getDestClient()).setStripedPassive());

//	        if (task != null)
//	        {
//	            try {
//	          	   task.setTotalSize(getSourceClient().length(srcPath));
//	            } catch (Exception e) {}
//
//	            task.setBytesTransferred(0);
//	            task.setAttempts(task.getAttempts() + 1);
//	            task.setStatus(TransferStatusType.TRANSFERRING);
//	            task.setStartTime(new Date());
//	            TransferTaskDao.persist(task);
//	        }

            if (getSourceClient().getHost().equals(getDestClient().getHost())) {
                ((GridFTP) getSourceClient()).extendedTransfer(srcPath,
                        (GridFTP) getDestClient(),
                        getDestClient().resolvePath(destPath),
                        listener);
            } else {
                ((GridFTP) getSourceClient()).extendedTransfer(srcPath,
                        (GridFTP) getDestClient(),
                        getDestClient().resolvePath(destPath),
                        listener);
            }

            log.debug(String.format(
                    "Completed third party transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())));

            return (TransferTask)listener.getTransferTask();
        }
        catch (ClosedByInterruptException e) {
            log.debug(String.format(
                    "Aborted third party transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())));

            log.info("Transfer task " + listener.getTransferTask().getUuid() + " killed by external request.");
            setKilled(true);

            //Thread.currentThread().interrupt();
            throw e;
        }
        catch (IOException e) {
            log.debug(String.format(
                    "Failed third party transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())));

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
                    getProtocolForClass(getSourceClient().getClass()),
                    getProtocolForClass(getDestClient().getClass())));

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
                        getProtocolForClass(getSourceClient().getClass()),
                        getProtocolForClass(getDestClient().getClass())));
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
    public void _doPublishEvent(String eventName, JsonObject body) throws IOException, InterruptedException {
        log.debug("_doPublishEvent({}, {})", eventName, body);
        getRetryRequestManager().request(eventName, body, 2);
    }


    /**
     * Update aggregate task info from the child task
     * @return updated {@link TransferTask}
     */
    public TransferTask updateAggregateTaskFromChildTask(TransferTask aggregateTransferTask, TransferTask childTransferTask){
        aggregateTransferTask.setBytesTransferred(childTransferTask.getBytesTransferred());
        aggregateTransferTask.setTotalFiles(childTransferTask.getTotalFiles());
        aggregateTransferTask.setTotalSkippedFiles(childTransferTask.getTotalSkippedFiles());
        aggregateTransferTask.setTotalSize(childTransferTask.getTotalSize());
        aggregateTransferTask.setAttempts(childTransferTask.getAttempts());
        aggregateTransferTask.setStartTime(childTransferTask.getStartTime());
        aggregateTransferTask.setEndTime(childTransferTask.getEndTime());
        aggregateTransferTask.updateTransferRate();
        aggregateTransferTask.setLastUpdated(childTransferTask.getLastUpdated());
        aggregateTransferTask.setStatus(childTransferTask.getStatus());
        return aggregateTransferTask;
    }

    /**
     * Kill the current thread for TransferTaskCancel event
     */
    public void killCopyTask(){
        //Thread.currentThread().interrupt();
        setKilled(true);
    }
}
