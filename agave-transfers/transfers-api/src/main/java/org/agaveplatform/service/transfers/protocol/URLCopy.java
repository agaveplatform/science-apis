/**
 *
 */
package org.agaveplatform.service.transfers.protocol;

import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.globus.ftp.GridFTPSession;
import org.iplantc.service.transfer.*;
import org.iplantc.service.transfer.exceptions.RangeValidationException;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.gridftp.GridFTP;
import org.iplantc.service.transfer.local.Local;
import org.iplantc.service.transfer.model.Range;

import java.io.*;
import java.net.URI;
import java.nio.channels.ClosedByInterruptException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles the copying of data between one {@link RemoteDataClient} and another. Situations where
 * server-side and third-party are handled as well as triage for relay vs streaming transfers. Transfer
 * tracking and notification listeners are managed through this class. Generally speaking, this is the
 * mechanism for which various QueueTask and AbstractWorkerActions will use to move data.
 * @author dooley
 *
 */
public class URLCopy {
    private static Logger log = Logger.getLogger(URLCopy.class);
    private RemoteDataClient sourceClient;
    private RemoteDataClient destClient;
    private AtomicBoolean killed = new AtomicBoolean(false);

    public URLCopy(RemoteDataClient sourceClient, RemoteDataClient destClient) {
        this.sourceClient = sourceClient;
        this.destClient = destClient;
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
     * via a {@link RemoteTransferListener}
     *
     * @param srcPath
     * @param destPath
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
     * via a {@link RemoteTransferListener}
     *
     * @param srcPath
     * @param destPath
     * @param transferTask the reference transfer task
     * @param exclusions blacklist of paths relative to {@code srcPath} not to copy
     * @throws RemoteDataException
     * @throws IOException
     * @throws TransferException
     * @throws ClosedByInterruptException
     */
    public TransferTask copy(TransferTask transferTask, List<String> exclusions)
            throws RemoteDataException, RemoteDataSyntaxException, IOException, TransferException, ClosedByInterruptException {
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

        try {
            String srcPath = URI.create(transferTask.getSource()).getPath();
            String destPath = URI.create(transferTask.getDest()).getPath();

            // we're transferring a file

            // don't copy the redundant destPath or parent of destPath returned from unix style listings dir
            // to avoid infinite loops and full file system copies.
            if (List.of(".", "..").contains(FilenameUtils.getName(srcPath))) {
                return transferTask;
            }

            // source and dest are the same host, so do a server-side copy
            if (sourceClient.equals(destClient)) {

                transferTask.setStartTime(Instant.now());

                sourceClient.copy(srcPath, destPath);

                transferTask.setEndTime(Instant.now());
                transferTask.setTotalSize(len);
                transferTask.setBytesTransferred(len);
                transferTask.setLastUpdated(transferTask.getEndTime());
            }
            // delegate to third-party transfer if supported
            else if (sourceClient.isThirdPartyTransferSupported() &&
                    destClient.isThirdPartyTransferSupported() &&
                    sourceClient.getClass().equals(destClient.getClass())) {
                dothirdPartyTransfer(srcPath, destPath);
            }
            // otherwise, we're doing the heavy lifting ourselves
            else {
//                    listener = new RemoteTransferListener(transferTask);

                try {

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

                            relayTransfer(srcPath, destPath, transferTask);
                        } else {
                            log.debug("Local disk has insufficient space (" + availableBytes +
                                    " < " + srcFileLength + ") for relay transfer of transfer task "
                                    + transferTask.getUuid() + ". Switching to streaming transfer instead.");
                            streamingTransfer(srcPath, destPath);
                        }
                    }
                    // only streaming transfers are supported at this point, so carry on with those.
                    else {
                        streamingTransfer(srcPath, destPath);
                    }

                    if (transferTask != null) {
                        if (isKilled()) {
                            transferTask.setStatus(TransferStatusType.CANCELLED);
                        } else {
                            transferTask.setStatus(TransferStatusType.COMPLETED);
                        }

                        transferTask.setEndTime(Instant.now());

                        TransferTaskDao.persist(transferTask);
                    }
                } catch (ClosedByInterruptException e) {
                    try {
                        TransferTaskDao.cancelAllRelatedTransfers(transferTask.getId());
                    } catch (TransferException e1) {
                        throw new RemoteDataException("Failed to cancel related transfer tasks.", e1);
                    } finally {
                        Thread.currentThread().interrupt();
                    }
                    throw e;
                } catch (TransferException e) {
                    throw new RemoteDataException("Failed to udpate transfer record.", e);
                }
            }

            return listener.getTransferTask();
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
     * Proxies a file/folder transfer from source to destination by using the underlying
     * {@link RemoteDataClient#get(String, String, RemoteTransferListener)} and {@link RemoteDataClient#put(String, String, RemoteTransferListener)}
     * methods to stage the data to the local host, then push to the destination system.
     * This can be significantly faster than the standard {@link #streamingTransfer(String, String, RemoteTransferListener)}
     * method when the underlying protocols support parallelism and/or threading. Care must
     * be taken with this approach to properly check that there is available disk space to
     * perform the copy.
     *
     * @param srcPath path on the src system to move to the destPath
     * @param destPath path on the dest system where the data will be copied.
     * @param aggregateTransferTask the top level TransferTask tracking the aggregate data movement
     * @throws RemoteDataException
     * @throws ClosedByInterruptException
     */
    protected void relayTransfer(String srcPath, String destPath, TransferTask aggregateTransferTask)
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
                    sourceClient.get(srcPath, tmpFile.getPath(),
                            srcChildRemoteTransferListener);


                    aggregateTransferTask.updateSummaryStats(srcChildTransferTask);

                    if (isKilled()) {
                        srcChildTransferTask.setStatus(TransferStatusType.CANCELLED);
                    } else {
                        srcChildTransferTask.setStatus(TransferStatusType.COMPLETED);
                    }

                    srcChildTransferTask.setEndTime(new Date());

                    // must be in here as the LOCAL files will not have a src transfer listener associated with them.
                    checkCancelled(srcChildRemoteTransferListener);

                } catch (RemoteDataException e) {

                    try {
                        if (srcChildTransferTask != null) {
                            srcChildTransferTask.setStatus(TransferStatusType.FAILED);
                            srcChildTransferTask.setEndTime(new Date());
                            TransferTaskDao.updateProgress(srcChildTransferTask);
                        }
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
                        if (srcChildTransferTask != null) {
                            srcChildTransferTask.setStatus(TransferStatusType.FAILED);
                            srcChildTransferTask.setEndTime(new Date());
                            TransferTaskDao.updateProgress(srcChildTransferTask);
                        }
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
                            aggregateTransferTask,
                            aggregateTransferTask);

                    TransferTaskDao.persist(destChildTransferTask);

                    destChildRemoteTransferListener = new RemoteTransferListener(destChildTransferTask);

                    destClient.put(tmpFile.getPath(), destPath,
                            destChildRemoteTransferListener);

                    destChildTransferTask = destChildRemoteTransferListener.getTransferTask();

                    aggregateTransferTask.updateSummaryStats(destChildTransferTask);

                    if (isKilled()) {
                        destChildTransferTask.setStatus(TransferStatusType.CANCELLED);
                    } else {
                        destChildTransferTask.setStatus(TransferStatusType.COMPLETED);
                    }

                    destChildTransferTask.setEndTime(new Date());

                    TransferTaskDao.updateProgress(destChildTransferTask);

                } catch (RemoteDataException e) {
                    try {
                        if (destChildTransferTask != null) {
                            destChildTransferTask.setStatus(TransferStatusType.FAILED);
                            destChildTransferTask.setEndTime(new Date());
                            TransferTaskDao.updateProgress(destChildTransferTask);
                        }
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
                        if (destChildTransferTask != null) {
                            destChildTransferTask.setStatus(TransferStatusType.FAILED);
                            destChildTransferTask.setEndTime(new Date());
                            TransferTaskDao.updateProgress(destChildTransferTask);
                        }
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
                        aggregateTransferTask,
                        aggregateTransferTask);

                destChildTransferTask.setStartTime(destChildTransferTask.getCreated());
                destChildTransferTask.setEndTime(destChildTransferTask.getCreated());
                destChildTransferTask.setBytesTransferred(0);
                destChildTransferTask.setAttempts(0);
                destChildTransferTask.setLastUpdated(destChildTransferTask.getCreated());
                if (srcChildTransferTask != null) {
                    destChildTransferTask.setTotalFiles(srcChildTransferTask.getTotalFiles());
                    destChildTransferTask.setTotalSize(srcChildTransferTask.getTotalSize());
                    destChildTransferTask.setTotalSkippedFiles(srcChildTransferTask.getTotalSkippedFiles());
                    destChildTransferTask.setTransferRate(srcChildTransferTask.getTransferRate());
                } else {
                    destChildTransferTask.setTotalFiles(1);
                    destChildTransferTask.setTotalSize(tmpFile.length());
                    destChildTransferTask.setTotalSkippedFiles(0);
                    destChildTransferTask.setTransferRate(0);
                }

                TransferTaskDao.persist(destChildTransferTask);

                aggregateTransferTask.updateSummaryStats(destChildTransferTask);
            }
        } catch (ClosedByInterruptException e) {
            log.debug(String.format(
                    "Aborted relay transfer for task %s. %s to %s . Protocol: %s => %s",
                    aggregateTransferTask.getUuid(),
                    aggregateTransferTask.getSource(),
                    aggregateTransferTask.getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())), e);
            Thread.currentThread().interrupt();
            throw e;
        } catch (RemoteDataException e) {
            try {
                aggregateTransferTask.setEndTime(new Date());
                aggregateTransferTask.setStatus(TransferStatusType.FAILED);
                TransferTaskDao.persist(aggregateTransferTask);
            } catch (TransferException e1) {
                log.error("Failed to update parent transfer task "
                        + aggregateTransferTask.getUuid() + " status to FAILED", e1);
            }

//			checkCancelled(remoteTransferListener);

            throw e;
        } catch (Exception e) {
            try {
                aggregateTransferTask.setEndTime(new Date());
                aggregateTransferTask.setStatus(TransferStatusType.FAILED);
                TransferTaskDao.persist(aggregateTransferTask);
            } catch (TransferException e1) {
                log.error("Failed to update parent transfer task "
                        + aggregateTransferTask.getUuid() + " status to FAILED", e1);
            }

//			checkCancelled(remoteTransferListener);

            throw new RemoteDataException(
                    getDefaultErrorMessage(
                            srcPath, new RemoteTransferListener(aggregateTransferTask)), e);
        } finally {
            if (aggregateTransferTask != null) {
                log.info(String.format(
                        "Total of %s bytes transferred in task %s . Protocol %s => %s",
                        aggregateTransferTask.getBytesTransferred(),
                        aggregateTransferTask.getUuid(),
                        getProtocolForClass(sourceClient.getClass()),
                        getProtocolForClass(destClient.getClass())));
            }
            if (sourceClient instanceof Local) {
                log.info("Skipping deleting relay cache file " + tempDir.getPath() + " as source originated from this host.");
            } else {
                log.info("Deleting relay cache file " + tempDir.getPath());
                FileUtils.deleteQuietly(tempDir);
            }
        }

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
     * @throws RemoteDataException
     * @throws IOException
     * @throws TransferException
     * @throws ClosedByInterruptException
     */
    protected void streamingTransfer(String srcPath, String destPath, RemoteTransferListener listener)
            throws RemoteDataException, IOException, TransferException, ClosedByInterruptException {
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

            log.debug(String.format(
                    "Completed streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())));

        } catch (ClosedByInterruptException e) {
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
        } catch (RemoteDataException | IOException e) {
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
        } catch (Throwable e) {
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
        } finally {
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
                RemoteTransferListener listener = null;
                listener = new RemoteTransferListener(transferTask);
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

                        proxyRangeTransfer(srcPath, absoluteSourceIndex, srcRangeSize, destPath, absoluteDestIndex, listener);
                    } catch (ClosedByInterruptException e) {
                        try {
                            TransferTaskDao.cancelAllRelatedTransfers(transferTask.getId());
                        } catch (TransferException e1) {
                            throw new RemoteDataException("Failed to cancel related transfer tasks.", e1);
                        } finally {
                            Thread.currentThread().interrupt();
                        }

                        throw e;
                    } catch (RangeValidationException e) {
                        throw new RemoteDataException(e.getMessage(), e);
                    } catch (TransferException e) {
                        throw new RemoteDataException("Failed to udpate transfer record.", e);
                    }
                }

                return listener.getTransferTask();
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
     * @throws RemoteDataException
     * @throws IOException
     * @throws TransferException
     * @throws ClosedByInterruptException
     */
    protected void proxyRangeTransfer(String srcPath, long srcRangeOffset, long srcRangeSize,
                                      String destPath, long destRangeOffset, RemoteTransferListener listener)
            throws RemoteDataException, IOException, TransferException, ClosedByInterruptException {
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
            while (remainingBytes < totalSize &&
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
            listener.completed();

            // now replace the original with the patched temp file
            destClient.doRename(tmpFilename, destPath);

            // and we're spent
            log.debug(String.format(
                    "Completed streaming transfer for task %s. %s to %s . Protocol: %s => %s",
                    listener.getTransferTask().getUuid(),
                    listener.getTransferTask().getSource(),
                    listener.getTransferTask().getDest(),
                    getProtocolForClass(sourceClient.getClass()),
                    getProtocolForClass(destClient.getClass())));
        } catch (ClosedByInterruptException e) {
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
        } catch (RemoteDataException | IOException e) {
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
        } catch (Throwable e) {
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
        } finally {
            if (listener != null && listener.getTransferTask() != null) {
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
    private String getDefaultErrorMessage(String srcPath, RemoteTransferListener listener) {
        return String.format(
                "Transfer %s cancelled while copying from source.",
                (listener.getTransferTask() == null ?
                        "of " + srcPath :
                        listener.getTransferTask().getUuid()));
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
     */
    protected void dothirdPartyTransfer(String srcPath, String destPath, RemoteTransferListener listener) throws RemoteDataException, IOException {
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
        } catch (ClosedByInterruptException e) {
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
        } catch (IOException e) {
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
        } catch (Throwable e) {
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
        } finally {
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
}
