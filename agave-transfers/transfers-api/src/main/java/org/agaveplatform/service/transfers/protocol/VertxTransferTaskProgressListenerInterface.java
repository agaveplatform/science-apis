package org.agaveplatform.service.transfers.protocol;

import com.sshtools.sftp.FileTransferProgress;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.codehaus.plexus.util.StringUtils;
import org.globus.ftp.*;
import org.globus.ftp.exception.PerfMarkerException;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.transfer.TransferStatus;
import org.irods.jargon.core.transfer.TransferStatusCallbackListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_UPDATED;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;

public class VertxTransferTaskProgressListenerInterface implements MarkerListener, TransferStatusCallbackListener, FileTransferProgress, InterfaceRemoteTransfer {
    private static final Logger log = LoggerFactory.getLogger(VertxTransferTaskProgressListenerInterface.class);
    private final TransferTask transferTask;

    private RetryRequestManager retryRequestManager;
    public ByteRangeList list = new ByteRangeList();

    private String firstRemoteFilepath;
    private TransferStatus persistentTransferStatus;
    private long aggregateStripedDateTransferred = 0;
    private long lastUpdated = System.currentTimeMillis();
    private long bytesLastCheck = 0;

    /**
     * Vertx reference of calling class used to locate the event bus to throw messages
     */
    private final Vertx vertx;

    public VertxTransferTaskProgressListenerInterface(TransferTask transferTask, Vertx vertx) {
        this.transferTask = transferTask;
        this.vertx = vertx;
    }

    /**
     * @return the transferTask
     */
    public synchronized TransferTask getTransferTask()
    {
        return this.transferTask;
    }

    /**
     * @param transferTask the transferTask to set
     * @throws InterruptedException
     */
    private synchronized void setTransferTask(TransferTask transferTask)
    {
       _doPublishEvent(TRANSFERTASK_UPDATED, transferTask.toJson());
    }

    public RetryRequestManager getRetryRequestManager() {
        log.debug("Got into the getRetryRequestManager call");
        if (retryRequestManager == null) {
            log.trace("getRetryRequestManager check for null");
            retryRequestManager = new RetryRequestManager(getVertx());
        }
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

    @Override
    public TransferTask setTransferTask(org.iplantc.service.transfer.model.TransferTask transferTask) {
        TransferTask tt = convToAgaveTransferTask(transferTask);
        return tt;
    }

    TransferTask convToAgaveTransferTask(org.iplantc.service.transfer.model.TransferTask txfrTsk){
        TransferTask newTransferTask = new TransferTask(txfrTsk.getSource(), txfrTsk.getDest());
        newTransferTask.setOwner(txfrTsk.getOwner());
        newTransferTask.setEventId(txfrTsk.getEventId());
        newTransferTask.setAttempts(txfrTsk.getAttempts());
        newTransferTask.setStatus(org.agaveplatform.service.transfers.enumerations.TransferStatusType.valueOf(txfrTsk.getStatus().toString()));
        newTransferTask.setTotalSize(txfrTsk.getTotalSize());
        newTransferTask.setTotalFiles(txfrTsk.getTotalFiles());
        newTransferTask.setTotalSkippedFiles(txfrTsk.getTotalSkippedFiles());
        newTransferTask.setBytesTransferred(txfrTsk.getBytesTransferred());
        newTransferTask.setTransferRate(txfrTsk.getTransferRate());
        newTransferTask.setTenantId(txfrTsk.getTenantId());
        newTransferTask.setStartTime(txfrTsk.getStartTime().toInstant());
        newTransferTask.setEndTime(txfrTsk.getEndTime().toInstant());
        newTransferTask.setCreated(txfrTsk.getCreated().toInstant());
        newTransferTask.setLastUpdated(txfrTsk.getLastUpdated().toInstant());
        newTransferTask.setUuid(txfrTsk.getUuid());

        newTransferTask.setParentTaskId(txfrTsk.getParentTask().getUuid());
        newTransferTask.setRootTaskId(txfrTsk.getRootTask().getUuid());

        return newTransferTask;
    }

    org.iplantc.service.transfer.model.TransferTask convTransferTask(TransferTask transferTask){

        org.iplantc.service.transfer.model.TransferTask convTransferTask = new org.iplantc.service.transfer.model.TransferTask();
        convTransferTask.setId(transferTask.getId());
        convTransferTask.setSource(transferTask.getSource());
        convTransferTask.setDest(transferTask.getDest());
        convTransferTask.setOwner(transferTask.getOwner());
        convTransferTask.setEventId(transferTask.getEventId());
        convTransferTask.setAttempts(transferTask.getAttempts());
        convTransferTask.setStatus( org.iplantc.service.transfer.model.enumerations.TransferStatusType.valueOf(transferTask.getStatus().toString()));
        convTransferTask.setTotalSize(transferTask.getTotalSize());
        convTransferTask.setTotalFiles(transferTask.getTotalFiles());
        convTransferTask.setTotalSkippedFiles(transferTask.getTotalSkippedFiles());
        convTransferTask.setBytesTransferred(transferTask.getBytesTransferred());
        convTransferTask.setTransferRate(transferTask.getTransferRate());
        convTransferTask.setTenantId(transferTask.getTenantId());

        org.iplantc.service.transfer.model.TransferTask tParent = new org.iplantc.service.transfer.model.TransferTask( transferTask.getSource(), transferTask.getDest() );
        org.iplantc.service.transfer.model.TransferTask tRoot = new org.iplantc.service.transfer.model.TransferTask( transferTask.getSource(), transferTask.getDest() );
        convTransferTask.setParentTask(tParent);
        convTransferTask.setRootTask(tRoot);

        if (transferTask.getStartTime() == null){
            log.error("transferTask.getStartTime is null");
        }else {
            convTransferTask.setStartTime(Date.from(transferTask.getStartTime()));
        }

        if (transferTask.getEndTime() == null){
            log.error("transferTask.getEndTime is null");
        }else {
            convTransferTask.setEndTime(Date.from(transferTask.getEndTime()));
        }

        if (transferTask.getCreated() == null){
            log.error("transferTask.getEndTime is null");
        }else {
            convTransferTask.setCreated(Date.from(transferTask.getCreated()));
        }

        if (transferTask.getLastUpdated() == null){
            log.error("transferTask.getEndTime is null");
        }else {
            convTransferTask.setLastUpdated(Date.from(transferTask.getLastUpdated()));
        }


        convTransferTask.setUuid(transferTask.getUuid());
        convTransferTask.setVersion(0);

        return convTransferTask;
    }

    public TransferStatus getOverallStatusCallback() {
        return persistentTransferStatus;
    }

    public void skipped(long totalSize, String remoteFile)
    {
        if (StringUtils.isEmpty(firstRemoteFilepath)) {
            firstRemoteFilepath = remoteFile;
        }

        TransferTask task = getTransferTask();
        if (task != null)
        {
            task.setStatus(COMPLETED);
            task.setStartTime(Instant.now());
            task.setEndTime(Instant.now());
            task.setTotalSize(totalSize);
            task.setBytesTransferred(0);
            task.setLastUpdated(Instant.now());
            setTransferTask(task);
        }
    }

    /*************************************************************
     * 		IRODS - TransferStatusCallbackListener methods
     *************************************************************/

    @Override
    public void overallStatusCallback(TransferStatus transferStatus)
            throws JargonException
    {
        if (transferStatus.getTransferException() != null) {
            persistentTransferStatus = transferStatus;
//        } else if (isCancelled()) {
//            notifyObservers(CANCELLED);
        }

        TransferTask task = getTransferTask();
        if (task != null)
        {
            if (log.isDebugEnabled())
                log.debug("Transfer " + transferStatus.getTransferState().name() + " callback received for task " + transferTask.getUuid() + ".\n" + transferStatus.toString());

            if (transferStatus.getTransferState().equals(TransferStatus.TransferState.OVERALL_INITIATION)) {
                task.setStatus(TRANSFERRING);
                task.setStartTime(Instant.now());
                task.setAttempts(task.getAttempts() + 1);
                if (task.getTotalSize() == 0) {
                    task.setTotalSize(transferStatus.getTotalSize());
                } else {
                    task.setTotalSize(task.getTotalSize() + transferStatus.getTotalSize());
                }
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.OVERALL_COMPLETION)) {
                task.setStatus(COMPLETED);
                task.setEndTime(Instant.now());
            }

            setTransferTask(task);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transfer " + transferStatus.getTransferState().name() + " callback received for task unknown.\n" + transferStatus.toString());
        }

//        if (hasChanged()) {
//            throw new JargonException("Listener received a cancel request for transfer "
//                    + transferTask.getUuid());
//        }
    }

    @Override
    public FileStatusCallbackResponse statusCallback(TransferStatus transferStatus)
            throws JargonException
    {
        if (transferStatus.getTransferException() != null) {
            persistentTransferStatus = transferStatus;
//        } else if (isCancelled()) {
//            notifyObservers(CANCELLED);
        }

        TransferTask task = getTransferTask();
        if (task != null)
        {
            if (log.isDebugEnabled())
                log.debug("Transfer " + transferStatus.getTransferState().name() + " callback received for task " + transferTask.getUuid() + ".\n" + transferStatus.toString());

            if (transferStatus.getTransferState().equals(TransferStatus.TransferState.OVERALL_INITIATION)) {
                task.setStatus(TRANSFERRING);
                task.setStartTime(Instant.now());
                task.setTotalSize(task.getTotalSize() + transferStatus.getTotalSize());
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.IN_PROGRESS_START_FILE)) {
                task.setStatus(TRANSFERRING);
                task.setStartTime(Instant.now());
                task.setTotalSize(task.getTotalSize() + transferStatus.getTotalSize());
                task.setTotalFiles(task.getTotalFiles() + 1);
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.CANCELLED)) {
                task.setStatus(CANCELLED);
                task.setEndTime(Instant.now());
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.FAILURE) ||
                    transferStatus.getTransferException() != null) {
                task.setStatus(FAILED);
                task.setEndTime(Instant.now());
                task.setBytesTransferred(task.getBytesTransferred() + transferStatus.getBytesTransfered());
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.PAUSED)) {
                task.setStatus(PAUSED);
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.OVERALL_COMPLETION) ||
                    transferStatus.getTransferState().equals(TransferStatus.TransferState.SUCCESS)) {
                task.setStatus(COMPLETED);
                task.setEndTime(Instant.now());
                task.setBytesTransferred(task.getBytesTransferred() + transferStatus.getBytesTransfered());
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.RESTARTING)) {
                task.setAttempts(task.getAttempts() + 1);
                task.setStatus(RETRYING);
                task.setEndTime(null);
                task.setStartTime(Instant.now());
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.IN_PROGRESS_COMPLETE_FILE)) {
                task.setBytesTransferred(task.getBytesTransferred() + transferStatus.getBytesTransfered());
            } else {
                log.debug("Unrecognized transfer status during transfer for task " + task.getUuid());
            }

            task.setLastUpdated(Instant.now());

            setTransferTask(task);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transfer " + transferStatus.getTransferState().name() + " callback received for task unknown.\n" + transferStatus.toString());
        }
        return FileStatusCallbackResponse.CONTINUE;
    }

    @Override
    public CallbackResponse transferAsksWhetherToForceOperation(
            String irodsAbsolutePath, boolean isCollection)
    {
        return CallbackResponse.YES_FOR_ALL;
    }

    /*************************************************************
     * 		Sftp - FileTransferProgress listener methods
     *************************************************************/

    @Override
    public void started(long bytesTotal, String remoteFile)
    {
        if (StringUtils.isEmpty(firstRemoteFilepath)) {
            firstRemoteFilepath = remoteFile;
        }

        TransferTask task = getTransferTask();
        if (task != null)
        {
            if (log.isDebugEnabled())
                log.debug("Transfer started callback received for task " + task.getUuid() + ".\n" + remoteFile + " => " + bytesTotal);

            task.setTotalFiles(task.getTotalFiles() + 1);
            task.setStatus(TRANSFERRING);
            task.setStartTime(Instant.now());

            if (remoteFile.equals(firstRemoteFilepath)) {
                task.setTotalSize(bytesTotal);
                task.setAttempts(task.getAttempts() + 1);
            } else {
                task.setTotalSize(task.getTotalSize() + bytesTotal);
            }

            setTransferTask(task);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transfer started callback received for task unknown.\n" + remoteFile + " => " + bytesTotal);
        }
    }

    @Override
    public synchronized boolean isCancelled()
    {
        return getTransferTask() != null && getTransferTask().getStatus().isCancelled();
    }

    /* (non-Javadoc)
     * @see com.maverick.sftp.FileTransferProgress#progressed(long)
     */
    @Override
    public void progressed(long bytesSoFar)
    {
        TransferTask task = getTransferTask();
        if (task != null)
        {
            task.setBytesTransferred(bytesSoFar);
            long currentTime = System.currentTimeMillis();
            if (( currentTime - lastUpdated) >= 15000) {
                double progress = bytesSoFar - bytesLastCheck;
                task.setTransferRate(((double)progress / ((double)(currentTime - lastUpdated) /  1000.0) ));
                setTransferTask(task);
                lastUpdated = currentTime;
                bytesLastCheck = bytesSoFar;

                if (log.isDebugEnabled())
                    log.debug("Transfer prograess callback received for task " + task.getUuid() + ".\n" + task.getSource() + " => " + bytesSoFar);

            }
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transfer progress callback received for task unknown.\nunknown => " + bytesSoFar);
        }
    }

    /**
     * Updates the current transfer task to cancelled. The actual
     * workers will pick up on this the next time they check the
     * callback status and send notifications to observers as
     * needed.
     */
    public void cancel() {
        TransferTask task = getTransferTask();
        if (task != null)
        {
            if (log.isDebugEnabled())
                log.debug("Transfer progress callback received for task " + task.getUuid() + ".\n" + task.getSource());

            task.setStatus(CANCELLED);
            task.setEndTime(Instant.now());
            setTransferTask(task);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transfer progress callback received for task unknown.\nunknown");
        }

        // set this listener to dirty
        if (log.isDebugEnabled())
            log.debug("RemoteTransferListender for " + (task == null ? " anonymous transfer " : task.getUuid()) +
                    " was notified of an interrupt");
    }

    /* (non-Javadoc)
     * @see com.maverick.sftp.FileTransferProgress#completed()
     */
    @Override
    public void completed()
    {
        TransferTask task = getTransferTask();
        if (task != null)
        {
            if (log.isDebugEnabled())
                log.debug("Transfer completed callback received for task " + task.getUuid() + ".\n" + task.getSource());

            task.setStatus(COMPLETED);
            task.setEndTime(Instant.now());
            setTransferTask(task);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transfer completed callback received for task unknown.\nunknown");

        }
    }

    public void failed()
    {
        TransferTask task = getTransferTask();
        if (task != null)
        {
            if (log.isDebugEnabled())
                log.debug("Transfer failed callback received for task " + task.getUuid() + ".\n" + task.getSource());

            task.setStatus(FAILED);
            task.setEndTime(Instant.now());
            setTransferTask(task);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transfer failed callback received for task unknown.\nunknown");
        }
    }

    /*************************************************************
     * 		JGlobus - MarkerListener methods
     *************************************************************/

    /* (non-Javadoc)
     * @see org.globus.ftp.MarkerListener#markerArrived(org.globus.ftp.Marker)
     */
    @Override
    public void markerArrived(Marker m) {
//        if (isCancelled()) {
//            notifyObservers(CANCELLED);
//        }

        if (m instanceof GridFTPRestartMarker) {
            restartMarkerArrived((GridFTPRestartMarker) m);
        } else if (m instanceof PerfMarker) {
            perfMarkerArrived((PerfMarker) m);
        } else {
            log.error("Received unsupported marker type");
        }
    };

    public void restartMarkerArrived(GridFTPRestartMarker marker)
    {
        list.merge(marker.toVector());

        TransferTask task = getTransferTask();
        if (task != null)
        {
            ByteRange ef = (ByteRange)list.toVector().lastElement();

            try
            {
                TransferStatus.TransferState status = null;
                if (ef.to == task.getTotalSize()) {
                    status = TransferStatus.TransferState.SUCCESS;
                } else {
                    status = TransferStatus.TransferState.RESTARTING;
                }

                statusCallback(TransferStatus.instance(TransferStatus.TransferType.PUT,
                        task.getSource(), task.getDest(), "marker",
                        task.getTotalSize(), 0,
                        (int)task.getTotalFiles(), (int)task.getTotalSkippedFiles(),
                        (int)task.getTotalFiles(), status,
                        "marker", "marker"));
            }
            catch (JargonException e) {
                log.error("Failed to register restart marker on gridftp transfer " + task.getUuid(), e);
            }
        }
    }

    public void perfMarkerArrived(PerfMarker marker)
    {
        long transferedBytes = 0;
        try {
            transferedBytes = marker.getStripeBytesTransferred();
        } catch (PerfMarkerException e) {
            log.error("Failed to handle perf marker.",e);
        }

        this.aggregateStripedDateTransferred  += transferedBytes;

        TransferTask task = getTransferTask();
        if (task != null)
        {
            // if this is the first marker to arrive
            if (aggregateStripedDateTransferred == transferedBytes) {
                task.setAttempts(1);
                task.setStatus(TRANSFERRING);
            }
            task.setBytesTransferred(aggregateStripedDateTransferred);
            setTransferTask(task);
        }

        // total stripe count
        if (marker.hasTotalStripeCount()) {
            try {
                log.info("Total stripe count = "
                        + marker.getTotalStripeCount());
            } catch (PerfMarkerException e) {
                log.error(e.toString());
            }
        }else {
            log.info("Total stripe count: not present");
        }
    }//PerfMarkerArrived


    public Vertx getVertx() {
        return vertx;
    }
}
