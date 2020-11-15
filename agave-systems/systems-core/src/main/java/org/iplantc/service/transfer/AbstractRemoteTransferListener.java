package org.iplantc.service.transfer;

import org.apache.log4j.Logger;
import org.codehaus.plexus.util.StringUtils;
import org.globus.ftp.*;
import org.globus.ftp.exception.PerfMarkerException;
import org.iplantc.service.transfer.model.TransferTask;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.transfer.TransferStatus;

import java.time.Instant;
import java.util.Observable;

public abstract class AbstractRemoteTransferListener extends Observable implements RemoteTransferListener {
    private static final Logger log = Logger.getLogger(AbstractRemoteTransferListener.class);

    protected ByteRangeList list = new ByteRangeList();
    //    private ByteRange range;
    protected TransferTask transferTask;
    protected String firstRemoteFilepath;
    protected TransferStatus persistentTransferStatus;
    protected long aggregateStripedDateTransferred = 0;
    protected long lastUpdated = System.currentTimeMillis();
    protected long bytesLastCheck = 0;

    public AbstractRemoteTransferListener(TransferTask transferTask)
    {
        this.transferTask = transferTask;
    }

    /**
     * @return the transferTask
     */
    @Override
    public synchronized TransferTask getTransferTask()
    {
        return this.transferTask;
    }

    @Override
    public TransferStatus getOverallStatusCallback() {
        return persistentTransferStatus;
    }

    @Override
    public void skipped(long totalSize, String remoteFile)
    {
        if (StringUtils.isEmpty(firstRemoteFilepath)) {
            firstRemoteFilepath = remoteFile;
        }

        TransferTask task = getTransferTask();
        if (task != null)
        {
            task.setStatus(TransferStatusType.COMPLETED.name());
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
    public void overallStatusCallback(TransferStatus transferStatus) throws JargonException
    {
        if (transferStatus.getTransferException() != null) {
            persistentTransferStatus = transferStatus;
        } else if (isCancelled()) {
            notifyObservers(TransferStatusType.CANCELLED.name());
        }

        TransferTask task = getTransferTask();
        if (task != null)
        {
            if (log.isDebugEnabled())
                log.debug("Transfer " + transferStatus.getTransferState().name() + " callback received for task " + transferTask.getUuid() + ".\n" + transferStatus.toString());

            if (transferStatus.getTransferState().equals(TransferStatus.TransferState.OVERALL_INITIATION)) {
                task.setStatus(TransferStatusType.TRANSFERRING.name());
                task.setStartTime(Instant.now());
                task.setAttempts(task.getAttempts() + 1);
                if (task.getTotalSize() == 0) {
                    task.setTotalSize(transferStatus.getTotalSize());
                } else {
                    task.setTotalSize(task.getTotalSize() + transferStatus.getTotalSize());
                }
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.OVERALL_COMPLETION)) {
                task.setStatus(TransferStatusType.COMPLETED.name());
                task.setEndTime(Instant.now());
            }

            setTransferTask(task);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transfer " + transferStatus.getTransferState().name() + " callback received for task unknown.\n" + transferStatus.toString());
        }

        if (hasChanged()) {
            throw new JargonException("Listener received a cancel request for transfer "
                    + transferTask.getUuid());
        }
    }

    @Override
    public FileStatusCallbackResponse statusCallback(TransferStatus transferStatus) throws JargonException
    {
        if (transferStatus.getTransferException() != null) {
            persistentTransferStatus = transferStatus;
        } else if (isCancelled()) {
            notifyObservers(TransferStatusType.CANCELLED);
        }

        TransferTask task = getTransferTask();
        if (task != null)
        {
            if (log.isDebugEnabled())
                log.debug("Transfer " + transferStatus.getTransferState().name() + " callback received for task " + transferTask.getUuid() + ".\n" + transferStatus.toString());

            if (transferStatus.getTransferState().equals(TransferStatus.TransferState.OVERALL_INITIATION)) {
                task.setStatus(TransferStatusType.TRANSFERRING.name());
                task.setStartTime(Instant.now());
                task.setTotalSize(task.getTotalSize() + transferStatus.getTotalSize());
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.IN_PROGRESS_START_FILE)) {
                task.setStatus(TransferStatusType.TRANSFERRING.name());
                task.setStartTime(Instant.now());
                task.setTotalSize(task.getTotalSize() + transferStatus.getTotalSize());
                task.setTotalFiles(task.getTotalFiles() + 1);
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.CANCELLED)) {
                task.setStatus(TransferStatusType.CANCELLED.name());
                task.setEndTime(Instant.now());
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.FAILURE) ||
                    transferStatus.getTransferException() != null) {
                task.setStatus(TransferStatusType.FAILED.name());
                task.setEndTime(Instant.now());
                task.setBytesTransferred(task.getBytesTransferred() + transferStatus.getBytesTransfered());
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.PAUSED)) {
                task.setStatus(TransferStatusType.PAUSED.name());
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.OVERALL_COMPLETION) ||
                    transferStatus.getTransferState().equals(TransferStatus.TransferState.SUCCESS)) {
                task.setStatus(TransferStatusType.COMPLETED.name());
                task.setEndTime(Instant.now());
                task.setBytesTransferred(task.getBytesTransferred() + transferStatus.getBytesTransfered());
            } else if (transferStatus.getTransferState().equals(TransferStatus.TransferState.RESTARTING)) {
                task.setAttempts(task.getAttempts() + 1);
                task.setStatus(TransferStatusType.RETRYING.name());
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
            task.setStatus(TransferStatusType.TRANSFERRING.name());
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
    @Override
    public void cancel() {
        TransferTask task = getTransferTask();
        if (task != null)
        {
            if (log.isDebugEnabled())
                log.debug("Transfer progress callback received for task " + task.getUuid() + ".\n" + task.getSource());

            task.setStatus(TransferStatusType.CANCELLED.name());
            task.setEndTime(Instant.now());
            task.updateTransferRate();
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
        setChanged();
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

            task.setStatus(TransferStatusType.COMPLETED.name());
            task.setEndTime(Instant.now());
            task.updateTransferRate();
            setTransferTask(task);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transfer completed callback received for task unknown.\nunknown");

        }
    }

    @Override
    public void failed()
    {
        TransferTask task = getTransferTask();
        if (task != null)
        {
            if (log.isDebugEnabled())
                log.debug("Transfer failed callback received for task " + task.getUuid() + ".\n" + task.getSource());

            task.setStatus(TransferStatusType.FAILED.name());
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
        if (isCancelled()) {
            notifyObservers(TransferStatusType.CANCELLED);
        }

        if (m instanceof GridFTPRestartMarker) {
            restartMarkerArrived((GridFTPRestartMarker) m);
        } else if (m instanceof PerfMarker) {
            perfMarkerArrived((PerfMarker) m);
        } else {
            log.error("Received unsupported marker type");
        }
    }

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
                task.setStatus(TransferStatusType.TRANSFERRING.name());
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
}
