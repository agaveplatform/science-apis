package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.codehaus.plexus.util.StringUtils;
import org.iplantc.service.transfer.RemoteTransferListener;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;

public class RemoteStreamingTransferListenerImpl extends RemoteTransferListenerImpl {
    private static final Logger log = LoggerFactory.getLogger(RemoteStreamingTransferListenerImpl.class);

    public RemoteStreamingTransferListenerImpl(TransferTask transferTask, RetryRequestManager retryRequestManager) {
        super(transferTask, retryRequestManager);
    }

    public RemoteStreamingTransferListenerImpl(TransferTask transferTask, Vertx vertx, RetryRequestManager retryRequestManager) {
        super(transferTask, vertx, retryRequestManager);
    }
    /**
     * Creates a new concrete {@link RemoteTransferListener} using the context of current object and the
     * paths of the child.
     *
     * @param childSourcePath the source of the child {@link TransferTask}
     * @param childDestPath   the dest of the child {@link TransferTask}
     * @return the persisted {@link RemoteTransferListener}
     * @throws TransferException if the child remote transfer task listener cannot be saved
     */
    @Override
    public RemoteTransferListener createChildRemoteTransferListener(String childSourcePath, String childDestPath) throws TransferException {
        TransferTask childTransferTask = createAndPersistChildTransferTask(childSourcePath, childDestPath);
        return new RemoteStreamingTransferListenerImpl(childTransferTask, getVertx(), getRetryRequestManager());
    }

    /**
     * Creates a new concrete {@link RemoteTransferListener} using the context of current object and the
     * child {@link TransferTask}.
     *
     * @param childTransferTask the the child {@link TransferTask}
     * @return the persisted {@link RemoteTransferListener}
     */
    @Override
    public RemoteTransferListener createChildRemoteTransferListener(TransferTask childTransferTask) {
        return new RemoteStreamingTransferListenerImpl(childTransferTask, getVertx(), getRetryRequestManager());
    }

    @Override
    public void started(long bytesTotal, String remoteFile)
    {
        if (StringUtils.isEmpty(getFirstRemoteFilepath())) {
            setFirstRemoteFilepath(remoteFile);
        }

        TransferTask task = getTransferTask();
        if (task != null)
        {
            if (log.isDebugEnabled())
                log.debug("Transfer started callback received for task " + task.getUuid() + ".\n" + remoteFile + " => " + bytesTotal);

            task.setTotalFiles(task.getTotalFiles() + 1);
            task.setStatusString(STREAM_COPY_STARTED.name());
            task.setStartTime(Instant.now());

            if (remoteFile.equals(getFirstRemoteFilepath())) {
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
            task.setStatusString(STREAM_COPY_IN_PROGRESS.name());
            task.setBytesTransferred(bytesSoFar);
            Instant instantCurrentTime = Instant.now();
            long currentTime = instantCurrentTime.toEpochMilli();

            //TODO: parameterize the update interval used by each verticle
            if (( currentTime - getLastUpdated()) >= 15000) {
                double progress = bytesSoFar - getBytesLastCheck();
                task.setTransferRate(((double)progress / ((double)(currentTime - getLastUpdated()) /  1000.0) ));
                task.setLastUpdated(instantCurrentTime);
                // send an event to notifiy an update listener that this task was updated.
                setTransferTask(task);

                setLastUpdated(currentTime);
                setBytesLastCheck(bytesSoFar);

                if (log.isDebugEnabled())
                    log.debug("Transfer progress callback received for task " + task.getUuid() + ".\n" + task.getSource() + " => " + bytesSoFar);
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

            task.setStatusString(CANCELLED.name());
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

            task.setStatusString(STREAM_COPY_COMPLETED.name());
            task.setEndTime(Instant.now());
            task.updateTransferRate();
            setTransferTask(task);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Transfer completed callback received for task unknown.\nunknown");

        }
    }
}
