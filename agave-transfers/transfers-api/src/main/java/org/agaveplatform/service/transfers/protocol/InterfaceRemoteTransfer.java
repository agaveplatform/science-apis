package org.agaveplatform.service.transfers.protocol;

import org.globus.ftp.GridFTPRestartMarker;
import org.globus.ftp.Marker;
import org.globus.ftp.PerfMarker;
import org.iplantc.service.transfer.model.TransferTask;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.transfer.TransferStatus;
import org.irods.jargon.core.transfer.TransferStatusCallbackListener;

public interface InterfaceRemoteTransfer {

    public org.agaveplatform.service.transfers.model.TransferTask getTransferTask();
    public org.agaveplatform.service.transfers.model.TransferTask setTransferTask(TransferTask transferTask);
    public TransferStatus getOverallStatusCallback();
    public void skipped(long totalSize, String remoteFile);

    public void overallStatusCallback(TransferStatus transferStatus) throws JargonException;
    public TransferStatusCallbackListener.FileStatusCallbackResponse statusCallback(TransferStatus transferStatus) throws JargonException;
    public TransferStatusCallbackListener.CallbackResponse transferAsksWhetherToForceOperation(
            String irodsAbsolutePath, boolean isCollection);
    public void started(long bytesTotal, String remoteFile);
    public boolean isCancelled();
    public void progressed(long bytesSoFar);
    public void cancel();
    public void completed();
    public void failed();
    public void markerArrived(Marker m);
    public void restartMarkerArrived(GridFTPRestartMarker marker);
    public void perfMarkerArrived(PerfMarker marker);
}
