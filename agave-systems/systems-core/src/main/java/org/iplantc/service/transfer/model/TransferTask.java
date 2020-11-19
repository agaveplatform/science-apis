package org.iplantc.service.transfer.model;

import java.time.Instant;

public interface TransferTask {

    String getSource();

    void setSource(String source);

    String getDest();

    void setDest(String dest);

    String getOwner();

    void setOwner(String owner);

    void setStatusString(String status);

    long getTotalFiles();

    void setTotalFiles(long totalFiles);

    long getTotalSkippedFiles();

    void setTotalSkippedFiles(long totalSkippedFiles);

    long getBytesTransferred();

    void setBytesTransferred(long bytesTransferred);

    int getAttempts();

    void setAttempts(int attempts);

    long getTotalSize();

    void setTotalSize(long totalSize);

    String getUuid();

    void setUuid(String uuid);

    Instant getEndTime();

    void setEndTime(Instant instant);

    Instant getStartTime();

    void setStartTime(Instant instant);

    void setTransferRate(double transferRate);

    double getTransferRate();

    Instant getLastUpdated();

    void setLastUpdated(Instant lastUpdated);

    Instant getCreated();

    void setCreated(Instant created);

    /**
     * Calculates the transfer rate by using the bytes transferred divided by the
     * elapsed time in seconds.
     *
     * @return transfer rate in bytes per second
     */
    double calculateTransferRate();

    /**
     * Convenience method to calculate and save the transfer rate.
     * calls out to {link@ #calculateTransferRate()}
     */
    default void updateTransferRate()
    {
        setTransferRate(calculateTransferRate());
    }
}
