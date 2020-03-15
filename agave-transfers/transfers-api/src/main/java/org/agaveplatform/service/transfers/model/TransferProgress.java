package org.agaveplatform.service.transfers.model;

import java.time.Instant;

public class TransferProgress {
	private long bytesTransferred = 0;
	private long totalBytes = 0;
	private long skippedTransfers = 0;
	private long totalTransfers = 0;
	private Instant created = Instant.now();

	public long getBytesTransferred() {
		return bytesTransferred;
	}

	public void setBytesTransferred(long bytesTransferred) {
		this.bytesTransferred = bytesTransferred;
	}

	public long getTotalBytes() {
		return totalBytes;
	}

	public void setTotalBytes(long totalBytes) {
		this.totalBytes = totalBytes;
	}

	public long getSkippedTransfers() {
		return skippedTransfers;
	}

	public void setSkippedTransfers(long skippedTransfers) {
		this.skippedTransfers = skippedTransfers;
	}

	public long getTotalTransfers() {
		return totalTransfers;
	}

	public void setTotalTransfers(long totalTransfers) {
		this.totalTransfers = totalTransfers;
	}

	public Instant getCreated() {
		return created;
	}

	public void setCreated(Instant created) {
		this.created = created;
	}
}
