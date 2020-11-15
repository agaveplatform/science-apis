package org.iplantc.service.transfer;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.StaleObjectStateException;
import org.iplantc.service.transfer.dao.TransferTaskDao;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.model.TransferTask;
import org.iplantc.service.transfer.model.TransferTaskImpl;

public class RemoteTransferListenerImpl extends AbstractRemoteTransferListener {
	private static final Logger log = Logger.getLogger(RemoteTransferListenerImpl.class);

	public RemoteTransferListenerImpl(TransferTask transferTask)
	{
	    super(transferTask);
	}

	/**
	 * Persists the given {@link TransferTask} and sets the updated value to the current task
	 * @param transferTask the transferTask to set
	 */
	public synchronized void setTransferTask(TransferTask transferTask)
	{
		try {
			if (transferTask != null) {
				TransferTaskDao.updateProgress((TransferTaskImpl) transferTask);
			}

			this.transferTask = transferTask;

		} catch (StaleObjectStateException ignored) {
			// nothing to do here
		} catch (Throwable e) {
			log.error("Failed to update transfer task " + transferTask.getUuid()
					+ " in callback listener.", e);
		}
	}

	@Override
	public synchronized boolean isCancelled()
	{
		return hasChanged() ||
				(getTransferTask() != null && ((TransferTaskImpl)getTransferTask()).getStatus().isCancelled());
	}

	/**
	 * Creates a new child transfer task at the given source and destination paths with this listener's
	 * {@link #transferTask} as the parent. This is called from within recursive operations in each
	 * @link RemoteDataClient} class and allows child {@link TransferTask}s to be created independent of the
	 * concrete implementation. In this manner we get portability between legacy and new transfer packages.
	 *
	 * @param sourcePath the source of the child {@link TransferTask}
	 * @param destPath the dest of the child {@link TransferTask}
	 * @return the persisted {@link TransferTask}
	 * @throws TransferException if the cild transfer task cannot be saved
	 */
	public TransferTask createAndPersistChildTransferTask(String sourcePath, String destPath) throws TransferException {
		String srcPath = getTransferTask().getSource() +
				(StringUtils.endsWith(getTransferTask().getSource(), "/") ? "" : "/") +
				sourcePath;
		TransferTaskImpl parentTask = (TransferTaskImpl) getTransferTask();
		TransferTaskImpl childTask = new TransferTaskImpl(srcPath,
				destPath,
				parentTask.getOwner(),
				parentTask.getRootTask(),
				parentTask);

		TransferTaskDao.persist(childTask);

		return childTask;
	}
}
