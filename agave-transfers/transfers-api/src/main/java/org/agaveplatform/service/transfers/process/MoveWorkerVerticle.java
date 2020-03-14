package org.agaveplatform.service.transfers.process;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.hibernate.HibernateException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class MoveWorkerVerticle extends AbstractVerticle {

	private final Logger logger = LoggerFactory.getLogger(MoveWorkerVerticle.class);

	public MoveWorkerVerticle(Vertx vertx) {
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer("mkdir", msg -> {
			JsonObject body = msg.body();
			String username = body.getString("owner");
			String tenantId = body.getString("tenant_id");
			String systemId = body.getString("system_id");
			String oldPath = body.getString("src");
			String newPath = body.getString("dest");
			doMoveOperation(systemId, oldPath, newPath, username, tenantId, null);
		});
	}

	private void resultHandler(AsyncResult<String> ar) {
		if (ar.succeeded()) {
			logger.info("Blocking code result: {}", ar.result());
		} else {
			logger.error("Woops", ar.cause());
		}
	}

	/**
	 * Moves a file from one path to another on the same system.
	 * @param systemId
	 * @param oldPath
	 * @param newPath
	 * @param tenantId
	 * @param username
	 * @param internalUsername
	 */
	protected void doMoveOperation(String systemId, String oldPath, String newPath, String tenantId, String username, String internalUsername) {
		RemoteSystem system;
		RemoteDataClient remoteDataClient;
		try {
			TenancyHelper.setCurrentTenantId(tenantId);
			TenancyHelper.setCurrentEndUser(username);

			// find the system for which the directory should be created
			system = new SystemDao().findBySystemId(systemId);

			if (system == null) {
				logger.error("No system found with id: " + systemId);
			} else {
				// get a handle on a remote data client to create the remote directory
				remoteDataClient = new RemoteDataClientFactory().getInstance(system, internalUsername);
				remoteDataClient.doRename(oldPath, newPath);
			}
		} catch (RemoteDataException|RemoteCredentialException|RemoteDataSyntaxException|IOException e) {
			logger.error(e.getMessage());
//            throw e;
		} catch (HibernateException e) {
			String msg = "An unexpected internal error occurred while processing the request. " +
					"If this persists, please contact your tenant administrator";
			logger.error(e.getMessage(), e);
//            throw new RemoteDataException(msg, e);
		}
//
//		String destPath = null;
//		if (jsonInput.has("path")) {
//			destPath = jsonInput.get("path").textValue();
//		}
//
//		// move is essentially just a rename on the same system
//
//		try {
//			remoteDataClient.doRename(path, destPath);
//		} catch (RemoteDataException e) {
//			if (e.getMessage().contains("Destination already exists")) {
//				log.error(e.getMessage(), e);
//				throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e.getMessage(), e);
//			}
//		}
//
//		message = "Move success";
//		if (remoteDataClient.isPermissionMirroringRequired())
//		{
//			remoteDataClient.setOwnerPermission(remoteDataClient.getUsername(), destPath, true);
//
//			String pemUser = StringUtils.isEmpty(owner) ? username : owner;
//			try {
//				remoteDataClient.setOwnerPermission(pemUser, destPath, true);
//			} catch (Exception e) {
//				message = "Move was successful, but unable to mirror permissions for user " +
//						pemUser + " on new directory " + path;
//				log.error(message, e);
//			}
//		}
//
//		// now keep the logical file up to date
//		LogicalFile destLogicalFile = null;
//		try {
//			destLogicalFile = LogicalFileDao.findBySystemAndPath(system, remoteDataClient.resolvePath(destPath));
//		} catch (Exception e) {
//			if (log.isDebugEnabled()) {
//				String msg = "LogicalFileDao.findBySystemAndPath() failed, cloning destination logical file " +
//						destPath + ".";
//				log.debug(msg, e);
//			}
//		}
//
//		String sourceUrl = logicalFile.getPublicLink();
//		String destUrl = null;
//
//		// no logical file for destination, this is an add or update
//		if (destLogicalFile == null)
//		{
//			try {
//				destLogicalFile = logicalFile.clone();
//				destLogicalFile.setSourceUri(logicalFile.getPublicLink());
//				destLogicalFile.setPath(remoteDataClient.resolvePath(destPath));
//				destLogicalFile.setName(FilenameUtils.getName(destLogicalFile.getPath()));
//				destLogicalFile.setSystem(logicalFile.getSystem());
//				destLogicalFile.setOwner(username);
//				destLogicalFile.setInternalUsername(internalUsername);
//				destLogicalFile.setLastUpdated(new DateTime().toDate());
//
//				// set the resulting url of the destination for use in events
//				destUrl = destLogicalFile.getPublicLink();
//
//				// fire event before record update so the notification event references the source record
//				logicalFile.addContentEvent(new FileEvent(FileEventType.MOVED,
//						"Moved from " + sourceUrl + " to " + destUrl,
//						getAuthenticatedUsername()));
//
//				// now update source path and name to reference the new location. This will
//				// carry the history with it.
//				logicalFile.setPath(remoteDataClient.resolvePath(destPath));
//				logicalFile.setName(FilenameUtils.getName(logicalFile.getPath()));
//				logicalFile.setLastUpdated(new DateTime().toDate());
//
//				LogicalFileDao.persist(logicalFile);
//			} catch (Exception e) {
//				String msg = "Unable to record cloned logical file " + destLogicalFile.getPath() + ".";
//				log.error(msg, e);
//				throw e;
//			}
//		}
//		else
//		{
//			try {
//				destLogicalFile.setName(FilenameUtils.getName(destLogicalFile.getPath()));
//
//				// set the resulting url of the destination for use in events
//				destUrl = destLogicalFile.getPublicLink();
//
//				destLogicalFile.addContentEvent(new FileEvent(FileEventType.OVERWRITTEN,
//						"Overwritten by a move from " + logicalFile.getPublicLink() +
//								" to " + destLogicalFile.getPublicLink(),
//						getAuthenticatedUsername()));
//				LogicalFileDao.persist(destLogicalFile);
//			} catch (Exception e) {
//				String msg = "Unable to record logical file " + destLogicalFile.getPath() + ".";
//				log.error(msg, e);
//				throw e;
//			}
//		}
//
//		if (logicalFile.isDirectory())
//		{
//			try {
//				// we also need to replicate any children that were not copied over before
//				List<LogicalFile> nonOverlappingChildren =
//						LogicalFileDao.findNonOverlappingChildren(logicalFile.getPath(),
//								system.getId(),
//								destLogicalFile.getPath(),
//								system.getId());
//
//				for (LogicalFile child: nonOverlappingChildren) {
//					// capture the original url to the child
//					String sourceChildUrl = child.getPublicLink();
//
//					// update the path and timestamp
//					child.setPath(StringUtils.replaceOnce(child.getPath(), logicalFile.getPath(), destLogicalFile.getPath()));
//					child.setLastUpdated(new DateTime().toDate());
//
//					// add event
//					child.addContentEvent(new FileEvent(FileEventType.MOVED,
//							"File item moved from " + sourceChildUrl + " to " + child.getPublicLink(),
//							getAuthenticatedUsername()));
//
//					// update afterwards so the event has the original child path
//					LogicalFileDao.persist(child);
//				}
//
//				// now that the file item is moved over, we need to alert the children that the file has been copied
//				for (LogicalFile child: LogicalFileDao.findChildren(destLogicalFile.getPath(), system.getId())) {
//					if (!nonOverlappingChildren.contains(child))
//					{
//						child.addContentEvent(new FileEvent(FileEventType.OVERWRITTEN,
//								"Possibly overwritten as part of file item move from " +
//										StringUtils.replace(child.getPublicLink(), destUrl, sourceUrl) +
//										" to " + child.getPublicLink(),
//								getAuthenticatedUsername()));
//						LogicalFileDao.persist(child);
//					}
//				}
//			} catch (Exception e) {
//				String msg = "Processing failure for logical directory " + logicalFile.getPath() + ".";
//				log.error(msg, e);
//				throw e;
//			}
//		}
//		return logicalFile.toJSON();
	}

}
