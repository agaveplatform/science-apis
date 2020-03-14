package org.agaveplatform.service.transfers.process;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.hibernate.HibernateException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CopyWorkerVerticle extends AbstractVerticle {

	private final Logger logger = LoggerFactory.getLogger(CopyWorkerVerticle.class);

	public CopyWorkerVerticle(Vertx vertx) {
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer("mkdir", msg -> {
			JsonObject body = msg.body();
			String username = body.getString("owner");
			String tenantId = body.getString("tenant_id");
			String systemId = body.getString("system_id");
			String srcPath = body.getString("src");
			String destPath = body.getString("dest");
			Boolean append = body.getBoolean("append");
			doCopyOperation(systemId, srcPath, destPath, append, username, tenantId, null);
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
	 * Copies a file item at <pre>srcPath</pre> to <pre>destPath</pre> on the same system. Contents
	 * may be optionally appended with the <pre>append</pre> flag is true.
	 * @param systemId
	 * @param srcPath
	 * @param destPath
	 * @param append
	 * @param tenantId
	 * @param username
	 * @param internalUsername
	 */
	protected void doCopyOperation(String systemId, String srcPath, String destPath, Boolean append, String tenantId, String username, String internalUsername)
	{
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

				if (append) {
					remoteDataClient.append(srcPath, destPath);
				} else {
					remoteDataClient.copy(srcPath, destPath);
				}
			}
		} catch (RemoteDataException | RemoteCredentialException | RemoteDataSyntaxException | IOException e) {
			logger.error(e.getMessage());
//            throw e;
		} catch (HibernateException e) {
			String msg = "An unexpected internal error occurred while processing the request. " +
					"If this persists, please contact your tenant administrator";
			logger.error(e.getMessage(), e);
//            throw new RemoteDataException(msg, e);
		}

//		String destPath = null;
//		if (jsonInput.has("path")) {
//			destPath = jsonInput.get("path").asText();
//		} else {
//			String msg = "Please specify a destination location";
//			log.error(msg);
//			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
//					msg, new RemoteDataException());
//		}
//
//		if (!pm.canWrite(remoteDataClient.resolvePath(destPath))) {
//			String msg = "User does not have access to copy data to " + destPath;
//			log.error(msg);
//			throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
//					msg, new FileNotFoundException());
//		}
//		else if (destPath.equalsIgnoreCase(path)) {
//			String msg = "Source and destination locations cannot be the same.";
//			log.error(msg);
//			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
//					msg, new RemoteDataException());
//		}
//
//		boolean append = false;
//		if (jsonInput.has("append")) {
//			append = jsonInput.get("append").asBoolean();
//		}
//
//		if (append) {
//			remoteDataClient.append(path, destPath);
//		} else {
//			remoteDataClient.copy(path, destPath);
//		}
//
//		message = "Copy success";
//		if (remoteDataClient.isPermissionMirroringRequired())
//		{
//			remoteDataClient.setOwnerPermission(remoteDataClient.getUsername(), destPath, true);
//
//			String pemUser = StringUtils.isEmpty(owner) ? username : owner;
//			try {
//				remoteDataClient.setOwnerPermission(pemUser, destPath, true);
//			} catch (Exception e) {
//				message = "Rename was successful, but unable to mirror permissions for user " +
//						pemUser + " on new directory " + path;
//				log.error(message + ": " + e.getMessage(), e);
//			}
//		}
//
//		LogicalFile copiedLogicalFile = null;
//
//		if (logicalFile != null)
//		{
//			copiedLogicalFile = null;
//
//			try {
//				copiedLogicalFile = LogicalFileDao.findBySystemAndPath(system, remoteDataClient.resolvePath(destPath));
//			} catch(Exception e) {
//				if (log.isDebugEnabled()) {
//					String msg = "LogicalFileDao.findBySystemAndPath() failed, cloning copied logical file " +
//							destPath + ".";
//					log.debug(msg, e);
//				}
//			}
//
//			if (copiedLogicalFile == null) {
//				try {
//					copiedLogicalFile = logicalFile.clone();
//					copiedLogicalFile.setSourceUri(logicalFile.getPublicLink());
//					copiedLogicalFile.setPath(remoteDataClient.resolvePath(destPath));
//					copiedLogicalFile.setSystem(system);
//					copiedLogicalFile.setName(FilenameUtils.getName(copiedLogicalFile.getPath()));
//					copiedLogicalFile.setOwner(StringUtils.isEmpty(owner) ? username : owner);
//					copiedLogicalFile.setInternalUsername(internalUsername);
//					copiedLogicalFile.setLastUpdated(new DateTime().toDate());
//					LogicalFileDao.persist(copiedLogicalFile);
//					copiedLogicalFile.addContentEvent(new FileEvent(FileEventType.CREATED,
//							"File item copied from " + logicalFile.getPublicLink(),
//							getAuthenticatedUsername()));
//					LogicalFileDao.persist(copiedLogicalFile);
//				} catch (Exception e) {
//					String msg = "Unable to save cloned logical file " + destPath + ".";
//					log.error(msg, e);
//					throw e;
//				}
//			}
//		}
//
//		// note that we do not send notifications for every subfile that may have been updated in a copy
//		// operation. That could flood the notification queue and spam people.
//		if (logicalFile.isDirectory())
//		{
//			try {
//				// we also need to replicate any children that were not copied over before
//				List<LogicalFile> nonOverlappingChildren =
//						LogicalFileDao.findNonOverlappingChildren(logicalFile.getPath(),
//								system.getId(),
//								copiedLogicalFile.getPath(),
//								system.getId());
//
//				for (LogicalFile child: nonOverlappingChildren)
//				{
//					LogicalFile copiedChild = child.clone();
//					copiedChild.setSourceUri(child.getPublicLink());
//					copiedChild.setPath(StringUtils.replaceOnce(child.getPath(), logicalFile.getPath(), copiedLogicalFile.getPath()));
//					copiedChild.setSystem(child.getSystem());
//					copiedChild.setOwner(child.getOwner());
//					copiedChild.setInternalUsername(internalUsername);
//					copiedChild.setLastUpdated(new DateTime().toDate());
//					LogicalFileDao.persist(copiedChild);
//					copiedChild.addContentEvent(new FileEvent(FileEventType.CREATED,
//							"File item copied from " + child.getPublicLink(),
//							getAuthenticatedUsername()));
//					LogicalFileDao.persist(copiedChild);
//				}
//			} catch (Exception e) {
//				String msg = "Unable to clone non-overlapping children of " + logicalFile.getPath() + ".";
//				log.error(msg, e);
//				throw e;
//			}
//		}
//		return new AgaveSuccessRepresentation(message, copiedLogicalFile.toJSON());
	}
}
