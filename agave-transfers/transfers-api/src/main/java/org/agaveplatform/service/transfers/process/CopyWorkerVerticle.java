package main.java.org.agaveplatform.service.transfers.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonObject;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.hibernate.HibernateException;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.representation.AgaveSuccessRepresentation;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.model.FileEvent;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.permissions.PermissionManager;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.restlet.data.Status;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class CopyWorkerVerticle extends AbstractVerticle {

	private final Logger logger = LoggerFactory.getLogger(CopyWorkerVerticle.class);

	public CopyWorkerVerticle(Vertx vertx) {
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer("mkdir", msg -> {
			JsonObject body = msg.body();
			doCopyOperation(body, (LogicalFile) body.get("logicalFile"), PermissionManager body.get("pm"));
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
	 * @param jsonInput
	 * @param logicalFile
	 * @param absolutePath
	 * @param pm
	 * @return
	 * @throws PermissionException
	 * @throws FileNotFoundException
	 * @throws ResourceException
	 * @throws IOException
	 * @throws RemoteDataException
	 * @throws JSONException
	 * @throws RemoteDataSyntaxException
	 * @throws HibernateException
	 */
	protected AgaveSuccessRepresentation doCopyOperation(JsonNode jsonInput, String absolutePath, LogicalFile logicalFile, PermissionManager pm)
			throws PermissionException, FileNotFoundException, ResourceException, IOException, RemoteDataException, JSONException, RemoteDataSyntaxException
	{
		String message;
		AgaveLogServiceClient.log(AgaveLogServiceClient.ServiceKeys.FILES02.name(),
				AgaveLogServiceClient.ActivityKeys.IOCopy.name(),
				username, "", getRequest().getClientInfo().getUpstreamAddress());

		String destPath = null;
		if (jsonInput.has("path")) {
			destPath = jsonInput.get("path").asText();
		} else {
			String msg = "Please specify a destination location";
			log.error(msg);
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					msg, new RemoteDataException());
		}

		if (!pm.canWrite(remoteDataClient.resolvePath(destPath))) {
			String msg = "User does not have access to copy data to " + destPath;
			log.error(msg);
			throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
					msg, new FileNotFoundException());
		}
		else if (destPath.equalsIgnoreCase(path)) {
			String msg = "Source and destination locations cannot be the same.";
			log.error(msg);
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					msg, new RemoteDataException());
		}

		boolean append = false;
		if (jsonInput.has("append")) {
			append = jsonInput.get("append").asBoolean();
		}

		if (append) {
			remoteDataClient.append(path, destPath);
		} else {
			remoteDataClient.copy(path, destPath);
		}

		message = "Copy success";
		if (remoteDataClient.isPermissionMirroringRequired())
		{
			remoteDataClient.setOwnerPermission(remoteDataClient.getUsername(), destPath, true);

			String pemUser = StringUtils.isEmpty(owner) ? username : owner;
			try {
				remoteDataClient.setOwnerPermission(pemUser, destPath, true);
			} catch (Exception e) {
				message = "Rename was successful, but unable to mirror permissions for user " +
						pemUser + " on new directory " + path;
				log.error(message + ": " + e.getMessage(), e);
			}
		}

		LogicalFile copiedLogicalFile = null;

		if (logicalFile != null)
		{
			copiedLogicalFile = null;

			try {
				copiedLogicalFile = LogicalFileDao.findBySystemAndPath(system, remoteDataClient.resolvePath(destPath));
			} catch(Exception e) {
				if (log.isDebugEnabled()) {
					String msg = "LogicalFileDao.findBySystemAndPath() failed, cloning copied logical file " +
							destPath + ".";
					log.debug(msg, e);
				}
			}

			if (copiedLogicalFile == null) {
				try {
					copiedLogicalFile = logicalFile.clone();
					copiedLogicalFile.setSourceUri(logicalFile.getPublicLink());
					copiedLogicalFile.setPath(remoteDataClient.resolvePath(destPath));
					copiedLogicalFile.setSystem(system);
					copiedLogicalFile.setName(FilenameUtils.getName(copiedLogicalFile.getPath()));
					copiedLogicalFile.setOwner(StringUtils.isEmpty(owner) ? username : owner);
					copiedLogicalFile.setInternalUsername(internalUsername);
					copiedLogicalFile.setLastUpdated(new DateTime().toDate());
					LogicalFileDao.persist(copiedLogicalFile);
					copiedLogicalFile.addContentEvent(new FileEvent(FileEventType.CREATED,
							"File item copied from " + logicalFile.getPublicLink(),
							getAuthenticatedUsername()));
					LogicalFileDao.persist(copiedLogicalFile);
				} catch (Exception e) {
					String msg = "Unable to save cloned logical file " + destPath + ".";
					log.error(msg, e);
					throw e;
				}
			}
		}

		// note that we do not send notifications for every subfile that may have been updated in a copy
		// operation. That could flood the notification queue and spam people.
		if (logicalFile.isDirectory())
		{
			try {
				// we also need to replicate any children that were not copied over before
				List<LogicalFile> nonOverlappingChildren =
						LogicalFileDao.findNonOverlappingChildren(logicalFile.getPath(),
								system.getId(),
								copiedLogicalFile.getPath(),
								system.getId());

				for (LogicalFile child: nonOverlappingChildren)
				{
					LogicalFile copiedChild = child.clone();
					copiedChild.setSourceUri(child.getPublicLink());
					copiedChild.setPath(StringUtils.replaceOnce(child.getPath(), logicalFile.getPath(), copiedLogicalFile.getPath()));
					copiedChild.setSystem(child.getSystem());
					copiedChild.setOwner(child.getOwner());
					copiedChild.setInternalUsername(internalUsername);
					copiedChild.setLastUpdated(new DateTime().toDate());
					LogicalFileDao.persist(copiedChild);
					copiedChild.addContentEvent(new FileEvent(FileEventType.CREATED,
							"File item copied from " + child.getPublicLink(),
							getAuthenticatedUsername()));
					LogicalFileDao.persist(copiedChild);
				}
			} catch (Exception e) {
				String msg = "Unable to clone non-overlapping children of " + logicalFile.getPath() + ".";
				log.error(msg, e);
				throw e;
			}
		}
		return new AgaveSuccessRepresentation(message, copiedLogicalFile.toJSON());
	}
}
