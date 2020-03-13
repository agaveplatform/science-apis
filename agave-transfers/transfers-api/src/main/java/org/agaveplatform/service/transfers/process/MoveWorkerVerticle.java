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

public class MoveWorkerVerticle extends AbstractVerticle {

	private final Logger logger = LoggerFactory.getLogger(MoveWorkerVerticle.class);

	public MoveWorkerVerticle(Vertx vertx) {
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer("mkdir", msg -> {
			JsonObject body = msg.body();
			doMoveOperation(body, (LogicalFile) body.get("logicalFile"), PermissionManager body.get("pm"));
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
	 * @param pm
	 * @return
	 * @throws PermissionException
	 * @throws FileNotFoundException
	 * @throws ResourceException
	 * @throws IOException
	 * @throws RemoteDataException
	 * @throws HibernateException
	 * @throws JSONException
	 * @throws RemoteDataSyntaxException
	 */
	protected AgaveSuccessRepresentation doMoveOperation(JsonNode jsonInput,
														 LogicalFile logicalFile, PermissionManager pm)
			throws PermissionException, FileNotFoundException,
			ResourceException, IOException, RemoteDataException,
			HibernateException, JSONException, RemoteDataSyntaxException {
		String message;
		AgaveLogServiceClient.log(AgaveLogServiceClient.ServiceKeys.FILES02.name(),
				AgaveLogServiceClient.ActivityKeys.IOMove.name(),
				username, "", getRequest().getClientInfo().getUpstreamAddress());

		String destPath = null;
		if (jsonInput.has("path")) {
			destPath = jsonInput.get("path").textValue();
		}

		// do they have permission to write to this new folder
		if (!pm.canWrite(remoteDataClient.resolvePath(destPath))) {
			String msg = "User does not have access to move data to " + destPath;
			log.error(msg);
			throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
					msg, new PermissionException());
		}

		// support overwriting, they need to be aware of what is and is not there
		if (remoteDataClient.doesExist(destPath))
		{
			if (remoteDataClient.isDirectory(path) && remoteDataClient.isFile(destPath)) {
				String msg = "File at " + destPath + " already exists";
				log.error(msg);
				throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
						msg, new RemoteDataException());
			} else if (remoteDataClient.isFile(path) && remoteDataClient.isDirectory(destPath)) {
				String msg = "Folder at " + destPath + " already exists";
				log.error(msg);
				throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
						msg, new RemoteDataException());
			}
		}

		// move is essentially just a rename on the same system
		try {
			remoteDataClient.doRename(path, destPath);
		} catch (RemoteDataException e) {
			if (e.getMessage().contains("Destination already exists")) {
				log.error(e.getMessage(), e);
				throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, e.getMessage(), e);
			}
		}

		message = "Move success";
		if (remoteDataClient.isPermissionMirroringRequired())
		{
			remoteDataClient.setOwnerPermission(remoteDataClient.getUsername(), destPath, true);

			String pemUser = StringUtils.isEmpty(owner) ? username : owner;
			try {
				remoteDataClient.setOwnerPermission(pemUser, destPath, true);
			} catch (Exception e) {
				message = "Move was successful, but unable to mirror permissions for user " +
						pemUser + " on new directory " + path;
				log.error(message, e);
			}
		}

		// now keep the logical file up to date
		LogicalFile destLogicalFile = null;
		try {
			destLogicalFile = LogicalFileDao.findBySystemAndPath(system, remoteDataClient.resolvePath(destPath));
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				String msg = "LogicalFileDao.findBySystemAndPath() failed, cloning destination logical file " +
						destPath + ".";
				log.debug(msg, e);
			}
		}

		String sourceUrl = logicalFile.getPublicLink();
		String destUrl = null;

		// no logical file for destination, this is an add or update
		if (destLogicalFile == null)
		{
			try {
				destLogicalFile = logicalFile.clone();
				destLogicalFile.setSourceUri(logicalFile.getPublicLink());
				destLogicalFile.setPath(remoteDataClient.resolvePath(destPath));
				destLogicalFile.setName(FilenameUtils.getName(destLogicalFile.getPath()));
				destLogicalFile.setSystem(logicalFile.getSystem());
				destLogicalFile.setOwner(username);
				destLogicalFile.setInternalUsername(internalUsername);
				destLogicalFile.setLastUpdated(new DateTime().toDate());

				// set the resulting url of the destination for use in events
				destUrl = destLogicalFile.getPublicLink();

				// fire event before record update so the notification event references the source record
				logicalFile.addContentEvent(new FileEvent(FileEventType.MOVED,
						"Moved from " + sourceUrl + " to " + destUrl,
						getAuthenticatedUsername()));

				// now update source path and name to reference the new location. This will
				// carry the history with it.
				logicalFile.setPath(remoteDataClient.resolvePath(destPath));
				logicalFile.setName(FilenameUtils.getName(logicalFile.getPath()));
				logicalFile.setLastUpdated(new DateTime().toDate());

				LogicalFileDao.persist(logicalFile);
			} catch (Exception e) {
				String msg = "Unable to record cloned logical file " + destLogicalFile.getPath() + ".";
				log.error(msg, e);
				throw e;
			}
		}
		else
		{
			try {
				destLogicalFile.setName(FilenameUtils.getName(destLogicalFile.getPath()));

				// set the resulting url of the destination for use in events
				destUrl = destLogicalFile.getPublicLink();

				destLogicalFile.addContentEvent(new FileEvent(FileEventType.OVERWRITTEN,
						"Overwritten by a move from " + logicalFile.getPublicLink() +
								" to " + destLogicalFile.getPublicLink(),
						getAuthenticatedUsername()));
				LogicalFileDao.persist(destLogicalFile);
			} catch (Exception e) {
				String msg = "Unable to record logical file " + destLogicalFile.getPath() + ".";
				log.error(msg, e);
				throw e;
			}
		}

		if (logicalFile.isDirectory())
		{
			try {
				// we also need to replicate any children that were not copied over before
				List<LogicalFile> nonOverlappingChildren =
						LogicalFileDao.findNonOverlappingChildren(logicalFile.getPath(),
								system.getId(),
								destLogicalFile.getPath(),
								system.getId());

				for (LogicalFile child: nonOverlappingChildren) {
					// capture the original url to the child
					String sourceChildUrl = child.getPublicLink();

					// update the path and timestamp
					child.setPath(StringUtils.replaceOnce(child.getPath(), logicalFile.getPath(), destLogicalFile.getPath()));
					child.setLastUpdated(new DateTime().toDate());

					// add event
					child.addContentEvent(new FileEvent(FileEventType.MOVED,
							"File item moved from " + sourceChildUrl + " to " + child.getPublicLink(),
							getAuthenticatedUsername()));

					// update afterwards so the event has the original child path
					LogicalFileDao.persist(child);
				}

				// now that the file item is moved over, we need to alert the children that the file has been copied
				for (LogicalFile child: LogicalFileDao.findChildren(destLogicalFile.getPath(), system.getId())) {
					if (!nonOverlappingChildren.contains(child))
					{
						child.addContentEvent(new FileEvent(FileEventType.OVERWRITTEN,
								"Possibly overwritten as part of file item move from " +
										StringUtils.replace(child.getPublicLink(), destUrl, sourceUrl) +
										" to " + child.getPublicLink(),
								getAuthenticatedUsername()));
						LogicalFileDao.persist(child);
					}
				}
			} catch (Exception e) {
				String msg = "Processing failure for logical directory " + logicalFile.getPath() + ".";
				log.error(msg, e);
				throw e;
			}
		}
		return new AgaveSuccessRepresentation(message, logicalFile.toJSON());
	}

}
