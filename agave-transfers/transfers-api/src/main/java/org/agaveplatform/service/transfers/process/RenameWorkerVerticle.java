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
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class RenameWorkerVerticle extends AbstractVerticle {

	private final Logger logger = LoggerFactory.getLogger(RenameWorkerVerticle.class);

	public RenameWorkerVerticle(Vertx vertx) {
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer("mkdir", msg -> {
			JsonObject body = msg.body();
			doRenameOperation(body, (LogicalFile) body.get("logicalFile"), PermissionManager body.get("pm"));
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
	 * @param absolutePath
	 * @param logicalFile
	 * @param pm
	 * @return the new file item representation
	 * @throws ResourceException
	 * @throws PermissionException
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws RemoteDataException
	 * @throws HibernateException
	 * @throws JSONException
	 * @throws RemoteDataSyntaxException
	 */
	protected Representation doRenameOperation(JsonNode jsonInput, String absolutePath,
											   LogicalFile logicalFile, PermissionManager pm)
			throws ResourceException, PermissionException,
			FileNotFoundException, IOException, RemoteDataException,
			HibernateException, JSONException, RemoteDataSyntaxException {
		String message;
		AgaveLogServiceClient.log(AgaveLogServiceClient.ServiceKeys.FILES02.name(),
				AgaveLogServiceClient.ActivityKeys.IORename.name(),
				username, "", getRequest().getClientInfo().getUpstreamAddress());

		String newName = null;
		if (jsonInput.has("path")) {
			newName = jsonInput.get("path").textValue();
		}

		if (StringUtils.isEmpty(newName)) {
			String msg = "No 'path' value provided. Please provide the new name of "
					+ "the file or folder in the 'path' attribute.";
			log.error(msg);
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					msg, new RemoteDataException());
		}

		String newPath = path;
		if (newPath.endsWith("/")) {
			newPath = StringUtils.removeEnd(newPath, "/");
		}

		if (StringUtils.isEmpty(newPath) || newPath.equals("/")) {
			String msg = "No path specified.";
			log.error(msg);
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST, msg);
		}
		else {
			String currentTargetDirectory = org.codehaus.plexus.util.FileUtils.getPath(newPath);
			if (StringUtils.isEmpty(currentTargetDirectory)) {
				newPath = newName;
			} else {
				newPath = currentTargetDirectory + "/" + newName;
			}
		}

		// do they have permission to write to this new folder
		if (!pm.canWrite(remoteDataClient.resolvePath(newPath))) {
			String msg = "User does not have access to rename the requested resource";
			log.error(msg);
			throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
					msg, new PermissionException());
		}

		if (remoteDataClient.doesExist(newPath)) {
			String msg = "A File or folder at " + newPath + " already exists";
			log.error(msg);
			throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
					msg, new RemoteDataException());
		}

		// move is essentially just a rename on the same system
		remoteDataClient.doRename(path, newPath);
		message = "Rename success";
		if (remoteDataClient.isPermissionMirroringRequired())
		{
			remoteDataClient.setOwnerPermission(remoteDataClient.getUsername(), newPath, true);

			String pemUser = StringUtils.isEmpty(owner) ? username : owner;
			try {
				remoteDataClient.setOwnerPermission(pemUser, newPath, true);
			} catch (Exception e) {
				message = "Rename was successful, but unable to mirror permissions for user " +
						pemUser + " on new directory " + path;
				log.error(message, e);
			}
		}

		// now keep the logical file up to date
		try {
			logicalFile.setPath(remoteDataClient.resolvePath(newPath));
			logicalFile.setName(FilenameUtils.getName(newPath));
			logicalFile.addContentEvent(new FileEvent(FileEventType.RENAME,
					"Renamed by " + getAuthenticatedUsername() + " from " + path + " to " + newPath,
					getAuthenticatedUsername()));
			LogicalFileDao.persist(logicalFile);
		} catch (Exception e) {
			String msg = "Unable to update renamed logical file " + newPath + ".";
			log.error(msg);
			throw e;
		}

		if (logicalFile.isDirectory())
		{
			try {
				// we can delete any children under the destination root since they could
				// not actually exist if the rename operation worked.
				for (LogicalFile child: LogicalFileDao.findChildren(logicalFile.getPath(), system.getId())) {
					child.addContentEvent(new FileEvent(FileEventType.DELETED,
							"Detected that file item was deleted by an outside source"
									+ " as part of a rename operation on " + getPublicLink(system, path),
							getAuthenticatedUsername()));
					LogicalFileDao.remove(child);
				}

				// we also need to replicate any children that were not copied over before
				List<LogicalFile> renamedChildren =
						LogicalFileDao.findChildren(logicalFile.getPath(), system.getId());

				for (LogicalFile child: renamedChildren) {
					child.setPath(StringUtils.replaceOnce(child.getPath(), absolutePath, logicalFile.getPath()));
					child.setLastUpdated(new DateTime().toDate());
					LogicalFileDao.persist(child);
					child.addContentEvent(new FileEvent(FileEventType.MOVED,
							"File item moved from " + child.getPublicLink() +
									" as part of a rename operation on " + logicalFile.getPublicLink(),
							getAuthenticatedUsername()));
					LogicalFileDao.persist(child);
				}
			} catch (Exception e) {
				String msg = "Unable to update descendents fo logical directory " + logicalFile.getPath() + ".";
				log.error(msg);
				throw e;
			}
		}

		return new AgaveSuccessRepresentation(message, logicalFile.toJSON());
	}
}
