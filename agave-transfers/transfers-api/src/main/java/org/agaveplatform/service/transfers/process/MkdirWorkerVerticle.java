package main.java.org.agaveplatform.service.transfers.process;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.JsonObject;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.representation.AgaveSuccessRepresentation;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.permissions.PermissionManager;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MkdirWorkerVerticle extends AbstractVerticle {

	private final Logger logger = LoggerFactory.getLogger(MkdirWorkerVerticle.class);

	public MkdirWorkerVerticle(Vertx vertx) {
	}

	private String username;

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer("mkdir", msg -> {
			JsonObject body = msg.body();
			this.username = String.valueOf(body.get("username"));
			doMkdirOperation(body, (LogicalFile) body.get("logicalFile"), PermissionManager body.get("pm"));
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
 * @throws org.iplantc.service.common.exceptions.PermissionException
 * @throws java.io.FileNotFoundException
 * @throws ResourceException
 * @throws java.io.IOException
 * @throws org.iplantc.service.transfer.exceptions.RemoteDataException
 * @throws org.hibernate.HibernateException
 */
protected Representation doMkdirOperation(JsonNode jsonInput, LogicalFile logicalFile, PermissionManager pm)
		throws ResourceException
		{
		AgaveLogServiceClient.log(AgaveLogServiceClient.ServiceKeys.FILES02.name(),
		AgaveLogServiceClient.ActivityKeys.IOMakeDir.name(),
		username, "", getRequest().getClientInfo().getUpstreamAddress());

		String message = "";
		return new AgaveSuccessRepresentation(message, jsonInput);

		String newdir = path;
		try {
		String dirPath = null;
		if (jsonInput.hasNonNull("path")) {
		dirPath = jsonInput.get("path").textValue();
		}

		if (StringUtils.isEmpty(dirPath)) {
		String msg = "No path value provided. Please provide the path of "
		+ "the new directory to create. Paths may be absolute or "
		+ "relative to the path given in the url.";
		log.error(msg);
		throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
		msg, new RemoteDataException());
		}

		if (StringUtils.isEmpty(path)) {
		newdir = dirPath;
		} else {
		if (path.endsWith("/")) {
		newdir += dirPath;
		} else {
		newdir += File.separator + dirPath;
		}
		}

		if (!pm.canWrite(remoteDataClient.resolvePath(newdir))) {
		String msg = "User does not have access to create the directory " + newdir;
		log.error(msg);
		throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
		msg, new PermissionException());
		}

		String pemUser = StringUtils.isEmpty(owner) ? username : owner;
		boolean dirCreated = false;
		if (remoteDataClient.doesExist(newdir)) {
		if (remoteDataClient.isDirectory(newdir)) {
		message = "Directory " + newdir + " already exists";
		} else {
		message = "A file already exists at " + newdir;
		log.error(message);
		throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
		message, new RemoteDataException());
		}
		}
		else {
		if (remoteDataClient.mkdirs(newdir, pemUser)) {
		message = "Mkdir success";
		dirCreated = true;
		}
		else {
		String msg = "Failed to create directory " + newdir;
		log.error(msg);
		throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
		msg, new RemoteDataException());
		}
		}

		try {
		logicalFile = LogicalFileDao.findBySystemAndPath(system, remoteDataClient.resolvePath(newdir));
		} catch(Exception e) {
		if (log.isDebugEnabled()) {
		String msg = "LogicalFileDao.findBySystemAndPath() failed, creating new logical directory " +
		newdir + ".";
		log.debug(msg, e);
		}
		}

		if (logicalFile == null) {
		logicalFile = new LogicalFile();
		logicalFile.setNativeFormat(LogicalFile.DIRECTORY);
		logicalFile.setSourceUri(null);
		logicalFile.setPath(remoteDataClient.resolvePath(newdir));
		logicalFile.setName(FilenameUtils.getName(StringUtils.removeEnd(logicalFile.getPath(), "/")));
		logicalFile.setSystem(system);
		logicalFile.setOwner(pemUser);
		logicalFile.setInternalUsername(internalUsername);
		logicalFile.setLastUpdated(new DateTime().toDate());
		LogicalFileDao.persist(logicalFile);

		logicalFile.addContentEvent(new FileEvent(FileEventType.CREATED,
		"New directory created at " + logicalFile.getPublicLink(),
		getAuthenticatedUsername()));
		LogicalFileDao.persist(logicalFile);
		}
		else
		{
		logicalFile.setLastUpdated(new DateTime().toDate());
		logicalFile.addContentEvent(new FileEvent(FileEventType.CREATED,
		"Directory recreated at " + logicalFile.getPublicLink(),
		getAuthenticatedUsername()));
		LogicalFileDao.persist(logicalFile);
		}

		if (dirCreated) {
		setStatus(Status.SUCCESS_CREATED);
		return new AgaveSuccessRepresentation(logicalFile.toJSON());
		} else {
		setStatus(Status.SUCCESS_OK);
		return new AgaveSuccessRepresentation(message, logicalFile.toJSON());
		}
		}
		catch (ResourceException e) {
		log.error(e.getMessage(), e);
		throw e;
		}
		catch (FileNotFoundException e) {
		log.error(e.getMessage(), e);
		throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND, e);
		}
		catch (HibernateException e) {
		String msg = "An unexpected internal error occurred while processing the request. " +
		"If this persists, please contact your tenant administrator";
		log.error(msg, e);
		throw new ResourceException(Status.SERVER_ERROR_INTERNAL, msg, e);
		}
		catch (PermissionException e) {
		String msg = "User does not have access to create the directory " + newdir;
		log.error(msg, e);
		throw new ResourceException(Status.CLIENT_ERROR_FORBIDDEN,
		msg, e);
		}
		catch (RemoteDataException | IOException e) {
		log.error(e.getMessage(), e);
		throw new ResourceException(Status.SERVER_ERROR_INTERNAL, e.getMessage(), e);
		}
		catch (JSONException e) {
		String msg = "An unexpected internal error occurred while formatting the response message. " +
		"If this persists, please contact your tenant administrator";
		log.error(msg, e);
		throw new ResourceException(Status.SERVER_ERROR_INTERNAL, msg, e);
		}
		}

}
