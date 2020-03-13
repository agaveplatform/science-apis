package main.java.org.agaveplatform.service.transfers.process;

import com.fasterxml.jackson.databind.JsonNode;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.iplantc.service.common.clients.AgaveLogServiceClient;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.representation.AgaveErrorRepresentation;
import org.iplantc.service.common.representation.AgaveSuccessRepresentation;
import org.iplantc.service.common.util.AgaveStringUtils;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.model.FileEvent;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.FileOperationType;
import org.iplantc.service.io.permissions.PermissionManager;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.manager.SystemManager;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.model.Range;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import static org.iplantc.service.io.model.enumerations.FileOperationType.*;

/**
 * Class to handle get and post requests for jobs
 *
 * @author dooley
 *
 */
public class Verticle extends AbstractVerticle
{
	private static final Logger log = Logger.getLogger(Verticle.class);

	private String owner;  		// username listed at the root of the url path
	private String internalUsername;  		// username listed at the root of the url path
	private String systemId;
	private RemoteSystem system;

	private String username;	// authenticated user
	private String path;		// path of the file
	private List<Range> ranges = null;	// ranges of the file to return, given by byte index for start and a size.
	private SystemManager sysManager = null;

	private RemoteDataClient remoteDataClient;
	private SystemDao systemDao;
	private Vertx vertx;

	/**
	 * Accepts mkdir, copy, move, touch, index, and rename functionality.
	 */
	public void put(JsonNode jsonInput, LogicalFile logicalFile, PermissionManager pm, FileOperationType operation)
	{
		try {
			if (operation == MKDIR)
			{
				return doMkdirOperation(jsonInput, logicalFile, pm);
			}
			else if (operation == RENAME)
			{
				return doRenameOperation(jsonInput, absolutePath, logicalFile, pm);
			}
			else if (operation == COPY)
			{
				return doCopyOperation(jsonInput, absolutePath, logicalFile, pm);
			}
			else if (operation == MOVE)
			{
				return doMoveOperation(jsonInput, absolutePath, logicalFile, pm);
			}
			else
			{
				String msg = "Action " + operation + " not supported";
				log.error(msg);
				throw new ResourceException(Status.SERVER_ERROR_NOT_IMPLEMENTED, msg);
			}
			}catch (FileNotFoundException e) {
				log.error(e.getMessage(), e);
				throw new ResourceException(Status.CLIENT_ERROR_NOT_FOUND,
						AgaveStringUtils.convertWhitespace(e.getMessage()), e);
			}catch (RemoteDataSyntaxException e) {
				log.error(e.getMessage(), e);
				throw new ResourceException(Status.CLIENT_ERROR_BAD_REQUEST,
						AgaveStringUtils.convertWhitespace(e.getMessage()), e);
			}catch (ResourceException e) {
				log.error(e.getMessage(), e);
				throw e;
			}catch (RemoteDataException e) {
				log.error(e.getMessage(), e);
				throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
						AgaveStringUtils.convertWhitespace(e.getMessage()), e);
			}catch (Exception e) {
				log.error("Error performing file operation", e);
				throw new ResourceException(Status.SERVER_ERROR_INTERNAL,
						"File operation failed " +
								AgaveStringUtils.convertWhitespace(e.getMessage()), e);
			}
		}catch (ResourceException e) {
			setStatus(e.getStatus());
			return new AgaveErrorRepresentation(AgaveStringUtils.convertWhitespace(e.getMessage()));
		}
		finally {
			try {remoteDataClient.disconnect();} catch (Exception e) {}
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
	 */
	protected Representation doMkdirOperation(JsonNode jsonInput, LogicalFile logicalFile, PermissionManager pm)
			throws ResourceException
	{
		AgaveLogServiceClient.log(AgaveLogServiceClient.ServiceKeys.FILES02.name(),
				AgaveLogServiceClient.ActivityKeys.IOMakeDir.name(),
				username, "", getRequest().getClientInfo().getUpstreamAddress());

		String message = "";
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
