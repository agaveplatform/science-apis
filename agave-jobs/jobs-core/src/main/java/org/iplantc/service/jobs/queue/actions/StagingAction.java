/**
 *
 */
package org.iplantc.service.jobs.queue.actions;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.uri.AgaveUriRegex;
import org.iplantc.service.common.uri.UrlPathEscaper;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.permissions.PermissionManager;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.exceptions.JobDependencyException;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.JobInputStagingException;
import org.iplantc.service.jobs.exceptions.MissingDataException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.managers.JobPermissionManager;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.JobEvent;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.iplantc.service.systems.util.ApiUriUtil;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.URLCopy;
import org.iplantc.service.transfer.dao.TransferTaskDao;
import org.iplantc.service.transfer.exceptions.AuthenticationException;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.model.TransferTask;
import org.joda.time.DateTime;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * @author dooley
 *
 */
public class StagingAction extends AbstractWorkerAction {

    private static final Logger log = Logger.getLogger(StagingAction.class);

    public StagingAction(Job job) {
        super(job);
    }

    /**
     * Stages the user-supplied {@link Job#getInputs()} to the {@link Job#getWorkPath()} on the {@link ExecutionSystem}.
     * If an input, system, or permissions are absent and the job cannot continue without them, then a @link {JobDependencyException}
     * will be thrown and no retries will be made. Otherwise, any failures will result in the job following the normal
     * retry policy.
     *
     * @throws SystemUnavailableException when one of the input or execution systems is not available.
     * @throws JobException when a non-critical failure occurs such as a connection failure, db lookup failure, etc.
     * @throws JobDependencyException when a critical job dependency is absent and the job cannot continue. This will
     *   effectively cancel any further attempts.
     * @throws ClosedByInterruptException when the action is interrupted by an external process
     */
    public void run() throws SystemUnavailableException, JobException, ClosedByInterruptException, JobDependencyException {

        RemoteDataClient jobInputRemoteDataClient = null;
        RemoteDataClient jobExecutionSystemRemoteDataClient = null;

        try {
            ExecutionSystem executionSystem = getExecutionSystem();

            // copy to remote execution work directory
            jobExecutionSystemRemoteDataClient = getRemoteDataClientForExecutionSystem(executionSystem);

            // calculate the job work directory. we do this here in the event the execution system
            // root, home, work, or scratch directories change between job request time and  input staging.
            String remoteJobWorkPath = calculateRemoteJobPath(executionSystem);
            createJobRemoteWorkPath(executionSystem, jobExecutionSystemRemoteDataClient, remoteJobWorkPath);

            getJob().setWorkPath(remoteJobWorkPath);

            log.debug("Beginning staging inputs for job " + getJob().getUuid() + " to " +
                    executionSystem.getSystemId() + ":" + getJob().getWorkPath());

            // we need a way to parallelize this task. Ideally we'd just throw each input
            // file to the staging queue and let 'em rip
            JobManager.updateStatus(getJob(), JobStatusType.PROCESSING_INPUTS);

            // Get a well-formed map of user-supplied + default + hidden/required inputs for the job
            // Inputs are stored as a JSON object with the Job table, so we use this convenience
            // method to make it less nasty to work with
            Map<String, String[]> jobInputMap = JobManager.getJobInputMap(getJob());

            // each job input corresponds to a SoftwareInput, which may have multiple values, so we iterate over
            // each job input, processing all values for that input in turn.
            for (String inputKey : jobInputMap.keySet()) {
                // circuit breaker
                checkStopped();

                String[] rawInputValues = jobInputMap.get(inputKey);

                URI singleRawInputUri = null;
                RemoteSystem jobInputSystem = null;

                // inputs can have multiple values provided to them by the user and/or their defaults.
                // here we iterate over each input value, staging them to the remote system.
                for (String singleRawInputValue : rawInputValues) {
                    checkStopped();

                    // ensure it's a valid URI we can pass for parsing.
                    singleRawInputUri = new URI(singleRawInputValue);

                    // URL will be handled differently depending on the scheme and whether it points to an internal
                    // API resource. We check that first before proceeding.
                    String remoteJobInputPath = null;
                    if (ApiUriUtil.isInternalURI(singleRawInputUri)) {
                        // get system for an input URI representing an internal or agave url
                        jobInputSystem = ApiUriUtil.getRemoteSystem(getJob().getOwner(), singleRawInputUri);

                        // get a client to the remote system for the input
                        jobInputRemoteDataClient = getRemoteDataClientForInputSystem(jobInputSystem, singleRawInputValue);

                        remoteJobInputPath = getRemoteJobInputPathIfJobOwnerHasPermission(singleRawInputUri, jobInputSystem, jobInputRemoteDataClient);
                    } else {
                        // We handle non-internal URL as standard URL. We parse based on URI schema and generate
                        // a client dynamically based on URI components.
                        jobInputRemoteDataClient = getRemoteDataClientForUri(singleRawInputUri);

                        // for non-internal URL, the client will use "/" as the rootdir and homedir, so the given path
                        // will be treated as absolute regardless.
                        remoteJobInputPath = singleRawInputUri.getPath();
                    }

                    // figure out the agave relative path of the job input on the execution system
                    String destPath = Paths.get(remoteJobWorkPath).resolve(FilenameUtils.getName(remoteJobInputPath)).toString();

                    // circuit breaker
                    checkStopped();

                    try {
                        // see if we can skip this transfer due to prior success
                        if (!isJobInputAlreadyTransferred(singleRawInputValue, destPath, jobExecutionSystemRemoteDataClient, remoteJobInputPath, jobInputRemoteDataClient)) {
                            // finally ok to make the remote transfer
                            transferJobInput(jobInputRemoteDataClient, jobExecutionSystemRemoteDataClient, singleRawInputValue, remoteJobInputPath, destPath);
                        }
                    } finally {
                        // close connections every time since we reuse these RDC.
                        try { jobExecutionSystemRemoteDataClient.disconnect(); } catch (Exception ignored) {}
                        try { jobInputRemoteDataClient.disconnect(); } catch (Exception ignored) {}
                    }
                }
            }

            log.debug("Completed staging inputs for job " + getJob().getUuid() + " to " +
                    executionSystem.getSystemId() + ":" + getJob().getWorkPath());

        }
        catch (JobException|ClosedByInterruptException e) {
            throw e;
        }
        catch (SystemUnknownException | MissingDataException e) {
            log.error(e.getMessage());
            throw new JobDependencyException(e.getMessage(), e);
        }
        catch (PermissionException e) {
            String message = "User lacks permissions to access input for job " + getJob().getUuid() + ". " +
                    e.getMessage();
            log.error(message);
            throw new JobDependencyException(message, e);
        }
        catch (AuthenticationException e) {
            log.error("Unable to authenticate to stage input file for job " + getJob().getUuid() + ". " +
                    e.getMessage());
            throw new JobDependencyException(e.getMessage(), e);
        }
        catch (URISyntaxException e) {
            String message = "Invalid input url, " + e.getInput() + " provided as input for job " +
                    getJob().getUuid();
            log.error(message);
            throw new JobDependencyException(message, e);
        }
        catch (AgaveNamespaceException e) {
            String message = "Invalid url format provided as input for job " +
                    getJob().getUuid() + ". " + e.getMessage();
            log.error(message);
            throw new JobDependencyException(message, e);
        }
        catch (NotImplementedException e) {
            log.error("Invalid input protocol provided for job " + getJob().getUuid());
            throw new JobDependencyException(e.getMessage());
        }
        catch (JobInputStagingException e) {
            log.error("Failed to create work directory for job " + getJob().getUuid(), e);
            throw new JobException(e.getMessage(), e);
        }
        catch (TransferException | RemoteDataException e) {
            // these will be self-describing, non-fatal exceptions
            log.error(e.getMessage());
            throw new JobException(e.getMessage(), e);
        }
        catch (Throwable e) {
            log.error("Failed to stage input for job " + getJob().getUuid(), e);
            throw new JobException(e.getMessage(), e);
        }
        finally {
            if (jobInputRemoteDataClient != null) jobInputRemoteDataClient.disconnect();
            if (jobExecutionSystemRemoteDataClient != null) jobExecutionSystemRemoteDataClient.disconnect();
        }

        if (!isStopped()) {
            // status should have been updated in job object if anything was
            // staged
            if (getJob().getStatus() == JobStatusType.STAGING_INPUTS) {
                updateJobStatus(JobStatusType.STAGED, JobStatusType.STAGED.getDescription());
                log.debug("Completed staging inputs for job " + getJob().getUuid() + " " + getJob().getName());
            }
        }
    }

    /**
     * Calculates the job work path if not already set for the current job.
     * @param executionSystem the {@link ExecutionSystem} on which the job will run
     * @return the remote job work directory
     * @see JobManager#calculateJobRemoteWorkPath(Job, ExecutionSystem)
     */
    protected String calculateRemoteJobPath(ExecutionSystem executionSystem) {
        String remoteJobPath = getJob().getWorkPath();
        if (StringUtils.isBlank(remoteJobPath)) {
            remoteJobPath = getJobManager().calculateJobRemoteWorkPath(getJob(), executionSystem);
        }
        return remoteJobPath;
    }

    /**
     * Checks that the user has permission to access the job input given by an internal agave URI, then returns the
     * agave relative path. If the input is a job url, then we look up the current job owner's permissions for the
     * input job and make an entitlement decision based on that.
     *
     * @param singleRawInputUri the original input uri given in the job request.
     * @param jobInputSystem the {@link RemoteSystem} on which the input file item resides.
     * @param jobInputRemoteDataClient the pre-authenticated client to access the input file item on the {@code jobInputSystem}
     * @return an agave relative path of the job input on the {@code jobInputSystem}.
     * @throws PermissionException if the job owner does not have permission to read the job input file item and/or system
     * @throws RemoteDataException when unable to carry out a remote existence check on the job input
     * @throws MissingDataException if the job input file item does not exist
     */
    protected String getRemoteJobInputPathIfJobOwnerHasPermission(URI singleRawInputUri, RemoteSystem jobInputSystem, RemoteDataClient jobInputRemoteDataClient)
    throws PermissionException, RemoteDataException, MissingDataException {

        String remoteJobInputPath;
        // Since this is an internal URL, check permissions to ensure the user has the proper
        // permissions to access it.

        String remoteAbsolutePathOnJobInputSystem = null;
        try {
            remoteAbsolutePathOnJobInputSystem = UrlPathEscaper.decode(ApiUriUtil.getAbsolutePath(getJob().getOwner(), singleRawInputUri));
        } catch (AgaveNamespaceException | SystemUnknownException ignored) {
            // This exception should not be possible since we have already resolved the system and passed it in
            // as an argument to this method.
        }

        LogicalFile logicalFile = LogicalFileDao.findBySystemAndPath(
                jobInputSystem, remoteAbsolutePathOnJobInputSystem);

        PermissionManager pm = new PermissionManager(
                jobInputSystem, jobInputRemoteDataClient, logicalFile, getJob().getOwner());

        if (logicalFile == null) {
            remoteJobInputPath = new LogicalFile(getJob().getOwner(), jobInputSystem, remoteAbsolutePathOnJobInputSystem)
                    .getAgaveRelativePathFromAbsolutePath();
        } else {
            // normalize remote path to relative path for copying later on
            remoteJobInputPath = logicalFile.getAgaveRelativePathFromAbsolutePath();
        }

        // check for existence of the job input.
        boolean jobInputExists = false;
        try {
            jobInputExists = jobInputRemoteDataClient.doesExist(remoteJobInputPath);
        } catch (IOException ignored) {
            // ignore invalid path issues. the path had to be resolved when the logical file was created
        }

        if (jobInputExists) {
            if (!pm.canRead(remoteAbsolutePathOnJobInputSystem)) {
                // the file permission check won't catch the job permissions implicitly granted on
                // the output folder because we don't have a complete manifest of the job output folder.
                // If file permissions fail, we still need to manually check the job permissions.
                if (AgaveUriRegex.JOBS_URI.matches(singleRawInputUri)) {
                    Matcher jobOutputMatcher = AgaveUriRegex.getMatcher(singleRawInputUri);
                    String referencedJobIdFromInputUrl = jobOutputMatcher.group(1);
                    Job referenceJob = null;
                    try {
                        // lookup the job from the input url. This will return null if not found
                        referenceJob = JobDao.getByUuid(referencedJobIdFromInputUrl);

                        // a null response means the job does not exist for this tenant and will be treated like
                        // a missing input file.
                        if (referenceJob == null) {
                            throw new MissingDataException("Unable to locate Agave job " + referencedJobIdFromInputUrl +
                                    " corresponding to the input value " + singleRawInputUri + " of job " +
                                    getJob().getUuid());
                        } else if (!new JobPermissionManager(referenceJob, getJob().getOwner()).canRead(getJob().getOwner())) {
                            throw new PermissionException("User does not have permission to access " +
                                    "the output folder of job " + referencedJobIdFromInputUrl);
                        } else {
                            // permissions look good for the job input. we're good to submit and return here
                        }
                    } catch (JobException e) {
                        // unable to look up the job by uuid. we'll treat this as a
                        // failed input resolution so we can retry in a bit
                        throw new MissingDataException("Unable to lookup job " + referencedJobIdFromInputUrl +
                                " corresponding to the input value " + singleRawInputUri + " of job " +
                                getJob().getUuid());
                    }
                } else {
                    throw new PermissionException("User does not have permission to access " +
                            remoteJobInputPath + " on " + jobInputSystem.getSystemId());
                }
            }
        } else {
            // the data does not exist. We craft a message explaining why.
            if (AgaveUriRegex.JOBS_URI.matches(singleRawInputUri)) {
                Matcher jobOutputMatcher = AgaveUriRegex.getMatcher(singleRawInputUri);
                throw new MissingDataException("Unable to locate " + jobOutputMatcher.group(2) +
                        " in output/archive directory of job " + jobOutputMatcher.group(1));
            } else {
                throw new MissingDataException("Unable to locate " +
                        remoteJobInputPath + " on " + jobInputSystem.getSystemId());
            }
        }

        // no exceptions were thrown, so the input is valid and we return the path.
        return remoteJobInputPath;
    }

    /**
     * Carries out the transfer of the job input file using a {@link URLCopy} operation. This will handle both
     * files and folders regardless of protocol and track the transfers with {@link TransferTask} linked to
     * {@link JobStatusType#STAGING_INPUTS} events associated with the current job.
     *
     * @param jobInputRemoteDataClient a preauthenticated clien to the job input
     * @param jobExecutionSystemRemoteDataClient a preauthenticated client to the execution system
     * @param singleRawInputValue the raw job input value
     * @param remoteJobInputPath the path to the job input on the source system
     * @param destPath the target path to which data will be copied
     * @throws TransferException when the transfer cannot be carried out due to procedural issues
     * @throws JobException when the job status and/or event cannot be udpated
     * @throws ClosedByInterruptException when the transfer is interrupted by an outside process
     */
    protected void transferJobInput(RemoteDataClient jobInputRemoteDataClient, RemoteDataClient jobExecutionSystemRemoteDataClient, String singleRawInputValue, String remoteJobInputPath, String destPath)
    throws TransferException, JobException, ClosedByInterruptException {
        // nope. still have to copy them. proceed
        TransferTask rootTransferTask = new TransferTask(
                singleRawInputValue,
                "agave://" + getJob().getSystem() + "/" + destPath,
                getJob().getOwner(),
                null,
                null);

        TransferTaskDao.persist(rootTransferTask);

        JobEvent event = new JobEvent(
                JobStatusType.STAGING_INPUTS,
                "Copy in progress",
                rootTransferTask,
                getJob().getOwner());

        getJob().setStatus(JobStatusType.STAGING_INPUTS, event);
        getJob().setLastUpdated(new DateTime().toDate());
        JobDao.persist(getJob());

        URLCopy urlCopy = getURLCopy(jobInputRemoteDataClient, jobExecutionSystemRemoteDataClient);

        try {
            urlCopy.copy(remoteJobInputPath, destPath, rootTransferTask);
        } catch (ClosedByInterruptException e) {
            throw e;
        } catch (Throwable e) {
            // we may not be able to kill the gridftp threads associated with this transfer,
            // so in that event, the transfer will time out and we can catch the exception
            // here to rethrow as a ClosedByInterruptException.
            checkStopped();
            if (urlCopy.isKilled()) {
                throw new ClosedByInterruptException();
            }
            throw new TransferException("Failed to transfer input " + singleRawInputValue + " for job " +
                    getJob().getUuid(), e);
        }
    }

    /**
     * Getter to create a {@link URLCopy} top copy data from a job request input url to the job work directory.
     * This is primarily here for easier mocking during tests.
     *
     * @param srcRemoteDataClient the source data client
     * @param destSystemRemoteDataClient the dest data client
     * @return an initialized URLCopy class for the given clients
     */
    protected URLCopy getURLCopy(RemoteDataClient srcRemoteDataClient, RemoteDataClient destSystemRemoteDataClient) {
        return new URLCopy(srcRemoteDataClient, destSystemRemoteDataClient);
    }

    /**
     * Checks whether the given job input exists in the job work directory already and, if so, whether the size, type,
     * and checksums match up. Since checksums are not supported by all {@link RemoteDataClient} implementations, we
     * fall back to a standard size comparison when not available.
     *
     * @param singleRawInputValue the raw job input value
     * @param destPath the path on the execution system where the input should be copied
     * @param jobExecutionSystemRemoteDataClient a preauthenticated client to the execution system
     * @param remoteJobInputPath the path to the job input on the source system
     * @param jobInputRemoteDataClient a preauthenticated clien to the job input
     * @return true if the job input file item exists on the execution system and matches boht type and size (if a file)
     * @throws JobException if unable to update job status to reflect the file will be skipped
     */
    protected boolean isJobInputAlreadyTransferred(String singleRawInputValue, String destPath, RemoteDataClient jobExecutionSystemRemoteDataClient, String remoteJobInputPath, RemoteDataClient jobInputRemoteDataClient)
    throws JobException {
        boolean skipTransfer = false;
        String message = null;
        try {
            // we use a stat here to avoid having to do both an existence check and length query
            RemoteFileInfo destFileInfo = jobExecutionSystemRemoteDataClient.getFileInfo(destPath);
            // directories will be sorted out on their own. here we check for files already being copied
            // in with a length check. If that passes, we can compute the checksum, though, those are not
            // implemented for every remote data client at this time.
            if (destFileInfo.isFile()) {
                // get source file info. We checked for existence when we first resolved the path for this input
                RemoteFileInfo srcFileInfo = jobExecutionSystemRemoteDataClient.getFileInfo(remoteJobInputPath);
                if (srcFileInfo.getSize() == destFileInfo.getSize()) {
                    // verify the checksums are the same before skipping?
                    try {
                        String sourceChecksum = jobInputRemoteDataClient.checksum(remoteJobInputPath);
                        String destChecksum = jobExecutionSystemRemoteDataClient.checksum(destPath);

                        if (StringUtils.equals(sourceChecksum, destChecksum)) {
                            message = "Input file " + singleRawInputValue + " of idential size was found in the work folder of job "
                                    + getJob().getUuid() + ". The checksums were identical. This input will not be recopied.";
                            skipTransfer = true;
                        } else {
                            message = "Input file " + singleRawInputValue + " of idential size was found in the work folder of job "
                                    + getJob().getUuid() + ". The checksums did not match, so the input file will be transfered to the "
                                    + "target system and overwrite the existing file.";
                        }
                    }
                    catch (NotImplementedException e) {
                        // checksum comparison not available. we'll go ahead and trust the size
                        message = "Input file " + singleRawInputValue + " of idential size was found in the work folder of job "
                                + getJob().getUuid() + ". Unable to calculate checksums. This input will not be recopied.";

                        skipTransfer = true;
                    } catch (Throwable e) {
                        // couldn't calculate the checksum due to server side error
                        // we'll err on the side of caution and recopy
                        message = "Unable to locate file " + singleRawInputValue + " in the work folder of job "
                                + getJob().getUuid() + ". This input will not be recopied.";
                    }
                }
            }
        } catch (IOException | RemoteDataException ignored) {
            // destination path does not exist or is unreadable. The transfer will proceed.
            message = singleRawInputValue + " is not present in the work folder of job "
                    + getJob().getUuid() + ". This input will be copied.";
        }

        log.debug(message);

        if (skipTransfer) {
            getJob().setStatus(JobStatusType.STAGING_INPUTS, message);
        }

        return skipTransfer;
    }

    /**
     * Creates the remote job work directory if not already present
     * @param executionSystem the system on which the job will run
     * @param jobExecutionSystemRemoteDataClient the client used to create the directory
     * @param remoteJobDirectoryPath path to the job directory to create
     * @throws RemoteDataException when unable to create the directory
     * @throws IOException when unable to connect to the remote system due to base IO issues.
     */
    protected void createJobRemoteWorkPath(ExecutionSystem executionSystem, RemoteDataClient jobExecutionSystemRemoteDataClient, String remoteJobDirectoryPath)
    throws JobInputStagingException {
        try {
            jobExecutionSystemRemoteDataClient.mkdirs(remoteJobDirectoryPath, getJob().getOwner());
        } catch (RemoteDataException e) {
            if (e.getMessage().toLowerCase().contains("permission")) {
                throw new JobInputStagingException("Unable to create the remote job directory, " +
                        remoteJobDirectoryPath + ", on " + executionSystem.getSystemId() +
                        " for job " + getJob().getUuid() + ". Response from the server was: " + e.getMessage(), e);
            } else {
                throw new JobInputStagingException("Failed to create the remote job directory " +
                        remoteJobDirectoryPath + " on " + executionSystem.getSystemId() +
                        " for job " + getJob().getUuid() + ". Response from server was: " + e.getMessage(), e);
            }
        } catch (Throwable e) {
            throw new JobInputStagingException("Failed to connect to " + executionSystem.getSystemId() +
                    " to create the remote job directory for job " + getJob().getUuid() +
                    ". Response from server was: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a {@link RemoteDataClient} for the job execution system. If the system is unavailable, unknown, or
     * inaccessible, an exception will be thrown.
     * @param jobInputSystem the job input system for which to create a remote data client.
     * @param singleRawInputValue the job input value for which the {@link RemoteDataClient} is being created.
     * @return an authenticated client capable of staging data to the remote system.
     * @throws AuthenticationException when credentials are bad or {@link RemoteDataClient#authenticate()} fails.
     * @throws RemoteDataException when unable to establish a connection due to system outage or connectivity issues.
     * @throws SystemUnavailableException if the job input system is unknown or unavailable.
     */
    protected RemoteDataClient getRemoteDataClientForInputSystem(RemoteSystem jobInputSystem, String singleRawInputValue)
    throws SystemUnavailableException, RemoteDataException, AuthenticationException {

        if (!isSystemAvailable(jobInputSystem)) {
            throw new SystemUnavailableException("Unable to stage input " + singleRawInputValue +
                    " for job " + getJob().getUuid() + " from system " + jobInputSystem.getSystemId() +
                    ". The system is currently unavailable.");
        }

        RemoteDataClient remoteDataClient = null;
        try {
            remoteDataClient = jobInputSystem.getRemoteDataClient(getJob().getInternalUsername());
        } catch (RemoteCredentialException e) {
            throw new RemoteDataException("Invalid authentication provided for job input system " +
                    jobInputSystem.getSystemId() + " for job " + getJob().getUuid() + ". " + e.getMessage(), e);
        } catch (Throwable t) {
            // can happen if protocol not supported, socket issues, etc.
            throw new RemoteDataException("Unable to create client connection to input system " +
                    jobInputSystem.getSystemId() + " for job " + getJob().getUuid() + ". " + t.getMessage(), t);
        }

        try {
            remoteDataClient.authenticate();
        } catch (Throwable e) {
            remoteDataClient.disconnect();
            throw new AuthenticationException("Authentication failed when connecting to " + singleRawInputValue +
                    " for job " + getJob().getUuid() + ". " + e.getMessage(), e);
        }

        return remoteDataClient;
    }

    /**
     * Creates a {@link RemoteDataClient} for the job execution system. If the system is unavailable, unknown, or
     * inaccessible, an exception will be thrown.
     * @param executionSystem the system for which to create a remote data client.
     * @return an authenticated client capable of staging data to the remote system.
     * @throws AuthenticationException when credentials are bad or {@link RemoteDataClient#authenticate()} fails.
     * @throws RemoteDataException when unable to establish a connection due to system outage or connectivity issues.
     */
    protected RemoteDataClient getRemoteDataClientForExecutionSystem(ExecutionSystem executionSystem) throws AuthenticationException, RemoteDataException {
        RemoteDataClient remoteDataClient = null;
        try {
            remoteDataClient = executionSystem.getRemoteDataClient(getJob().getInternalUsername());
        } catch (RemoteCredentialException e) {
            throw new AuthenticationException("Authentication failed when connecting to execution system " +
                    executionSystem.getSystemId() + " for job " + getJob().getUuid() + ". " + e.getMessage(), e);
        } catch (Throwable t) {
            // can happen if protocol not supported, socket issues, etc.
            throw new RemoteDataException("Unable to create client connection to " + executionSystem.getSystemId() +
                    " for job " + getJob().getUuid() + ". " + t.getMessage(), t);
        }

        try {
            remoteDataClient.authenticate();
        } catch (Throwable e) {
            remoteDataClient.disconnect();
            throw new AuthenticationException("Authentication failed when connecting to execution system " +
                    executionSystem.getSystemId() + " for job " + getJob().getUuid() + ". " + e.getMessage(), e);
        }

        return remoteDataClient;
    }

    /**
     * Fetches a remote data client for a non-internal URI provided as a job input. If the URI does not adhere to
     * a valid fully authenticated or fully unauthenticated schema, it will be rejected.
     * @param singleRawInputUri a single raw URI provided as an input location for the job
     * @return an authenticated client capable of fetching the URI
     * @throws RemoteDataException when the URI is invalid, inaccessible, or authentication is invalid
     * @throws PermissionException when the user does not have permission to access a system represented by the remote url
     * @throws AuthenticationException when unable to authenticate the remote data client
     */
    protected RemoteDataClient getRemoteDataClientForUri(URI singleRawInputUri) throws RemoteDataException, AuthenticationException, PermissionException {
        RemoteDataClient remoteDataClient = null;
        try {
            remoteDataClient = new RemoteDataClientFactory().getInstance(
                    getJob().getOwner(), getJob().getInternalUsername(), singleRawInputUri);
        } catch (IOException | SystemUnknownException e) {
            throw new RemoteDataException("Unable to locate input " + singleRawInputUri +
                    " for job " + getJob().getUuid() + ". " + e.getMessage(), e);
        } catch (AgaveNamespaceException | RemoteCredentialException e) {
            throw new AuthenticationException("Invalid authentication credentials provided for job input " +
                    singleRawInputUri + " for job " + getJob().getUuid() + ". " + e.getMessage(), e);
        } catch (Throwable t) {
            // can happen if protocol not supported, socket issues, etc.
            throw new RemoteDataException("Unable to create client connection to " + singleRawInputUri +
                    " for job " + getJob().getUuid() + ". " + t.getMessage(), t);
        }

        try {
            remoteDataClient.authenticate();
        } catch (Throwable e) {
            remoteDataClient.disconnect();
            throw new AuthenticationException("Authentication failed when connecting to " + singleRawInputUri +
                    " for job " + getJob().getUuid() + ". " + e.getMessage(), e);
        }

        return remoteDataClient;
    }

    /**
     * Gets the job execution system with proper avaialbility checks
     * @return the execution system for the job
     * @throws SystemUnavailableException if the system of unknown, offline, or not in an up state.
     * @see #isSystemAvailable(RemoteSystem) for availability check
     */
    protected ExecutionSystem getExecutionSystem() throws SystemUnavailableException {
        ExecutionSystem executionSystem = null;
        try {
            executionSystem = new JobManager().getJobExecutionSystem(getJob());
            if (!isSystemAvailable(executionSystem)) {
                throw new SystemUnavailableException("Job execution system " + executionSystem.getSystemId() +
                        " is currently unavailable.");
            }
            return executionSystem;
        } catch (SystemUnavailableException e) {
            throw new SystemUnavailableException("Unable to stage inputs for job " + getJob().getUuid() +
                    ". " + e.getMessage());
        }
    }

    /**
     * Null-safe checks for availability of a given system.
     * @param remoteSystem the system to check
     * @return true if the system is non-null, deleted, or not in an up state. False otherwise.
     */
    protected boolean isSystemAvailable(RemoteSystem remoteSystem) {
        return remoteSystem != null &&
                remoteSystem.isAvailable() &&
                remoteSystem.getStatus().equals(SystemStatusType.UP);
    }
}
