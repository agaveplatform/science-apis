package org.iplantc.service.jobs.managers.launchers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.apps.managers.SoftwareEventProcessor;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareInput;
import org.iplantc.service.apps.model.SoftwareParameter;
import org.iplantc.service.apps.model.enumerations.SoftwareEventType;
import org.iplantc.service.apps.model.enumerations.SoftwareParameterType;
import org.iplantc.service.apps.util.ServiceUtils;
import org.iplantc.service.apps.util.ZipUtil;
import org.iplantc.service.common.Settings;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.model.FileEvent;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.exceptions.*;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.JobEvent;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.model.enumerations.WrapperTemplateStatusVariableType;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.transfer.*;
import org.iplantc.service.transfer.dao.TransferTaskDao;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.model.TransferTask;
import org.iplantc.service.transfer.model.TransferTaskImpl;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;
import org.iplantc.service.transfer.util.MD5Checksum;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Interface to define how to launch applications on various resources
 *
 * @author dooley
 */
public abstract class AbstractJobLauncher implements JobLauncher {
    private static final Logger log = Logger.getLogger(AbstractJobLauncher.class);
    public static final String ARCHIVE_FILENAME = ".agave.archive";

    private AtomicBoolean stopped = new AtomicBoolean(false);

    protected File tempAppDir = null;
    protected String step;
    protected Job job;
    protected Software software;
    protected ExecutionSystem executionSystem;
    protected JobManager jobManager = new JobManager();
    protected RemoteDataClient remoteExecutionDataClient;
    protected URLCopy urlCopy;
    protected TransferTask transferTask;

    protected AbstractJobLauncher() {
    }

    /**
     * Default constructor for a {@link JobLauncher} assigning the app and execution system directly
     * to leverage their lookups already performed in the factory initializing the concrete classes.
     * @param job the job to launch
     * @param software the software corresponding to the {@link Job#getSoftwareName()}
     * @param executionSystem the system corresponding to the {@link Job#getSystem()}
     */
    public AbstractJobLauncher(Job job, Software software, ExecutionSystem executionSystem) {
        this.setJob(job);
        this.setSoftware(software);//SoftwareDao.getSoftwareByUniqueName(job.getSoftwareName()));
        this.setExecutionSystem(executionSystem);//((ExecutionSystem) new SystemDao().findBySystemId(job.getSystem()));
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#isStopped()
     */
    @Override
    public synchronized boolean isStopped() {
        return stopped.get();
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#setStopped(boolean)
     */
    @Override
    public synchronized void setStopped(boolean stopped) {
        this.stopped.set(stopped);

        if (getUrlCopy() != null) {
            getUrlCopy().setKilled(true);
        }
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#getUrlCopy()
     */
    @Override
    public synchronized URLCopy getUrlCopy() {
        return urlCopy;
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#setUrlCopy(org.iplantc.service.transfer.URLCopy)
     */
    @Override
    public synchronized void setUrlCopy(URLCopy urlCopy) {
        this.urlCopy = urlCopy;
    }

    public JobManager getJobManager() {
        return jobManager;
    }

    public void setJobManager(JobManager jobManager) {
        this.jobManager = jobManager;
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#checkStopped()
     */
    @Override
    public void checkStopped() throws ClosedByInterruptException {
        if (isStopped()) {
            if (log.isDebugEnabled()) log.debug("Interrupt detected by checkStopped.");
            throw new ClosedByInterruptException();
        }
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#launch()
     */
    @Override
    public abstract void launch() throws IOException, JobException, SoftwareUnavailableException, SchedulerException, SystemUnknownException, SystemUnavailableException;

    /**
     * Calculates the job work path if not already set for the current job.
     * @return the remote job work directory
     * @see JobManager#calculateJobRemoteWorkPath(Job, ExecutionSystem)
     */
    protected String calculateRemoteJobPath() {
        String remoteJobPath = getJob().getWorkPath();
        if (StringUtils.isBlank(remoteJobPath)) {
            remoteJobPath = getJobManager().calculateJobRemoteWorkPath(getJob(), getExecutionSystem());
        }
        return remoteJobPath;
    }

    /**
     * Creates the remote job work directory if not already present
     * @param executionSystem the system on which the job will run
     * @param jobExecutionSystemRemoteDataClient the client used to create the directory
     * @param remoteJobDirectoryPath path to the job directory to create
     * @throws JobException when unable to create the directory or connect to the system
     */
    protected void createJobRemoteWorkPath(ExecutionSystem executionSystem, RemoteDataClient jobExecutionSystemRemoteDataClient, String remoteJobDirectoryPath) throws JobException {
        try {
            jobExecutionSystemRemoteDataClient.mkdirs(remoteJobDirectoryPath, getJob().getOwner());
        } catch (RemoteDataException e) {
            throw new JobException("Unable to create the remote job directory, " +
                    remoteJobDirectoryPath + ", on " + executionSystem.getSystemId() +
                    " for job " + getJob().getUuid() + ". Response from the server was: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new JobException("Failed to connect to " + executionSystem.getSystemId() +
                    " to create the remote job directory for job " + getJob().getUuid() +
                    ". Response from server was: " + e.getMessage(), e);
        }
    }

    /**
     * Users can include any of the {@link WrapperTemplateStatusVariableType#userAccessibleJobCallbackMacros()} in their
     * wrapper template. All other status callback values will cause issues in the proper execution and monitoring of
     * the job. This method removes all {@link WrapperTemplateStatusVariableType} that are not availble to the user
     * from the wrapper template.
     *
     * @param wrapperTemplate the wrapper template to filter
     * @return the wrapper with the reserved {@link WrapperTemplateStatusVariableType} macros removed
     */
    public String removeReservedJobStatusMacros(String wrapperTemplate) {
        WrapperTemplateMacroResolver resolver = new WrapperTemplateMacroResolver(getJob(), getExecutionSystem());
        return resolver.removeReservedJobStatusMacros(wrapperTemplate);
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#resolveRuntimeNotificationMacros(java.lang.String)
     */
    @Override
    public String resolveRuntimeNotificationMacros(String wrapperTemplate) {
        WrapperTemplateMacroResolver resolver = new WrapperTemplateMacroResolver(getJob(), getExecutionSystem());
        return resolver.resolveRuntimeNotificationMacros(wrapperTemplate);
    }

    /**
     * Filters out runtime internal status macros that are not allowed for the user leverage due to conflicts with the
     * job's lifecycle management.
     *
     * @param appTemplate the wrapper template to filter
     * @return the wrapper template resolved of all runtime status macros
     */
    protected String filterRuntimeStatusMacros(String appTemplate) {
        WrapperTemplateMacroResolver resolver = new WrapperTemplateMacroResolver(getJob(), getExecutionSystem());
        return resolver.removeReservedJobStatusMacros(appTemplate);
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#resolveMacros(java.lang.String)
     */
    @Override
    public String resolveMacros(String wrapperTemplate) throws JobMacroResolutionException {
        WrapperTemplateMacroResolver resolver = new WrapperTemplateMacroResolver(getJob(), getExecutionSystem());
        return resolver.resolve(wrapperTemplate);
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#resolveStartupScriptMacros(java.lang.String)
     */
    public String resolveStartupScriptMacros(String startupScript) throws JobMacroResolutionException {
        StartupScriptJobMacroResolver resolver = new StartupScriptJobMacroResolver(getJob(), getExecutionSystem());
        return resolver.resolve();
    }

    /**
     * Generates the command to source the {@link ExecutionSystem#getStartupScript()} and log the
     * response to the job's {@code .agave.log} file in the job work directory.
     *
     * @param absoluteRemoteWorkPath the absolute path to the job work directory on the remote system.
     * @return the properly escaped command to be run on the remote system.
     * @throws JobMacroResolutionException when the startup script cannot be resolved. This is usually due ot the system not being available
     */
    public String getStartupScriptCommand(String absoluteRemoteWorkPath) throws JobMacroResolutionException {
        String resolvedStartupScript = new StartupScriptJobMacroResolver(getJob(), getExecutionSystem()).resolve();
        if (resolvedStartupScript != null) {
            return String.format("printf \"[%%s] %%b\\n\" $(date '+%%Y-%%m-%%dT%%H:%%M:%%S%%z') \"$(source %s 2>&1)\" >> %s/.agave.log ",
                    resolvedStartupScript,
                    absoluteRemoteWorkPath);
        } else {
            return String.format("printf \"[%%s] %%b\\n\" $(date '+%%Y-%%m-%%dT%%H:%%M:%%S%%z') \"$(echo 'No startup script defined. Skipping...')\" >> %s/.agave.log ",
                    absoluteRemoteWorkPath);
        }
    }

    /**
     * Sets up the temp directory to stage application deployment assets, build and resolve wrapper templates, create
     * manifests, etc.
     *
     * @throws IOException when directory cannot be created
     */
    protected void createTempAppDir() throws IOException {
        step = "Creating local temp directory to process " + getJob().getSoftwareName() +
                " wrapper template for job " + getJob().getUuid();
        log.debug(step);

        File tempAppDir = Paths.get(Settings.TEMP_DIRECTORY).resolve(FilenameUtils.getName(getJob().getWorkPath())).toFile();

        // ensure directory exists
        if (tempAppDir.exists()) {
            log.debug("Local temp directory " + tempAppDir.getPath() + " for job " + getJob().getUuid() +
                    " already exists.");
        } else {
            if (!tempAppDir.mkdirs()) {
                throw new IOException("Unable to create temp directory at " + tempAppDir.getPath() + " to process " +
                        getJob().getSoftwareName() + " wrapper template for job " +
                        getJob().getUuid());
            } else {
                log.debug("Successfully created local temp directory " + tempAppDir.getPath() + " to process " +
                        getJob().getSoftwareName() + " wrapper template for job " +
                        getJob().getUuid());
            }
        }

        // update the tempAppDir with the calcualted directory
        setTempAppDir(tempAppDir);
    }

    /**
     * Copies the {@link Software#getDeploymentPath()} from the {@link Software#getStorageSystem()} for the current job
     * to the local job temp directory. If the software is public, then the deployment path represents a zipped archive
     * and will be unpacked before returning. The resulting files in either situation are checked for consistency in
     * checksums and {@link Software#getExecutablePath()} existence.
     * @throws JobException if unable to fetch the remote assets
     * @throws SoftwareUnavailableException when the software assets cannot be verified.
     * @throws SystemUnknownException if the {@link Software#getStorageSystem()} has been deleted.
     * @throws SystemUnavailableException when the {@link Software#getStorageSystem()} is unavailable to fetch the assets from
     */
    protected void copySoftwareToTempAppDir() throws JobException, SoftwareUnavailableException, SystemUnknownException, SystemUnavailableException {
        step = "Fetching app assets for job " + getJob().getUuid() + " from " +
                "agave://" + getSoftware().getStorageSystem().getSystemId() + "/" +
                getSoftware().getDeploymentPath() + " to temp application directory " +
                getTempAppDir().getPath();

        log.debug(step);

        try {

            // downloads the software deployment directory to the tempAppDir, optimizing data movement whenever possible
            fetchSoftwareDeploymentDirectory();

            checkStopped();

            // public apps have thier assets compressed in a zip archive. If the app is public, we need
            // to decompress the assets in the local tempAppDir before continuing.
            if (getSoftware().isPubliclyAvailable()) {
                unzipPublicSoftwareArchive();
            }
        } catch (ClosedByInterruptException e) {
            log.debug("Software asset copying for job " + getJob().getUuid() + " aborted due to interrupt by worker process.");
            // check is made upon return. Exception will be rethrown then.
        } catch (SystemUnknownException e) {
            throw new SystemUnknownException("No system found matching id of app deployment system, " +
                    getSoftware().getStorageSystem().getSystemId() + ".");
        } catch (SystemUnavailableException e) {
            throw new SystemUnavailableException("App deployment system " + getSoftware().getStorageSystem().getSystemId() +
                    " is currently unavailable.");
//        } catch(TransferException | RemoteDataException | RemoteCredentialException e) {
//            throw new JobException("Unable to fetch deployment path for app " + getSoftware().getUniqueName() +
//                    " from " + getSoftware().getStorageSystem().getSystemId(), e);
        } catch(IOException e) {
            throw new JobException("Failed to process staged app deployment path.", e);
        } catch (SoftwareUnavailableException | JobException e) {
            throw e;
        } catch (Exception e) {
            String msg = "Remote data connection to " + getSoftware().getExecutionSystem().getSystemId() + " threw exception and stopped job execution";
            log.error(msg, e);
            throw new JobException(msg, e);
        }
    }

    /**
     * Unzips a public {@link Software} deployment archive and ensures the contents corresponding to the
     * {@link Software#getExecutablePath()} are rooted at the tempAppDir.
     *
     * @throws SoftwareUnavailableException if the archive cannot be unzipped
     */
    protected void unzipPublicSoftwareArchive() throws IOException, SoftwareUnavailableException {
        // validate the checksum to make sure the app itself hasn't  changed
        File zippedFile = new File(getTempAppDir(), FilenameUtils.getName(getSoftware().getDeploymentPath()));
        File[] unzippedFiles = null;
        String checksum = MD5Checksum.getMD5Checksum(zippedFile);
        if (StringUtils.isBlank(getSoftware().getChecksum()) ||
                StringUtils.equals(checksum, getSoftware().getChecksum())) {
            try {
                ZipUtil.unzip(zippedFile, getTempAppDir());
                unzippedFiles = getTempAppDir().listFiles();
                if (unzippedFiles != null && unzippedFiles.length > 1) {
                    // remove zip archive after unpacking
                    FileUtils.deleteQuietly(zippedFile);
                } else {
                    throw new SoftwareUnavailableException("Failed to unpack the application bundle.");
                }
            } catch (IOException e) {
                throw new SoftwareUnavailableException("Failed to unzip the application bundle.", e);
            }
        } else {
            notifyOfSoftwareChecksumChangeEvent(getSoftware(), checksum);
            throw new SoftwareUnavailableException("Public app bundle for " + getSoftware().getUniqueName() +
                    " has changed. Please verify this app and try again.");
        }

        // if the archive was created by zipping the deployment folder rather than it's contents, then
        // the executable path will be relative to the enclosing directory into which the deployment assets
        // were unpacked. Here we check for the expected path and, if not found, iterate over the first-level
        // directories (should only be one) to find the executable. If not found at that point, we
        // cannot invoke the app as we have no wrapper template to launch.
        boolean foundDeploymentPath = false;
        if (!Files.exists(getTempAppDir().toPath().resolve(getSoftware().getExecutablePath()))) {
            for (File unzippedFileItem : unzippedFiles) {
                if (unzippedFileItem.isDirectory()) {
                    // if the executable path exists within the dir, then make that dir the tempAppDir
                    // by
                    if (Files.exists(unzippedFileItem.toPath().resolve(getSoftware().getExecutablePath()))) {
                        File renamedUnzippedFileItem = new File(getTempAppDir().getPath() + ".copy");
                        FileUtils.moveDirectory(unzippedFileItem, renamedUnzippedFileItem);
                        FileUtils.deleteDirectory(getTempAppDir());
                        renamedUnzippedFileItem.renameTo(getTempAppDir());
                        foundDeploymentPath = true;
                        break;
                    }
                }
            }

            if (!foundDeploymentPath) {
                log.error("Unable to find app path for public app " + getSoftware().getUniqueName());
                throw new SoftwareUnavailableException("Unable to find the wrapper template for " +
                        getSoftware().getUniqueName() + ". The executablePath in the app definition does not exist.");
            }
        }
    }

    /**
     * Sends notifications for {@link SoftwareEventType#UPDATED} and {@link FileEventType#CONTENT_CHANGE} in response to
     * detecting a change in the {@link Software#getChecksum()} of a public app.
     */
    protected void notifyOfSoftwareChecksumChangeEvent(Software software, String currentChecksum) {
        String message = "While processing a job request, it was observed that the checksum " +
                "of the public app " + software.getUniqueName() + " had changed from " +
                software.getChecksum() + " to " + currentChecksum + ". This " +
                "will impact provenance and could impact experiment reproducibility. " +
                "Please restore the app zip archive at " + software.getDeploymentPath() +
                " from the original deployment directory, or disable the app. All jobs submitted using " +
                "this app will fail until the zip archive is restored.";

        // Create software event indicating that the app is updated because of the changed zip archive checksum.
        new SoftwareEventProcessor().processSoftwareContentEvent(
                software, SoftwareEventType.UPDATED, message, software.getStorageSystem().getOwner());

        String deploymentPathUrl = String.format("agave://%s/%s",
                software.getStorageSystem().getSystemId(), software.getDeploymentPath());

        // Create logical file event indicating that the app zip archive is updated because of the changed checksum
        try {
            // lookup the logical file
            LogicalFile logicalFile = LogicalFileDao.findBySourceUrl(deploymentPathUrl);
            // create event for it.
            logicalFile.addContentEvent(new FileEvent(FileEventType.CONTENT_CHANGE, message, software.getStorageSystem().getOwner()));
            // save the updated logical file to perist the event
            LogicalFileDao.persist(logicalFile);
        } catch (Throwable e) {
            log.error("Failed to send notification of a checksum change in the public application " +
                    "bundle of " + software.getUniqueName() + " at " + deploymentPathUrl, e);
        }
    }

    /**
     * Creates a {@link TransferTask} and associates it with a {@link JobStatusType#STAGING_JOB} event, then saves the
     * {@link Job} history. Separated here for easier testing and to avoid mocking static DAO methods.
     * @return transfer task for the overall {@link Software#getDeploymentPath()} download
     * @throws JobException if unable to persist the transfer task or job event.
     */
    protected TransferTaskImpl createJobSoftwareDeploymentDirectoryTransferTask() throws JobException{
        TransferTaskImpl transferTask;
        try {
            // create the parent transfer task to copy the deployment directory on the software storage system.
            transferTask = new TransferTaskImpl(
                    "agave://" + getSoftware().getStorageSystem().getSystemId() + "/" + getSoftware().getDeploymentPath(),
                    "https://workers.prod.agaveplatform.org/" + getTempAppDir().getPath(),
                    getJob().getOwner(), null, null);

            TransferTaskDao.persist(transferTask);

            JobDao.refresh(getJob());

            getJob().addEvent(new JobEvent(JobStatusType.STAGING_JOB,
                    "Fetching app assets from " + transferTask.getSource(),
                    null,
                    getJob().getOwner()));

            JobDao.persist(getJob());

            return transferTask;
        } catch (TransferException e) {
            throw new JobException("Unable to save transfer task to track staging of job app assets from software deployment system.", e);
        } catch (JobException e) {
            throw new JobException("Unable to add STAGING_JOB event to job history.", e);
        }
    }

    /**
     * Downloads the {@link Software#getDeploymentPath()} from the {@link Software#getStorageSystem()} for the current
     * {@link Job}. Proper {@link JobEvent} and {@link TransferTask} are created and saved with the job history.
     *
     * @return path to the downloaded software deployment directory
     * @throws ClosedByInterruptException
     * @throws JobException
     */
    protected Path fetchSoftwareDeploymentDirectory() throws ClosedByInterruptException, JobException, SystemUnknownException, SystemUnavailableException, SoftwareUnavailableException {
        Path result;
        TransferTaskImpl transferTask;
        RemoteDataClient remoteSoftwareDataClient = null;
        StorageSystem storageSystem = null;
        try {
            storageSystem = (StorageSystem) getJobManager().getAvailableSystem(getSoftware().getStorageSystem().getSystemId());
            remoteSoftwareDataClient = storageSystem.getRemoteDataClient();
            remoteSoftwareDataClient.authenticate();

            checkStopped();

            transferTask = createJobSoftwareDeploymentDirectoryTransferTask();

            try {
                // stat the remote deployment directory. This gives us file info an an existence check.
                RemoteFileInfo deploymentPathFileInfo = remoteSoftwareDataClient.getFileInfo(getSoftware().getDeploymentPath());

                checkStopped();

                // if the remote is a directory, we copy its contents to this host. Because the RDC classes always
                // include the parent directory in a get/put operation, we iterate through the remote directory and
                // copy each value to the tempAppDirectory so we can skip existing file items on retries.
                if (deploymentPathFileInfo.isDirectory()) {
                    // update the parent task to show progress
                    transferTask.setStatus(TransferStatusType.TRANSFERRING);
                    updateTransferTask(transferTask);

                    // iterate over remote directory contents, copying each child in turn
                    for (RemoteFileInfo child : remoteSoftwareDataClient.ls(getSoftware().getDeploymentPath())) {
                        // break if interrupt received
                        checkStopped();

                        // create a transfer task to track the child
                        TransferTaskImpl childTransferTask = new TransferTaskImpl(
                                transferTask.getSource() + "/" + child.getName(),
                                transferTask.getDest() + "/" + child.getName(),
                                getJob().getOwner(), transferTask, transferTask);
                        RemoteTransferListenerImpl remoteListener = new RemoteTransferListenerImpl(childTransferTask);
                        Path childPath = Paths.get(getSoftware().getDeploymentPath()).resolve(child.getName());

                        // skip existing files already present in the local tempAppDir
                        if (child.isFile()) {
                            childTransferTask.setTotalSize(child.getSize());
                            childTransferTask.setTotalFiles(1);

                            File localChild = new File(getTempAppDir(), child.getName());
                            // only copy if the file is missing and size differs duplicate copies
                            if (localChild.exists() && localChild.length() != child.getSize()) {
                                // skip existing, unchanged file
                                childTransferTask.setTotalSkippedFiles(1);
                            }
                        }

                        // save the transfer task regardless
                        try {
                            updateTransferTask(childTransferTask);
                        } catch (TransferException e) {
                            // have to fail the entire directory copy if this fails since we can't record progress
                            childTransferTask.setStatusString(TransferStatusType.CANCELLED.name());
                            try {
                                transferTask.updateSummaryStats(childTransferTask);
                            } catch (Throwable ignored) {
                            }
                            throw new JobException("Unable to save child transfer task during staging of software deployment directory", e);
                        }

                        // fetch the data
                        try {
                            // we have a valid task, make the copy unless we're skipping due to prior success
                            if (childTransferTask.getTotalSkippedFiles() == 0) {
                                remoteSoftwareDataClient.get(childPath.toString(), getTempAppDir().getPath(), remoteListener);
                            } else {
                                remoteListener.skipped(childTransferTask.getTotalSize(), childPath.toString());
                            }
                        } catch (IOException | RemoteDataException e) {
                            try {
                                // ensure the child task has been properly terminated and recorded, then tidy up the
                                // parent transfertask and exit
                                if (((TransferTaskImpl)remoteListener.getTransferTask()).getStatus() != TransferStatusType.FAILED) {
                                    remoteListener.failed();
                                }

                                transferTask.updateSummaryStats(((TransferTaskImpl)remoteListener.getTransferTask()));
                                updateTransferTask(transferTask);
                            } catch (Throwable ignored) {
                                log.error("Failed to update parent transfer task " + transferTask.getUuid() +
                                        " after failing to fetch " + childTransferTask.getSource() +
                                        " while staging deployment directory of " + getSoftware().getUniqueName() +
                                        " for job " + getJob().getUuid());
                            }

                            throw new RemoteDataException("Failed to fetch " + childTransferTask.getSource() +
                                    " while staging deployment directory of " + getSoftware().getUniqueName(), e);
                        }

                        // update parent task with results of child transfer task
                        transferTask.updateSummaryStats(((TransferTaskImpl)remoteListener.getTransferTask()));
                        updateTransferTask(transferTask);
                    }

                    // once everything completes, complete the parent task
                    transferTask.setStatus(TransferStatusType.COMPLETED);
                    updateTransferTask(transferTask);

                } else {
                    // otherwise it's a file and we can can just copy the file. This is usually because the software is
                    // public.
                    remoteSoftwareDataClient.get(getSoftware().getDeploymentPath(), getTempAppDir().getPath(), new RemoteTransferListenerImpl(transferTask));
                }
            } catch (FileNotFoundException e) {
                // if the deployment path does not exist, we can't run the software, so we report it as unavailble
                // to the calling method.
                throw new SoftwareUnavailableException("Deployment path " + getSoftware().getDeploymentPath() +
                        " does not exist on the deployment system, " + storageSystem.getSystemId());
            }

            result = getTempAppDir().toPath();

        } catch (ClosedByInterruptException e) {
            throw e;
        } catch (SystemUnknownException e) {
            throw new SystemUnknownException("No system found matching id of app deployment system, " +
                    getSoftware().getStorageSystem().getSystemId() + ".");
        } catch (SystemUnavailableException e) {
            throw new SystemUnavailableException("App deployment system " + getSoftware().getStorageSystem().getSystemId() +
                    " is currently unavailable.");
        } catch (IOException | TransferException | RemoteDataException | RemoteCredentialException e) {
            throw new JobException("Unable to fetch deployment path for app " + getSoftware().getUniqueName() +
                    " from " + getSoftware().getStorageSystem().getSystemId(), e);
        } finally {
            if (remoteSoftwareDataClient != null) {
                remoteSoftwareDataClient.disconnect();
            }
        }

        return result;
    }

    /**
     * Saves or updates a {@link TransferTask} object, updating timestamps based on status.
     *
     * @param transferTask the object to save
     * @erturns the updated {@link TransferTask}
     * @throws TransferException if the sql operation failed.
     */
    protected TransferTask updateTransferTask(TransferTaskImpl transferTask) throws TransferException {
        Instant currentTime = Instant.now();

        transferTask.setLastUpdated(currentTime);
        if (!TransferStatusType.getActiveStatusValues().contains(transferTask.getStatus())) {
            if (transferTask.getEndTime() == null) {
                transferTask.setEndTime(currentTime);
            }
            transferTask.updateTransferRate();
        } else if (transferTask.getStatus() == TransferStatusType.TRANSFERRING) {
            if (transferTask.getStartTime() == null) {
                transferTask.setStartTime(currentTime);
            }
        }

        TransferTaskDao.persist(transferTask);

        return transferTask;
    }

//    public abstract String processApplicationWrapperTemplate() throws JobException;

    @Override
    public void writeToRemoteJobDir(String filePathRelativeToRemoteJobDir, String content) throws JobException {
        // write the file at a path relative to the remote job work directory
        Path remoteJobWorkPath = Paths.get(getJob().getWorkPath());
        Path remoteTargetPath = remoteJobWorkPath.resolve(filePathRelativeToRemoteJobDir);

        // if the target is not a file, ensure the subdirectory is present as this method can be called
        // before the software assets are staged out and the directory structure is created on the
        // execution system.
        if (!remoteTargetPath.getParent().equals(remoteJobWorkPath)) {
            String remoteParentDirectory = remoteTargetPath.getParent().toString();
            log.debug("Creating remote directory " + remoteParentDirectory + " prior to staging file " +
                    remoteTargetPath.toString() + " for job " + getJob().getUuid());

            try {
                if (!getRemoteExecutionDataClient().doesExist(remoteParentDirectory)) {
                    getRemoteExecutionDataClient().mkdirs(remoteParentDirectory);
                }
            } catch (IOException|RemoteDataException e) {
                throw new JobException("Failed to create parent directories prior to staging file " +
                        remoteTargetPath.toString() + " to remote job work directory.", e);
            }
        }
        log.debug("Staging file " + filePathRelativeToRemoteJobDir + " for job " + getJob().getUuid());

        try (RemoteOutputStream<?> remoteOutputStream =
                     getRemoteExecutionDataClient().getOutputStream(remoteTargetPath.toString(), false, false);
             OutputStreamWriter batchWriter = new OutputStreamWriter(remoteOutputStream);) {

            batchWriter.write(content);

            batchWriter.flush();
        }
        catch (IOException | RemoteDataException e) {
            throw new JobException("Failed to stage file " + filePathRelativeToRemoteJobDir +
                    " to remote job work directory ", e);
        }
    }

    /**
     * Creates a {@link TransferTask} to copy the tempAppDir contents to the job work directory and associates
     * it with a {@link JobStatusType#STAGING_JOB} event, then saves the
     * {@link Job} history. Separated here for easier testing and to avoid mocking static DAO methods.
     *
     * @return transfer task for the overall {@link #getTempAppDir()} upload
     * @throws JobException if unable to persist the transfer task or job event.
     */
    protected TransferTaskImpl createJobWorkDirectoryTransferTask() throws JobException {
        TransferTaskImpl transferTask;
        try {
            transferTask = new TransferTaskImpl(
                    "https://workers.prod.agaveplatform.org/" + getTempAppDir().getPath(),
                    "agave://" + getJob().getSystem() + "/" + getJob().getWorkPath(),
                    getJob().getOwner(), null, null);

            updateTransferTask(transferTask);

            JobDao.refresh(getJob());

            getJob().addEvent(new JobEvent(JobStatusType.STAGING_JOB,
                    "Staging runtime assets to " + transferTask.getDest(),
                    transferTask,
                    getJob().getOwner()));

            JobDao.persist(getJob());

            return transferTask;
        } catch (TransferException e) {
            throw new JobException("Unable to save transfer task to track deploying of job app assets to execution system.", e);
        } catch (JobException e) {
            throw new JobException("Unable to add STAGING_JOB event to job history.", e);
        }
    }

    /**
     * Pushes the app assets which are currently on the staging server to the job
     * work directory on the execution system.
     *
     * @throws JobException when unable to complete the transfer
     */
    protected void stageSofwareApplication() throws ClosedByInterruptException, JobException {
        TransferTaskImpl transferTask;

        try {
            if (getRemoteExecutionDataClient() == null) {
                setRemoteExecutionDataClient(getExecutionSystem().getRemoteDataClient(getJob().getInternalUsername()));
                getRemoteExecutionDataClient().authenticate();
            }

            log.debug("Staging " + getJob().getSoftwareName() + " app dependencies for job " + getJob().getUuid() +
                    " to agave://" + getJob().getSystem() + "/" + getJob().getWorkPath());
            getRemoteExecutionDataClient().mkdirs(getJob().getWorkPath(), getJob().getOwner());

            checkStopped();

            transferTask = createJobWorkDirectoryTransferTask();

            // first time around we copy everything
            if (getJob().getRetries() <= 0) {
                getRemoteExecutionDataClient().put(getTempAppDir().getPath(),
                        Paths.get(getJob().getWorkPath()).getParent().toString(),
                        new RemoteTransferListenerImpl(transferTask));
            }
            // after that we try to save some time on retries by only copying the assets
            // that are missing or changed since the last attempt.
            else {
                getRemoteExecutionDataClient().syncToRemote(getTempAppDir().getPath(),
                        new File(getJob().getWorkPath()).getParent(),
                        new RemoteTransferListenerImpl(transferTask));
            }
        } catch (ClosedByInterruptException e) {
            throw e;
        } catch (IOException | RemoteDataException | RemoteCredentialException e) {
            throw new JobException("Unable to stage application dependencies to execution system " + getJob().getSystem(), e);
        } finally {
            try {
                if (getRemoteExecutionDataClient() != null) getRemoteExecutionDataClient().disconnect();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * This method creates a manifest file for the job work directory that contains all the file items that will be
     * present when the job starts. These file items will be ignored during archiving.
     *
     * @return the newline separated manifest of existing files and folders
     * @throws JobException if the file cannot be created and opened
     */
    protected String processJobArchiveManifest() throws JobException {
        step = "Creating an archive manifest file for job " + getJob().getUuid();
        log.debug(step);

        StringWriter archiveLogWriter = new StringWriter();
        try {
//            File archiveLog = new File(getTempAppDir(), logFileName);
//            if (archiveLog.createNewFile()) {
                printListing(getTempAppDir(), getTempAppDir(), archiveLogWriter);

                return archiveLogWriter.toString();
//            } else {
//                throw new JobException("Failed to create manifest file for job " + getJob().getUuid());
//            }
        } catch (IOException e) {
            String msg = "Failed to create manifest file for job " + getJob().getUuid();
            log.error(msg, e);
            throw new JobException(msg, e);
        }
    }

    /**
     * Generates recursive listing of file items relative to the given baseFolder.
     * @param file the file to explore
     * @param baseFolder the root of the original listing
     * @param writer the {@link Writer} to where the file item should be written
     * @throws IOException if unable to write to the given writer
     */
    protected void printListing(File file, File baseFolder, Writer writer) throws IOException {
        if (file.isFile()) {
            String relativeFilePath = StringUtils.removeStart(file.getPath(), baseFolder.getPath());
            if (relativeFilePath.startsWith("/"))
                relativeFilePath = relativeFilePath.substring(1);
            writer.append(relativeFilePath).append("\n");
        } else {
            File[] fileListing = file.listFiles();
            if (fileListing != null) {
                for (File child : fileListing) {
                    String relativeChildPath = StringUtils.removeStart(child.getPath(), baseFolder.getPath());
                    if (relativeChildPath.startsWith("/"))
                        relativeChildPath = relativeChildPath.substring(1);
                    writer.append(relativeChildPath).append("\n");

                    if (child.isDirectory()) {
                        printListing(child, baseFolder, writer);
                    }
                }
            }
        }
    }

    /**
     * Make the remote call to start the job on the remote system. This will need to
     * handle the invocation command, remote connection, parsing of the response to
     * get the job id, and updating of the job status on success or failure.
     *
     * @return the response from the remote scheduler
     * @throws JobException       when an error occurs trying to submit the job
     * @throws SchedulerException when the remote scheduler response cannot be parsed
     */
    protected abstract String submitJobToQueue() throws JobException, SchedulerException;

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#parseSoftwareParameterValueIntoTemplateVariableValue(org.iplantc.service.apps.model.SoftwareParameter, com.fasterxml.jackson.databind.JsonNode)
     */
    @Override
    public String parseSoftwareParameterValueIntoTemplateVariableValue(SoftwareParameter softwareParameter, JsonNode jsonJobParamValue) {
        // check for arrays of values. enquote if needed, then join as space-delimited values
        String[] paramValues = null;
        try {
            if (jsonJobParamValue == null || jsonJobParamValue.isNull() || (jsonJobParamValue.isArray() && jsonJobParamValue.size() == 0)) {
                // null value for bool parameter is interpreted as false. It should not happen here, though,
                // as a null bool value passed into the job should go false
                if (softwareParameter.getType().equals(SoftwareParameterType.bool)) {
                    // filter the param value to a zero or 1
                    String singleParamValue = "0";

                    if (softwareParameter.isEnquote()) {
                        singleParamValue = ServiceUtils.enquote(singleParamValue);
                    }
                    if (softwareParameter.isShowArgument()) {
                        paramValues = new String[]{softwareParameter.getArgument() + singleParamValue};
                    } else {
                        paramValues = new String[]{singleParamValue};
                    }
                } else {
                    paramValues = new String[]{};
                }
            } else if (jsonJobParamValue.isArray()) {
                paramValues = new String[jsonJobParamValue.size()];

                if (softwareParameter.getType().equals(SoftwareParameterType.flag)) {
                    // show the flag only if present and value is true
                    if (StringUtils.equals(jsonJobParamValue.iterator().next().asText(), "1") ||
                            jsonJobParamValue.iterator().next().asBoolean(false)) {
                        paramValues[0] = softwareParameter.getArgument();
                    }
                } else if (softwareParameter.getType().equals(SoftwareParameterType.bool)) {
                    // filter the param value to a zero or 1
                    String singleParamValue = null;
                    if (StringUtils.equals(jsonJobParamValue.iterator().next().asText(), "1") ||
                            jsonJobParamValue.iterator().next().asBoolean(false)) {
                        singleParamValue = "1";
                    } else {
                        singleParamValue = "0";
                    }

                    if (softwareParameter.isEnquote()) {
                        singleParamValue = ServiceUtils.enquote(singleParamValue);
                    }
                    if (softwareParameter.isShowArgument()) {
                        paramValues[0] = softwareParameter.getArgument() + singleParamValue;
                    } else {
                        paramValues[0] = singleParamValue;
                    }
                } else {
                    int childIndex = 0;
                    for (JsonNode child : jsonJobParamValue) {
                        String singleParamValue = child.asText();
                        if (softwareParameter.isEnquote()) {
                            singleParamValue = ServiceUtils.enquote(singleParamValue);
                        }

                        if (softwareParameter.isShowArgument() && (softwareParameter.isRepeatArgument() || childIndex == 0)) {
                            paramValues[childIndex] = softwareParameter.getArgument() + singleParamValue;
                        } else {
                            paramValues[childIndex] = singleParamValue;
                        }
                        childIndex++;
                    }
                }
            } else // textual node
            {
                paramValues = new String[1];

                if (softwareParameter.getType().equals(SoftwareParameterType.flag)) {
                    // show the flag only if present and value is true
                    if (StringUtils.equals(jsonJobParamValue.asText(), "1") ||
                            jsonJobParamValue.asBoolean(false)) {
                        paramValues[0] = softwareParameter.getArgument();
                    }
                } else if (softwareParameter.getType().equals(SoftwareParameterType.bool)) {
                    // filter the param value to a zero or 1
                    String singleParamValue = null;
                    if (StringUtils.equals(jsonJobParamValue.asText(), "1") ||
                            jsonJobParamValue.asBoolean(false)) {
                        singleParamValue = "1";
                    } else {
                        singleParamValue = "0";
                    }

                    if (softwareParameter.isEnquote()) {
                        singleParamValue = ServiceUtils.enquote(singleParamValue);
                    }
                    if (softwareParameter.isShowArgument()) {
                        paramValues[0] = softwareParameter.getArgument() + singleParamValue;
                    } else {
                        paramValues[0] = singleParamValue;
                    }
                } else {
                    String singleParamValue = jsonJobParamValue.asText();

                    if (softwareParameter.isEnquote()) {
                        singleParamValue = ServiceUtils.enquote(singleParamValue);
                    }
                    if (softwareParameter.isShowArgument()) {
                        paramValues[0] = softwareParameter.getArgument() + singleParamValue;
                    } else {
                        paramValues[0] = singleParamValue;
                    }
                }
            }
        } catch (Exception e) {
            if (jsonJobParamValue != null) {
                String singleParamValue = jsonJobParamValue.textValue();

                if (softwareParameter.isEnquote()) {
                    singleParamValue = ServiceUtils.enquote(singleParamValue);
                }

                if (paramValues == null) {
                    paramValues = new String[1];
                }

                if (softwareParameter.isShowArgument()) {
                    paramValues[0] = softwareParameter.getArgument() + singleParamValue;
                } else {
                    paramValues[0] = singleParamValue;
                }
            }
        }

        return StringUtils.join(paramValues, " ");
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#parseSoftwareInputValueIntoTemplateVariableValue(org.iplantc.service.apps.model.SoftwareInput, com.fasterxml.jackson.databind.JsonNode)
     */
    @Override
    public String parseSoftwareInputValueIntoTemplateVariableValue(SoftwareInput softwareInput, JsonNode jsonJobInputValue)
            throws URISyntaxException {
        // check for arrays of values. enquote if needed, then join as space-delimited values
        String[] paramValues = null;
        try {
            if (jsonJobInputValue == null || jsonJobInputValue.isNull() || (jsonJobInputValue.isArray() && jsonJobInputValue.size() == 0)) {
                paramValues = new String[]{};
            } else {
                if (jsonJobInputValue.isArray()) {
                    paramValues = new String[jsonJobInputValue.size()];

                    int childIndex = 0;
                    for (JsonNode child : jsonJobInputValue) {
                        String singleInputRawValue = child.asText();
                        URI uri = new URI(singleInputRawValue);

                        // TODO: we should handle url decoding/escaping here so it is possible to use enquote without fear of having to do shellfoo magic to make a script bulletproof
                        String singleInputValue = FilenameUtils.getName(uri.getPath());

                        if (softwareInput.isEnquote()) {
                            singleInputValue = ServiceUtils.enquote(singleInputValue);
                        }

                        if (softwareInput.isShowArgument() && (softwareInput.isRepeatArgument() || childIndex == 0)) {
                            paramValues[childIndex] = softwareInput.getArgument() + singleInputValue;
                        } else {
                            paramValues[childIndex] = singleInputValue;
                        }
                        childIndex++;
                    }
                } else // textual node
                {
                    paramValues = new String[1];

                    String singleInputRawValue = jsonJobInputValue.asText();
                    URI uri = new URI(singleInputRawValue);
                    String singleInputValue = FilenameUtils.getName(uri.getPath());

                    if (softwareInput.isEnquote()) {
                        singleInputValue = ServiceUtils.enquote(singleInputValue);
                    }
                    if (softwareInput.isShowArgument()) {
                        paramValues[0] = softwareInput.getArgument() + singleInputValue;
                    } else {
                        paramValues[0] = singleInputValue;
                    }
                }
            }
        } catch (Exception e) {
            String singleInputRawValue = jsonJobInputValue.asText();
            URI uri = new URI(singleInputRawValue);
            String singleInputValue = FilenameUtils.getName(uri.getPath());

            if (softwareInput.isEnquote()) {
                singleInputValue = ServiceUtils.enquote(singleInputValue);
            }

            if (paramValues == null) {
                paramValues = new String[1];
            }

            if (softwareInput.isShowArgument()) {
                paramValues[0] = softwareInput.getArgument() + singleInputValue;
            } else {
                paramValues[0] = singleInputValue;
            }
        }

        return StringUtils.join(paramValues, " ");
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#getTempAppDir()
     */
    @Override
    public File getTempAppDir() {
        return tempAppDir;
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#setTempAppDir(java.io.File)
     */
    @Override
    public void setTempAppDir(File tempAppDir) {
        this.tempAppDir = tempAppDir;
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.JobLauncher#getJob()
     */
    @Override
    public synchronized Job getJob() {
        return this.job;
    }

    /**
     * @return the executionSystem
     */
    public ExecutionSystem getExecutionSystem() {
        return executionSystem;
    }

    /**
     * @param executionSystem the executionSystem to set
     */
    public void setExecutionSystem(ExecutionSystem executionSystem) {
        this.executionSystem = executionSystem;
    }

    /**
     * @return the software
     */
    public Software getSoftware() {
        return software;
    }

    /**
     * @param software the software to set
     */
    public void setSoftware(Software software) {
        this.software = software;
    }

    /**
     * @param job the job to set
     */
    public void setJob(Job job) {
        this.job = job;
    }

    /**
     * Gets the {@link RemoteDataClient} for the job {@link ExecutionSystem}.
     * @return the client for the remote execution system
     */
    public RemoteDataClient getRemoteExecutionDataClient() {
        return this.remoteExecutionDataClient;
    }

    /**
     * Sets the {@link RemoteDataClient} for the job {@link ExecutionSystem}
     *
     * @param remoteExecutionDataClient the {@link RemoteDataClient} to set
     */
    public void setRemoteExecutionDataClient(RemoteDataClient remoteExecutionDataClient){
        this.remoteExecutionDataClient = remoteExecutionDataClient;
    }
}
