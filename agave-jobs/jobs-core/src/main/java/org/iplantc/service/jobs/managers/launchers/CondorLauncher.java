package org.iplantc.service.jobs.managers.launchers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareInput;
import org.iplantc.service.apps.model.SoftwareParameter;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.exceptions.*;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.managers.launchers.parsers.CondorJobIdParser;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.model.scripts.CommandStripper;
import org.iplantc.service.jobs.model.scripts.SubmitScript;
import org.iplantc.service.jobs.model.scripts.SubmitScriptFactory;
import org.iplantc.service.remote.RemoteSubmissionClient;
import org.iplantc.service.remote.exceptions.RemoteExecutionException;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.transfer.exceptions.AuthenticationException;
import org.iplantc.service.transfer.exceptions.RemoteConnectionException;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;

/**
 * Class to fork a background task on a HTCondor system. The condor job id will be stored as {@link Job#getLocalJobId()}
 * for querying by the monitoring queue.
 *
 * @author dooley
 *
 */
@SuppressWarnings("unused")
public class CondorLauncher  extends AbstractJobLauncher {
    
	private static final Logger log = Logger.getLogger(CondorLauncher.class);
    
    private SubmitScript submitFileObject;
//    private File condorSubmitFile;
    private String timeMark;
    private String tag;
    
	private boolean jobFailed = false;

	/**
	 * Default no-args constructor for mock testing
	 */
	protected CondorLauncher() {
		super();
	}

    /**
     * Creates an instance of a JobLauncher capable of submitting jobs to batch
     * queuing systems on HTCondor resources.
     * @param job the job to launch
     * @param software the software corresponding to the {@link Job#getSoftwareName()}
     * @param executionSystem the system corresponding to the {@link Job#getSystem()}
     */
    public CondorLauncher(Job job, Software software, ExecutionSystem executionSystem) {
        super(job, software, executionSystem);

        this.submitFileObject = SubmitScriptFactory.getInstance(getJob(), getSoftware(), getExecutionSystem());
    }

//    /**
//     * This method gets the current working directory
//     *
//     * @return String of directory path
//     */
//    private String getWorkDirectory() {
//        String wd = "";
//        try {
//            wd = new File("test.txt").getCanonicalPath().replace("/test.txt", "");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return wd;
//    }

    /**
     * makes and sets new time marker for creation of job run directories
     */
    private void setTimeMarker() {
    	DateTime date = new DateTime(getJob().getCreated());
        Long time = date.getMillis();
        timeMark = time.toString();
        tag = getJob().getName() + "-" + timeMark;
    }

    /**
     * generates a condor submit file for execution
     *
     * @param time string
     * @return a string representing the condor_submit file name
     */
    private String createSubmitFileName(String time) {
        return "condorSubmit-" + getJob().getName() + "-" + time;
    }

    /**
     * Creates the condor submit file to be handed to condor_submit
     *
     * @param timeMark current time
     */
    private String createCondorSubmitFile(String timeMark) throws JobException
    {
        step = "Creating the " + getJob().getSoftwareName() + " condor submit file for job " + getJob().getUuid();
        log.debug(step);

        // todo need to add classAd info to submit file
        submitFileObject = SubmitScriptFactory.getInstance(getJob(), getSoftware(), getExecutionSystem());
        
        try 
        {
	        // generate a condor submit script. the user may have overridden certain 
        	// directives or the system owner may have templatized their custom directives,
        	// so we need to resolve the macros here.
	        return resolveTemplateMacros(submitFileObject.getScriptText());
	        
//	        String submitFileName = "condorSubmit";   //createSubmitFileName(timeMark);
//	        condorSubmitFile = new File(getTempAppDir(), submitFileName);
//          Files.write(condorSubmitFile.toPath(), condorSubmitFileContents.getBytes());

//            String remoteApplicationWrapperPath = getJob().getWorkPath() + File.separator + submitFileName;
//            writeWrapperTemplateToRemoteJobDir(remoteApplicationWrapperPath, condorSubmitFileContents);
        }
        catch (JobMacroResolutionException e) {
            String msg = "Unable to resolve macros in the submit script: " + e.getMessage();
//            log.error(msg);
            throw new JobException(msg, e);
        }
        catch (JobException e) {
        	String msg = "Unable to create condor submit file.";
//        	log.error(msg, e);
        	throw e;
        } 
        catch (URISyntaxException e) {
        	String msg = "Failed to resolve condor submit file macros. " + e.getMessage();
//        	log.error(msg, e);
            throw new JobException(msg, e);
        }
    }

//    /**
//     * Method to help with multiple calls to outside processes for a variety of tasks
//     *
//     * @param command String of command line parameters
//     * @param cmdName Name of the command being executed
//     * @return an Object that includes int exit code, String out and String err from the execution
//     * @throws JobException if the exitCode is not equal to 0
//     */
//    private CmdLineProcessOutput executeLocalCommand(String command, String cmdName) throws JobException {
//        CmdLineProcessHandler cmdLineProcessHandler = new CmdLineProcessHandler();
//        int exitCode = cmdLineProcessHandler.executeCommand(command);
//
//        if (exitCode != 0) {
//        	String msg = "Job exited with error code " + exitCode + " please check your arguments and try again.";
//        	log.error(msg);
//            throw new JobException(msg);
//        }
//        return cmdLineProcessHandler.getProcessOutput();
//    }
//
//    /**
//     * Takes a String representing the directory path and extracts the last directory name
//     *
//     * @param path String directory path
//     * @return String of last directory name in path
//     * @throws Exception if path is null or empty
//     */
//    private String getDirectoryName(String path) throws Exception {
//        String name = null;
//
//        // is it null or empty throw Exception
//        if (path == null || path.isEmpty()) {
//            throw new Exception("path can't be null or empty");
//        }
//        path = path.trim();
//        String separator = File.separator;
//
//        StringTokenizer st = new StringTokenizer(path, separator);
//        while (st.hasMoreElements()) {
//            name = (String) st.nextElement();
//        }
//        return name;
//    }

    /**
	 * This method will write the generic transfer_wrapper.sh file that condor_submit
	 * uses as it's executable. The file wraps the defined executable for the job along
	 * with the data all tar'd and zipped to transfer to OSG
     *
     * @return the content of the condor transfer_wrapper.sh script
     * @throws JobException if unable to write or set permissions on template and executable files.
	 */
	private String createTransferWrapper() throws JobException {
	    step = "Creating Condor submission transfer wrapper to run " + 
	            getJob().getSoftwareName() + " for job " + getJob().getUuid();
	    log.debug(step);

        String executablePath = getExecutablePath();

//        try(FileWriter transferWriter = new FileWriter(getTempAppDir() + File.separator + "transfer_wrapper.sh");) {
//	    try {

        return "#!/bin/bash" +
                "\n\n" +
                "tar xzvf transfer.tar.gz" +
                "\n" +
                "# we supply the executable and path from the software definition" +
                "\n" +
                //transferScript.append("chmod u+x " + executablePath + " \n");
                String.format("./%s # this in turn wraps the final executable along with inputs, parameters and output.\n", executablePath);

            // write the transfer_wrapper.sh file
//	        transferWriter.write(transferScript.toString());
//	        transferWriter.flush();
//
//            // need to make sure that transfer_wrapper.sh is executable
//            Files.setPosixFilePermissions(getTempAppDir().toPath().resolve("transfer_wrapper.sh"),
//                    PosixFilePermissions.fromString("rwxr-x---"));
//
//            Files.setPosixFilePermissions(getTempAppDir().toPath().resolve(executablePath),
//                    PosixFilePermissions.fromString("rwxr-x---"));

//	    } catch (IOException e) {
//	        String msg = "Failure to write transfer script for Condor submission to " + getTempAppDir() + ".";
//	        log.error(msg, e);
//	        throw new JobException(msg, e);
//	    }
	}

    /**
     * Gets the executable path relative to the job work directory.
     *
     * @return the executable path run by condor
     */
    protected String getExecutablePath() {
        String executablePath = getSoftware().getExecutablePath();
        if (executablePath.startsWith("/")) {
            executablePath = executablePath.substring(1);
        }
        return executablePath;
    }

    /* (non-Javadoc)
     * @see org.iplantc.service.jobs.managers.launchers.AbstractJobLauncher#processApplicationTemplate()
     */
    @Override
    public String processApplicationWrapperTemplate() throws JobException
    {
        step = "Processing " + getJob().getSoftwareName() + " wrapper template for job " + getJob().getUuid();
        log.debug(step);

        // need getTempAppDir() + software.getExecutablePath()
        // read in the template file
        // create the submit script in the temp folder
        File appTemplateFile = new File(getTempAppDir() + File.separator + getSoftware().getExecutablePath());
        // replace the executable script file references with the file names
        Map<String, String> inputMap = null;
        String appTemplate = null;
        StringBuilder batchScript;
//        StringWriter batchWriter = null;

        // throw exception if file not found
        try {
            if (!appTemplateFile.exists()) {
            	String msg = "Unable to locate wrapper script for \"" +
                             getSoftware().getUniqueName() + "\" at " +
                             getSoftware().getDeploymentPath() + "/" + getSoftware().getExecutablePath();
            	log.error(msg);
                throw new JobException(msg);
            }
            appTemplate = FileUtils.readFileToString(appTemplateFile);

            batchScript = new StringBuilder();
            
            // agave log file environment variable
            batchScript.append("##########################################################\n");
            batchScript.append("# Agave Environment Settings \n");
            batchScript.append("##########################################################\n\n");
            
            batchScript.append("# Location of agave job lifecycle log file\n");
            batchScript.append("AGAVE_LOG_FILE=$(pwd)/.agave.log\n\n\n");
			
            batchScript.append("##########################################################\n");
            batchScript.append("# Agave Utility functions \n");
            batchScript.append("##########################################################\n\n");
            
            // datetime function
            batchScript.append("# cross-plaltform function to print an ISO8601 formatted timestamp \n");
            batchScript.append("function agave_datetime_iso() { \n  date '+%Y-%m-%dT%H:%M:%S%z'; \n} \n\n");
	
			// logging function
            batchScript.append("# standard logging function to write agave job lifecycle logs\n");
            batchScript.append("function agave_log_response() { \n  echo \"[$(agave_datetime_iso)] ${@}\"; \n} 2>&1 >> \"${AGAVE_LOG_FILE}\"\n\n");
         			
            
            // write the callback to trigger status update at start
            batchScript.append("# Callback to signal the job has started executing user-defined logic\n");
            batchScript.append(resolveMacros("${AGAVE_JOB_CALLBACK_RUNNING}"));
            
            
            batchScript.append("##########################################################\n");
            batchScript.append("# Agave App and System Environment Settings \n");
            batchScript.append("##########################################################\n\n");
            
            
            List<String> appModules = getSoftware().getModulesAsList();
            if (!appModules.isEmpty()) {
	            batchScript.append("# App specific module commands\n");
	            
	            // add modules if specified by the app. Generally these won't be used in a condor app,
	            // but in the event they're running mpi or gliding in, these are available.
				for (String module : appModules) {
					batchScript.append("module " + module + "\n");
				}
				batchScript.append("\n");
            }
            else {
            	batchScript.append("# No modules commands configured for this app\n\n");   
            }
			
            // add in any custom environment variables that need to be set
            if (!StringUtils.isEmpty(getSoftware().getExecutionSystem().getEnvironment())) {
            	batchScript.append("# App specific environment variables\n");
            	batchScript.append(getSoftware().getExecutionSystem().getEnvironment());
            	batchScript.append("\n\n\n");
        	}
            else {
            	batchScript.append("# No custom environment variables configured for this app\n\n\n");   
            }
            
            
            batchScript.append("##########################################################\n");
            batchScript.append("# Begin App Wrapper Template Logic \n");
            batchScript.append("##########################################################\n\n");
           
            appTemplate = appTemplate + 
					"\n\n\n" +
            		"##########################################################\n" +
            		"# End App Wrapper Template Logic \n" +
            		"##########################################################\n\n";
           
            // replace the parameters with their passed in values
            appTemplate = resolveTemplateMacros(appTemplate);

            // append the template with
            batchScript.append(appTemplate);

            // remove the previous executable as we must copy the filtered file to the remote system and we do not
            // want to overwrite it.
            appTemplateFile.delete();

//            batchWriter = new FileWriter(appTemplateFile);

            // write new contents to appTemplate for execution
//            batchWriter.write(batchScript.toString());

//            writeWrapperTemplateToRemoteJobDir(remoteApplicationWrapperPath, batchScript.toString());
            return batchScript.toString();

//            return remoteApplicationWrapperPath;
//            // ensure the file is executable so permissions are set when deploying and running
//            Files.setPosixFilePermissions(appTemplateFile.toPath(), PosixFilePermissions.fromString("rwxr-x---"));
//
//            return appTemplateFile;
        } 
        catch (IOException e) {
        	String msg = "FileUtil operation failed.";
//        	log.error(msg, e);
            throw new JobException(msg, e);
        } 
        catch (JobException e) {
        	String msg = "Json failure from job inputs or parameters.";
//        	log.error(msg);
            throw new JobException(msg, e);
        }
        catch(JobMacroResolutionException e) {
//            log.error(e);
            throw new JobException(e.getMessage(), e);
        }
		catch (URISyntaxException e) {
			String msg = "Failed to parse input URI.";
//			log.error(msg, e);
			throw new JobException(msg, e);
		}
//        finally {
//            try {
//                if (batchWriter != null) batchWriter.close();
//            } catch (IOException e) {
//                log.error("Unable to close batch writer after job wrapper template processing", e);
//            }
//        }

    }

	/**
	 * Parses all the inputs, parameters, and macros for the given string. This is appropriate for both resolving
     * wrapper templates and condor submit scripts.
	 * 
	 * @param appTemplate the wrapper template as provided by the app definition
	 * @return fully resolved template to run and start the condor job
	 * @throws JobException if unable to parse the job template
	 * @throws URISyntaxException when invalid input URI are found in the job inputs.
	 */
	protected String resolveTemplateMacros(String appTemplate)
	throws JobException, URISyntaxException, JobMacroResolutionException {
		// replace the parameters with their passed in values
		JsonNode jobParameters = getJob().getParametersAsJsonObject();
		
		for (SoftwareParameter param: getSoftware().getParameters())
		{
			if (jobParameters.has(param.getKey())) 
			{
				JsonNode jobJsonParam = jobParameters.get(param.getKey());

				// serialized the runtime parameters into a string of space-delimited 
				// values after enquoting and adding relevant argument(s)
				String templateVariableValue = parseSoftwareParameterValueIntoTemplateVariableValue(param, jobJsonParam);
				
				// now actually filter the template for this parameter
				appTemplate = appTemplate.replaceAll("(?i)\\$\\{" + param.getKey() + "\\}", templateVariableValue);
			}
			else if (!param.isVisible())
			{
				// serialized the runtime parameters into a string of space-delimited 
				// values after enquoting and adding relevant argument(s)
				String templateVariableValue = parseSoftwareParameterValueIntoTemplateVariableValue(param, param.getDefaultValueAsJsonArray());
				
				// now actually filter the template for this parameter
				appTemplate = appTemplate.replaceAll("(?i)\\$\\{" + param.getKey() + "\\}", templateVariableValue);
			}
			else 
			{
				appTemplate = appTemplate.replaceAll("(?i)\\$\\{" + param.getKey() + "\\}", "");
			}
		}
		
		// replace the parameters with their passed in values
		JsonNode jobInputs = getJob().getInputsAsJsonObject();
		
		for (SoftwareInput input: getSoftware().getInputs())
		{
			if (jobInputs.has(input.getKey())) 
			{
				JsonNode jobJsonInput = jobInputs.get(input.getKey());

				// serialized the runtime parameters into a string of space-delimited 
				// values after enquoting and adding relevant argument(s)
				String templateVariableValue = parseSoftwareInputValueIntoTemplateVariableValue(input, jobJsonInput);
				
				// now actually filter the template for this parameter
				appTemplate = appTemplate.replaceAll("(?i)\\$\\{" + input.getKey() + "\\}", templateVariableValue);
			}
			else if (!input.isVisible())
			{
				// serialized the runtime parameters into a string of space-delimited 
				// values after enquoting and adding relevant argument(s)
				String templateVariableValue = parseSoftwareInputValueIntoTemplateVariableValue(input, input.getDefaultValueAsJsonArray());
				
				// now actually filter the template for this parameter
				appTemplate = appTemplate.replaceAll("(?i)\\$\\{" + input.getKey() + "\\}", templateVariableValue);
			}
			else 
			{
				appTemplate = appTemplate.replaceAll("(?i)\\$\\{" + input.getKey() + "\\}", "");
			}
		}
		
		// strip out all references to icommands and it irods shadow files
		if (getExecutionSystem().isPubliclyAvailable()) {
			appTemplate = CommandStripper.strip(appTemplate);
		}
		
		// strip out premature completion callbacks that might result in archiving starting before
		// the script exists.
		appTemplate = filterRuntimeStatusMacros(appTemplate);
		
		// Replace all the runtime callback notifications
		appTemplate = resolveRuntimeNotificationMacros(appTemplate);

        // Replace all the agave job attribute macros
		appTemplate = resolveMacros(appTemplate);
		return appTemplate;
	}

    /**
     * Packages up the app assets and wrapper template into a tarball for transfer and submission to
     * the condor {@link ExecutionSystem} where they can be submitted as a single job to the condor
     * scheduler by the condor executable.
     *
     * @throws JobException when an error occurs trying to submit the job
     */
    private void createRemoteTransferPackage() throws JobException {
        step = "Creating the " + getJob().getSoftwareName() + " transfer package for job " + getJob().getUuid();
        log.debug(step);
        
        String createTransferPackageCmd = "cd " + getJob().getWorkPath() + "; "
                + "tar czvf ./transfer.tar.gz  --exclude condorSubmit --warning=no-file-changed .";

        try (RemoteSubmissionClient submissionClient = getExecutionSystem().getRemoteSubmissionClient(getJob().getInternalUsername())) {

            String response = submissionClient.runCommand(createTransferPackageCmd);
            response = StringUtils.lowerCase(response);
            if (StringUtils.contains(response, "cannot") ||
                    StringUtils.contains(response, "command not found")) {

                throw new JobException("Failed to create Condor transfer archive on remote execution system: " + response);
            }
        }
        catch (AuthenticationException e) {
            throw new JobException("Failed to authenticate to execution system when creating the Condor " +
                    "scheduler input transfer archive: " + e.getMessage(), e);
        }
        catch (JobException e) {
            throw e;
        }
        catch (Exception e) {
            throw new JobException("Failed to create Condor transfer archive on " +
                    "remote execution system: " + e.getMessage(), e);
        }
    }

    /**
     * Currently for the OSG condor submit host, files pushed to the execution system may not retain their executable
     * permission, so they need to be explicitly set.
     *
     * @throws JobException if unable to change permissions on the remote execution scripts, or communicate with the system
     */
    private void addExecutionPermissionsToWrapper() throws JobException {
        step = "Changing execute permissions on condor transfer wrapper and executable for job " + getJob().getUuid();
        log.debug(step);

        String changePermissionsCmd = String.format("cd \"%s\"; chmod +x *.sh ; chmod +x \"%s\"", getJob().getWorkPath(), getExecutablePath());

        try (RemoteSubmissionClient submissionClient = getExecutionSystem().getRemoteSubmissionClient(getJob().getInternalUsername())) {
            String response = submissionClient.runCommand(changePermissionsCmd);
            if (response.contains("Cannot")) {
                throw new JobException("Failed to set execute permission on Condor launch scripts: " + response);
            }
        }
        catch (AuthenticationException e) {
            throw new JobException("Failed to authenticate to execution system when setting execute permission " +
                    "on Condor launch scripts: " + e.getMessage(), e);
        }
        catch (JobException e) {
            throw e;
        }
        catch (Exception e) {
            throw new JobException("Failed to set execute permission on Condor launch scripts: " + e.getMessage(), e);
        }
    }

    /**
     * Submit job to the condor scheduler.
     *
     * @throws IOException when there is an issue parsing or locating the application template and deployment assets.
     * @throws JobException when an error occurs interacting with the remote system or updating the job details.
     * @throws SchedulerException when the scheduler on the {@link ExecutionSystem} rejects the job.
     * @throws SoftwareUnavailableException when the job software is disabled, or deployment assets are missing.
     * @throws SystemUnavailableException when one or more dependent systems is unavailable.
     * @throws SystemUnknownException when a dependent system (job execution, software deployment, etc) are no longer in the db.
     */
    @Override
    public void launch() throws IOException, JobException, SoftwareUnavailableException, SchedulerException, SystemUnknownException, SystemUnavailableException
    {
        String condorSubmitCmdString = null;
        try 
        {
            try {
                if (getRemoteExecutionDataClient() == null) {
                    setRemoteExecutionDataClient(getExecutionSystem().getRemoteDataClient(getJob().getInternalUsername()));
                    getRemoteExecutionDataClient().authenticate();
                }
            } catch (IOException | RemoteDataException | RemoteCredentialException e) {
                String msg = "Failed to create a remote connection to " + getJob().getSystem() +
                        ". App assets cannot be staged to the job execution system.";
                throw new JobException(msg, e);
            }

            // calculate remote job path if not already set
            calculateRemoteJobPath();

        	// this is used to set the tempAppDir identifier so that we can come back
            // later to find the directory when the job is done (Failed for cleanup or Successful for archiving)
            setTimeMarker();
            
            checkStopped();
            
            // sets up the application directory to execute this job launch; see comments in method
            createTempAppDir();
            
            checkStopped();

            // copy our application package from the software.deploymentPath to our tempAppDir
            copySoftwareToTempAppDir();

            checkStopped();
            
            // create the generic transfer wrapper that condor_submit will use to un-tar the execution
            // package after the files have been transferred to OSG
            String transferWrapperContent = createTransferWrapper();
            writeToRemoteJobDir("transfer_wrapper.sh", transferWrapperContent);

            checkStopped();
            
            // prepend the application template with call back to let the Job service know the job has started
            // parse the application template and replace tokens with the inputs, parameters and outputs.
            String applicationWrapperContent = processApplicationWrapperTemplate();
            writeToRemoteJobDir(getSoftware().getExecutablePath(), applicationWrapperContent);

            checkStopped();
            
            // create the shadow file containing the exclusion files for archiving
            String jobArchiveManifestContent = processJobArchiveManifest();
            writeToRemoteJobDir(ARCHIVE_FILENAME, jobArchiveManifestContent);

			
            checkStopped();
            
            // create the Condor submit file should include the path to executable, the transfer.tar.gz and input(s)
            // default classAds for Linux
            String condorSubmitFileContent = createCondorSubmitFile(timeMark);
            writeToRemoteJobDir("condorSubmit", condorSubmitFileContent);
            
            checkStopped();
            
            // move the local temp directory to the remote system
            stageSofwareApplication();

            checkStopped();
            
            // change permissions to executable for wrapper.sh and transfer_wrapper.sh
            addExecutionPermissionsToWrapper();
            
            checkStopped();
            
            // remotely tar up the entire executable with input data on the execution system
            createRemoteTransferPackage();
            
            checkStopped();
            
            // run condor_submit
            String condorJobId = submitJobToQueue();
            
            getJob().setSubmitTime(new DateTime().toDate());   // Date job submitted to Condor
            getJob().setLastUpdated(new DateTime().toDate());  // Date job started by Condor
            getJob().setLocalJobId(condorJobId);
            getJob().setStatus(JobStatusType.QUEUED, "Condor job successfully placed into queue");
            
            JobDao.persist(getJob());
            
        }
        catch (JobException e) {
        	jobFailed = true;
        	log.error(step, e);
        	this.setJob(JobManager.updateStatus(this.getJob(), JobStatusType.FAILED, e.getMessage()));
        	throw e;
        } 
        catch (SystemUnavailableException e) {
        	jobFailed = true;
        	log.error(step, e);
        	throw e;
        } 
        catch (Exception e) {
            jobFailed = true;
            log.error(step, e);
            this.setJob(JobManager.updateStatus(this.getJob(), JobStatusType.FAILED, e.getMessage()));
            throw new JobException("Failed to invoke app: \"" + getSoftware().getUniqueName() + "\n\"   with command:  " + condorSubmitCmdString + "\n" + e.getMessage(), e);
        } 
        finally {
            // do we always clean up, or just on success so we have the cache available for retry?
            if (getRemoteExecutionDataClient() != null) { getRemoteExecutionDataClient().disconnect(); }
            FileUtils.deleteQuietly(getTempAppDir());
        }
    }

    @Override
    protected String submitJobToQueue() throws JobException
    {
    	try (RemoteSubmissionClient submissionClient = getExecutionSystem().getRemoteSubmissionClient(getJob().getInternalUsername())) {
			
	        //String[] condorSubmitCmdString = new String[]{"/bin/bash", "-cl", "cd " + tempAppDir.getAbsolutePath() + "; condor_submit " + condorSubmitFile.getName()};

    		// Get the remote work directory for the log file
			String remoteWorkPath = getRemoteExecutionDataClient().resolvePath(getJob().getWorkPath());
						
			// Resolve the startupScript and generate the command to run it and log the response to the
			// remoteWorkPath + "/.agave.log" file
			String startupScriptCommand = getStartupScriptCommand(remoteWorkPath);
						
			// command to cd to the remoteWorkPath
			String cdCommand = "cd " + remoteWorkPath;
			
			// command to submit the condor submit file we built to wrap the 
			// job assets to the remote condor master
			String submitCommand = "condor_submit condorSubmit";
			
			// run the aggregate command on the remote system
			String submissionResponse = submissionClient.runCommand(
	        		startupScriptCommand + " ; " + cdCommand + " ; " + submitCommand);
	    	
			if (StringUtils.isBlank(submissionResponse)) 
			{
				// retry the remote command once just in case it was a flicker
				submissionResponse = submissionClient.runCommand(
						startupScriptCommand + " ; " + cdCommand + " ; " + submitCommand);
				
				// blank response means the job didn't go in...twice. Fail the attempt
				if (StringUtils.isBlank(submissionResponse)) {
					String msg = "Failed to submit condor job. " + submissionResponse;
					log.error(msg);
					throw new JobException(msg);
				}
			}
			
			// parse the response from the remote command invocation to get the localJobId
			// by which we'll reference the job during monitoring, etc.
			CondorJobIdParser jobIdParser = new CondorJobIdParser();
	        
	        return jobIdParser.getJobId(submissionResponse);
    	}
        catch (RemoteJobIDParsingException | RemoteExecutionException e) {
    	    String msg = "Error submitting job " + getJob().getUuid() + " to the remote condor queue. " +
                    "Unable to parse the response from the scheduler: " + e.getMessage();
            throw new JobException(msg, e);
        }
    	catch (JobException e) {
    		throw e;
    	}
    	catch (Exception e) {
    		String msg = "Failed to submit job to condor queue.";
    		throw new JobException(msg, e);
    	}
    }
    
    /**
     * Cleanup all the loose ends if the job failed
     */
    private void cleanup() {
        try {
            HibernateUtil.closeSession();
        } catch (Exception ignored) {}
        try {
            FileUtils.deleteDirectory(getTempAppDir());
        } catch (Exception ignored) {}
    }
}