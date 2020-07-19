/**
 * 
 */
package org.iplantc.service.jobs.managers.launchers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.apps.model.SoftwareInput;
import org.iplantc.service.apps.model.SoftwareParameter;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.RemoteJobIDParsingException;
import org.iplantc.service.jobs.exceptions.SchedulerException;
import org.iplantc.service.jobs.exceptions.SoftwareUnavailableException;
import org.iplantc.service.jobs.managers.launchers.parsers.RemoteJobIdParser;
import org.iplantc.service.jobs.managers.launchers.parsers.RemoteJobIdParserFactory;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.model.scripts.CommandStripper;
import org.iplantc.service.jobs.model.scripts.SubmitScript;
import org.iplantc.service.jobs.model.scripts.SubmitScriptFactory;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.remote.exceptions.RemoteExecutionException;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.enumerations.ExecutionType;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.channels.ClosedByInterruptException;
import java.util.List;

/**
 * @author dooley
 * 
 */
public class HPCLauncher extends AbstractJobLauncher 
{
	private static final Logger log = Logger.getLogger(HPCLauncher.class);

	protected String batchScriptName = null;
    
	/**
	 * Default no-args constructor for mock testing
	 */
	protected HPCLauncher() {
		super();
	}
	
	/**
	 * Creates an instance of a JobLauncher capable of submitting jobs to batch
	 * queuing systems on HPC resources.
	 */
	public HPCLauncher(Job job) {
		super(job);
	}

	public String getBatchScriptName() {
		return batchScriptName;
	}

	public void setBatchScriptName(String batchScriptName) {
		this.batchScriptName = batchScriptName;
	}


	/*
	 * Put the job in the batch scheduling queue
	 */
	@Override
	public void launch() throws JobException, ClosedByInterruptException, SchedulerException, IOException, SystemUnavailableException
	{
		File tempAppDir = null;
		try
		{
			if (getSoftware() == null) {
				String msg = "Cannot launch a null application.";
				log.error(msg);
				throw new SoftwareUnavailableException(msg);
			}
			
			if (!getSoftware().isAvailable()) {
				String msg = "Application " + getJob().getSoftwareName() + " is not longer available for execution.";
				log.error(msg);
				throw new SoftwareUnavailableException(msg);
			}
			
			// if the system is down, return it to the queue to wait for the system to come back up.
			if (getExecutionSystem() == null) 
			{
				String msg = "Null execution system assigned in application " + getSoftware().getName() + ".";
				log.error(msg);
				throw new SystemUnavailableException(msg);
			} 
			
			// if the system is down, return it to the queue to wait for the system to come back up.
			if (!getExecutionSystem().getStatus().equals(SystemStatusType.UP)) 
			{
				String msg = "Execution system " + getExecutionSystem().getName() + " is not up.";
				log.error(msg);
				throw new SystemUnavailableException(msg);
			} 
			
			// sets up the application directory to execute this job launch; see comments in method
            createTempAppDir();
            
            checkStopped();
            
            // copy our application package from the software.deploymentPath to our tempAppDir
            copySoftwareToTempAppDir();
            
            checkStopped();
            
            // prepend the application template with call back to let the Job service know the job has started
            // parse the application template and replace tokens with the inputs, parameters and outputs.
            processApplicationTemplate();
			
            checkStopped();
            
            // create the shadow file containing the exclusion files for archiving
            createArchiveLog(ARCHIVE_FILENAME);
			
            checkStopped();
            
            // move the local temp directory to the remote system
            stageSofwareApplication();
			
            checkStopped();
            
            String remoteJobId = submitJobToQueue();
            
            getJob().setSubmitTime(new DateTime().toDate());   // Date job submitted to queue
            getJob().setLastUpdated(new DateTime().toDate());  // Date job started by queue
            getJob().setLocalJobId(remoteJobId);
            
            if (!getJob().getStatus().equals(JobStatusType.RUNNING)) {
                String message = "Job successfully submitted to execution system as local id " + getJob().getLocalJobId();
	            if (getExecutionSystem().getExecutionType() == ExecutionType.HPC) {
	            	message = "HPC job successfully placed into " + 
	            			getJob().getBatchQueue() + " queue as local job " + getJob().getLocalJobId();
	            } else if (getExecutionSystem().getExecutionType() == ExecutionType.CLI) {
	            	message = "CLI job successfully forked as process id " + getJob().getLocalJobId();
	            }
	            
	            if (log.isDebugEnabled()) 
	            	log.debug(message.replaceFirst("job", "job " + getJob().getUuid()));
	            
	            getJob().setStatus(JobStatusType.QUEUED, message);
	            
	            //   Forked jobs start running right away. if they bomb out right after submission,
	            // then they would stay in the queued state for the entire job runtime before being
	            // cleaned up. By setting the job status to running here, we can activate the monitor
	            // immediately and keep the feedback loop on failed jobs tight. 
	            //   It's worth noting that the RUNNING status on valid jobs will still come through, 
	            // but it will be ignored since the job state is already running. no harm no foul.  
	            if (getSoftware().getExecutionType() == ExecutionType.CLI) {
	            	getJob().setStatus(JobStatusType.RUNNING, message);
	            }
            }
            else
            {
            	if (log.isDebugEnabled())
            		log.debug("Callback already received for job " + getJob().getUuid() +
                              " skipping duplicate status update.");    
            }
            
            JobDao.persist(getJob());
		}
		catch (ClosedByInterruptException e) {
			if (log.isDebugEnabled()) log.debug("Launch interrupted for job " + getJob().getUuid(), e);
            throw e;
        }
        catch (SchedulerException e) {
        	log.error("Scheduler exception occurred during launch of job " + getJob().getUuid(), e);
			throw e;
		}
		catch (Exception e)
		{
			String msg = "Launch exception for job " + getJob().getUuid() + ": " + e.getMessage();
			log.error(msg, e);
			throw new JobException(msg, e);
		}
		finally
		{
			try { HibernateUtil.closeSession(); } catch (Exception e) {}
			try { FileUtils.deleteDirectory(tempAppDir); } catch (Exception e) {}
			try { submissionClient.close(); } catch (Exception e) {}
		}
	}
	
	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.managers.launchers.AbstractJobLauncher#processApplicationTemplate()
	 */
	@Override
    public File processApplicationTemplate() throws JobException 
    {
		step = "Processing the " + getJob().getSoftwareName() + " wrapper template for job " + getJob().getUuid();
		log.debug(step);

		setBatchScriptName(Slug.toSlug(getJob().getName()) + ".ipcexe");

		String appTemplate = null;
        File ipcexeFile = null;
		// create the submit script in the temp folder
		ipcexeFile = new File(getTempAppDir() + File.separator + getBatchScriptName());

		try (FileWriter batchWriter = new FileWriter(ipcexeFile);) {

			SubmitScript script = SubmitScriptFactory.getInstance(getJob(), getSoftware(), getExecutionSystem());
	
			// write the script header
			batchWriter.write(script.getScriptText());

			String absWorkDir = getAbsoluteRemoteJobDirPath();
			
			batchWriter.write("##########################################################\n");
            batchWriter.write("# Agave Environment Settings \n");
            batchWriter.write("##########################################################\n\n");
            
            batchWriter.write("# Ensure we're in the job work directory \n");
            batchWriter.write("cd " + absWorkDir + "\n\n");
			
            batchWriter.write("# Location of agave job lifecycle log file \n");
            batchWriter.write("AGAVE_LOG_FILE=" + absWorkDir + "/.agave.log\n\n\n");
			
            batchWriter.write("##########################################################\n");
            batchWriter.write("# Agave Utility functions \n");
            batchWriter.write("##########################################################\n\n");
            
            // datetime function
            batchWriter.write("# cross-plaltform function to print an ISO8601 formatted timestamp \n");
            batchWriter.write("function agave_datetime_iso() { \n  date '+%Y-%m-%dT%H:%M:%S%z'; \n} \n\n");
	
			// logging function
            batchWriter.write("# standard logging function to write agave job lifecycle logs\n");
            batchWriter.write("function agave_log_response() { \n  echo \"[$(agave_datetime_iso)] ${@}\"; \n} 2>&1 >> \"${AGAVE_LOG_FILE}\"\n\n");
         			
            // write the callback to trigger status update at start
            batchWriter.write("# Callback to signal the job has started executing user-defined logic\n");
            batchWriter.write(resolveMacros("${AGAVE_JOB_CALLBACK_RUNNING}"));
            
            
            batchWriter.write("##########################################################\n");
            batchWriter.write("# Agave App and System Environment Settings \n");
            batchWriter.write("##########################################################\n\n");
            
            
            List<String> appModules = getSoftware().getModulesAsList();
            if (!appModules.isEmpty()) {
	            batchWriter.write("# App specific module commands\n");
	            
	            // add modules if specified by the app. Generally these won't be used in a condor app,
	            // but in the event they're running mpi or gliding in, these are available.
				for (String module : appModules) {
					batchWriter.write("module " + module + "\n");
				}
				batchWriter.write("\n");
            }
            else {
            	batchWriter.write("# No modules commands configured for this app\n\n");   
            }
			
            // add in any custom environment variables that need to be set
            if (!StringUtils.isEmpty(getExecutionSystem().getEnvironment())) {
            	batchWriter.write("# App specific environment variables\n");
            	batchWriter.write(getExecutionSystem().getEnvironment());
            	batchWriter.write("\n");
        	}
            else {
            	batchWriter.write("# No custom environment variables configured for this app\n\n\n");   
            }

			appTemplate = "##########################################################\n" +
            			  "# Begin App Wrapper Template Logic \n" +
					      "##########################################################\n\n";

            // read in the template file
			appTemplate += getAppTemplateFileContents();

			// replace parameters
			appTemplate = resolveJobRequestParameters(appTemplate);

			// replace inputs
			appTemplate = resolveJobRequestInputs(appTemplate);

			// strip out all references to banned commands such as icommands, etc
			if (getExecutionSystem().isPubliclyAvailable()) {
				appTemplate = CommandStripper.strip(appTemplate); 
			}
			
			// strip out premature completion callbacks that might result in archiving starting before
			// the script exists.
			appTemplate = filterRuntimeStatusMacros(appTemplate);
			
			// add the success statement after the template by default. The user can add failure catches
			// in their scripts that will trump a later success status.
			appTemplate += "\n\n\n" +
					"##########################################################\n" +
            		"# End App Wrapper Template Logic \n" +
            		"##########################################################\n\n" +
            		"# Callback to signal the job has completed all user-defined logic\n" +
            		"${AGAVE_JOB_CALLBACK_CLEANING_UP}";
           
			// Replace all the runtime callback notifications
			appTemplate = resolveRuntimeNotificationMacros(appTemplate);
						
			// Replace all the agave job attribute macros
			appTemplate = resolveMacros(appTemplate);
			
			batchWriter.write(appTemplate);

			batchWriter.flush();

			return ipcexeFile;
		} 
        catch (IOException e) {
        	String msg = "FileUtil operation failed.";
            log.error(msg, e);
            throw new JobException(msg, e);
        } 
        catch (JobException e) {
        	String msg = "Json failure from job inputs or parameters.";
        	log.error(msg, e);
            throw new JobException(msg, e);
        }
		catch (URISyntaxException e) {
			String msg = "Failed to parse input URI.";
			log.error(msg, e);
            throw new JobException(msg, e);
		} 
        catch (Exception e) {
			String msg = "Failed to resolve remote execution system prior to staging application.";
			log.error(msg, e);
			throw new JobException(msg, e);
		}
    }

	/**
	 * Utility method to resolve the {@link Job#getWorkPath()} to an absolute path on the remote host.
	 * @return the absolute path of the job directory on the execution system
	 * @throws FileNotFoundException if the path cannot be found on the remote system
	 * @throws RemoteCredentialException if unable to auth to the remote system or credentials missing
	 * @throws RemoteDataException if unable to contact the remote system.
	 */
	protected String getAbsoluteRemoteJobDirPath() throws FileNotFoundException, RemoteCredentialException, RemoteDataException {
		RemoteDataClient remoteExecutionDataClient = null;
		try {
			remoteExecutionDataClient = getExecutionSystem().getRemoteDataClient(getJob().getInternalUsername());
			return remoteExecutionDataClient.resolvePath(getJob().getWorkPath());
		} finally {
			try {
				if (remoteExecutionDataClient != null) { remoteExecutionDataClient.disconnect(); }
			} catch (Exception ignored) {}
		}
	}

	/**
	 * Filters the app template with the parameter values from the job request
	 * @param appTemplate the app wrapper template content
	 * @return the filtered content
	 * @throws JobException if the parameters cannot be fetched as a JsonObject
	 */
	protected String resolveJobRequestParameters(String appTemplate) throws JobException {
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

		return appTemplate;
	}

	/**
	 * Filters the app template with the input values from the job request
	 * @param appTemplate the app wrapper template content
	 * @return the filtered content
	 * @throws JobException if the parameters cannot be fetched as a JsonObject
	 */
	protected String resolveJobRequestInputs(String appTemplate) throws JobException, URISyntaxException {
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

		return appTemplate;
	}

	/**
	 * Reads the contents of the app wrapper template already staged to the server host.
	 *
	 * @return the raw contents of the app wrapper template
	 * @throws IOException if the file cannot be read or found
	 */
	protected String getAppTemplateFileContents() throws IOException {
		File appTemplateFile = new File(getTempAppDir()
				+ File.separator + getSoftware().getExecutablePath());

		if (!appTemplateFile.exists()) {
			throw new FileNotFoundException("Unable to locate wrapper script for \"" +
					getSoftware().getUniqueName() + "\" at " +
					appTemplateFile.getAbsolutePath());
		}

		return FileUtils.readFileToString(appTemplateFile);
	}

	/* (non-Javadoc)
	 * @see org.iplantc.service.jobs.managers.launchers.AbstractJobLauncher#submitJobToQueue()
	 */
	@Override
	protected String submitJobToQueue() throws JobException, SchedulerException
	{
		step = String.format("Submitting job %s to the %s scheduler on %s %s",
				getJob().getUuid(), getJob().getSchedulerType().name(), getJob().getExecutionType().name(), getJob().getSystem());

		log.debug(step);

		String submissionResponse = null;
		RemoteDataClient remoteExecutionDataClient = null;
		try
		{
			submissionClient = getExecutionSystem().getRemoteSubmissionClient(getJob().getInternalUsername());
			
			remoteExecutionDataClient = getExecutionSystem().getRemoteDataClient(getJob().getInternalUsername());
			
			// Get the remote work directory for the log file
			String remoteWorkPath = remoteExecutionDataClient.resolvePath(getJob().getWorkPath());
			
			// Resolve the startupScript and generate the command to run it and log the response to the
			// remoteWorkPath + "/.agave.log" file
			String startupScriptCommand = getStartupScriptCommand(remoteWorkPath);
			
			// command to cd to the remoteWorkPath
			String cdCommand = "cd " + remoteExecutionDataClient.resolvePath(getJob().getWorkPath());
			
			// ensure the wrapper template has execute permissions
			String chmodCommand = "chmod +x " + batchScriptName;
			
			// command to submit the batch script to the scheduler. 
			String submitCommand = getExecutionSystem().getScheduler().getBatchSubmitCommand() + " "
					+ batchScriptName;
			
			// run the aggregate command on the remote system
			submissionResponse = submissionClient.runCommand(
					startupScriptCommand + "; " + cdCommand + "; " + chmodCommand + "; " + submitCommand);
					
			if (StringUtils.isBlank(submissionResponse)) 
			{
			    // Log response problem.
			    log.warn("Empty response from remote submission command for job " + getJob().getUuid() + ".");
			    
				// retry the remote command once just in case it was a flicker
				submissionResponse = submissionClient.runCommand(
						startupScriptCommand + "; " + cdCommand + "; " + chmodCommand + "; " + submitCommand);
				
				// blank response means the job didn't go in...twice. Fail the attempt
				if (StringUtils.isBlank(submissionResponse)) {
					String msg = "Failed to submit HPC job " + getJob().getUuid() + ".";
					log.error(msg);
					throw new JobException(msg);
				}
			}
			
			// Tracing.
			if (log.isDebugEnabled())
			    log.debug("Response from submission command for job " + getJob().getUuid() + ": " + submissionResponse);
			
			// parse the response from the remote command invocation to get the localJobId
			// by which we'll reference the job during monitoring, etc.
			RemoteJobIdParser jobIdParser = 
					new RemoteJobIdParserFactory().getInstance(getExecutionSystem().getScheduler());
			
			return jobIdParser.getJobId(submissionResponse);
		}
		catch (RemoteJobIDParsingException | RemoteExecutionException e) {
			log.error("Error submitting job " + getJob().getUuid() + ".", e);
			throw new JobException(e.getMessage(), e);
		}
		catch (JobException e) {
			log.error("Error submitting job " + getJob().getUuid() + ".", e);
			throw e;
		}
		catch (SchedulerException e) {
			log.error("Error returned from remote scheduler while submitting job " + getJob().getUuid() + ".", e);
			throw e;
		}
		catch (Exception e)
		{
			String msg = "Failed to submit HPC job " + getJob().getUuid() + ".";
			log.error(msg, e);
			throw new JobException(msg, e);
		}
		finally
		{
			try { submissionClient.close(); } catch (Exception ignored){}
			try {
				if (remoteExecutionDataClient != null) {
					remoteExecutionDataClient.disconnect();
				}
			} catch (Exception e){}
		}
	}
}