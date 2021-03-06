package org.iplantc.service.jobs.managers.launchers;

import com.fasterxml.jackson.databind.JsonNode;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareInput;
import org.iplantc.service.apps.model.SoftwareParameter;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.exceptions.SchedulerException;
import org.iplantc.service.jobs.exceptions.SoftwareUnavailableException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.WrapperTemplateStatusVariableType;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.transfer.URLCopy;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.channels.ClosedByInterruptException;

/**
 * This interface defines the methods required of all job launching
 * implementing classes. Individual classes are responsible for 
 * communicating with the remote system or scheduler, filtering 
 * the job wrapper template, resolving macros, validating inputs 
 * and parameters, etc.
 * 
 * @author dooley
 *
 */
public interface JobLauncher
{
    /**
     * Checks whether this {@link JobLauncher} has been stopped. 
     * @return true if it has been stopped, false otherwise
     */
    boolean isStopped();
    
    /**
     * Stops the submission task asynchronously.
     * 
     * @param stopped true if the launcher should be set to stopped, false otherwise
     */
    void setStopped(boolean stopped);
    
	/** 
	 * Performs the tasks required to start a job on a remote execution system.
	 *  
	 * @throws IOException when there is an issue parsing or locating the application template and deployment assets.
	 * @throws JobException when an error occurs interacting with the remote system or updating the job details.
	 * @throws SchedulerException when the scheduler on the {@link ExecutionSystem} rejects the job.
	 * @throws SoftwareUnavailableException when the job software is disabled, or deployment assets are missing.
	 * @throws SystemUnavailableException when one or more dependent systems is unavailable.
	 * @throws SystemUnknownException when a dependent system (job execution, software deployment, etc) are no longer in the db.
	 * @
	 */
    void launch() throws IOException, JobException, SchedulerException, SoftwareUnavailableException, SystemUnavailableException, SystemUnknownException;

	/**
	 * Resolves a parameter JSON value or JSON array of values into a serialized string of variables
	 * adding in the appropriate argument value(s) and applying enquote as needed.
	 *  
	 * @param softwareParameter The SoftwareParameter associated with this value.
	 * @param jsonJobParamValue JsonNode representing value or ArrayNode of values for this parameter
	 * @return serialized String of space-delimited values after enquoting and adding relevant argument(s) 
	 */
    String parseSoftwareParameterValueIntoTemplateVariableValue(SoftwareParameter softwareParameter, JsonNode jsonJobParamValue);
	
	/**
	 * Takes the wrapper template for an app, resolves all template variables for registered software inputs and parameters, 
	 * callbacks, black and whitelist commands, etc. Resulting content is used to create the *.ipcexe file that will be staged
	 * to the remote system and invoked to start the job.
	 * @throws JobException
     * @return
	 */
    String processApplicationWrapperTemplate() throws JobException;

	/**
	 * Takes the content of the application wrapper and writes it to the remote system via an output stream to avoid
	 * any platform-specific IO issues found during copy.
	 *
	 * @param filePathRelativeToRemoteJobDir the agave relative path where the app wrapper will be written
	 * @param content the processed application wrapper template to be written
	 * @throws JobException when unable to connect or write the content.
	 */
    void writeToRemoteJobDir(String filePathRelativeToRemoteJobDir, String content) throws JobException;

	/**
	 * Resolves a input JSON value or JSON array of values into a serialized string of variables
	 * adding in the appropriate argument value(s) and applying enquote as needed.
	 *  
	 * @param softwareInput The SoftwareInput associated with this value.
	 * @param jsonJobInputValue JsonNode representing value or ArrayNode of values for this input
	 * @return serialized String of space-delimited values after enquoting and adding relevant argument(s) 
	 * @throws URISyntaxException 
	 */
    String parseSoftwareInputValueIntoTemplateVariableValue(SoftwareInput softwareInput, JsonNode jsonJobInputValue) throws URISyntaxException;
	
	/**
	 * Replaces all job macros in a wrapper template with the tenant and job-specific values for this job.
	 *   
	 * @param wrapperTemplate
	 * @return content of wrapper template with all macros resolved.
	 */
    String resolveMacros(String wrapperTemplate) throws JobMacroResolutionException;
	
	/**
	 * Returns the temp directory used by this {@link JobLauncher} to cache app assets as 
	 * they move to the execution system.
	 * 
	 * @return {@link File} object reference to the local folder.
	 */
    File getTempAppDir();

	/**
	 * Sets the temp directory used by this {@link JobLauncher} to cache the intermediate app
	 * assets during the submission process.
	 * 
	 * @param tempAppDir
	 */
    void setTempAppDir(File tempAppDir);
    
    /**
     * Checks whether this launcher has been stopped and if so, 
     * throws a {@link ClosedByInterruptException}
     * 
     * @throws ClosedByInterruptException
     */
    void checkStopped() throws ClosedByInterruptException;

    /**
     * Returns the local reference to the {@link URLCopy} instanced used
     * for this job submission.
     * 
     * @return 
     */
    URLCopy getUrlCopy();

    /**
     * Sets the {@link URLCopy} instance used for data transfer 
     * by this {@link JobLauncher}.
     * 
     * @param urlCopy
     */
    void setUrlCopy(URLCopy urlCopy);
    
    /**
     * Threadsafe getter of the job passed to the launcher.
     * @return
     */
    Job getJob();
    
    /**
     * Replaces all {@link WrapperTemplateStatusVariableType#AGAVE_JOB_CALLBACK_NOTIFICATION} macros in a
     * wrapper template with a code snippet that will take a comma-separated list of environment variable 
     * names and post them back to the API to be forwarded as a JSON object to a notification event. 
     * The form of the macros with variables.
     *   
     * @param wrapperTemplate
     * @return content of wrapper template with all macros resolved.
     */
    String resolveRuntimeNotificationMacros(String wrapperTemplate);
    
    /**
	 * Replaces all {@link ExecutionSystem#getStartupScript()} macros with the system and job-specific
	 * values for this job.
	 *   
	 * @param startupScript
	 * @return null if the value is blank, the value of the {@code resolveStartupScriptMacros) 
	 * filtered with job and system macros otherwise.
	 */
    String resolveStartupScriptMacros(String startupScript) throws JobMacroResolutionException;
	
	/**
	 * @return the executionSystem
	 */
    ExecutionSystem getExecutionSystem();

	/**
	 * @param executionSystem the executionSystem to set
	 */
    void setExecutionSystem(ExecutionSystem executionSystem);

	/**
	 * @return the software
	 */
    Software getSoftware();

	/**
	 * @param software the software to set
	 */
    void setSoftware(Software software);

	/**
	 * @param job the job to set
	 */
    void setJob(Job job);

}