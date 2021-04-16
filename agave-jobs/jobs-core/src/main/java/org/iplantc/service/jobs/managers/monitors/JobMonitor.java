package org.iplantc.service.jobs.managers.monitors;

import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitoringException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.jobs.managers.launchers.JobLauncher;
import org.iplantc.service.jobs.managers.monitors.parsers.JobStatusResponseParser;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.remote.RemoteSubmissionClient;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.AuthenticationException;
import org.iplantc.service.transfer.exceptions.RemoteDataException;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;

public interface JobMonitor {
    
    /**
     * Checks whether this {@link JobLauncher} has been stopped. 
     * @return true if it has been stopped, false otherwise
     */
    public boolean isStopped();
    
    /**
     * Stops the submission task asynchronously.
     * 
     * @param stopped true if the worker should be stopped, false otherwise
     */
    public void setStopped(boolean stopped);
    
    /**
     * Checks whether this launcher has been stopped and if so, 
     * throws a {@link ClosedByInterruptException}
     * 
     * @throws ClosedByInterruptException when the worker is interrupted by an external process
     */
    void checkStopped() throws ClosedByInterruptException;
   
	/**
	 * Checks the status of the job on the {@link ExecutionSystem} and updates the job record as needed.
	 * 
	 * @throws RemoteJobMonitoringException when the monitor fails to run
	 * @throws SystemUnavailableException when the {@link ExecutionSystem} is offline, deleted, or unavailable
	 */
	public Job monitor() throws RemoteJobMonitoringException, SystemUnavailableException, ClosedByInterruptException;

	/**
	 * Performs the remote call to make the job status check.
	 * @param responseParser the parser to be used to analyze the response from running the queryCommand
	 * @param queryCommand the command to run on the {@link ExecutionSystem}
	 * @return the response from the job check command.
	 * @throws RemoteJobMonitoringException if unable to query the remote job status
	 * @throws RemoteJobMonitorEmptyResponseException if no repsonse comes back from the server when it should never be emtpy
	 * @throws RemoteJobMonitorResponseParsingException if the response from the server cannot be parsed
	 */
	public JobStatusResponse<?> getJobStatusResponse(JobStatusResponseParser responseParser, String queryCommand)
			throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException, RemoteJobMonitoringException;

	/**
	 * Thread safe getter of the job passed to the {@link JobMonitor}.
     * @return the {@link Job} being monitored.
     */
	public Job getJob();

	/**
	 * Thread safe setter of the job passed to the {@link JobMonitor}.
	 * @param job the {@link Job} to set.
	 */
	public void setJob(Job job);
	
	/**
	 * Creates a new {@link RemoteSubmissionClient} for the {@link Job} 
	 * {@link ExecutionSystem}.
	 * @return an active {@link RemoteSubmissionClient}
	 * @throws SystemUnavailableException when the {@link ExecutionSystem} is offline, deleted, or unavailable
	 * @throws AuthenticationException authentication failed when establishing the connection
	 */
	public RemoteSubmissionClient getRemoteSubmissionClient() throws SystemUnavailableException, AuthenticationException;
	
	
	/**
	 * Creates a preauthenticated {@link RemoteDataClient} to the {@link ExecutionSystem} running the {@link Job}.
	 *
	 * @return an active {@link RemoteDataClient}
	 * @throws RemoteDataException if unable to create the {@link RemoteDataClient}
	 * @throws IOException if no connection can be made to the {@link ExecutionSystem}
	 * @throws AuthenticationException when authentication fails to the {@link RemoteDataClient}
	 * @throws SystemUnavailableException when the {@link ExecutionSystem} is offline, deleted, or unavailable
	 * @throws RemoteCredentialException if credentials cannot be fetched for the {@link ExecutionSystem}
	 */
	public RemoteDataClient getAuthenticatedRemoteDataClient() throws RemoteDataException, IOException, AuthenticationException, SystemUnavailableException, RemoteCredentialException;

}