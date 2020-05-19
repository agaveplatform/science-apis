/**
 * 
 */
package org.iplantc.service.remote;


import org.iplantc.service.remote.exceptions.RemoteExecutionException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.transfer.exceptions.RemoteConnectionException;

/**
 * Defines the interface required of all classes supporting
 * remote job submission. This essentially consists of being able
 * to connect to a remote system and start a job.
 * 
 * @author dooley
 *
 */
public interface RemoteSubmissionClient {

	/**
	 * Run a command on a remote host. Authentication is handled prior
	 * to this command being run.
	 * @param command the command to run
	 * @return the response from the command on the {@link ExecutionSystem}
	 * @throws RemoteExecutionException when the command fails or cannot be run
	 */
	public String runCommand(String command) throws RemoteExecutionException, RemoteConnectionException;

	/**
	 * Explicitly force the connection to the remote host to close.
	 * All exceptions are swallowed in this operation.
	 */
	public void close();
	
	/**
	 * Check whether authentication is valid on the remote host.
	 * @return true if auth succeeds, false otherwise.
	 */
	public boolean canAuthentication();
	
	/**
	 * Get the hostname of the remote system
	 * @return the host to use
	 */
	public String getHost();
	
	/**
	 * Get the port on which the remote system is listening
	 * @return the port to sue
	 */
	public int getPort();
}
