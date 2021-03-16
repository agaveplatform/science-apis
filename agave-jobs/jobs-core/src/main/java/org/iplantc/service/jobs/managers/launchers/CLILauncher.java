/**
 * 
 */
package org.iplantc.service.jobs.managers.launchers;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.managers.launchers.parsers.RemoteJobIdParser;
import org.iplantc.service.jobs.managers.launchers.parsers.RemoteJobIdParserFactory;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.remote.RemoteSubmissionClient;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.ExecutionType;
import org.iplantc.service.transfer.RemoteDataClient;

/**
 * Class to fork a background task on a remote linux system. The process
 * id will be stored as the {@link Job#getLocalJobId()} for querying by the monitoring
 * queue.
 *  
 * @author dooley
 * 
 */
public class CLILauncher extends HPCLauncher 
{
	private static final Logger log = Logger.getLogger(CLILauncher.class);
	
	/**
	 * Default no-args constructor for mock testing
	 */
	protected CLILauncher() {
		super();
	}

	/**
	 * Creates an instance of a JobLauncher capable of forking processes on remote hosts.
	 *
	 * @param job the job to launch
	 * @param software the software corresponding to the {@link Job#getSoftwareName()}
	 * @param executionSystem the system corresponding to the {@link Job#getSystem()}
	 */
	public CLILauncher(Job job, Software software, ExecutionSystem executionSystem) {
		super(job, software, executionSystem);
	}

	@Override
	protected String submitJobToQueue() throws JobException {
		step = String.format("Submitting job %s to the %s scheduler on %s %s",
				getJob().getUuid(), getJob().getSchedulerType().name(), getJob().getExecutionType().name(), getJob().getSystem());

		log.debug(step);

		RemoteDataClient remoteExecutionDataClient = null;
		try (RemoteSubmissionClient submissionClient = getExecutionSystem().getRemoteSubmissionClient(getJob().getInternalUsername())) {

			remoteExecutionDataClient = getExecutionSystem().getRemoteDataClient(getJob().getInternalUsername());

			// Get the remote work directory for the log file
			String remoteWorkPath = remoteExecutionDataClient.resolvePath(getJob().getWorkPath());

			// Resolve the startupScript and generate the command to run it and log the response to the
			// remoteWorkPath + "/.agave.log" file
			String startupScriptCommand = getStartupScriptCommand(remoteWorkPath);

			// command to cd to the remoteWorkPath
			String cdCommand = " cd " + remoteWorkPath;

			// ensure the wrapper template has execute permissions
			String chmodCommand = " chmod +x " + batchScriptName + " > /dev/null ";

			// get the logfile name so we can redirect output properly
			String logFileBaseName = Slug.toSlug(getJob().getName());

			// command to submit the resolved wrapper template (*.ipcexe) script to the
			// scheduler. This forks the command in a child process redirecting stderr
			// to the logFileBaseName + ".err" and stdout to logFileBaseName + ".out".
			// The process id of the invoked wrapper script is captured and quickly
			// evaluated to see if the script failed immediately. If so, the error
			// response is echoed back to the service. Otherwise, the process id is
			// echoed to stdout for the RemoteJobIdParser to extract and associate with
			// the job record.
			String submitCommand = String.format(" sh -c './%s 2> %s.err 1> %s.out & " +
							" export AGAVE_PID=$! && " +
							" if [ -n \"$(ps -o pid= -o comm= | grep $AGAVE_PID | grep %s 3>&2 2>>%s.err)\" ] || [ -e %s.pid ]; then " +
							" echo $AGAVE_PID; " +
							" else " +
							" cat %s.err; " +
							" fi'",
					batchScriptName,
					logFileBaseName,
					logFileBaseName,
					logFileBaseName,
					logFileBaseName,
					logFileBaseName,
					logFileBaseName);

			// run the aggregate command on the remote system
			String submissionResponse = submissionClient.runCommand(
					startupScriptCommand + " ; " + cdCommand + " && " + chmodCommand + " && " + submitCommand);

			if (StringUtils.isBlank(submissionResponse)) {
				// Log response problem.
				log.warn("Empty response from remote submission command for job " + getJob().getUuid() + ".");

				// retry the remote command once just in case it was a flicker
				submissionResponse = submissionClient.runCommand(
						startupScriptCommand + " ; " + cdCommand + " &&  " + chmodCommand + " && " + submitCommand);

				// blank response means the job didn't go in...twice. Fail the attempt
				if (StringUtils.isBlank(submissionResponse)) {
					String msg = "Failed to submit CLI job " + getJob().getUuid() + ".";
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
		} catch (JobException e) {
			log.error("Error submitting job " + getJob().getUuid() + ".", e);
			throw e;
		} catch (Exception e) {
			String msg = "Failed to submit CLI job " + getJob().getUuid() + ".";
			log.error(msg, e);
			throw new JobException(msg, e);
		} finally {
			try {
				if (remoteExecutionDataClient != null) remoteExecutionDataClient.disconnect();
			} catch (Exception ignored) {}
		}
	}

	@Override
	protected void checkJobStatus() throws JobException {
		if (!getJob().getStatus().equals(JobStatusType.RUNNING)) {

			getJob().setStatus(JobStatusType.QUEUED, "CLI job " + getJob().getUuid() +
					" successfully forked as process id " + getJob().getLocalJobId());

			//   Forked jobs start running right away. if they bomb out right after submission,
			// then they would stay in the queued state for the entire job runtime before being
			// cleaned up. By setting the job status to running here, we can activate the monitor
			// immediately and keep the feedback loop on failed jobs tight.
			//   It's worth noting that the RUNNING status on valid jobs will still come through,
			// but it will be ignored since the job state is already running. no harm no foul.
			getJob().setStatus(JobStatusType.RUNNING, "CLI job started running as process id " +
					getJob().getLocalJobId());
		}
		else
		{
			if (log.isDebugEnabled())
				log.debug("Callback already received for job " + getJob().getUuid() +
						" skipping duplicate status update.");
		}
	}
}