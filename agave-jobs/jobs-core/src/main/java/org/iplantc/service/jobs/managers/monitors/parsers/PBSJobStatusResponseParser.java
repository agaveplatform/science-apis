package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.systems.model.enumerations.SchedulerType;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Handles parsing of PBS Professional and PBS Professional Community Edition qstat output. The expected
 * format of the output is:
 *
 * <pre>
 * Job Id: 0.e5a207891675
 *     Job_Name = pbspro.submit
 *     Job_Owner = testuser@e5a207891675
 *     resources_used.cpupercent = 0
 *     resources_used.cput = 00:00:00
 *     resources_used.mem = 0kb
 *     resources_used.ncpus = 1
 *     resources_used.vmem = 0kb
 *     resources_used.walltime = 00:00:06
 *     job_state = F
 *     queue = debug
 *     server = e5a207891675
 *     Checkpoint = u
 *     ctime = Thu May 14 07:04:20 2020
 *     Error_Path = e5a207891675:/home/testuser/pbspro.submit.e0
 *     exec_host = e5a207891675/0
 *     exec_vnode = (e5a207891675:ncpus=1)
 *     Hold_Types = n
 *     Join_Path = n
 *     Keep_Files = n
 *     Mail_Points = a
 *     mtime = Thu May 14 07:04:26 2020
 *     Output_Path = e5a207891675:/home/testuser/pbspro.submit.o0
 *     Priority = 0
 *     qtime = Thu May 14 07:04:20 2020
 *     Rerunable = True
 *     Resource_List.ncpus = 1
 *     Resource_List.nice = 19
 *     Resource_List.nodect = 1
 *     Resource_List.nodes = 1
 *     Resource_List.place = scatter
 *     Resource_List.select = 1:ncpus=1
 *     Resource_List.walltime = 00:01:00
 *     stime = Thu May 14 07:04:20 2020
 *     session_id = 1622
 *     jobdir = /home/testuser
 *     substate = 92
 *     Variable_List = PBS_O_HOME=/home/testuser,PBS_O_LOGNAME=testuser,
 *         PBS_O_PATH=/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/opt
 *         /pbs/bin:/home/testuser/.local/bin:/home/testuser/bin,
 *         PBS_O_MAIL=/var/spool/mail/testuser,PBS_O_SHELL=/bin/bash,
 *         PBS_O_WORKDIR=/home/testuser,PBS_O_SYSTEM=Linux,PBS_O_QUEUE=debug,
 *         PBS_O_HOST=e5a207891675
 *     comment = Job run at Thu May 14 at 07:04 on (e5a207891675:ncpus=1) and fini
 *         shed
 *     etime = Thu May 14 07:04:20 2020
 *     run_count = 1
 *     Stageout_status = 1
 *     Exit_status = 0
 *     Submit_arguments = pbspro.submit
 *     history_timestamp = 1589439866
 *     project = _pbs_project_default
 * </pre>
 */
public class PBSJobStatusResponseParser implements JobStatusResponseParser {
	
	private static final Logger log = Logger.getLogger(PBSJobStatusResponseParser.class);

	/**
	 * The job status query to PBS was of the form {@code "qstat -x -f <job_id>}. That means the response should
	 * come back in a format that can be parsed into a Properties object.
	 *
	 * @param remoteJobId the remote job id to parse from the response
	 * @param schedulerResponseText the response text from the remote scheduler
	 * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
	 * @throws RemoteJobMonitorEmptyResponseException if {@code schedulerResponseText} is blank
	 * @throws RemoteJobMonitorResponseParsingException if {@code schedulerResponseText} could not be parsed
	 */
	@Override
	public JobStatusResponse parse(String remoteJobId, String schedulerResponseText) throws RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException {
		if (StringUtils.isBlank(schedulerResponseText)) {
			throw new RemoteJobMonitorEmptyResponseException(
					"Empty response received from job status check on the remote system. Since the job was " +
							"successfully placed into queue, this is likely caused by a communication issue with the " +
							"scheduler. Retrying may clear up the issue.");

		} else if (schedulerResponseText.toLowerCase().contains("qstat: unknown job id")) {
			throw new RemoteJobMonitorEmptyResponseException(
					"Error response from job status check on the remote system. This is likely due to the job " +
							"completing and being purged from the qstat job cache. Calling tracejob with sufficient " +
							"permissions or examining the job logs should provide more information about the job.");
		}
		else if (!schedulerResponseText.toLowerCase().startsWith("job id:")) {
			throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler: " + schedulerResponseText);
		} else {
//			List<String> lines = Arrays.asList(StringUtils.stripToEmpty(schedulerResponseText).split("[\\r\\n]+"));
//			// iterate over each line, finding the one starting with the job id
//			for (String line: lines) {
//				// PBS status lines start with the job id
//				if (line.startsWith(remoteJobId)) {
//					try {
//						// parse the line with the status info in it
//						return parseLine(remoteJobId, line);
//					} catch (RemoteJobMonitorResponseParsingException e) {
//						throw new RemoteJobMonitorResponseParsingException(e.getMessage() + ": " + schedulerResponseText, e);
//					}
//				}
//			}
//			throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler");

			return parseFullStatus(remoteJobId, schedulerResponseText);
		}
	}

	/**
	 * Provides a mapping from {@link JobStatusResponseParser} to one or more {@link SchedulerType} for which this
	 * can parse the job status query response.
	 *
	 * @return list of the {@link SchedulerType} supported buy this parser
	 */
	@Override
	public List<SchedulerType> getSupportedSchedulerType() {
		return List.of(SchedulerType.PBS, SchedulerType.CUSTOM_PBS);
	}

	/**
	 * Parses a job status response from the remote server response. This should be the line containing the actual
	 * status info for the PBS job, trimmed of any headers, debug info, etc.
	 *
	 * @param remoteJobId the remote job id to parse from the response
	 * @param schedulerResponseText the qstat job status line
	 * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
	 * @throws RemoteJobMonitorResponseParsingException if unable to parse the job status
	 */
	protected JobStatusResponse<PBSJobStatus> parseLine(String remoteJobId, String schedulerResponseText) throws RemoteJobMonitorResponseParsingException {
		Properties props = new Properties();
		PBSJobStatus remoteJobStatus = PBSJobStatus.UNKNOWN;
		String exitCode = null;
		try (InputStream bis = new ByteArrayInputStream(schedulerResponseText.getBytes())) {
			props.load(bis);

			if (props.containsKey("job_state")) {
				remoteJobStatus = PBSJobStatus.valueOfCode(props.getProperty("job_state"));
			} else {
				throw new RemoteJobMonitorResponseParsingException("Unable to find job status in the response from the scheduler");
			}

			if (props.containsKey("Exit_status")) {
				exitCode = props.getProperty("Exit_status");
			}

			return new JobStatusResponse<>(remoteJobId, remoteJobStatus, exitCode);
		} catch (IOException e) {
			throw new RemoteJobMonitorResponseParsingException("Unable to parse the response from the scheduler");
		}
	}

	/**
	 * Parses a job full status response from the remote server response. This should be the full multi-line response.
	 *
	 * @param remoteJobId the remote job id to parse from the response
	 * @param schedulerResponseText the full job status response for the job id
	 * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
	 * @throws RemoteJobMonitorResponseParsingException if unable to parse the job status
	 */
	protected JobStatusResponse<PBSJobStatus> parseFullStatus(String remoteJobId, String schedulerResponseText) throws RemoteJobMonitorResponseParsingException {

		Properties props = new Properties();
		PBSJobStatus remoteJobStatus = PBSJobStatus.UNKNOWN;
		String exitCode = null;
		try (InputStream bis = new ByteArrayInputStream(schedulerResponseText.getBytes())) {
			props.load(bis);

			if (props.isEmpty()) {
				throw new RemoteJobMonitorResponseParsingException("Unable to find job status in the response from the scheduler");
			}

			// if the response does not have a job id, reject due to parsing error
			Map.Entry<Object, Object> entry = getCaseInsensitiveEntry(props, "job");
			if (entry == null || ! entry.getValue().toString().toLowerCase().startsWith("id: " + remoteJobId + ".")) {
				throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler: " + props.getProperty("job"));
			}

			entry = getCaseInsensitiveEntry(props, "job_state");
			if (entry != null) {
				remoteJobStatus = PBSJobStatus.valueOfCode(entry.getValue().toString());
			} else {
				throw new RemoteJobMonitorResponseParsingException("Unable to find job status in the response from the scheduler");
			}

			entry = getCaseInsensitiveEntry(props, "exit_status");
			if (entry != null) {
				exitCode = entry.getValue().toString();
			}

			return new JobStatusResponse<>(remoteJobId, remoteJobStatus, exitCode);
		} catch (IOException e) {
			throw new RemoteJobMonitorResponseParsingException("Unable to parse the response from the scheduler");
		}
	}

	/**
	 * Returns the value of the first key matching the value of {@code key} regardless of case.
	 * @param props the property set to search
	 * @param key the key to search for
	 * @return the matching entry or null if not found
	 */
	private Map.Entry<Object, Object> getCaseInsensitiveEntry(Properties props, String key) {

		for (Map.Entry<Object, Object> entry : props.entrySet()) {
			if (((String) entry.getKey()).equalsIgnoreCase(key)) {
				return entry;
			}
		}

		return null;
	}
}
