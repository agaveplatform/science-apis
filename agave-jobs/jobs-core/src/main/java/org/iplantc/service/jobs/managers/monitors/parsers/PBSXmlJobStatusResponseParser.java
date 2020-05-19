package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.systems.model.enumerations.SchedulerType;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parses the XML response from Torque and PBSPro {@code qstat} job status checks.
 * @deprecated
 */
public class PBSXmlJobStatusResponseParser implements JobStatusResponseParser {
	
	private static final Logger log = Logger.getLogger(PBSXmlJobStatusResponseParser.class);

	class PBSData {
		private List<PBSJob> jobs;

		public List<PBSJob> getJobs() {
			return jobs;
		}

		public void setQueueInfo(List<PBSJob> jobs) {
			this.jobs = jobs;
		}
	}

	class PBSJobResources {
		String cput;
		String memory;
		String virtualMemory;
		String walltime;

		public String getCput() {
			return cput;
		}

		public void setCput(String cput) {
			this.cput = cput;
		}

		public String getMemory() {
			return memory;
		}

		public void setMemory(String memory) {
			this.memory = memory;
		}

		public String getVirtualMemory() {
			return virtualMemory;
		}

		public void setVirtualMemory(String virtualMemory) {
			this.virtualMemory = virtualMemory;
		}

		public String getWalltime() {
			return walltime;
		}

		public void setWalltime(String walltime) {
			this.walltime = walltime;
		}
	}

	class PBSJob {
		String id;
		String name;
		String state;
		String errorPath;
		String queue;
		Long ctime;
		Long startTime;
		Long queueTime;
		Long mtime;
		Long endTime;
		Long compTime;
		Double totalRuntime;
		String exitCode;
		String outputPath;

		public Long getQueueTime() {
			return queueTime;
		}

		public void setQueueTime(Long queueTime) {
			this.queueTime = queueTime;
		}

		public String getOutputPath() {
			return outputPath;
		}

		public void setOutputPath(String outputPath) {
			this.outputPath = outputPath;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getState() {
			return state;
		}

		public void setState(String state) {
			this.state = state;
		}

		public String getErrorPath() {
			return errorPath;
		}

		public void setErrorPath(String errorPath) {
			this.errorPath = errorPath;
		}

		public String getQueue() {
			return queue;
		}

		public void setQueue(String queue) {
			this.queue = queue;
		}

		public Long getCtime() {
			return ctime;
		}

		public void setCtime(Long ctime) {
			this.ctime = ctime;
		}

		public Long getStartTime() {
			return startTime;
		}

		public void setStartTime(Long startTime) {
			this.startTime = startTime;
		}

		public Long getMtime() {
			return mtime;
		}

		public void setMtime(Long mtime) {
			this.mtime = mtime;
		}

		public Long getEndTime() {
			return endTime;
		}

		public void setEndTime(Long endTime) {
			this.endTime = endTime;
		}

		public Long getCompTime() {
			return compTime;
		}

		public void setCompTime(Long compTime) {
			this.compTime = compTime;
		}

		public Double getTotalRuntime() {
			return totalRuntime;
		}

		public void setTotalRuntime(Double totalRuntime) {
			this.totalRuntime = totalRuntime;
		}

		public String getExitCode() {
			return exitCode;
		}

		public void setExitCode(String exitCode) {
			this.exitCode = exitCode;
		}
	}

	class PBSHandler extends DefaultHandler {
		private static final String QUEUE_DATA = "Job";
		private static final String JOB = "Data";
		private static final String JOB_ID = "Job_Id";
		private static final String JOB_NAME = "Job_Name";
		private static final String JOB_STATE = "job_state";
		private static final String JOB_START_TIME = "start_time";
		private static final String JOB_END_TIME = "etime";
		private static final String JOB_QUEUE = "queue";
		private static final String JOB_EXIT_STATUS = "exit_status";
		private static final String JOB_COMP_TIME = "comp_time";
		private static final String JOB_TOTAL_RUNTIME = "total_runtime";
		private static final String JOB_QUEUE_TIME = "qtime";
		private static final String JOB_MTIME = "mtime";
		private static final String JOB_ERROR_PATH = "Error_Path";
		private static final String JOB_OUTPUT_PATH = "Output_Path";
		private static final String JOB_RESOURCES_USED = "resources_used";
		private static final String JOB_RESOURCES_USED_CPUT = "cput";
		private static final String JOB_RESOURCES_USED_ENERGY_USED = "energy_used";
		private static final String JOB_RESOURCES_USED_MEM = "mem";
		private static final String JOB_RESOURCES_USED_VMEM = "vmem";
		private static final String JOB_RESOURCES_USED_WALLTIME = "walltime";

		private PBSData pbsData;
		private String elementValue;

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			elementValue = new String(ch, start, length);
		}

		@Override
		public void startDocument() throws SAXException {
			pbsData = new PBSData();
		}

		@Override
		public void startElement(String uri, String lName, String qName, Attributes attr) throws SAXException {
			switch (qName) {
				case QUEUE_DATA:
					pbsData.jobs = new ArrayList<>();
					break;
				case JOB:
					PBSJob job = new PBSJob();
					pbsData.jobs.add(job);
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			switch (qName) {
				case JOB_ID:
					latestPBSJob().id = elementValue;
					break;
				case JOB_NAME:
					latestPBSJob().name = elementValue;
					break;
				case JOB_STATE:
					latestPBSJob().state = elementValue;
					break;
				case JOB_START_TIME:
					latestPBSJob().startTime = Long.parseLong(elementValue);
					break;
				case JOB_END_TIME:
					latestPBSJob().endTime = Long.parseLong(elementValue);
					break;
				case JOB_QUEUE:
					latestPBSJob().queue = elementValue;
					break;
				case JOB_EXIT_STATUS:
					latestPBSJob().exitCode = elementValue;
					break;
				case JOB_COMP_TIME:
					latestPBSJob().ctime = Long.parseLong(elementValue);
					break;
				case JOB_TOTAL_RUNTIME:
					latestPBSJob().name = elementValue;
					break;
				case JOB_QUEUE_TIME:
					latestPBSJob().queueTime = Long.parseLong(elementValue);
					break;
				case JOB_MTIME:
					latestPBSJob().mtime = Long.parseLong(elementValue);
					break;
				case JOB_ERROR_PATH:
					latestPBSJob().errorPath = elementValue;
					break;
				case JOB_OUTPUT_PATH:
					latestPBSJob().outputPath = elementValue;
					break;
			}
		}

		private PBSJob latestPBSJob() {
			List<PBSJob> jobList = pbsData.jobs;
			int latestArticleIndex = jobList.size() - 1;
			return jobList.get(latestArticleIndex);
		}

		public PBSData getJobData() {
			return pbsData;
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
	 * The job status query to PBS was of the form {@code "qstat -a | grep ^<job_id>}. That means the response should
	 * come back in a tabbed column with the following fields
	 *
	 * <pre>
	 * JobID                         Username        Queue           Jobname         SessID   NDS  TSK   ReqMemory ReqTime  S ElapsedTime
	 * </pre>
	 *
	 * A sample matching status like will look like:
	 * <pre>
	 * 0.33cf4118fced          testuser    debug    torque.submit       --    --     --     --   00:01:00 Q       --
	 * </pre>
	 *
	 * It is sufficient to split the string and look at the value in column 10.
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
		else if (schedulerResponseText.toLowerCase().contains("unknown")
				|| schedulerResponseText.toLowerCase().contains("error")
				|| schedulerResponseText.toLowerCase().contains("not ")) {
			throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler: " + schedulerResponseText);
		} else {
			List<String> lines = Arrays.asList(StringUtils.stripToEmpty(schedulerResponseText).split("[\\r\\n]+"));
			// iterate over each line, finding the one starting with the job id
			for (String line: lines) {
				// PBS status lines start with the job id
				if (line.startsWith(remoteJobId)) {
					try {
						// parse the line with the status info in it
						return parseLine(remoteJobId, line);
					} catch (RemoteJobMonitorResponseParsingException e) {
						throw new RemoteJobMonitorResponseParsingException(e.getMessage() + ": " + schedulerResponseText, e);
					}
				}
			}
			throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler");
		}
	}

	/**
	 * Parses a single status line from the remote server response. This should be the line containing the actual
	 * status info for the PBS job, trimmed of any headers, debug info, etc.
	 *
	 * @param remoteJobId the remote job id to parse from the response
	 * @param statusLine the qstat job status line
	 * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
	 * @throws RemoteJobMonitorResponseParsingException if unable to parse the job status
	 */
	protected JobStatusResponse<PBSJobStatus> parseLine(String remoteJobId, String statusLine) throws RemoteJobMonitorResponseParsingException {

		List<String> tokens = Arrays.asList(StringUtils.split(statusLine));

		// output from {@code qstat -a <job_id> } should be similar to
		// <pre>Job ID                         Username        Queue           Jobname         SessID   NDS  TSK   Memory Time  S Time</pre>
		if (tokens.size() == 11) {
			try {
				PBSJobStatus remoteJobStatus = PBSJobStatus.valueOfCode(tokens.get(9));
				String exitCode = "0";

				return new JobStatusResponse<>(remoteJobId, remoteJobStatus, exitCode);
			}
			catch (Throwable e) {
				throw new RemoteJobMonitorResponseParsingException("Unexpected fields in the response from the scheduler");
			}
		}
		// in case the response is customized, we start checking from the last column inward,
		// looking for single character values, which is what the status would be.
		else {
			for (int i=tokens.size()-1; i >= 0; i--) {
				if (tokens.get(i).matches("^([a-zA-Z]{1})$")) {
					try {
						PBSJobStatus remoteJobStatus = PBSJobStatus.valueOfCode(tokens.get(i));
						String exitCode = "0";

						return new JobStatusResponse<>(remoteJobId, remoteJobStatus, exitCode);
					}
					catch (Throwable e) {
						throw new RemoteJobMonitorResponseParsingException("Unexpected fields in the response from the scheduler");
					}
				}
			}
		}
		throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler");
	}
}
