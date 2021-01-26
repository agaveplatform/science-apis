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

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class SGEJobStatusResponseParser implements JobStatusResponseParser {

    private static final Logger log = Logger.getLogger(SGEJobStatusResponseParser.class);

    class SGEJobInfo {
        private List<SGEJob> queueInfo;

        public List<SGEJob> getQueueInfo() {
            return queueInfo;
        }

        public void setQueueInfo(List<SGEJob> queueInfo) {
            this.queueInfo = queueInfo;
        }
    }

    class SGEJob {
        String state;
        String JB_job_number;
        Double JAT_prio;
        Double JAT_ntix;
        Double JB_nurg;
        Integer JB_urg;
        Integer JB_rrcontr;
        Integer JB_wtcontr;
        Integer JB_dlcontr;
        String JB_name;
        String JB_owner;

		public String getState() {
			return state;
		}

		public void setState(String state) {
			this.state = state;
		}

		public String getJB_job_number() {
			return JB_job_number;
		}

		public void setJB_job_number(String JB_job_number) {
			this.JB_job_number = JB_job_number;
		}

		public Double getJAT_prio() {
			return JAT_prio;
		}

		public void setJAT_prio(Double JAT_prio) {
			this.JAT_prio = JAT_prio;
		}

		public Double getJAT_ntix() {
			return JAT_ntix;
		}

		public void setJAT_ntix(Double JAT_ntix) {
			this.JAT_ntix = JAT_ntix;
		}

		public Double getJB_nurg() {
			return JB_nurg;
		}

		public void setJB_nurg(Double JB_nurg) {
			this.JB_nurg = JB_nurg;
		}

		public Integer getJB_urg() {
			return JB_urg;
		}

		public void setJB_urg(Integer JB_urg) {
			this.JB_urg = JB_urg;
		}

		public Integer getJB_rrcontr() {
			return JB_rrcontr;
		}

		public void setJB_rrcontr(Integer JB_rrcontr) {
			this.JB_rrcontr = JB_rrcontr;
		}

		public Integer getJB_wtcontr() {
			return JB_wtcontr;
		}

		public void setJB_wtcontr(Integer JB_wtcontr) {
			this.JB_wtcontr = JB_wtcontr;
		}

		public Integer getJB_dlcontr() {
			return JB_dlcontr;
		}

		public void setJB_dlcontr(Integer JB_dlcontr) {
			this.JB_dlcontr = JB_dlcontr;
		}

		public String getJB_name() {
			return JB_name;
		}

		public void setJB_name(String JB_name) {
			this.JB_name = JB_name;
		}

		public String getJB_owner() {
			return JB_owner;
		}

		public void setJB_owner(String JB_owner) {
			this.JB_owner = JB_owner;
		}
	}

    class SGEHandler extends DefaultHandler {
        private static final String QUEUE_INFO = "queue_info";
        private static final String JOB = "job_list";
        private static final String JOB_ID = "JB_job_number";
        private static final String JOB_NAME = "JB_name";
		private static final String JOB_STATE = "state";

        private SGEJobInfo sgeJobInfo;
        private String elementValue;

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            elementValue = new String(ch, start, length);
        }

        @Override
        public void startDocument() throws SAXException {
            sgeJobInfo = new SGEJobInfo();
        }

        @Override
        public void startElement(String uri, String lName, String qName, Attributes attr) throws SAXException {
            switch (qName) {
                case QUEUE_INFO:
					sgeJobInfo.queueInfo = new ArrayList<>();
                    break;
                case JOB:
                	SGEJob sgeJob = new SGEJob();
                	sgeJob.state = attr.getValue(JOB_STATE);
					sgeJobInfo.queueInfo.add(sgeJob);
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            switch (qName) {
                case JOB_ID:
					latestSGEJob().JB_job_number = elementValue;
                    break;
                case JOB_NAME:
					latestSGEJob().JB_name = elementValue;
                    break;
            }
        }

        private SGEJob latestSGEJob() {
            List<SGEJob> articleList = sgeJobInfo.queueInfo;
            int latestArticleIndex = articleList.size() - 1;
            return articleList.get(latestArticleIndex);
        }

        public SGEJobInfo getJobInfo() {
            return sgeJobInfo;
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
		return List.of(SchedulerType.SGE, SchedulerType.CUSTOM_GRIDENGINE);
	}

    /**
     * The job status query to SGE was of the form {@code "qstat -U <username>}. That means the response should
     * come back in a tabbed column with the following fields:
     *
     * <pre>
     * $ qstat -U testuser
     * job-ID  prior   name       user         state submit/start at     queue                          slots ja-task-ID
     * -----------------------------------------------------------------------------------------------------------------
     *       3 0.55500 test_job   testuser     r     05/08/2020 17:46:03 debug@21ec669ebd0c                 1
     *       4 0.55500 test_job   testuser     r     05/08/2020 18:46:03 debug@21ec669ebd0c                 1
     *       5 0.55500 test_job   testuser     qw    05/08/2020 17:46:02                                    1
     * ...
     * </pre>
     * <p>
     * But because job names can have spaces, we use the xml formatting option of the form
	 * {@code qstat -ext -urg -xml -U <username>}. That means we need to parse an XML structure to get the job info
     *
     * <pre>
     * $ qstat -ext -urg -xml -U testuser
     * <?xml version='1.0'?>
	 * <job_info  xmlns:xsd="...">
	 *   <queue_info>
     *     <job_list state="running">
	 *        <JB_job_number>7</JB_job_number>
	 *        <JAT_prio>0.55500</JAT_prio>
	 *        <JAT_ntix>0.50000</JAT_ntix>
	 *        <JB_nurg>0.50000</JB_nurg>
	 *        <JB_urg>1000</JB_urg>
	 *        <JB_rrcontr>1000</JB_rrcontr>
	 *        <JB_wtcontr>0</JB_wtcontr>
	 *        <JB_dlcontr>0</JB_dlcontr>
	 *        <JB_name>test_job</JB_name>
	 *        <JB_owner>testuser</JB_owner>
	 *		  <JB_project></JB_project>
	 *       <JB_department>defaultdepartment</JB_department>
	 *       <state>r</state>
	 *       <JAT_start_time>2020-05-08T21:46:18</JAT_start_time>
	 *       <cpu_usage>0.00000</cpu_usage>
	 *       <mem_usage>0.01719</mem_usage>
	 *       <io_usage>0.00000</io_usage>
	 *       <tickets>0</tickets>
	 *       <JB_override_tickets>0</JB_override_tickets>
	 *       <JB_jobshare>0</JB_jobshare>
	 *       <otickets>0</otickets>
	 *       <ftickets>0</ftickets>
	 *       <stickets>0</stickets>
	 *       <JAT_share>0.00000</JAT_share>
     * </pre>
     * <p>
     * It is sufficient to trim and split the string and look at the value in column 10.
     *
     * @param remoteJobId           the remote job id to parse from the response
     * @param schedulerResponseText the response text from the remote scheduler
     * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
     * @throws RemoteJobMonitorEmptyResponseException   if {@code schedulerResponseText} is blank
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
        } else if (schedulerResponseText.toLowerCase().contains("unknown")
                || schedulerResponseText.toLowerCase().contains("error")
                || schedulerResponseText.toLowerCase().contains("not ")) {
            throw new RemoteJobMonitorResponseParsingException("Unable to obtain job status in the response from the scheduler: " + schedulerResponseText);
        } else {
        	// otherwise, parse the XML response and return the resulting JobStatusResponse
			return parseXMLResponse(remoteJobId, schedulerResponseText);
        }
    }

    /**
     * Parses a single status line from the remote server response. This should be the line containing the actual
     * status info for the PBS job, trimmed of any headers, debug info, etc.
     *
     * @param remoteJobId the remote job id to parse from the response
     * @param statusLine  the qstat job status line
     * @return a {@link JobStatusResponse} containing remote status info about the job with {@code remoteJobId}
     * @throws RemoteJobMonitorResponseParsingException if unable to parse the job status
     */
    protected JobStatusResponse<SGEJobStatus> parseXMLResponse(String remoteJobId, String statusLine) throws RemoteJobMonitorResponseParsingException {

        try (InputStream in = new ByteArrayInputStream(statusLine.getBytes())) {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
			SGEHandler handler = new SGEHandler();
			saxParser.parse(in, handler);
			SGEJobInfo info = handler.sgeJobInfo;

			SGEJob sgeJob = null;
			for (SGEJob job: info.queueInfo) {
				if (job.getJB_job_number().equals(remoteJobId)) {
					sgeJob = job;
					break;
				}
			}

			if (sgeJob == null) {
				throw new RemoteJobMonitorResponseParsingException(
						"Unable to obtain job status in the response from the scheduler.");
			}

			String exitCode = "0";
			SGEJobStatus remoteJobStatus = SGEJobStatus.valueOfCode(sgeJob.state);

			return new JobStatusResponse<>(sgeJob.JB_job_number, remoteJobStatus, exitCode );

        } catch (SAXException|ParserConfigurationException|IOException e) {
			throw new RemoteJobMonitorResponseParsingException("Unexpected fields in the response from the scheduler: " + statusLine);
		}
    }
}
