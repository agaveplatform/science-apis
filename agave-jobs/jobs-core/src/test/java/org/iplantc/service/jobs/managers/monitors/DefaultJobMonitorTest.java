package org.iplantc.service.jobs.managers.monitors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorEmptyResponseException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitorResponseParsingException;
import org.iplantc.service.jobs.exceptions.RemoteJobMonitoringException;
import org.iplantc.service.jobs.managers.JobStatusResponse;
import org.iplantc.service.jobs.managers.monitors.parsers.*;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.LoginConfig;
import org.iplantc.service.systems.model.enumerations.ExecutionType;
import org.iplantc.service.systems.model.enumerations.LoginProtocolType;
import org.iplantc.service.systems.model.enumerations.SchedulerType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.AdditionalMatchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.fail;

@Test(groups={"unit"})
public class DefaultJobMonitorTest  {

    private final RemoteSchedulerJobStatus remoteSchedulerJobStatus = PBSJobStatus.UNKNOWN;

    protected final SchedulerType schedulerType = SchedulerType.FORK;
    protected final ObjectMapper mapper = new ObjectMapper();

//    public DefaultJobMonitorTest(SchedulerType schedulerType, RemoteSchedulerJobStatus remoteSchedulerJobStatus) {
//        this.schedulerType = schedulerType;
//        this.remoteSchedulerJobStatus = remoteSchedulerJobStatus;
//    }

    /**
     * The scheduler type under test
     * @return
     */
    public SchedulerType getSchedulerType() {
        return schedulerType;
    }


    /**
     * Generator for test execution system.
     * @param executionType the execution type to assign
     * @param schedulerType the scheduler type to assign
     * @return an execution system to use in testing
     */
    private ExecutionSystem createMockExecutionSystem(ExecutionType executionType, SchedulerType schedulerType) {
        ExecutionSystem executionSystem = new ExecutionSystem();
        try {
            executionSystem.setScheduler(schedulerType);
            executionSystem.setExecutionType(executionType);
            executionSystem.setSystemId(UUID.randomUUID().toString());
            executionSystem.setLoginConfig(new LoginConfig("192.168.0.1",22, LoginProtocolType.SSH, null));

        } catch (SystemArgumentException e) {
            fail("Test system id should not throw exception", e);
        }

        return executionSystem;
    }

    /**
     * Generator for test {@link Software}
     * @param executionSystem the execution system to assign to the software
     * @param executionType the execution type to assign to software
     * @return a software instance to use in testing
     */
    private Software createMockSoftware(ExecutionSystem executionSystem, ExecutionType executionType) {
        Software software = new Software();
        software.setExecutionSystem(executionSystem);
        software.setExecutionType(executionType);
        software.setName(UUID.randomUUID().toString());
        software.setVersion("0.1");
        return software;
    }

    /**
     * Ceates a mock {@link Job} for testing using the given system and software. The job has mocked
     * system and software getters, but the actual software and system objects will not exist.
     * @return a mock job instance for use in testing
     */
    private Job createMockJob() {
//        ExecutionSystem executionSystem = createMockExecutionSystem(executionType, schedulerType);
//        Software software = createMockSoftware(executionSystem, executionType);

        Job job = mock(Job.class);
        when(job.getSoftwareName()).thenReturn(UUID.randomUUID().toString());
        when(job.getSystem()).thenReturn(UUID.randomUUID().toString());
        when(job.getUuid()).thenReturn(UUID.randomUUID().toString());
        when(job.getId()).thenReturn(1L);

        return job;
    }
//
//    /**
//     * Ceates a mock {@link Job} for testing using the given system and software.
//     * @param executionSystem the execution system to assign to the job
//     * @param software the software to assign to the job
//     * @return a mock job instance for use in testing
//     */
//    private Job createMockJob(ExecutionSystem executionSystem, Software software) {
//        Job job = mock(Job.class);
//        when(job.getSoftwareName()).thenReturn(software.getUniqueName());
//        when(job.getExecutionType()).thenReturn(software.getExecutionType());
//        when(job.getSchedulerType()).thenReturn(executionSystem.getScheduler());
//        when(job.getSystem()).thenReturn(executionSystem.getSystemId());
//        when(job.getUuid()).thenReturn(AUUID.randomUUID().toString());
//        when(job.getId()).thenReturn(1L);
//
//        return job;
//    }

    /**
     * Ceates a mock {@link Job} for testing using the given system and software.
     * @param executionType the execution type of the execution system to assign to the job
     * @param schedulerType the type of scheduler to which the job will be submitted
     * @return a mock job instance for use in testing
     */
    private Job createMockJob(ExecutionType executionType, SchedulerType schedulerType) {
        Job job = mock(Job.class);
        when(job.getSoftwareName()).thenReturn(UUID.randomUUID().toString() + "-0.1");
        when(job.getExecutionType()).thenReturn(executionType);
        when(job.getSchedulerType()).thenReturn(schedulerType);
        when(job.getSystem()).thenReturn(UUID.randomUUID().toString());
        when(job.getUuid()).thenReturn(new AgaveUUID(UUIDType.JOB).toString());
        when(job.getId()).thenReturn(1L);
        when(job.getLocalJobId()).thenReturn("12345");
        when(job.isArchiveOutput()).thenReturn(false);

        return job;
    }

    /**
     * Simple generator to work around the lack of generic support in our RemoteSchedulerJobStatus enum classes.
     *
     * @return list of "UNKNOWN" instances of every remote scheduler job status types
     */
    private List<RemoteSchedulerJobStatus> getAllRemoteSchedulerJobStatusTypes() {
        return List.of(CondorLogJobStatus.UNKNOWN,
                DefaultJobStatus.UNKNOWN,
                ForkJobStatus.UNKNOWN,
                LoadLevelerJobStatus.UNKNOWN,
                LSFJobStatus.UNKNOWN,
                PBSJobStatus.UNKNOWN,
                SGEJobStatus.UNKNOWN,
                SlurmJobStatus.UNKNOWN,
                TorqueJobStatus.UNKNOWN);
    }

    @DataProvider
    protected Object[][] testMonitorRemoteSchedulerStatusUpdatesJobProvider() {
        List<Object[]> testData = new ArrayList<>();

        for (ExecutionType executionType: List.of(ExecutionType.HPC, ExecutionType.CLI)) {
            for (RemoteSchedulerJobStatus remoteStatusType: getAllRemoteSchedulerJobStatusTypes()) {
                for (Object status : remoteStatusType.getRunningStatuses()) {
                    testData.add(new Object[]{executionType, status, JobStatusType.RUNNING});
                }
                for (Object status : remoteStatusType.getFailedStatuses()) {
                    testData.add(new Object[]{executionType, status, JobStatusType.FAILED});
                }
                for (Object status : remoteStatusType.getUnrecoverableStatuses()) {
                    testData.add(new Object[]{executionType, status, JobStatusType.CLEANING_UP});
                }
                for (Object status : remoteStatusType.getPausedStatuses()) {
                    testData.add(new Object[]{executionType, status, JobStatusType.PAUSED});
                }
            }
        }

        return testData.toArray(new Object[][]{});
    }



    /**
     * Tests the agave job status is updated when the remote job response status indicates the job has changed state.
     * This test only checks for state changes for remote scheduler job statuses that map directly to agave job statues.
     *
     * @param executionType the type of job to test
     * @param status the remote job status that should be returned from the remote scheduler
     * @param expectedJobStatus the resulting status that should be had after update.
     */
    @Test(dataProvider = "testMonitorRemoteSchedulerStatusUpdatesJobProvider")
    public void testMonitorRemoteSchedulerStatusUpdatesQueuedJob(ExecutionType executionType, RemoteSchedulerJobStatus<?> status, JobStatusType expectedJobStatus) {
        _genericTestRemoteJobStatusUpdates(executionType, JobStatusType.QUEUED, status, expectedJobStatus);
    }

    /**
     * Tests the agave job status is updated when the remote job response status indicates the job has changed state.
     * This test only checks for state changes for remote scheduler job statuses that map directly to agave job statues.
     *
     * @param executionType the type of job to test
     * @param status the remote job status that should be returned from the remote scheduler
     * @param expectedJobStatus the resulting status that should be had after update.
     */
    @Test(dataProvider = "testMonitorRemoteSchedulerStatusUpdatesJobProvider")
    public void testMonitorRemoteSchedulerStatusUpdatesRunningJob(ExecutionType executionType, RemoteSchedulerJobStatus<?> status, JobStatusType expectedJobStatus) {
        _genericTestRemoteJobStatusUpdates(executionType, JobStatusType.RUNNING, status, expectedJobStatus);
    }

    /**
     * Tests the agave job status is updated when the remote job response status indicates the job has changed state.
     * This test only checks for state changes for remote scheduler job statuses that map directly to agave job statues.
     *
     * @param executionType the type of job to test
     * @param status the remote job status that should be returned from the remote scheduler
     * @param expectedJobStatus the resulting status that should be had after update.
     */
    @Test(dataProvider = "testMonitorRemoteSchedulerStatusUpdatesJobProvider")
    public void testMonitorRemoteSchedulerStatusUpdatesausedJob(ExecutionType executionType, RemoteSchedulerJobStatus<?> status, JobStatusType expectedJobStatus) {
        _genericTestRemoteJobStatusUpdates(executionType, JobStatusType.PAUSED, status, expectedJobStatus);
    }







    @DataProvider
    protected Object[][] testMonitorLeavesJobStatusUnchangedIfRemoteScheduleStatusUnchangedProvider() {
        List<Object[]> testData = new ArrayList<>();

        for (ExecutionType executionType: List.of(ExecutionType.HPC, ExecutionType.CLI)) {
            for (RemoteSchedulerJobStatus remoteStatusType: getAllRemoteSchedulerJobStatusTypes()) {
                for (Object status : remoteStatusType.getQueuedStatuses()) {
                    testData.add(new Object[]{executionType, status, JobStatusType.QUEUED});
                }

                for (Object status : remoteStatusType.getRunningStatuses()) {
                    testData.add(new Object[]{executionType, status, JobStatusType.RUNNING});
                }

                for (Object status : remoteStatusType.getPausedStatuses()) {
                    testData.add(new Object[]{executionType, status, JobStatusType.PAUSED});
                }

                for (Object status : remoteStatusType.getFailedStatuses()) {
                    testData.add(new Object[]{executionType, status, JobStatusType.FAILED});
                }
            }
        }

        return testData.toArray(new Object[][]{});
    }

    /**
     * Tests the job status is left unchanged and the message is unchanged if the remote scheduler response indicates
     * the remote job is in the same state as the agave job status.
     *
     * @param executionType the type of job to test
     * @param status the remote job status that should be returned from the remote scheduler
     * @param expectedJobStatus the resulting status that should be had after update.
     */
    @Test(dataProvider = "testMonitorLeavesJobStatusUnchangedIfRemoteScheduleStatusUnchangedProvider")
    public void testMonitorLeavesJobStatusUnchangedIfRemoteScheduleStatusUnchanged(ExecutionType executionType, RemoteSchedulerJobStatus<?> status, JobStatusType expectedJobStatus) {
        _genericTestRemoteJobStatusUpdates(executionType, expectedJobStatus, status, expectedJobStatus);
    }

    @DataProvider
    protected Object[][] testMonitorLeavesJobStatusUnchangedUpdatesMessageIfRemoteScheduleStatusUnknownProvider() {
        List<Object[]> testData = new ArrayList<>();

        for (ExecutionType executionType: List.of(ExecutionType.HPC, ExecutionType.CLI)) {
            for (RemoteSchedulerJobStatus remoteStatusType: getAllRemoteSchedulerJobStatusTypes()) {
                for (Object status : remoteStatusType.getUnknownStatuses()) {
                    testData.add(new Object[]{executionType, status, JobStatusType.QUEUED});
                }
            }
        }

        return testData.toArray(new Object[][]{});
    }

    /**
     * Tests the job status is left unchanged, but the error message updated on a job when the remote status returned is
     * "UNKNOWN".
     * @param executionType the type of job to test
     * @param status the remote job status that should be returned from the remote scheduler
     * @param expectedJobStatus the resulting status that should be had after update.
     * @throws Exception if anything goes wrong, just let it fly
     */
    @Test(dataProvider="testMonitorLeavesJobStatusUnchangedUpdatesMessageIfRemoteScheduleStatusUnknownProvider")
    public void testMonitorLeavesJobStatusUnchangedUpdatesMessageIfRemoteScheduleStatusUnknown(ExecutionType executionType, RemoteSchedulerJobStatus<?> status, JobStatusType expectedJobStatus) throws Exception {
        JobStatusType initialJobStatus = JobStatusType.QUEUED;
        DefaultJobMonitor monitor = _genericMonitorMockTestHandler(executionType, initialJobStatus, status, expectedJobStatus);

        String initalJobErrorMessage = "Mock job " + monitor.getJob().getUuid() + " " + initialJobStatus.name();

        // job should be "touched" with the original status and error message when the method is first called
        verify(monitor, times(1)).updateJobStatus(eq(initialJobStatus), eq(initalJobErrorMessage));

        // check that job is updated with the same status, but updated error message
        verify(monitor).updateJobStatus(eq(expectedJobStatus), not(eq(initalJobErrorMessage)));
    }

    /**
     * Sets up a mock job and tests that the call to the {@link DefaultJobMonitor#monitor()} method updates the status to the {@code expectedJobStatus}.
     * @param executionType the type of job to test
     * @param initialJobStatus the initial job status of the job before running the monitor.
     * @param status the remote job status that should be returned from the remote scheduler
     * @param expectedJobStatus the resulting status that should be had after update.
     */
    protected void _genericTestRemoteJobStatusUpdates(ExecutionType executionType, JobStatusType initialJobStatus, RemoteSchedulerJobStatus<?> status, JobStatusType expectedJobStatus) {

        try {

//            JobStatusType initialJobStatus = JobStatusType.QUEUED;

            DefaultJobMonitor monitor = _genericMonitorMockTestHandler(executionType, initialJobStatus, status, expectedJobStatus);

            String initalJobErrorMessage = "Mock job " + monitor.getJob().getUuid() + " " + initialJobStatus.name();

            if (initialJobStatus == expectedJobStatus && status.getMappedJobStatusType() != null) {
                // job should be "touched" with the original status and error message when the method is first called
                verify(monitor, times(2)).updateJobStatus(eq(initialJobStatus), eq(initalJobErrorMessage));
            } else {
                // job should be "touched" with the original status and error message when the method is first called
                verify(monitor, times(1)).updateJobStatus(eq(initialJobStatus), eq(initalJobErrorMessage));

                // check that update status is called with the expected status for this test
                verify(monitor).updateJobStatus(eq(expectedJobStatus), not(eq(initalJobErrorMessage)));
            }

        } catch (SystemUnavailableException e) {
            fail("Mock execution system should not be unavailable", e);
        } catch (RemoteJobMonitorEmptyResponseException e) {
            fail("Mock response should be " + status.toString(), e);
        } catch (RemoteJobMonitorResponseParsingException e) {
            fail("Mock response should not throw a parsing exception", e);
        } catch (RemoteJobMonitoringException e) {
            fail("RemoteJobMonitoringException should not be thrown in mock response. No remote call should be made.", e);
        } catch (ClosedByInterruptException e) {
            fail("No interrupt should occur during test", e);
        } catch (JobException e) {
            fail("Handling an active response of " + status.toString() + " should not throw an exception.", e);
        }
    }

    /**
     * Sets up a mock job and tests that the call to the {@link DefaultJobMonitor#monitor()} method updates the status to the {@code expectedJobStatus}.
     * @param executionType the type of job to test
     * @param status the remote job status that should be returned from the remote scheduler
     * @param expectedJobStatus the resulting status that should be had after update.
     */
    private DefaultJobMonitor _genericMonitorMockTestHandler(ExecutionType executionType, JobStatusType initialJobStatus, RemoteSchedulerJobStatus<?> status, JobStatusType expectedJobStatus) throws SystemUnavailableException, JobException, RemoteJobMonitorEmptyResponseException, RemoteJobMonitorResponseParsingException, RemoteJobMonitoringException, ClosedByInterruptException {

//        try {
            ExecutionSystem executionSystem = createMockExecutionSystem(executionType, getSchedulerType());
//            Software software = createMockSoftware(executionSystem, executionType);

            // create the mock job
            Job job = createMockJob(executionType, getSchedulerType());
            when(job.getStatus()).thenReturn(initialJobStatus);
            String initalJobErrorMessage = "Mock job " + job.getUuid() + " " + initialJobStatus.name();
            when(job.getErrorMessage()).thenReturn(initalJobErrorMessage);

            // create a response so we can verify behavior of the monitor for the given response
            JobStatusResponse jobStatusResponse = new JobStatusResponse<>(job.getLocalJobId(), status, "-1");

            // mock the class under test so we can isolate behavior to the monitor method
            DefaultJobMonitor monitor = mock(DefaultJobMonitor.class);

            // return our mock execution when called so we don't get a false system unavailable exception
            when(monitor.getExecutionSystem()).thenReturn(executionSystem);

            // return our mock job when called
            when(monitor.getJob()).thenReturn(job);

            // return our mock job status when calling out for the remote job status response
            when(monitor.getJobStatusResponse(any(), any())).thenReturn(jobStatusResponse);

            // we want to test the behavior, of the run method, not the updateJobStatus method, which gets tested separately
            doNothing().when(monitor).updateJobStatus(any(JobStatusType.class), anyString());

            // pass through the invocation of the method under test
            when(monitor.monitor()).thenCallRealMethod();

            // The job should complete immediately, so we can run the monitor and check for the expected completed status.
            monitor.monitor();

            return monitor;

//        } catch (SystemUnavailableException e) {
//            fail("Mock execution system should not be unavailable", e);
//        } catch (RemoteJobMonitorEmptyResponseException e) {
//            fail("Mock response should be " + status.toString(), e);
//        } catch (RemoteJobMonitorResponseParsingException e) {
//            fail("Mock response should not throw a parsing exception", e);
//        } catch (RemoteJobMonitoringException e) {
//            fail("RemoteJobMonitoringException should not be thrown in mock response. No remote call should be made.", e);
//        } catch (ClosedByInterruptException e) {
//            fail("No interrupt should occur during test", e);
//        } catch (JobException e) {
//            fail("Handling an active response of " + status.toString() + " should not throw an exception.", e);
//        }
    }



//    @Test(dataProvider = "testMonitorRemoteSchedulerStatusUpdatesJobProvider")
//    public void testMonitorRemoteSchedulerStatusUpdatesJob(ExecutionType executionType, RemoteSchedulerJobStatus<?> status, JobStatusType expectedJobStatus) {
//
//        try {
//            ExecutionSystem executionSystem = createMockExecutionSystem(executionType, schedulerType);
////            Software software = createMockSoftware(executionSystem, executionType);
//
//            // create the mock job
//            Job job = createMockJob(executionType, schedulerType);
//            JobStatusType initialJobStatus = JobStatusType.QUEUED;
//            when(job.getStatus()).thenReturn(initialJobStatus);
//            String initalJobErrorMessage = "Mock job " + job.getUuid() + " " + initialJobStatus.name();
//            when(job.getErrorMessage()).thenReturn(initalJobErrorMessage);
//
//            // create a response so we can verify behavior of the monitor for the given response
//            JobStatusResponse jobStatusResponse = new JobStatusResponse<>(job.getLocalJobId(), status, "-1");
//
//            // mock the class under test so we can isolate behavior to the monitor method
//            DefaultJobMonitor monitor = mock(DefaultJobMonitor.class);
//
//            // return our mock execution when called so we don't get a false system unavailable exception
//            when(monitor.getExecutionSystem()).thenReturn(executionSystem);
//
//            // return our mock job when called
//            when(monitor.getJob()).thenReturn(job);
//
//            // return our mock job status when calling out for the remote job status response
//            when(monitor.getJobStatusResponse(any(), any())).thenReturn(jobStatusResponse);
//
//            // we want to test the behavior, of the run method, not the updateJobStatus method, which gets tested separately
//            doNothing().when(monitor).updateJobStatus(any(JobStatusType.class), anyString());
//
//            // pass through the invocation of the method under test
//            when(monitor.monitor()).thenCallRealMethod();
//
//            // The job should complete immediately, so we can run the monitor and check for the expected completed status.
//            monitor.monitor();
//
//            // job should be "touched" with the original status and error message when the method is first called
//            verify(monitor).updateJobStatus(eq(initialJobStatus), eq(initalJobErrorMessage));
//
//            // check that update status is called with the expected status for this test
//            verify(monitor).updateJobStatus(eq(expectedJobStatus), not(eq(initalJobErrorMessage)));
//
//        } catch (SystemUnavailableException e) {
//            fail("Mock execution system should not be unavailable", e);
//        } catch (RemoteJobMonitorEmptyResponseException e) {
//            fail("Mock response should be " + status.toString(), e);
//        } catch (RemoteJobMonitorResponseParsingException e) {
//            fail("Mock response should not throw a parsing exception", e);
//        } catch (RemoteJobMonitoringException e) {
//            fail("RemoteJobMonitoringException should not be thrown in mock response. No remote call should be made.", e);
//        } catch (ClosedByInterruptException e) {
//            fail("No interrupt should occur during test", e);
//        } catch (JobException e) {
//            fail("Handling an active response of " + status.toString() + " should not throw an exception.", e);
//        }
//    }

}
