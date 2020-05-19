package org.iplantc.service.jobs.managers.monitors.parsers;

import org.apache.commons.collections.CollectionUtils;
import org.iplantc.service.apps.exceptions.SoftwareException;
import org.iplantc.service.apps.exceptions.UnknownSoftwareException;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.ExecutionType;
import org.iplantc.service.systems.model.enumerations.SchedulerType;
import org.iplantc.service.systems.util.ServiceUtils;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.iplantc.service.systems.model.enumerations.SchedulerType.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@Test(groups={"unit"})
public class JobMonitorResponseParserFactoryTest {

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

        } catch (SystemArgumentException e) {
            Assert.fail("Test system id should not throw exception", e);
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

    /**
     * Ceates a mock {@link Job} for testing using the given system and software.
     * @param executionSystem the execution system to assign to the job
     * @param software the software to assign to the job
     * @return a mock job instance for use in testing
     */
    private Job createMockJob(ExecutionSystem executionSystem, Software software) {
        Job job = mock(Job.class);
        when(job.getSoftwareName()).thenReturn(software.getUniqueName());
        when(job.getExecutionType()).thenReturn(software.getExecutionType());
        when(job.getSchedulerType()).thenReturn(executionSystem.getScheduler());
        when(job.getSystem()).thenReturn(executionSystem.getSystemId());
        when(job.getUuid()).thenReturn(UUID.randomUUID().toString());
        when(job.getId()).thenReturn(1L);

        return job;
    }

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

        return job;
    }

    /**
     * Tests that job execution system is returned
     */
    @Test
    public void testGetExecutionSystemReturnsJobSystem() throws Exception {
        ExecutionSystem executionSystem = createMockExecutionSystem(ExecutionType.HPC, LSF);
        Software software = createMockSoftware(executionSystem, ExecutionType.CLI);
        Job job = createMockJob(executionSystem, software);

        // mock the job manager so we can intercept the actual db calls
        JobManager jobManager = mock(JobManager.class);
        when(jobManager.getJobExecutionSystem(eq(job))).thenReturn(executionSystem);

        JobMonitorResponseParserFactory factory = mock(JobMonitorResponseParserFactory.class);
        when(factory.getJobManager()).thenReturn(jobManager);
        when(factory.getExecutionSystem(eq(job))).thenCallRealMethod();

        ExecutionSystem testExecutionSystem = factory.getExecutionSystem(job);

        // verify it was called once
        verify(jobManager, times(1)).getJobExecutionSystem(job);
        verify(factory, times(1)).getExecutionSystem(job);

        // verify the response was what we thought it would be
        Assert.assertEquals(testExecutionSystem, executionSystem,
                "Test job execution system should be returned from factory call to getExecutionSystem");
    }

//    /**
//     * Tests that job execution system is returned
//     */
//    @Test(expectedExceptions = SystemUnavailableException.class)
//    public void testGetExecutionSystemThrowsSystemUnavailableException() throws SystemUnavailableException {
//        ExecutionSystem executionSystem = createMockExecutionSystem(ExecutionType.HPC, LSF);
//        Software software = createMockSoftware(executionSystem, ExecutionType.CLI);
//        Job job = createMockJob(executionSystem, software);
//
//        // mock the job manager so we can intercept the actual db calls
//        JobManager jobManager = mock(JobManager.class);
//        when(jobManager.getJobExecutionSystem(eq(job))).thenThrow(new SystemUnavailableException("No system found by that name"));
//
//        JobMonitorResponseParserFactory factory = mock(JobMonitorResponseParserFactory.class);
//        when(factory.getJobManager()).thenReturn(jobManager);
//        when(factory.getExecutionSystem(eq(job))).thenCallRealMethod();
//
//        ExecutionSystem testExecutionSystem = factory.getExecutionSystem(job);
//
//        // verify it was called once
//        verify(jobManager, times(1)).getJobExecutionSystem(job);
//        verify(factory, times(1)).getExecutionSystem(job);
//
//        Assert.fail("Exception should be thrown when system is unavailable.");
//    }
//
//    /**
//     * Tests that missing software exception is thrown from getSoftware method
//     */
//    @Test
//    public void testGetSoftware() throws UnknownSoftwareException {
//        ExecutionSystem executionSystem = createMockExecutionSystem(ExecutionType.HPC, LSF);
//        Software software = createMockSoftware(executionSystem, ExecutionType.CLI);
//        Job job = createMockJob(executionSystem, software);
//
//        // mock the job manager so we can intercept the actual db calls
//        JobManager jobManager = mock(JobManager.class);
//        when(jobManager.getJobSoftware(eq(job))).thenReturn(software);
//
//        JobMonitorResponseParserFactory factory = mock(JobMonitorResponseParserFactory.class);
//        when(factory.getJobManager()).thenReturn(jobManager);
//        when(factory.getSoftware(eq(job))).thenCallRealMethod();
//
//        Software testSoftware = factory.getSoftware(job);
//
//        // verify it was called once
//        verify(jobManager, times(1)).getJobSoftware(job);
//        verify(factory, times(1)).getSoftware(job);
//
//        // verify the response was what we thought it would be
//        Assert.assertEquals(testSoftware, software,
//                "Test job software should be returned from factory call to getSoftware");
//    }
//
//    /**
//     * Tests that missing software exception is thrown from getSoftware method
//     */
//    @Test(expectedExceptions = SoftwareException.class)
//    public void testGetSoftwareThrowsUnknownSoftwareException() throws SoftwareException {
//        ExecutionSystem executionSystem = createMockExecutionSystem(ExecutionType.HPC, LSF);
//        Software software = createMockSoftware(executionSystem, ExecutionType.CLI);
//        Job job = createMockJob(executionSystem, software);
//
//        // mock the job manager so we can intercept the actual db calls
//        JobManager jobManager = mock(JobManager.class);
//        when(jobManager.getJobSoftware(eq(job))).thenThrow(new SoftwareException("No software found by that name"));
//
//        JobMonitorResponseParserFactory factory = mock(JobMonitorResponseParserFactory.class);
//        when(factory.getJobManager()).thenReturn(jobManager);
//        when(factory.getSoftware(eq(job))).thenCallRealMethod();
//
//        Software testSoftware = factory.getSoftware(job);
//
//        // verify it was called once
//        verify(jobManager, times(1)).getJobSoftware(job);
//        verify(factory, times(1)).getSoftware(job);
//
//        Assert.fail("Exception should be thrown when system is unavailable.");
//    }

    /**
     * Generates happy path test cases where {@link ExecutionSystem} and {@link Software} both have the same execution
     * type. All supported scheduler types are tried for the given {@link SchedulerType} to get coverage.
     * @return test cases
     */
    @DataProvider
    protected Object[][] testgetInstanceExecutionTypeProvider() {
        List<Object[]> testCases = new ArrayList<Object[]>();
        for (ExecutionType executionyTpe: ExecutionType.values()) {
            for (SchedulerType schedType : executionyTpe.getCompatibleSchedulerTypes()) {
                testCases.add(new Object[]{executionyTpe, schedType});
            }
        }

        return testCases.toArray(new Object[][]{});
    }

    @Test(dataProvider = "testgetInstanceExecutionTypeProvider")
    public void testGetInstanceHPCExecutionType(ExecutionType executionyTpe, SchedulerType systemSchedulerType) {
//        ExecutionSystem executionSystem = createMockExecutionSystem(executionyTpe, systemSchedulerType);
//        Software software = createMockSoftware(executionSystem, executionyTpe);
//        Job job = createMockJob(executionSystem, software);
        Job job = createMockJob(executionyTpe, systemSchedulerType);

        JobMonitorResponseParserFactory factory = mock(JobMonitorResponseParserFactory.class);
        try {
//            when(factory.getExecutionSystem(eq(job))).thenReturn(executionSystem);
//            when(factory.getSoftware(eq(job))).thenReturn(software);
            when(factory.getInstance(eq(job))).thenCallRealMethod();

            JobStatusResponseParser parser = factory.getInstance(job);
            Assert.assertTrue(parser.getSupportedSchedulerType().contains(systemSchedulerType),
                    executionyTpe.name() + " execution type and " + systemSchedulerType.name() +
                            " should return a parser that supports the scheduler type. " +
                            systemSchedulerType.name() + " not in " +
                            ServiceUtils.explode(",", parser.getSupportedSchedulerType()) +
                            " returned from " + parser.getClass().getName());
        } catch (SystemUnknownException | SystemUnavailableException e) {
            Assert.fail("Mocked out call to get #getExecutionSystem should not fail.", e);
        }
    }

//    /**
//     * Generates test cases to show that a {@link Software#getExecutionType()} will take priority over a
//     * {@link ExecutionSystem#getExecutionType()} when determining the factory used to check job status.
//     *
//     * @return test cases
//     */
//    @DataProvider
//    private Object[][] testGetInstanceSoftwareExecutionTypeOverridesSystemExecutionTypeProvider() {
//        List<Object[]> testCases = new ArrayList<Object[]>();
//        for (ExecutionType softwareExecutionType: ExecutionType.values()) {
//            for (ExecutionType systemExecutionType : softwareExecutionType.getCompatibleExecutionTypes()) {
//                // disallow combinations of software and system execution types that overlap as those are tested
//                // by the happy path tests
//                if (softwareExecutionType == systemExecutionType) continue;
//
//                for (SchedulerType schedType : systemExecutionType.getCompatibleSchedulerTypes()) {
//                    // only allow compatible scheduler types with the software and system execution types
//                    if (systemExecutionType.getCompatibleSchedulerTypes().contains(schedType))
//                        testCases.add(new Object[]{softwareExecutionType, systemExecutionType, schedType});
//                }
//            }
//        }
//
//        return testCases.toArray(new Object[][]{});
//    }
//
//    @Test(dataProvider = "testGetInstanceSoftwareExecutionTypeOverridesSystemExecutionTypeProvider")
//    public void testGetInstanceSoftwareExecutionTypeOverridesSystemExecutionType(ExecutionType softwareExecutionType, ExecutionType systemExecutionType, SchedulerType systemSchedulerType) {
//        ExecutionSystem executionSystem = createMockExecutionSystem(systemExecutionType, systemSchedulerType);
//        Software software = createMockSoftware(executionSystem, softwareExecutionType);
//        Job job = createMockJob(executionSystem, software);
//
//        JobMonitorResponseParserFactory factory = mock(JobMonitorResponseParserFactory.class);
//        try {
//            when(factory.getExecutionSystem(eq(job))).thenReturn(executionSystem);
//            when(factory.getSoftware(eq(job))).thenReturn(software);
//            when(factory.getInstance(eq(job))).thenCallRealMethod();
//
//            JobStatusResponseParser parser = factory.getInstance(job);
//            Assert.assertTrue(parser.getSupportedSchedulerType().contains(systemSchedulerType),
//                    "Parser returned from factory should support the same scheduler type supported by the " +
//                            "software execution type " + softwareExecutionType + ". " + systemSchedulerType.name() + " not in " +
//                            ServiceUtils.explode(",", parser.getSupportedSchedulerType()) + " for " +
//                    parser.getClass().getName());
//        } catch (SystemUnknownException | SystemUnavailableException e) {
//            Assert.fail("Mocked out call to get #getExecutionSystem should not fail.", e);
//        }
//    }
//
//    @Test(dataProvider = "testGetInstanceSoftwareExecutionTypeOverridesSystemExecutionTypeProvider")
//    public void testGetInstanceNullSoftwareExecutionTypeDefaultsToExecutionSystemType(ExecutionType softwareExecutionType, ExecutionType systemExecutionType, SchedulerType systemSchedulerType) {
//        ExecutionSystem executionSystem = createMockExecutionSystem(systemExecutionType, systemSchedulerType);
//        Software software = createMockSoftware(executionSystem, softwareExecutionType);
//        Job job = createMockJob(executionSystem, software);
//
//        JobMonitorResponseParserFactory factory = mock(JobMonitorResponseParserFactory.class);
//        try {
//            when(factory.getExecutionSystem(eq(job))).thenReturn(executionSystem);
//            // return null software
//            when(factory.getSoftware(eq(job))).thenReturn(null);
//            when(factory.getInstance(eq(job))).thenCallRealMethod();
//
//            JobStatusResponseParser parser = factory.getInstance(job);
//            Assert.assertTrue(parser.getSupportedSchedulerType().contains(systemSchedulerType),
//                    "Null software should still result in a valid parser being chosen based on the execution type " +
//                            "of the exeuction system. " + systemSchedulerType.name() + " not in "
//                            + ServiceUtils.explode(",", parser.getSupportedSchedulerType()));
//        } catch (SystemUnknownException | SystemUnavailableException e) {
//            Assert.fail("Mocked out call to get #getExecutionSystem should not fail.", e);
//        }
//    }
//
//    @DataProvider
//    private Object[][] testGetInstanceSoftwareExecutionTypePreferredProvider() {
//        List<Object[]> testCases = new ArrayList<Object[]>();
//        for (SchedulerType schedType: SchedulerType.values()) {
//            testCases.add(new Object[]{schedType});
//        }
//
//        return testCases.toArray(new Object[][]{});
//    }
//
//    @Test(dataProvider = "testGetInstanceSoftwareExecutionTypePreferredProvider")
//    public void testGetInstanceSoftwareExecutionTypePreferred(SchedulerType schedulerType) {
//        ExecutionSystem executionSystem = createMockExecutionSystem(ExecutionType.HPC, schedulerType);
//        Software software = createMockSoftware(executionSystem, ExecutionType.CLI);
//        Job job = createMockJob(executionSystem, software);
//
//        JobMonitorResponseParserFactory factory = mock(JobMonitorResponseParserFactory.class);
//        try {
//            when(factory.getExecutionSystem(eq(job))).thenReturn(executionSystem);
//            when(factory.getSoftware(eq(job))).thenReturn(software);
//            when(factory.getInstance(eq(job))).thenCallRealMethod();
//
//            JobStatusResponseParser parser = factory.getInstance(job);
//            Assert.assertEquals(parser.getClass(), ForkJobStatusResponseParser.class,
//                    "Software execution type CLI should override the execution system scheduler type, but " +
//                    schedulerType.name() + " scheduler type was selected instead.");
//        } catch (SystemUnknownException | SystemUnavailableException e) {
//            Assert.fail("Mocked out call to get #getExecutionSystem should not fail.", e);
//        }
//    }


}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme