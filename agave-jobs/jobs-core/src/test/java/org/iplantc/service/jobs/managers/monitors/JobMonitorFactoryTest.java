package org.iplantc.service.jobs.managers.monitors;

import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.model.enumerations.ExecutionType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobMonitorFactoryTest {

    @DataProvider
    public Object[][] testGetInstanceProvider() {
        return new Object[][]{
                { ExecutionType.CLI, DefaultJobMonitor.class },
                { ExecutionType.HPC, DefaultJobMonitor.class },
                { ExecutionType.CONDOR, CondorJobMonitor.class },
        };
    }

    @Test(dataProvider = "testGetInstanceProvider")
    public void testGetInstance(ExecutionType executionType, Class clazz) {
        Job job = mock(Job.class);
        when(job.getExecutionType()).thenReturn(executionType);

        JobMonitor result = new JobMonitorFactory().getInstance(job);
        Assert.assertEquals(result.getClass(), clazz);
    }
}