package org.iplantc.service.jobs.managers.monitors;

import org.iplantc.service.apps.model.Software;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.managers.launchers.JobLauncherIT;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.systems.model.enumerations.ExecutionType;
import org.iplantc.service.systems.model.enumerations.SchedulerType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test(groups={"integration","broken"})
public class DefaultJobMonitorIT extends JobLauncherIT {

    public DefaultJobMonitorIT(ExecutionType executionType, SchedulerType schedulerType) {
        super(executionType, schedulerType);
    }

    @DataProvider
    protected Object[][] monitorJobProvider() {
        List<Object[]> testData = new ArrayList<>();
        for (Software app: List.of(software)) {
            testData.add(new Object[] { app, "Submission to " + app.getExecutionSystem().getSystemId() + " failed.", false });
        }

        return testData.toArray(new Object[][]{});
    }

    @Test(dataProvider = "monitorJobProvider")
    public void testRun(Software software, String message, boolean shouldThrowException)
            throws Exception {
        Job job = null;
        Job submittedJob;
        try {
            // created job will not be archived
            job = createAndPersistJob(software);

            // TODO: can we skip this step to speed things up? Just run a sleep command of varying lenghts to test
            //   we pick up the job status correctly?
            stageSoftwareDeploymentDirectory(software);

            // TODO: These aren't necessary for anything. we can skip this step for all but the actual staging queue
            //   tests.
            stageJobInputs(job);

            // This will submit the job to the remote queue. Recycled from launcher tests. The resulting callback will not
            // be received, so we can ensure the monitor testswill be the only thing checking the job status
            submittedJob = this.genericRemoteSubmissionTestCase(job, true, message, false);

            // Jobs will start instantly in our test environment, so we can run the monitor right away. To test multiple
            // stages in the remote job lifecycle, we can introduce delays and failures via job parameters to the test
            // app. This is the happy path.
            JobMonitor monitor = new JobMonitorFactory().getInstance(submittedJob, executionSystem);

            // The job should complete immediately, so we can run the monitor and check for the expected completed status.
            Job monitoredJob = monitor.monitor();

            Assert.assertEquals(monitoredJob.getStatus(), JobStatusType.FINISHED, "");
        }
        finally {
            try { JobDao.delete(job); } catch (Exception ignored) {}
        }
    }
}
