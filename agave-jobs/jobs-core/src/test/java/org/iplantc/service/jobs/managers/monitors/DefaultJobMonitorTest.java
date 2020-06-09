package org.iplantc.service.jobs.managers.monitors;

import org.apache.log4j.Logger;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.model.JSONTestDataUtil;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.mockito.Mock;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.mockito.Mockito.mock;

public class DefaultJobMonitorTest {
//    @Mock
//    Logger log;
////    @Mock
////    AtomicBoolean stopped;
////    @Mock
////    Job job;
////    @Mock
////    JobManager jobManager;
////
////    @InjectMocks
////    DefaultJobMonitor defaultJobMonitor;
////
////    @BeforeMethod
////    public void setUp() {
////        MockitoAnnotations.initMocks(this);
////    }
////
////    @Test
////    public void testMonitor() {
////        when(job.getUuid()).thenReturn("getUuidResponse");
////        when(job.getOwner()).thenReturn("getOwnerResponse");
////        when(job.getSystem()).thenReturn("getSystemResponse");
////        when(job.isArchiveOutput()).thenReturn(Boolean.TRUE);
////        when(job.getStatus()).thenReturn(JobStatusType.PENDING);
////        when(job.getStatusChecks()).thenReturn(0);
////        when(job.getLocalJobId()).thenReturn("getLocalJobIdResponse");
////        when(job.getErrorMessage()).thenReturn("getErrorMessageResponse");
////
////        Job result = defaultJobMonitor.monitor();
////        Assert.assertEquals(result, new Job());
////    }
////
////    @Test
////    public void testGetJobStatusResponse() {
////        when(job.getUuid()).thenReturn("getUuidResponse");
////
////        String result = defaultJobMonitor.getJobStatusResponse("command");
////        Assert.assertEquals(result, "replaceMeWithExpectedResult");
////    }
////
////    @Test
////    public void testIsStopped() {
////        boolean result = defaultJobMonitor.isStopped();
////        Assert.assertEquals(result, true);
////    }
////
////    @Test
////    public void testSetStopped() {
////        defaultJobMonitor.setStopped(true);
////    }
////
////    @Test
////    public void testCheckStopped() {
////        defaultJobMonitor.checkStopped();
////    }
////
////    @Test
////    public void testGetRemoteSubmissionClient() {
////        when(job.getInternalUsername()).thenReturn("getInternalUsernameResponse");
////        when(jobManager.getJobExecutionSystem(any())).thenReturn(new ExecutionSystem());
////
////        RemoteSubmissionClient result = defaultJobMonitor.getRemoteSubmissionClient();
////        Assert.assertEquals(result, new MaverickSSHSubmissionClient("host", 0, "username", "password", "proxyHost", 0, "publicKey", "privateKey"));
////    }
////
////    @Test
////    public void testGetExecutionSystem() {
////        when(jobManager.getJobExecutionSystem(any())).thenReturn(new ExecutionSystem());
////
////        ExecutionSystem result = defaultJobMonitor.getExecutionSystem();
////        Assert.assertEquals(result, new ExecutionSystem());
////    }
//
//    @DataProvider
//    public Object[][] testResolveStartupScriptMacrosReturnsEmptyStringWhenBlankProvider() {
//        return new Object[][]{
//                { "" },
//                { " " },
//                { "   " },
//                { null },
//        };
//    }
//
//    /**
//     * Ensures blank startup script values in the job's {@link ExecutionSystem} return null.
//     * @param pathToStartupScript the startupPath value to test
//     */
//    @Test(dataProvider = "testResolveStartupScriptMacrosReturnsEmptyStringWhenBlankProvider")
//    public void testResolveStartupScriptMacrosReturnsNullWhenBlank(String pathToStartupScript) throws SystemUnavailableException {
//        DefaultJobMonitor jobMonitor = mock(DefaultJobMonitor.class);
//        String result = jobMonitor.resolveStartupScriptMacros(pathToStartupScript);
//        Assert.assertNull(result, "Blank startup script value should return null");
//    }
//
//    @DataProvider
//    public Object[][] testResolveStartupScriptMacrosReturnsResolvedStringProvider() {
//        Object[][] testData = null;
//
//        try {
//            Job job = new Job();
//            job.setName(UUID.randomUUID().toString());
//            job.setSoftwareName(UUID.randomUUID().toString() + "-" + "0.1.0");
//            job.setSystem(UUID.randomUUID().toString());
//            job.setBatchQueue(UUID.randomUUID().toString());
//            job.setArchivePath(String.format("/%s/%s/%s", UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()));
//            job.setOwner(JSONTestDataUtil.TEST_OWNER);
//            job.setTenantId(UUID.randomUUID().toString());
//
//            testData = new Object[][] {
//                    {job, "~/.bashrc", "~/.bashrc"},
//                    {job, "${AGAVE_JOB_WORK_DIR}/startup.sh", String.format("%s/startup.sh", job.getWorkPath())},
//                    {job, "$HOME/${SYSTEM_ID}.sh", String.format("$HOME/%s.sh", job.getSystem())}
//            };
//
//        } catch (JobException e) {
//            Assert.fail("Failed to create test job", e);
//        }
//
//        return testData;
//    }
//
//    /**
//     * Ensures templated startup script values in the job's {@link ExecutionSystem} resolve the macros in the template.
//     * @param job the job to use to resolve the template macros
//     * @param pathToStartupScript the startupPath value to test
//     * @param expectedValue the
//     */
//    @Test(dataProvider = "testResolveStartupScriptMacrosReturnsResolvedStringProvider")
//    public void testResolveStartupScriptMacrosReturnsResolvedString(Job job, String pathToStartupScript, String expectedValue) throws SystemUnavailableException {
//        DefaultJobMonitor jobMonitor = mock(DefaultJobMonitor.class);
//        String result = jobMonitor.resolveStartupScriptMacros(pathToStartupScript);
//        Assert.assertEquals(result, expectedValue, "Startup script macro should have resolved to the expected value");
//    }
//
//
//
////    @Test
////    public void testGetAuthenticatedRemoteDataClient() {
////        when(job.getInternalUsername()).thenReturn("getInternalUsernameResponse");
////        when(jobManager.getJobExecutionSystem(any())).thenReturn(new ExecutionSystem());
////
////        RemoteDataClient result = defaultJobMonitor.getAuthenticatedRemoteDataClient();
////        Assert.assertEquals(result, null);
////    }
////
////    @Test
////    public void testGetStartupScriptCommandEmptyStartupStript() {
////        ExecutionSystem executionSystem = mock(ExecutionSystem.class);
////        when(executionSystem.getStartupScript()).thenReturn(null);
////        JobManager jobManager = mock(JobManager.class);
////        when(jobManager.getJobExecutionSystem(any())).thenReturn(executionSystem);
////        DefaultJobMonitor jobMonitor = mock(DefaultJobMonitor.class);
////        when(jobMonitor.getJobManager()).thenReturn(jobManager);
////        when(jobManager.getJobExecutionSystem(any())).thenReturn(new ExecutionSystem());
////
////        String result = defaultJobMonitor.getStartupScriptCommand();
////        Assert.assertEquals(result, "replaceMeWithExpectedResult");
////    }
////
////    @Test
////    public void testGetStartupScriptCommandReturnsOriginalWithoutMacros() {
////
////    }
////
////    @Test
////    public void testGetStartupScriptCommandReturnsResolvedMacros() {
////
////    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme