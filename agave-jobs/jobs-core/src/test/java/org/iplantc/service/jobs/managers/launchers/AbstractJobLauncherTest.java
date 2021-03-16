package org.iplantc.service.jobs.managers.launchers;

import org.apache.log4j.Logger;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.common.Settings;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.remote.RemoteSubmissionClient;
import org.iplantc.service.systems.model.*;
import org.iplantc.service.systems.model.enumerations.AuthConfigType;
import org.iplantc.service.systems.model.enumerations.CredentialServerProtocolType;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.iplantc.service.transfer.URLCopy;
import org.iplantc.service.transfer.model.TransferTask;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AbstractJobLauncherTest {
    @Mock
    Logger log;
    @Mock
    AtomicBoolean stopped;
    @Mock
    File tempAppDir;
    @Mock
    Job job;
    @Mock
    Software software;
    @Mock
    ExecutionSystem executionSystem;
    @Mock
    RemoteSubmissionClient submissionClient;
    @Mock
    URLCopy urlCopy;
    @Mock
    TransferTask transferTask;

    @InjectMocks
    AbstractJobLauncher abstractJobLauncher;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }
//
//    @Test
//    public void testIsStopped() {
//        boolean result = abstractJobLauncher.isStopped();
//        Assert.assertEquals(result, true);
//    }
//
//    @Test
//    public void testSetStopped() {
//        abstractJobLauncher.setStopped(true);
//    }
//
//    @Test
//    public void testCheckStopped() {
//        abstractJobLauncher.checkStopped();
//    }
//
//    @Test
//    public void testRemoveReservedJobStatusMacros() {
//        String result = abstractJobLauncher.removeReservedJobStatusMacros("wrapperTemplate");
//        Assert.assertEquals(result, "replaceMeWithExpectedResult");
//    }
//
//    @Test
//    public void testResolveRuntimeNotificationMacros() {
//        String result = abstractJobLauncher.resolveRuntimeNotificationMacros("wrapperTemplate");
//        Assert.assertEquals(result, "replaceMeWithExpectedResult");
//    }
//
//    @Test
//    public void testFilterRuntimeStatusMacros() {
//        String result = abstractJobLauncher.filterRuntimeStatusMacros("appTemplate");
//        Assert.assertEquals(result, "replaceMeWithExpectedResult");
//    }
//
//    @Test
//    public void testResolveMacros() {
//        String result = abstractJobLauncher.resolveMacros("wrapperTemplate");
//        Assert.assertEquals(result, "replaceMeWithExpectedResult");
//    }
//
//    @Test
//    public void testResolveStartupScriptMacros() {
//        String result = abstractJobLauncher.resolveStartupScriptMacros("startupScript");
//        Assert.assertEquals(result, "replaceMeWithExpectedResult");
//    }
//
//    @Test
//    public void testGetStartupScriptCommand() {
//        String result = abstractJobLauncher.getStartupScriptCommand("absoluteRemoteWorkPath");
//        Assert.assertEquals(result, "replaceMeWithExpectedResult");
//    }

    @Test
    public void testCreateTempAppDir() throws JobException {

        when(job.getUuid()).thenReturn("getUuidResponse");
        when(job.getName()).thenReturn("getNameResponse");
        when(job.getOwner()).thenReturn("getOwnerResponse");
        when(job.getSoftwareName()).thenReturn("getSoftwareNameResponse");
        when(job.getWorkPath()).thenReturn("getWorkPathResponse");
        when(software.getExecutionSystem()).thenReturn(new ExecutionSystem());
        when(executionSystem.getWorkDir()).thenReturn("getWorkDirResponse");
        when(executionSystem.getScratchDir()).thenReturn("getScratchDirResponse");

        doCallRealMethod().when(abstractJobLauncher).setTempAppDir(any(File.class));
        when(abstractJobLauncher.getTempAppDir()).thenCallRealMethod();
        doCallRealMethod().when(abstractJobLauncher).createTempAppDir();

        abstractJobLauncher.createTempAppDir();

        File actualTempDir = abstractJobLauncher.getTempAppDir();

        assertNotNull(actualTempDir, "tempAppDir should not be null after calling createTempAppDir()");
        assertTrue(actualTempDir.exists(), "tempAppDir should exist after calling createTempAppDir()");
        assertTrue(actualTempDir.isDirectory(), "tempAppDir should be a directory after calling createTempAppDir()");

        Path expectedTempDirAncestorPath = Paths.get(Settings.TEMP_DIRECTORY);
        Path actualTempDirPath = actualTempDir.toPath();
        assertTrue(actualTempDirPath.startsWith(expectedTempDirAncestorPath), "tempAppDir should reside within the Settings.TEMP_DIRECTORY");
    }

    @Test
    public void testCopySoftwareToTempAppDir() {
        when(job.getUuid()).thenReturn("getUuidResponse");
        when(job.getOwner()).thenReturn("getOwnerResponse");
        when(job.getCreated()).thenReturn(new GregorianCalendar(2021, Calendar.MARCH, 13, 17, 31).getTime());
        when(software.getExecutionSystem()).thenReturn(new ExecutionSystem());
        when(software.getStorageSystem()).thenReturn(new StorageSystem());
        when(software.getDeploymentPath()).thenReturn("getDeploymentPathResponse");
        when(software.getExecutablePath()).thenReturn("getExecutablePathResponse");
        when(software.isPubliclyAvailable()).thenReturn(true);
        when(software.getChecksum()).thenReturn("getChecksumResponse");
        when(software.getUniqueName()).thenReturn("getUniqueNameResponse");
        when(executionSystem.getStatus()).thenReturn(SystemStatusType.UP);
        when(executionSystem.getSystemId()).thenReturn("getSystemIdResponse");
        when(executionSystem.getStorageConfig()).thenReturn(new StorageConfig("host", 0, StorageProtocolType.GRIDFTP, "rootDir", "zone", "resource", new AuthConfig("internalUsername", "username", "password", "credential", AuthConfigType.TOKEN, new CredentialServer("name", "endpoint", 0, CredentialServerProtocolType.KERBEROSE))));
        when(executionSystem.isAvailable()).thenReturn(true);
        when(executionSystem.getRemoteDataClient()).thenReturn(null);
        when(transferTask.getSource()).thenReturn("getSourceResponse");

        abstractJobLauncher.copySoftwareToTempAppDir();
    }

    @Test
    public void testStageSofwareApplication() {
        when(job.getUuid()).thenReturn("getUuidResponse");
        when(job.getOwner()).thenReturn("getOwnerResponse");
        when(job.getInternalUsername()).thenReturn("getInternalUsernameResponse");
        when(job.getSystem()).thenReturn("getSystemResponse");
        when(job.getSoftwareName()).thenReturn("getSoftwareNameResponse");
        when(job.getWorkPath()).thenReturn("getWorkPathResponse");
        when(job.getRetries()).thenReturn(Integer.valueOf(0));
        when(executionSystem.getRemoteDataClient(anyString())).thenReturn(null);
        when(transferTask.getDest()).thenReturn("getDestResponse");

        abstractJobLauncher.stageSofwareApplication();
    }

    @Test
    public void testCreateArchiveLog() {
        when(job.getUuid()).thenReturn("getUuidResponse");

        File result = abstractJobLauncher.createArchiveLog("logFileName");
        Assert.assertEquals(result, new File(getClass().getResource("/org/iplantc/service/jobs/managers/launchers/PleaseReplaceMeWithTestFile.txt").getFile()));
    }

    @Test
    public void testPrintListing() {
        abstractJobLauncher.printListing(new File(getClass().getResource("/org/iplantc/service/jobs/managers/launchers/PleaseReplaceMeWithTestFile.txt").getFile()), new File(getClass().getResource("/org/iplantc/service/jobs/managers/launchers/PleaseReplaceMeWithTestFile.txt").getFile()), null);
    }

    @Test
    public void testParseSoftwareParameterValueIntoTemplateVariableValue() {
        String result = abstractJobLauncher.parseSoftwareParameterValueIntoTemplateVariableValue(null, null);
        Assert.assertEquals(result, "replaceMeWithExpectedResult");
    }

    @Test
    public void testParseSoftwareInputValueIntoTemplateVariableValue() {
        String result = abstractJobLauncher.parseSoftwareInputValueIntoTemplateVariableValue(null, null);
        Assert.assertEquals(result, "replaceMeWithExpectedResult");
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme