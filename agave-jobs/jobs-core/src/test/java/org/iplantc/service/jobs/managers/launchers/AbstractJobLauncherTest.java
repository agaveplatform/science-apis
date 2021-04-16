package org.iplantc.service.jobs.managers.launchers;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.common.Settings;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.SoftwareUnavailableException;
import org.iplantc.service.jobs.managers.JobManager;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.remote.RemoteSubmissionClient;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.*;
import org.iplantc.service.systems.model.enumerations.AuthConfigType;
import org.iplantc.service.systems.model.enumerations.CredentialServerProtocolType;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.RemoteTransferListener;
import org.iplantc.service.transfer.URLCopy;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.model.TransferTask;
import org.iplantc.service.transfer.model.TransferTaskImpl;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(enabled = false)
public class AbstractJobLauncherTest {
    @Mock
    Logger log;
    @Mock
    AtomicBoolean stopped;
    @Mock
    Job job;
    @Mock
    Software software;
    @Mock
    ExecutionSystem executionSystem;
    @Mock
    StorageSystem storageSystem;
    @Mock
    RemoteSubmissionClient submissionClient;
    @Mock
    URLCopy urlCopy;
    @Mock
    TransferTaskImpl transferTask;

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
    public void testCreateTempAppDir() throws IOException {

        when(software.getExecutionSystem()).thenReturn(new ExecutionSystem());
        when(executionSystem.getWorkDir()).thenReturn("getWorkDirResponse");
        when(executionSystem.getScratchDir()).thenReturn("getScratchDirResponse");

        when(job.getUuid()).thenReturn("getUuidResponse");
        when(job.getName()).thenReturn("getNameResponse");
        when(job.getOwner()).thenReturn("getOwnerResponse");
        when(job.getSoftwareName()).thenReturn("getSoftwareNameResponse");
        when(job.getWorkPath()).thenReturn(
                "getScratchDirResponse/getOwnerResponse/job-getUuidResponse-" + Slug.toSlug("getNameResponse"));

        AbstractJobLauncher abstractJobLauncher = mock(AbstractJobLauncher.class);
        doCallRealMethod().when(abstractJobLauncher).setTempAppDir(any(File.class));
        when(abstractJobLauncher.getTempAppDir()).thenCallRealMethod();
        doCallRealMethod().when(abstractJobLauncher).createTempAppDir();
        when(abstractJobLauncher.getJob()).thenReturn(job);

        File actualTempDir = null;
        try {
            abstractJobLauncher.createTempAppDir();

            actualTempDir = abstractJobLauncher.getTempAppDir();

            assertNotNull(actualTempDir, "tempAppDir should not be null after calling createTempAppDir()");
            assertTrue(actualTempDir.exists(), "tempAppDir should exist after calling createTempAppDir()");
            assertTrue(actualTempDir.isDirectory(), "tempAppDir should be a directory after calling createTempAppDir()");

            Path expectedTempDirAncestorPath = Paths.get(Settings.TEMP_DIRECTORY);
            Path actualTempDirPath = actualTempDir.toPath();
            assertTrue(actualTempDirPath.startsWith(expectedTempDirAncestorPath),
                    "tempAppDir should reside within the " + Settings.TEMP_DIRECTORY + " path, but found " +
                            actualTempDirPath.toString() + " instead.");
            assertTrue(actualTempDirPath.endsWith("job-" + job.getUuid() + "-" + Slug.toSlug(job.getName())),
                    "tempAppDir should end in standard path structure of " +
                            "job-" + job.getUuid() + "-" + Slug.toSlug(job.getName()) +
                            " but found " + actualTempDirPath.toString() + " instead.");
        }
        finally {
            try { if (actualTempDir != null) {
                FileUtils.deleteQuietly(actualTempDir);
            }} catch(Throwable ignored) {}
        }
    }

    @Test(expectedExceptions = IOException.class)
    public void testCreateTempAppDirThrowsIOExceptionWhenMkdirFails() throws IOException {

        when(software.getExecutionSystem()).thenReturn(new ExecutionSystem());
        when(executionSystem.getWorkDir()).thenReturn("getWorkDirResponse");
        when(executionSystem.getScratchDir()).thenReturn("getScratchDirResponse");

        when(job.getUuid()).thenReturn("getUuidResponse");
        when(job.getName()).thenReturn("getNameResponse");
        when(job.getOwner()).thenReturn("getOwnerResponse");
        when(job.getSoftwareName()).thenReturn("getSoftwareNameResponse");
        when(job.getWorkPath()).thenReturn(
                "getScratchDirResponse/getOwnerResponse/job-getUuidResponse-" + Slug.toSlug("getNameResponse"));

        AbstractJobLauncher abstractJobLauncher = mock(AbstractJobLauncher.class);
        doCallRealMethod().when(abstractJobLauncher).setTempAppDir(any(File.class));
        when(abstractJobLauncher.getTempAppDir()).thenCallRealMethod();
        doCallRealMethod().when(abstractJobLauncher).createTempAppDir();
        when(abstractJobLauncher.getJob()).thenReturn(job);

        File actualTempDir = null;
        String oldTempDir = Settings.TEMP_DIRECTORY;
        Settings.TEMP_DIRECTORY = "/this/directory/does/not/exist";
        try {
            abstractJobLauncher.createTempAppDir();

            Assert.fail("Unwriteable temp directory path should result in IOException");
        }
        finally {
            Settings.TEMP_DIRECTORY = oldTempDir;
        }
    }

    @Test
    public void testCopyPrivateSoftwareFileToTempAppDir() {
        String testDeploymentPath = "test/deployment/path/";

        try {
            File tempAppDir = new File(Settings.TEMP_DIRECTORY, "job-getUuidResponse-" + Slug.toSlug("getNameResponse"));

            RemoteFileInfo remoteFileInfo =  new RemoteFileInfo();
            remoteFileInfo.setName(testDeploymentPath);
            remoteFileInfo.setFileType(RemoteFileInfo.DIRECTORY_TYPE);
            remoteFileInfo.setLastModified(Date.from(Instant.now().minus(36, ChronoUnit.HOURS)));
            remoteFileInfo.setSize(20241);
            remoteFileInfo.setOwner("remoteFileOwner");

            RemoteDataClient softwareRemoteDataClient = mock(RemoteDataClient.class);
            doNothing().when(softwareRemoteDataClient).get(anyString(), anyString(), any(RemoteTransferListener.class));
            doNothing().when(softwareRemoteDataClient).authenticate();
            when(softwareRemoteDataClient.getFileInfo(anyString())).thenReturn(remoteFileInfo);


            when(executionSystem.getStatus()).thenReturn(SystemStatusType.UP);
            when(executionSystem.getSystemId()).thenReturn("getExecutionSystemIdResponse");
            when(executionSystem.getStorageConfig()).thenReturn(new StorageConfig("host", 0, StorageProtocolType.GRIDFTP, "rootDir", "zone", "resource", new AuthConfig("internalUsername", "username", "password", "credential", AuthConfigType.TOKEN, new CredentialServer("name", "endpoint", 0, CredentialServerProtocolType.KERBEROSE))));
            when(executionSystem.isAvailable()).thenReturn(true);
            when(executionSystem.getRemoteDataClient()).thenReturn(null);

            when(storageSystem.getStatus()).thenReturn(SystemStatusType.UP);
            when(storageSystem.getSystemId()).thenReturn("getStorageSystemIdResponse");
            when(storageSystem.isAvailable()).thenReturn(true);
            when(storageSystem.getRemoteDataClient()).thenReturn(softwareRemoteDataClient);
            when(storageSystem.isPubliclyAvailable()).thenReturn(false);

            when(transferTask.getSource()).thenReturn("getSourceResponse");

            when(software.getExecutionSystem()).thenReturn(new ExecutionSystem());
            when(software.getName()).thenReturn("testCopyPrivateSoftwareToTempAppDir");
            when(software.getVersion()).thenReturn("1.0.1");
            when(software.getUniqueName()).thenReturn(software.getName() + "-" + software.getVersion());
            when(software.getStorageSystem()).thenReturn(storageSystem);
            when(software.getDeploymentPath()).thenReturn("test/deployment/path/");
            when(software.getExecutablePath()).thenReturn("getExecutablePathResponse");
            when(software.isPubliclyAvailable()).thenReturn(true);
            when(software.getChecksum()).thenReturn("getChecksumResponse");
            when(software.getUniqueName()).thenReturn("getUniqueNameResponse");

            when(job.getUuid()).thenReturn("getUuidResponse");
            when(job.getName()).thenReturn("getNameResponse");
            when(job.getOwner()).thenReturn("getOwnerResponse");
            when(job.getSoftwareName()).thenReturn("getSoftwareNameResponse");
            when(job.getWorkPath()).thenReturn(
                    "getScratchDirResponse/getOwnerResponse/job-getUuidResponse-" + Slug.toSlug("getNameResponse"));

            AbstractJobLauncher abstractJobLauncher = mock(AbstractJobLauncher.class);
//            doCallRealMethod().when(abstractJobLauncher).setTempAppDir(any(File.class));
            when(abstractJobLauncher.getTempAppDir()).thenReturn(tempAppDir);
            when(abstractJobLauncher.getExecutionSystem()).thenReturn(executionSystem);
            when(abstractJobLauncher.getSoftware()).thenReturn(software);
            when(abstractJobLauncher.getJobManager()).thenReturn(new JobManager());
            when(abstractJobLauncher.getJob()).thenReturn(job);
            when(abstractJobLauncher.createJobSoftwareDeploymentDirectoryTransferTask()).thenReturn(transferTask);
            doNothing().when(abstractJobLauncher).checkStopped();
//            verify(abstractJobLauncher)


            doCallRealMethod().when(abstractJobLauncher).copySoftwareToTempAppDir();

            abstractJobLauncher.copySoftwareToTempAppDir();

//            when(job.getUuid()).thenReturn("getUuidResponse");
//            when(job.getOwner()).thenReturn("getOwnerResponse");
//            when(job.getCreated()).thenReturn(new GregorianCalendar(2021, Calendar.MARCH, 13, 17, 31).getTime());
//            when(software.getExecutionSystem()).thenReturn(new ExecutionSystem());
//            when(software.getStorageSystem()).thenReturn(new StorageSystem());
//            when(software.getDeploymentPath()).thenReturn("getDeploymentPathResponse");
//            when(software.getExecutablePath()).thenReturn("getExecutablePathResponse");
//            when(software.isPubliclyAvailable()).thenReturn(true);
//            when(software.getChecksum()).thenReturn("getChecksumResponse");
//            when(software.getUniqueName()).thenReturn("getUniqueNameResponse");
//            when(executionSystem.getStatus()).thenReturn(SystemStatusType.UP);
//            when(executionSystem.getSystemId()).thenReturn("getSystemIdResponse");
//            when(executionSystem.getStorageConfig()).thenReturn(new StorageConfig("host", 0, StorageProtocolType.GRIDFTP, "rootDir", "zone", "resource", new AuthConfig("internalUsername", "username", "password", "credential", AuthConfigType.TOKEN, new CredentialServer("name", "endpoint", 0, CredentialServerProtocolType.KERBEROSE))));
//            when(executionSystem.isAvailable()).thenReturn(true);
//            when(executionSystem.getRemoteDataClient()).thenReturn(null);
//            when(transferTask.getSource()).thenReturn("getSourceResponse");


        } catch (IOException e) {
            Assert.fail("Mock values should not throw exception", e);
        } catch (SystemArgumentException | RemoteDataException | RemoteCredentialException e) {
            Assert.fail("Mock values should not throw exception", e);
        } catch (JobException e) {
            e.printStackTrace();
        } catch (SystemUnavailableException e) {
            e.printStackTrace();
        } catch (SystemUnknownException e) {
            e.printStackTrace();
        } catch (SoftwareUnavailableException e) {
            e.printStackTrace();
        }
    }
//
//    @Test
//    public void testStageSofwareApplication() {
//        try {
//            when(job.getUuid()).thenReturn("getUuidResponse");
//            when(job.getOwner()).thenReturn("getOwnerResponse");
//            when(job.getInternalUsername()).thenReturn("getInternalUsernameResponse");
//            when(job.getSystem()).thenReturn("getSystemResponse");
//            when(job.getSoftwareName()).thenReturn("getSoftwareNameResponse");
//            when(job.getWorkPath()).thenReturn("getWorkPathResponse");
//            when(job.getRetries()).thenReturn(0);
//            when(executionSystem.getRemoteDataClient(anyString())).thenReturn(null);
//            when(transferTask.getDest()).thenReturn("getDestResponse");
//
//            abstractJobLauncher.stageSofwareApplication();
//        } catch (RemoteDataException | RemoteCredentialException e) {
//            Assert.fail("Mock values should not throw exception", e);
//        } catch (JobException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void testCreateArchiveLog() {
//        when(job.getUuid()).thenReturn("getUuidResponse");
//
//        try {
//            File result = abstractJobLauncher.createArchiveLog("logFileName");
//            Assert.assertEquals(result, new File(getClass().getResource("/org/iplantc/service/jobs/managers/launchers/PleaseReplaceMeWithTestFile.txt").getFile()));
//        } catch (JobException e) {
//            Assert.fail("Creation of archive log should not throw exception for valid file name and values", e);
//        }
//    }
//
//    @Test
//    public void testPrintListing() {
//        try {
//            abstractJobLauncher.printListing(new File(getClass().getResource("/org/iplantc/service/jobs/managers/launchers/PleaseReplaceMeWithTestFile.txt").getFile()), new File(getClass().getResource("/org/iplantc/service/jobs/managers/launchers/PleaseReplaceMeWithTestFile.txt").getFile()), null);
//        } catch (IOException e) {
//            Assert.fail("printListing should not throw exception for valid file name and values", e);
//        }
//    }
//
//    @Test
//    public void testParseSoftwareParameterValueIntoTemplateVariableValue() {
//        String result = abstractJobLauncher.parseSoftwareParameterValueIntoTemplateVariableValue(null, null);
//        Assert.assertEquals(result, "replaceMeWithExpectedResult");
//    }
//
//    @Test
//    public void testParseSoftwareInputValueIntoTemplateVariableValue() {
//        String result = null;
//        try {
//            result = abstractJobLauncher.parseSoftwareInputValueIntoTemplateVariableValue(null, null);
//            Assert.assertEquals(result, "replaceMeWithExpectedResult");
//        } catch (URISyntaxException e) {
//            Assert.fail("parseSoftwareInputValueIntoTemplateVariableValue should not throw exception for valid file name and values", e);
//        }
//
//    }
}
