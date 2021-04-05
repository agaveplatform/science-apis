package org.iplantc.service.jobs.managers;

import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@Test(groups={"unit"})
public class JobManagerTest {

    @DataProvider
    public static Object[][] calculateJobRemoteWorkPathUsesDefaultProvider() {
        return new Object[][]{
                {null, null, "", "No execution system scratch or work directory should make job work directory relative to homeDir"},
                {"/scratch", null, "/scratch/", "Existing execution system scratch scratch directory should always be used when present."},
                {"/scratch/", null, "/scratch/", "Existing execution system scratch scratch directory should always be used when present."},
                {"/scratch//", null, "/scratch/", "Existing execution system scratch scratch directory should always be used when present."},
                {"//scratch/", null, "/scratch/", "Existing execution system scratch scratch directory should always be used when present."},
                {"/scratch", "/work", "/scratch/", "Existing execution system scratch directory should always be used when present."},
                {null, "/work", "/work/", "Existing execution system work directory should be used when scratch is missing."},
                {null, "//work", "/work/", "Existing execution system work directory should be used when scratch is missing."},
                {null, "/work/", "/work/", "Existing execution system work directory should be used when scratch is missing."},
                {null, "/work//", "/work/", "Existing execution system work directory should be used when scratch is missing."},
        };
    }

    @Test(dataProvider = "calculateJobRemoteWorkPathUsesDefaultProvider")
    public void calculateJobRemoteWorkPathUsesDefault(String executionScratchDir, String executionWorkDir, String expectedPathPrefix, String message) {
        ExecutionSystem executionSystem = mock(ExecutionSystem.class);
        when(executionSystem.getScratchDir()).thenReturn(executionScratchDir);
        when(executionSystem.getWorkDir()).thenReturn(executionWorkDir);

        Job job = mock(Job.class);
        when(job.getOwner()).thenReturn("testuser");
        when(job.getUuid()).thenReturn(new AgaveUUID(UUIDType.JOB).toString());
        when(job.getName()).thenReturn("job manager test");

        String calculatedJobRemoteWorkPath = new JobManager().calculateJobRemoteWorkPath(job, executionSystem);

        String expectedWorkDir = String.format("%s%s/job-%s-%s",
                expectedPathPrefix, job.getOwner(), job.getUuid(), Slug.toSlug(job.getName()));

        assertNotNull(calculatedJobRemoteWorkPath, "Remote job work path should never be null");
        assertFalse(calculatedJobRemoteWorkPath.endsWith("/"), "Remote job work path should never have a trailing slash");
        assertFalse(calculatedJobRemoteWorkPath.contains("//"), "Remote job work path should never have a double slashes");
        assertEquals(calculatedJobRemoteWorkPath, expectedWorkDir, message);
        verify(executionSystem, times(1)).getScratchDir();

        if (executionScratchDir != null) {
            verify(executionSystem, never()).getWorkDir();
        }
    }

}
