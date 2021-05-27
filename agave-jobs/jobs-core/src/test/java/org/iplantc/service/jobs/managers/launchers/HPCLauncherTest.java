package org.iplantc.service.jobs.managers.launchers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareInput;
import org.iplantc.service.apps.model.SoftwareParameter;
import org.iplantc.service.apps.model.enumerations.SoftwareParameterType;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.model.JSONTestDataUtil;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.enumerations.ExecutionType;
import org.iplantc.service.systems.model.enumerations.SchedulerType;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(enabled=false)
public class HPCLauncherTest {
    ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = Logger.getLogger(HPCLauncherTest.class);

    /**
     * Generator for test execution system.
     * @param executionType the execution type to assign
     * @param schedulerType the scheduler type to assign
     * @return an execution system to use in testing
     */
    private ExecutionSystem createMockExecutionSystem(ExecutionType executionType, SchedulerType schedulerType) {
        ExecutionSystem executionSystem = null;
        try {
            JSONObject json = JSONTestDataUtil.getInstance().getTestDataObject(JSONTestDataUtil.TEST_EXECUTION_SYSTEM_FILE);
            json.put("id", UUID.randomUUID().toString());
            executionSystem = ExecutionSystem.fromJSON(json);
        } catch (IOException|JSONException|SystemArgumentException e) {
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
        final Software software = new Software();
        try {
            JSONObject json = JSONTestDataUtil.getInstance().getTestDataObject(JSONTestDataUtil.TEST_SOFTWARE_FOLDER + "/singularity-1.0/app.json");
            software.setExecutionSystem(executionSystem);
            software.setExecutionType(executionType);
            software.setName(UUID.randomUUID().toString());
            JSONArray params = json.getJSONArray("parameters");
            for(int i=0; i< params.length(); i++) {
                try
                {
                    JSONObject jsonParameter = params.getJSONObject(i);
                    SoftwareParameter parameter = SoftwareParameter.fromJSON(jsonParameter);
                    parameter.setSoftware(software);
                    if (!software.getParameters().contains(parameter)) {
                        software.addParameter(parameter);
                    }
                } catch (JSONException e) {
                    log.error("Invalid app parameters value. Please specify " +
                            "an array of JSON objects describing the parameters for this app.");
                }
            }

            JSONArray inputs = json.getJSONArray("inputs");
            for(int i=0; i< inputs.length(); i++) {
                try
                {
                    JSONObject jsonParameter = inputs.getJSONObject(i);
                    SoftwareInput input = SoftwareInput.fromJSON(jsonParameter);
                    input.setSoftware(software);
                    if (!software.getInputs().contains(input)) {
                        software.addInput(input);
                    }
                } catch (JSONException e) {
                    log.error("Invalid app input value. Please specify " +
                            "an array of JSON objects describing the inputs for this app.");
                }
            }

            software.setVersion("0.1");

        } catch (IOException|JSONException e) {
            fail("Test system id should not throw exception", e);
        }

        return software;
    }

    /**
     * Ceates a mock {@link Job} for testing using the given system and software.
     * @param executionSystem the system on which the app runs
     * @param software the app to run
     * @return a mock job instance for use in testing
     */
    private Job createMockJob(ExecutionSystem executionSystem, Software software) throws JobException {
        Job job = mock(Job.class);
        String jobUuid = new AgaveUUID(UUIDType.JOB).toString();
        when(job.getSoftwareName()).thenReturn(software.getUniqueName());
        when(job.getSystem()).thenReturn(executionSystem.getSystemId());
        when(job.getExecutionType()).thenReturn(executionSystem.getExecutionType());
        when(job.getSchedulerType()).thenReturn(executionSystem.getScheduler());
        when(job.getUuid()).thenReturn(jobUuid);
        when(job.getId()).thenReturn(1L);
        when(job.getWorkPath()).thenReturn("/scratch/" + JSONTestDataUtil.TEST_OWNER + "/job-" + jobUuid);
        when(job.isArchiveOutput()).thenReturn(false);
        when(job.getName()).thenReturn(UUID.randomUUID().toString());
        BatchQueue queue = executionSystem.getDefaultQueue();
        when(job.getBatchQueue()).thenReturn(queue.getName());
        when(job.getMemoryPerNode()).thenReturn(1D);
        when(job.getNodeCount()).thenReturn(1L);
        when(job.getMaxRunTime()).thenReturn("00:01:00");
        when(job.getProcessorsPerNode()).thenReturn(1L);

        final ObjectNode jsonParams = mapper.createObjectNode();
        software.getParameters().forEach(param -> {
            if (param.getType() == SoftwareParameterType.bool) {
                jsonParams.put(param.getKey(), false);
            } else {
                jsonParams.put(param.getKey(), param.getDefaultValueAsJsonArray());
            }
        });
        when(job.getParametersAsJsonObject()).thenReturn(jsonParams);

        final ObjectNode jsonInputs = mapper.createObjectNode();
        software.getInputs().forEach(input -> {
            jsonInputs.set(input.getKey(), input.getDefaultValueAsJsonArray());
        });
        when(job.getInputsAsJsonObject()).thenReturn(jsonInputs);

        return job;
    }

    @Test
    public void testProcessApplicationTemplate() throws JobException, RemoteDataException, RemoteCredentialException, IOException, URISyntaxException, JobMacroResolutionException {
        ExecutionSystem executionSystem = createMockExecutionSystem(ExecutionType.HPC, SchedulerType.SLURM);
        Software software = createMockSoftware(executionSystem, ExecutionType.HPC);
        Job job = createMockJob(executionSystem, software);

        HPCLauncher launcher = mock(HPCLauncher.class);
        // read in test wrapper script for use in the test
        String appTemplate = FileUtils.readFileToString(new File(JSONTestDataUtil.TEST_SOFTWARE_FOLDER + "/singularity-1.0/wrapper.sh"));
        when(launcher.getAppTemplateFileContents()).thenReturn(appTemplate);
        Path tempDir = Files.createTempDirectory("testProcessApplicationTemplate");
        when(launcher.getTempAppDir()).thenReturn(tempDir.toFile());
        when(launcher.getExecutionSystem()).thenReturn(executionSystem);
        when(launcher.getSoftware()).thenReturn(software);
        when(launcher.getJob()).thenReturn(job);
        String workPath = job.getWorkPath();
        when(launcher.getAbsoluteRemoteJobDirPath()).thenReturn(workPath);

        // pass through all the actual parsing scripts
        doCallRealMethod().when(launcher).setBatchScriptName(anyString());
        when(launcher.getBatchScriptName()).thenCallRealMethod();
        when(launcher.resolveJobRequestParameters(anyString())).thenCallRealMethod();
        when(launcher.resolveJobRequestInputs(anyString())).thenCallRealMethod();
        when(launcher.filterRuntimeStatusMacros(anyString())).thenCallRealMethod();
        when(launcher.resolveRuntimeNotificationMacros(anyString())).thenCallRealMethod();
        when(launcher.resolveMacros(anyString())).thenCallRealMethod();
        when(launcher.processApplicationWrapperTemplate()).thenCallRealMethod();
        when(launcher.parseSoftwareParameterValueIntoTemplateVariableValue(any(), any())).thenCallRealMethod();
        when(launcher.parseSoftwareInputValueIntoTemplateVariableValue(any(), any())).thenCallRealMethod();

        doNothing().when(launcher).writeToRemoteJobDir(anyString(), anyString());

//        String expectedExecutablePath = workPath + File.separator + Slug.toSlug(job.getName()) + ".ipcexe";
        String actualRemoteApplicationWrapperPath = launcher.processApplicationWrapperTemplate();

//        assertEquals(actualRemoteApplicationWrapperPath, expectedExecutablePath, "Remote executable wrapper path returned was incorrect");
        assertTrue(actualRemoteApplicationWrapperPath.contains("Begin App Wrapper Template Logic"), "App wrapper opening header should be present in rendered wrapper file.");
        assertTrue(actualRemoteApplicationWrapperPath.contains("End App Wrapper Template Logic"), "App wrapper closing header should be present in rendered wrapper file.");

//        ArgumentCaptor<String> wrapperTemplateContentCaptor = ArgumentCaptor.forClass(String.class);
//        verify(launcher).writeToRemoteJobDir(eq(expectedExecutablePath), wrapperTemplateContentCaptor.capture());

//        assertTrue(wrapperTemplateContentCaptor.getValue().contains("Begin App Wrapper Template Logic"), "App wrapper opening header should be present in rendered wrapper file.");
//        assertTrue(wrapperTemplateContentCaptor.getValue().contains("End App Wrapper Template Logic"), "App wrapper closing header should be present in rendered wrapper file.");
    }
}