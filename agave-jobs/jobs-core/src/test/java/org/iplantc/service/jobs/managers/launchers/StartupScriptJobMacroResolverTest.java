package org.iplantc.service.jobs.managers.launchers;

import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.exceptions.JobMacroResolutionException;
import org.iplantc.service.jobs.model.JSONTestDataUtil;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.StartupScriptJobVariableType;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.iplantc.service.jobs.model.enumerations.StartupScriptJobVariableType.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test(groups={"unit"})
public class StartupScriptJobMacroResolverTest {
	
	@DataProvider
	protected Object[][] testResolveStartupScriptJobMacroProvider() throws JobException {
		Job job = new Job();
		job.setName(UUID.randomUUID().toString());
		job.setSoftwareName(UUID.randomUUID() + "-" + "0.1.0");
		job.setSystem(UUID.randomUUID().toString());
		job.setBatchQueue(UUID.randomUUID().toString());
		job.setArchivePath(String.format("/%s/%s/%s", UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()));
		job.setOwner(JSONTestDataUtil.TEST_OWNER);
		job.setTenantId(UUID.randomUUID().toString());

		return new Object[][]{
				{job, AGAVE_JOB_NAME, String.format("${%s}", AGAVE_JOB_NAME.name()), job.getName()},
				{job, AGAVE_JOB_ID, String.format("${%s}", AGAVE_JOB_ID.name()), job.getUuid()},
				{job, AGAVE_JOB_APP_ID, String.format("${%s}", AGAVE_JOB_NAME.name()), job.getSoftwareName()},
				{job, AGAVE_JOB_EXECUTION_SYSTEM, String.format("${%s}", AGAVE_JOB_NAME.name()), job.getSystem()},
				{job, AGAVE_JOB_BATCH_QUEUE, String.format("${%s}", AGAVE_JOB_NAME.name()), job.getBatchQueue()},
				{job, AGAVE_JOB_BATCH_QUEUE_EFFECTIVE_NAME, String.format("${%s}", AGAVE_JOB_BATCH_QUEUE_EFFECTIVE_NAME.name()), "effective" + job.getBatchQueue()},
				{job, AGAVE_JOB_ARCHIVE_PATH, String.format("${%s}", AGAVE_JOB_NAME.name()), job.getArchivePath()},
				{job, AGAVE_JOB_OWNER, String.format("${%s}", AGAVE_JOB_NAME.name()), job.getOwner()},
				{job, AGAVE_JOB_TENANT, String.format("${%s}", AGAVE_JOB_TENANT.name()), job.getTenantId()},
				{job, AGAVE_JOB_WORK_PATH, String.format("${%s}", AGAVE_JOB_WORK_PATH.name()), job.getWorkPath()},
		};
	}
	
	@Test(dataProvider="testResolveStartupScriptJobMacroProvider")
	public void testResolveStartupScriptJobMacro(Job job, StartupScriptJobVariableType jobMacro, String startupScriptValue, String expectedValue) {
		try {
			ExecutionSystem executionSystem = new ExecutionSystem();
			executionSystem.setSystemId(job.getSystem());
			BatchQueue queue = new BatchQueue();
			queue.setName(job.getBatchQueue());
			queue.setMappedName("effective" + job.getBatchQueue());
			executionSystem.addBatchQueue(queue);

			StartupScriptJobMacroResolver resolver = mock(StartupScriptJobMacroResolver.class);
			when(resolver.getJob()).thenReturn(job);
			when(resolver.getExecutionSystem()).thenReturn(executionSystem);
			when(resolver.resolveStartupScriptJobMacro(eq(job), eq(executionSystem), eq(jobMacro))).thenCallRealMethod();

			String result = resolver.resolveStartupScriptJobMacro(job, executionSystem, jobMacro);
			Assert.assertEquals(result, expectedValue, startupScriptValue + " should be resolved to " + expectedValue + ". " + result + " found instead.");
		} catch (JobMacroResolutionException | SystemArgumentException e) {
			Assert.fail("Macro resolution should not fail for test job", e);
		}
	}

	@Test(expectedExceptions = JobMacroResolutionException.class)
	public void testResolveStartupScriptJobMacroThrowsExceptionOnMissingSystem() throws JobMacroResolutionException {
		try {
			Job job = new Job();
			job.setSystem(UUID.randomUUID().toString());
			job.setBatchQueue(UUID.randomUUID().toString());
			job.setTenantId(UUID.randomUUID().toString());

			ExecutionSystem executionSystem = mock(ExecutionSystem.class);
			when(executionSystem.getQueue(job.getSystem())).thenReturn(null);

			StartupScriptJobMacroResolver resolver = mock(StartupScriptJobMacroResolver.class);
			when(resolver.getJob()).thenReturn(job);
			when(resolver.getExecutionSystem()).thenReturn(executionSystem);
			when(resolver.resolveStartupScriptJobMacro(eq(job), eq(executionSystem), eq(AGAVE_JOB_BATCH_QUEUE_EFFECTIVE_NAME))).thenCallRealMethod();

			resolver.resolveStartupScriptJobMacro(job, executionSystem, AGAVE_JOB_BATCH_QUEUE_EFFECTIVE_NAME);
			Assert.fail("Unknown batch queue for job system should cause JobMacroResolutionException to be thrown");
		} catch (JobException e) {
			Assert.fail("Unexpected system exception when initializing startup script test data", e);
		}
	}

	@Test(expectedExceptions = JobMacroResolutionException.class)
	public void testResolveStartupScriptJobMacroThrowsExceptionOnMissingBatchQueue() throws JobMacroResolutionException {
		try {
			Job job = new Job();
			job.setSystem(UUID.randomUUID().toString());
			job.setBatchQueue(UUID.randomUUID().toString());
			job.setTenantId(UUID.randomUUID().toString());

			ExecutionSystem executionSystem = mock(ExecutionSystem.class);
			when(executionSystem.getQueue(job.getSystem())).thenReturn(null);

			StartupScriptJobMacroResolver resolver = mock(StartupScriptJobMacroResolver.class);
			when(resolver.getJob()).thenReturn(job);
			when(resolver.getExecutionSystem()).thenReturn(executionSystem);
			when(resolver.resolveStartupScriptJobMacro(eq(job), eq(executionSystem), eq(AGAVE_JOB_BATCH_QUEUE_EFFECTIVE_NAME))).thenCallRealMethod();

			resolver.resolveStartupScriptJobMacro(job, executionSystem, AGAVE_JOB_BATCH_QUEUE_EFFECTIVE_NAME);

			Assert.fail("Unknown batch queue for job system should cause JobMacroResolutionException to be thrown");
		} catch (JobException e) {
			Assert.fail("Unexpected system exception when initializing startup script test data", e);
		}
	}

//	@Test(expectedExceptions = JobMacroResolutionException.class)
//	public void testResolveThrowsExceptionOnMissingSystem() throws JobMacroResolutionException {
//		try {
//			Job job = new Job();
//			job.setSystem(UUID.randomUUID().toString());
//			job.setBatchQueue(UUID.randomUUID().toString());
//			job.setTenantId(UUID.randomUUID().toString());
//
//			StartupScriptJobMacroResolver resolver = mock(StartupScriptJobMacroResolver.class);
//			when(resolver.getJob()).thenReturn(job);
//			when(resolver.getExecutionSystem()).thenThrow(new SystemUnavailableException("This exception should cause JobMacroResolutionException to be thrown"));
//			when(resolver.resolve()).thenCallRealMethod();
//
//			resolver.resolve();
//			Assert.fail("Unknown system for job should cause JobMacroResolutionException to be thrown");
//		} catch (JobException e) {
//			Assert.fail("Unexpected system exception when initializing startup script test data", e);
//		}
//	}

	@DataProvider
	public Object[][] testResolveStartupScriptJobMacroReturnsNullWhenStartupScriptBlankProvider() {
		return new Object[][]{
				{ "" },
				{ " " },
				{ "   " },
				{ null },
		};
	}

	/**
	 * Ensures blank startup script values in the job's {@link ExecutionSystem} return null.
	 * @param pathToStartupScript the startupPath value to test
	 */
	@Test(dataProvider = "testResolveStartupScriptJobMacroReturnsNullWhenStartupScriptBlankProvider")
	public void testResolveStartupScriptJobMacroReturnsNullWhenStartupScriptBlank(String pathToStartupScript) {
		try {
			Job job = new Job();
			job.setSystem(UUID.randomUUID().toString());
			job.setBatchQueue(UUID.randomUUID().toString());
			job.setTenantId(UUID.randomUUID().toString());

			ExecutionSystem executionSystem = new ExecutionSystem();
			executionSystem.setSystemId(job.getSystem());
			executionSystem.setStartupScript(pathToStartupScript);

//			BatchQueue queue = new BatchQueue();
//			queue.setName(job.getBatchQueue());
//			queue.setMappedName("effective" + job.getBatchQueue());
//			executionSystem.addBatchQueue(queue);

			StartupScriptJobMacroResolver resolver = mock(StartupScriptJobMacroResolver.class);
			when(resolver.getJob()).thenReturn(job);
			when(resolver.getExecutionSystem()).thenReturn(executionSystem);
			when(resolver.resolve()).thenCallRealMethod();

			String result = resolver.resolve();
			Assert.assertNull(result, "Blank startup script value should return null");
		} catch (JobException | SystemArgumentException | JobMacroResolutionException e) {
			Assert.fail("Resolving an empty startup script should return empty string.", e);
		}
	}

	@DataProvider
	public Object[][] testResolveStartupScriptMacrosReturnsResolvedStringProvider() {
		Object[][] testData = null;

		try {
			Job job = new Job();
			job.setName(UUID.randomUUID().toString());
			job.setSoftwareName(UUID.randomUUID() + "-" + "0.1.0");
			job.setSystem(UUID.randomUUID().toString());
			job.setBatchQueue(UUID.randomUUID().toString());
			job.setArchivePath(String.format("/%s/%s/%s", UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()));
			job.setOwner(JSONTestDataUtil.TEST_OWNER);
			job.setTenantId(UUID.randomUUID().toString());
			job.setWorkPath(String.format("/scratch/work/%s/job-%s", job.getOwner(), job.getUuid()));

			testData = new Object[][] {
					{job, "~/.bashrc", "~/.bashrc"},
					{job, "${AGAVE_JOB_WORK_PATH}/startup.sh", String.format("%s/startup.sh", job.getWorkPath())},
					{job, "$HOME/${SYSTEM_ID}.sh", String.format("$HOME/%s.sh", job.getSystem())}
			};

		} catch (JobException e) {
			Assert.fail("Failed to create test job", e);
		}

		return testData;
	}

	/**
	 * Ensures templated startup script values in the job's {@link ExecutionSystem} resolve the macros in the template.
	 * @param job the job to use to resolve the template macros
	 * @param pathToStartupScript the startupPath value to test
	 * @param expectedValue the expected value of the startup script after resolution
	 */
	@Test(dataProvider = "testResolveStartupScriptMacrosReturnsResolvedStringProvider")
	public void testResolveStartupScriptMacrosReturnsResolvedString(Job job, String pathToStartupScript, String expectedValue) {
		try {

			ExecutionSystem executionSystem = new ExecutionSystem();
			executionSystem.setSystemId(job.getSystem());
			executionSystem.setStartupScript(pathToStartupScript);

			BatchQueue queue = new BatchQueue();
			queue.setName(job.getBatchQueue());
			queue.setMappedName("effective" + job.getBatchQueue());
			executionSystem.addBatchQueue(queue);

			StartupScriptJobMacroResolver resolver = mock(StartupScriptJobMacroResolver.class);
			when(resolver.getJob()).thenReturn(job);
			when(resolver.getExecutionSystem()).thenReturn(executionSystem);
			when(resolver.resolveStartupScriptSystemMacros(eq(executionSystem), any())).thenCallRealMethod();
			when(resolver.resolveStartupScriptJobMacro(eq(job), eq(executionSystem), any())).thenCallRealMethod();
			when(resolver.resolve()).thenCallRealMethod();

			String result = resolver.resolve();
			Assert.assertEquals(result, expectedValue, "Startup script macro should have resolved to the expected value");
		} catch (SystemArgumentException | JobMacroResolutionException e) {
			Assert.fail("Resolving an empty startup script should return empty string.", e);
		}
	}

}


