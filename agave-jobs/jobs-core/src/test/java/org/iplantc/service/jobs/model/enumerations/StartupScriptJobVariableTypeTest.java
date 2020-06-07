package org.iplantc.service.jobs.model.enumerations;

import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.model.JSONTestDataUtil;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.systems.model.enumerations.StartupScriptSystemVariableType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.iplantc.service.jobs.model.enumerations.StartupScriptJobVariableType.*;
import static org.mockito.Mockito.mock;

@Test(groups={"unit"})
public class StartupScriptJobVariableTypeTest {
	
	@DataProvider
	protected Object[][] resolveForJobProvider() throws JobException {
		Job job = new Job();
		job.setName(UUID.randomUUID().toString());
		job.setSoftwareName(UUID.randomUUID().toString() + "-" + "0.1.0");
		job.setSystem(UUID.randomUUID().toString());
		job.setBatchQueue(UUID.randomUUID().toString());
		job.setArchivePath(String.format("/%s/%s/%s", UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()));
		job.setOwner(JSONTestDataUtil.TEST_OWNER);
		job.setTenantId(UUID.randomUUID().toString());

		return new Object[][]{
				{job, AGAVE_JOB_NAME, String.format("${%s}", AGAVE_JOB_NAME.name()), job.getName()},
				{job, AGAVE_JOB_ID, String.format("${%s}", AGAVE_JOB_ID.name()), job.getUuid()},
				{job, AGAVE_JOB_APP_ID, String.format("${%s}", AGAVE_JOB_NAME.name()), job.getSoftwareName()},
				{job, AGAVE_JOB_EXECUTION_SYSTEM, String.format("${%s}", AGAVE_JOB_NAME.name()), job.getSystem()},
				{job, AGAVE_JOB_BATCH_QUEUE, String.format("${%s}", AGAVE_JOB_NAME.name()), job.getBatchQueue()},
				{job, AGAVE_JOB_ARCHIVE_PATH, String.format("${%s}", AGAVE_JOB_NAME.name()), job.getArchivePath()},
				{job, AGAVE_JOB_OWNER, String.format("${%s}", AGAVE_JOB_NAME.name()), job.getOwner()},
				{job, AGAVE_JOB_TENANT, String.format("${%s}", AGAVE_JOB_TENANT.name()), job.getTenantId()},
				{job, AGAVE_JOB_WORK_PATH, String.format("${%s}", AGAVE_JOB_WORK_PATH.name()), job.getWorkPath()},

		};
	}
	
	@Test(dataProvider="resolveForJobProvider", enabled=true)
	public void resolveForSystemProvider(Job job, StartupScriptJobVariableType variable, String startupScriptValue, String expectedValue) {
		String result = variable.resolveForJob(job);
		Assert.assertEquals(result, expectedValue, startupScriptValue + " should be resolved to " + expectedValue + ". " + result + " found instead.");
	}
}


