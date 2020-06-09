package org.iplantc.service.jobs.dao;

import static org.iplantc.service.jobs.model.JSONTestDataUtil.TEST_OWNER;
import static org.iplantc.service.jobs.model.JSONTestDataUtil.TEST_SHARED_OWNER;

import java.util.List;
import java.util.function.Predicate;

import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.JobPermission;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.model.enumerations.PermissionType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups={"broken", "integration"})
public class JobPermissionDaoTest extends AbstractDaoTest {

	@Test
	public void persist() throws Exception
	{
		Software software = createSoftware();
		Job job = createJob(JobStatusType.PENDING, software);
		JobPermission pem = new JobPermission(job, TEST_OWNER, PermissionType.READ);
		JobPermissionDao.persist(pem);
		Assert.assertNotNull(pem.getId(), "Job permission did not persist.");
	}

	@Test(dependsOnMethods={"persist", "getByJobId"})
	public void delete() throws Exception
	{
		Software software = createSoftware();
		Job job = createJob(JobStatusType.PENDING, software);
		
		JobPermission pem = new JobPermission(job, TEST_OWNER, PermissionType.READ);
		JobPermissionDao.persist(pem);
		Assert.assertNotNull(pem.getId(), "Job permission did not persist.");
		
		JobPermissionDao.delete(pem);
		List<JobPermission> pems = JobPermissionDao.getByJobId(job.getId());
		Assert.assertFalse(pems.contains(pem), "Job permission did not delete.");
	}

	@Test(dependsOnMethods={"persist"})
	public void getByJobId() throws Exception
	{
		Software software = createSoftware();
		Job job = createJob(JobStatusType.PENDING, software);
		
		JobPermission pem = new JobPermission(job, TEST_OWNER, PermissionType.READ);
		JobPermissionDao.persist(pem);
		Assert.assertNotNull(pem.getId(), "Job permission did not persist.");
		
		Job job2 = createJob(JobStatusType.PENDING, software);
		JobDao.persist(job2);
		Assert.assertNotNull(job2.getId(), "Failed to generate a job ID.");
		
		List<JobPermission> jobPermissions = JobPermissionDao.getByJobId(job.getId());
		Assert.assertNotNull(jobPermissions, "getByJobId did not return any permissions.");
		Assert.assertEquals(jobPermissions.size(), 1, "getByJobId did not return the correct number of permissions.");
		for(JobPermission p: jobPermissions) {
			Assert.assertEquals(pem.getJobId(), job.getId(), "getByJobId returned a permission from another job.");
		}
	}

	@Test(dependsOnMethods={"persist","delete"})
	public void getByUsernameAndJobId() throws Exception
	{
		Software software = createSoftware();
		Job job = createJob(JobStatusType.PENDING, software);
		
		JobPermission pem1 = new JobPermission(job, TEST_OWNER, PermissionType.READ);
		JobPermissionDao.persist(pem1);
		Assert.assertNotNull(pem1.getId(), "Job permission 1 did not persist.");
		
		JobPermission pem2 = new JobPermission(job, TEST_SHARED_OWNER, PermissionType.READ);
		JobPermissionDao.persist(pem2);
		Assert.assertNotNull(pem2.getId(), "Job permission 2 did not persist.");
		
		JobPermission userPem = JobPermissionDao.getByUsernameAndJobId(TEST_OWNER, job.getId());
		Assert.assertNotNull(userPem, "getByUsernameAndJobId did not return the user permission.");
		Assert.assertEquals(userPem, pem1, "getByJobId did not return the correct job permission for the user.");
	}
}
