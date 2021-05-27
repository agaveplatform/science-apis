package org.iplantc.service.jobs.scheduling;

import org.apache.commons.lang.math.RandomUtils;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test(groups={"integration"})
public class JobSchedulingQuotaTest extends AbstractJobSchedulingTest  
{
	/**
	 * Tests a fair selection of users for pending jobs when users all have
	 * even number of pending jobs.
	 *  
	 * @throws Exception
	 */
	@Test(enabled=false)
	public void getRandomUserForNextQueuedJobHonorsQueueMaxJobsPerUserQuotasWithoutContention() 
	throws Exception
	{
	    Software software = createSoftware();
		BatchQueue queue = software.getExecutionSystem().getQueue("medium");
		
		for (int i=0;i<queue.getMaxUserJobs();i++)
		{	
			// push the user to their quota for the system and queue.
			createJob(JobStatusType.RUNNING, software, software.getExecutionSystem(), queue, "user-1");
		}
		
		// add a pending job so there is something to be returned had the
		// user not been over quota
		createJob(JobStatusType.PENDING, software, software.getExecutionSystem(), queue, "user-1");
		createJob(JobStatusType.STAGED, software, software.getExecutionSystem(), queue, "user-1");
			
		// make the actual selection to verify uniform distribution
		for (int i=0;i<10; i++) {
		    for (JobStatusType testStatus: new JobStatusType[] { JobStatusType.PENDING, JobStatusType.STAGED }) {
    			String username = JobDao.getRandomUserForNextQueuedJobOfStatus(testStatus, null, null, null);			 
    			Assert.assertNull(username, "User should not be returned when have a " + testStatus + " job and they are over quota.");
		    }
		}
	}
	
	/**
     * Tests any other user under quota will be selected in the scheduling decision if
     * as long as they are under all quotas. 
     *  
     * @throws Exception
     */
    @Test(enabled=false)
    public void getRandomUserForNextQueuedJobHonorsQueueMaxJobsPerUserQuotasWithContention() 
    throws Exception
    {
        Software software = createSoftware();
        BatchQueue queue = software.getExecutionSystem().getQueue("medium");

        for (int i=0;i<queue.getMaxUserJobs();i++)
        {
            // push the user to their quota for the system and queue.
            createJob(JobStatusType.RUNNING, software, software.getExecutionSystem(), queue, "user-0");
        }

        // add a pending job so there is something to be returned had the
        // user not been over quota
        createJob(JobStatusType.PENDING, software, software.getExecutionSystem(), queue, "user-0");
        createJob(JobStatusType.STAGED, software, software.getExecutionSystem(), queue, "user-0");
        
//        for (int i=0;i<10; i++) {
            for (int j=1;j<TEST_USER_COUNT; j++) {
                String username = "user-" + j;
                for (JobStatusType testStatus: new JobStatusType[] { JobStatusType.PENDING, JobStatusType.STAGED }) {
                    Job underQuotaJob = createJob(testStatus, software, software.getExecutionSystem(), queue, username);
                    
                    String selectedUser = JobDao.getRandomUserForNextQueuedJobOfStatus(testStatus, null, null, null);             
                    Assert.assertNotNull(selectedUser, "User " + testStatus + " job should cause them to be returned when another user is over quota.");
                    Assert.assertEquals(selectedUser, username, "User " + testStatus + " job should always be returned when all other users are over quota.");
                    
                    JobDao.delete(underQuotaJob);
                }
            }
//        }
    }
	
	/**
     * Tests no user will not be selected in the scheduling decision if
     * the queue is in violation of its max job quota
     *  
     * @throws Exception
     */
    @Test(enabled=false)
    public void getRandomUserForNextQueuedJobHonorsQueueMaxJobsQuotas() 
    throws Exception
    {
        Software software = createSoftware();
        BatchQueue queue = software.getExecutionSystem().getQueue("long");

        for (int i=0;i<queue.getMaxJobs();i++)
        {
            // push the user to their quota for the system and queue.
            createJob(JobStatusType.RUNNING, software, software.getExecutionSystem(), queue, "user-" + RandomUtils.nextInt(TEST_USER_COUNT));
        }

        for (int i=0;i<10; i++) {
            for (int j=1;j<TEST_USER_COUNT; j++) {
                String username = "user-" + j;
                
                for (JobStatusType testStatus: new JobStatusType[] { JobStatusType.PENDING, JobStatusType.STAGED }) {
                    Job underQuotaJob = createJob(testStatus, software, software.getExecutionSystem(), queue, username);
                    
                    String selectedUser = JobDao.getRandomUserForNextQueuedJobOfStatus(testStatus, null, null, null);             
                    Assert.assertNull(selectedUser, "User should not be returned when they have a " + testStatus + 
                            " job and the queue is over quota with other user jobs.");
                    
                    JobDao.delete(underQuotaJob);
                }
            }
        }
    }
    
    /**
     * Tests the user will not be selected in the scheduling decision if
     * they are in violation of system user job quota
     *  
     * @throws Exception
     */
    @Test(enabled=true)
    public void getRandomUserForNextQueuedJobHonorsSystemMaxJobsQuotas() 
    throws Exception
    {
        Software quotaSoftware = createSoftware();
        BatchQueue queue = quotaSoftware.getExecutionSystem().getQueue("long");

        for (int i=0;i<queue.getMaxJobs();i++)
        {
            // push the user to their quota for the system and queue.
            createJob(JobStatusType.RUNNING, quotaSoftware, quotaSoftware.getExecutionSystem(), queue, "user-" + RandomUtils.nextInt(TEST_USER_COUNT));
        }

        try {
            // create a new execution system with a system-wide user quota for this test
            ExecutionSystem quotaSystem = quotaSoftware.getExecutionSystem();
            quotaSystem.setMaxSystemJobsPerUser(3); // we will max this out with running user jobs
            quotaSystem.setMaxSystemJobs(100);
            systemDao.persist(quotaSystem);

            List<Job> testJobs = new ArrayList<Job>();

            // verify the quota holds on a single queue when the queue user limit is greater than
            // the system limit
            for (int i=0;i<quotaSystem.getMaxSystemJobs();i++)
            {
                Job runningJob = createJob(JobStatusType.RUNNING, quotaSoftware, quotaSoftware.getExecutionSystem(), queue, "user-0");
                testJobs.add(runningJob);
            }

            for (JobStatusType testStatus: new JobStatusType[] { JobStatusType.PENDING, JobStatusType.STAGED }) {
                Job underQuotaJob = createJob(testStatus, quotaSoftware, quotaSoftware.getExecutionSystem(), queue, "user-0");

                String selectedUser = JobDao.getRandomUserForNextQueuedJobOfStatus(testStatus, null, null, null);
                Assert.assertNull(selectedUser, "User should not be returned when they have a " + testStatus + " job and they have exceeded the system user quota.");
                JobDao.delete(underQuotaJob);
            }
            
        } catch (Exception e) {
            Assert.fail("No exception should be thrown selecting next queued job", e);
        }
    }
}
