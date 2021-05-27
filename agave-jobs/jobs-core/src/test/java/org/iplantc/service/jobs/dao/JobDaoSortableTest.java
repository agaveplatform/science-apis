package org.iplantc.service.jobs.dao;

import org.iplantc.service.apps.model.Software;
import org.iplantc.service.common.search.AgaveResourceResultOrdering;
import org.iplantc.service.common.search.SearchTerm;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.search.JobSearchFilter;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

@Test(groups="integration")
public class JobDaoSortableTest extends AbstractDaoTest {

    @DataProvider
    public static Object[][] getByUsernameSortsByFieldProvider() {
        return new Object[][]{
                { "lastupdated",  AgaveResourceResultOrdering.DESC }
        };
    }

    @Test(dataProvider="getByUsernameSortsByFieldProvider")
    public void getByUsernameSortsByField(String orderBy, AgaveResourceResultOrdering order) throws Exception
    {
        Software software = createSoftware();
        Job job = createJob(JobStatusType.PENDING, software);

        JobDao.persist(job);
        Assert.assertNotNull(job.getId(), "Failed to generate a job ID.");

        Assert.assertFalse(job.getEvents().isEmpty(), "Job event was not saved.");
        Assert.assertEquals(job.getEvents().size(), 1, "Incorrect number of job events after saving");

        JobSearchFilter searchFilter = new JobSearchFilter();

        SearchTerm jobSearchTerm = searchFilter.filterAttributeName(orderBy);
        List<Job> results = JobDao.getByUsername(job.getOwner(), 0, 1000, order, jobSearchTerm);
        Assert.assertFalse(results.isEmpty(), "Result for search by " + orderBy + " " + order.name() + " should not be empty.");
        Assert.assertTrue(results.contains(job), "Test job should be in result set.");

    }
}
