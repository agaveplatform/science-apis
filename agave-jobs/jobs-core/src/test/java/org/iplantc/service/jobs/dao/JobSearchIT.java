package org.iplantc.service.jobs.dao;

import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.search.SearchTerm;
import org.iplantc.service.common.util.StringToTime;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.dto.JobDTO;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.search.JobSearchFilter;
import org.iplantc.service.profile.model.enumeration.SearchFieldType;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(singleThreaded = true, groups = {"integration"})
public class JobSearchIT extends AbstractDaoTest {

    private static final Logger log = Logger.getLogger(JobSearchIT.class);
    private final String[] DATETIME_FIELDS = new String[]{"created", "lastUpdated", "submittime", "starttime", "lastupdated", "lastmodified", "endtime"};

    @DataProvider(name = "searchJobsByDerivedRunTimeProvider", parallel = false)
    protected Object[][] searchJobsByDerivedRunTimeProvider() throws Exception {
        List<Object[]> searchCriteria = new ArrayList<Object[]>();

//        for (JobStatusType status: JobStatusType.values()) 
        {
            JobStatusType status = JobStatusType.FINISHED;
            searchCriteria.add(new Object[]{status, "runtime.lte", true, "Searching by less than or equal to  known exact runtime should not fail"});

            if (status.equals(JobStatusType.RUNNING) || status.equals(JobStatusType.PAUSED) || status.equals(JobStatusType.STOPPED) ||
                    status.equals(JobStatusType.ARCHIVING) || status.equals(JobStatusType.ARCHIVING_FAILED) || status.equals(JobStatusType.ARCHIVING_FINISHED) ||
                    status.equals(JobStatusType.FAILED) || status.equals(JobStatusType.FINISHED) || status.equals(JobStatusType.CLEANING_UP) ||
                    status.equals(JobStatusType.KILLED)) {
                searchCriteria.add(new Object[]{status, "runtime", false, "Searching by known exact runtime on a job that has not started should not fail"});
                searchCriteria.add(new Object[]{status, "runtime.eq", false, "Searching by known exact runtime on a job that has not started should not fail"});
                searchCriteria.add(new Object[]{status, "runtime.lte", true, "Searching by less than or equal to known exact runtime on a job that has not started should succeed"});
                searchCriteria.add(new Object[]{status, "runtime.lt", true, "Searching less than known exact runtime on a job that has not started should succeed"});
                searchCriteria.add(new Object[]{status, "runtime.gt", false, "Searching runtime strictly greater than than actual amount on a job that has not started should fail"});
                searchCriteria.add(new Object[]{status, "runtime.in", false, "Searching runtime with exact value in range on a job that has not started should not fail"});
                searchCriteria.add(new Object[]{status, "runtime.gte", false, "Searching by greater than or equal to known exact runtime on a job that has not started should not fail"});
            } else {
                searchCriteria.add(new Object[]{status, "runtime", true, "Searching by old runtime of unfinished job should fail"});
                searchCriteria.add(new Object[]{status, "runtime.eq", true, "Searching by old runtime of unfinished job should fail"});
                searchCriteria.add(new Object[]{status, "runtime.ge", true, "Searching by greater than or equal to old runtime of unfinished job should fail"});
                searchCriteria.add(new Object[]{status, "runtime.lt", false, "Searching by less than old runtime of unfinished job should succeed"});
                searchCriteria.add(new Object[]{status, "runtime.lte", true, "Searching by less than or equal to  known exact runtime should succeed"});
                searchCriteria.add(new Object[]{status, "runtime.gt", false, "Searching by value greater than or equal to old runtime of unfinished job should fail"});
                searchCriteria.add(new Object[]{status, "runtime.in", true, "Searching by range with values greater than or equal to old runtime of unfinished job should fail"});
            }
        }
        ;

        return searchCriteria.toArray(new Object[][]{});
    }

    @Test(dataProvider = "searchJobsByDerivedRunTimeProvider")
    public void searchJobsByDerivedRunTime(JobStatusType status, String searchField, boolean shouldSucceed, String message) throws Exception {
        Software software = createSoftware();
        Job job = createJob(status, software);
        Assert.assertNotNull(job.getId(), "Failed to generate a job ID.");

//        long endTime = (job.getEndTime() == null ? new Date().getTime() : job.getEndTime().getTime());
//        Integer searchValue = Math.round((endTime - job.getStartTime().getTime()) / 1000);
        int searchValue = (int) ((new DateTime(job.getEndTime()).getMillis() - new DateTime(job.getStartTime()).getMillis()) / 1000);
        log.debug("Searching for " + searchField + "=" + String.valueOf(searchValue) + " (" + JobDao.getJobRunTime(job.getUuid()) + ")");

        Map<String, String> map = new HashMap<String, String>();
        map.put(searchField, String.valueOf(searchValue));

        List<JobDTO> searchJobs = JobDao.findMatching(job.getOwner(), new JobSearchFilter().filterCriteria(map));

        if (shouldSucceed) {
            Assert.assertEquals(searchJobs.size(), 1, "findMatching returned the wrong number of jobs for search by " + searchField);
            Assert.assertEquals(searchJobs.get(0).getUuid(), job.getUuid(), "findMatching did not return the saved job when searching by runtime");
        } else {
            Assert.assertTrue(searchJobs.isEmpty(), "findMatching should return no jobs for search by " + searchField);
        }
    }

    @DataProvider(name = "searchJobsByDerivedWallTimeProvider")
    protected Object[][] searchJobsByDerivedTimeProvider() throws Exception {
        List<Object[]> searchCriteria = new ArrayList<Object[]>();

//	    for (JobStatusType status: JobStatusType.values()) 
        {
            JobStatusType status = JobStatusType.FINISHED;

            searchCriteria.add(new Object[]{status, "walltime.lte", true, "Searching by less than or equal to  known exact walltime should not fail"});

            if (JobStatusType.isFinished(status)) {
                searchCriteria.add(new Object[]{status, "walltime", true, "Searching by known exact walltime should not fail"});
                searchCriteria.add(new Object[]{status, "walltime.eq", true, "Searching by known exact walltime should not fail"});
                searchCriteria.add(new Object[]{status, "walltime.lte", true, "Searching by less than or equal to  known exact walltime should not fail"});
                searchCriteria.add(new Object[]{status, "walltime.lt", false, "Searching walltime less than actual amount should fail"});
                searchCriteria.add(new Object[]{status, "walltime.gt", false, "Searching walltime strictly greater than than actual amount should fail"});
                searchCriteria.add(new Object[]{status, "walltime.in", true, "Searching walltime with exact value in range should not fail"});
                searchCriteria.add(new Object[]{status, "walltime.gte", true, "Searching by greater than or equal to known exact walltime should not fail"});
            } else {
                searchCriteria.add(new Object[]{status, "walltime", false, "Searching by old walltime of unfinished job should fail"});
                searchCriteria.add(new Object[]{status, "walltime.eq", false, "Searching by old walltime of unfinished job should fail"});
                searchCriteria.add(new Object[]{status, "walltime.gte", false, "Searching by greater than or equal to old walltime of unfinished job should fail"});
                searchCriteria.add(new Object[]{status, "walltime.lt", true, "Searching by less than old walltime of unfinished job should succeed"});
                searchCriteria.add(new Object[]{status, "walltime.gt", false, "Searching by value greater than or equal to old walltime of unfinished job should fail"});
                searchCriteria.add(new Object[]{status, "walltime.in", false, "Searching by range with values greater than or equal to old walltime of unfinished job should fail"});
            }
        }
        ;

        return searchCriteria.toArray(new Object[][]{});
    }

    @Test(dataProvider = "searchJobsByDerivedWallTimeProvider", dependsOnMethods = {"searchJobsByDerivedRunTime"})
    public void searchJobsByDerivedWallTime(JobStatusType status, String searchField, boolean shouldSucceed, String message) throws Exception {
        Software software = createSoftware();
        Job job = createJob(status, software);
        JobDao.persist(job);
        Assert.assertNotNull(job.getId(), "Failed to generate a job ID.");

        int searchValue = (int) ((new DateTime(job.getEndTime()).getMillis() - new DateTime(job.getCreated()).getMillis()) / 1000);
        log.debug("Searching for " + searchField + "=" + String.valueOf(searchValue) + " (" + JobDao.getJobWallTime(job.getUuid()) + ")");

        Map<String, String> map = new HashMap<String, String>();
        map.put(searchField, String.valueOf(searchValue));

        List<JobDTO> searchJobs = JobDao.findMatching(job.getOwner(), new JobSearchFilter().filterCriteria(map));
//        Assert.assertEquals(searchJobs == null, shouldSucceed, message);

        if (shouldSucceed) {
            Assert.assertEquals(searchJobs.size(), 1, "findMatching returned the wrong number of jobs for search by " + searchField);
            Assert.assertEquals(searchJobs.get(0).getUuid(), job.getUuid(), "findMatching did not return the saved job when searching by walltime");
        } else {
            Assert.assertTrue(searchJobs.isEmpty(), "findMatching should return no jobs for search by " + searchField);
        }

    }

    @DataProvider(name = "searchJobsProvider")
    protected Object[][] searchJobsProvider() throws Exception {
        Software software = createSoftware();
        try {
            Job job = createJob(JobStatusType.FINISHED, software);

            return new Object[][]{
                    {"id", job.getUuid()},
                    {"appid", job.getSoftwareName()},
                    {"archive", job.isArchiveOutput()},
                    {"archivepath", job.getArchivePath()},
                    {"archivesystem", job.getArchiveSystem().getSystemId()},
                    {"batchqueue", job.getBatchQueue()},
                    {"executionsystem", job.getSystem()},
                    {"inputs", job.getInputs()},
//			{ "localId", job.getLocalJobId() },
                    {"maxruntime", job.getMaxRunTime()},
                    {"memorypernode", job.getMemoryPerNode()},
                    {"name", job.getName()},
                    {"nodecount", job.getNodeCount()},
                    {"outputpath", job.getOutputPath()},
                    {"owner", job.getOwner()},
                    {"parameters", job.getParameters()},
                    {"processorspernode", job.getProcessorsPerNode()},
                    {"retries", job.getRetries()},
                    {"visible", job.isVisible()},
            };
        }
        finally {
            super.afterMethod();
        }
    }

    @Test(dataProvider = "searchJobsProvider", dependsOnMethods = {"searchJobsByDerivedWallTime"})
    public void findMatching(String attribute, Object value) throws Exception {
        Software software = createSoftware();
        Job job = createJob(JobStatusType.FINISHED, software);
        Assert.assertNotNull(job.getId(), "Failed to generate a job ID.");

        Map<String, String> map = new HashMap<String, String>();
        if (org.apache.commons.lang3.StringUtils.equals(attribute, "appid")) {
            map.put(attribute, software.getUniqueName());
        } else if (org.apache.commons.lang3.StringUtils.equals(attribute, "archivesystem")) {
            map.put(attribute, job.getArchiveSystem().getSystemId());
        } else if (org.apache.commons.lang3.StringUtils.equals(attribute, "executionsystem")) {
            map.put(attribute, software.getExecutionSystem().getSystemId());
        } else if (org.apache.commons.lang3.StringUtils.equals(attribute, "name")) {
            map.put(attribute, job.getName());
        } else if (org.apache.commons.lang3.StringUtils.equals(attribute, "id")) {
            map.put(attribute, job.getUuid());
        } else {
            map.put(attribute, String.valueOf(value));
        }

        List<JobDTO> searchJobs = JobDao.findMatching(job.getOwner(), new JobSearchFilter().filterCriteria(map));
        Assert.assertNotNull(searchJobs, "findMatching failed to find any jobs.");
        Assert.assertEquals(searchJobs.size(), 1, "findMatching returned the wrong number of jobs for search by " + attribute);
        JobDTO dto = new JobDTO(job);
        Assert.assertTrue(searchJobs.contains(dto), "findMatching did not return the saved job.");
    }

    @DataProvider(parallel = false)
    protected Object[][] findMatchingTimeProvider() throws Exception {

        return new Object[][]{
                {null, "+0 second", true},
                {SearchTerm.Operator.AFTER.name(), "yesterday", true},
                {SearchTerm.Operator.AFTER.name(), "-1 second", true},
                {SearchTerm.Operator.AFTER.name(), "+1 second", false},
                {SearchTerm.Operator.AFTER.name(), "tomorrow", false},

                {SearchTerm.Operator.BEFORE.name(), "tomorrow", true},
                {SearchTerm.Operator.BEFORE.name(), "-2 second", false},
                {SearchTerm.Operator.BEFORE.name(), "+2 second", true},
                {SearchTerm.Operator.BEFORE.name(), "yesterday", false},

                {SearchTerm.Operator.EQ.name(), "yesterday", false},
                {SearchTerm.Operator.EQ.name(), "tomorrow", false},
                {SearchTerm.Operator.EQ.name(), "-1 hour", false},
                {SearchTerm.Operator.EQ.name(), "+1 hour", false},

                {SearchTerm.Operator.GT.name(), "yesterday", true},
                {SearchTerm.Operator.GT.name(), "-2 second", true},
                {SearchTerm.Operator.GT.name(), "+2 second", false},
                {SearchTerm.Operator.GT.name(), "tomorrow", false},

                {SearchTerm.Operator.GTE.name(), "yesterday", true},
                {SearchTerm.Operator.GTE.name(), "-2 second", true},
                {SearchTerm.Operator.GTE.name(), "+2 second", false},
                {SearchTerm.Operator.GTE.name(), "tomorrow", false},


                {SearchTerm.Operator.LT.name(), "tomorrow", true},
                {SearchTerm.Operator.LT.name(), "-2 second", false},
                {SearchTerm.Operator.LT.name(), "+2 second", true},
                {SearchTerm.Operator.LT.name(), "yesterday", false},

                {SearchTerm.Operator.LTE.name(), "tomorrow", true},
                {SearchTerm.Operator.LTE.name(), "-2 second", false},
                {SearchTerm.Operator.LTE.name(), "+2 second", true},
                {SearchTerm.Operator.LTE.name(), "yesterday", false},

                {SearchTerm.Operator.ON.name(), "yesterday", false},
                {SearchTerm.Operator.ON.name(), "tomorrow", false},
        };
    }

    @Test(dataProvider = "findMatchingTimeProvider", singleThreaded = true, dependsOnMethods = {"findMatching"})
    public void findMatchingCreatedTime(String operator, String relativeTimePhrase, boolean shouldMatch) throws Exception {
        Software software = createSoftware();
        Job job = createJob(JobStatusType.CLEANING_UP, software);

        Assert.assertNotNull(job.getId(), "Failed to generate a job ID.");
//        Job savedJob = JobDao.getByUuid(job.getUuid());
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

//        Assert.assertEquals(savedJob.getCreated().toString(), job.getCreated().toString(), "Job created time changed after saving.");
//        Assert.assertEquals(sdf.format(new DateTime(savedJob.getCreated()).withMillisOfSecond(0).toDate()), sdf.format(job.getCreated()), "Job created time changed after saving.");
//        Assert.assertEquals(savedJob.getCreated(), job.getCreated(), "Job created time changed after saving.");

        String searchTerm = "created" + (operator == null ? "" : "." + operator);

        _doFindMatchingTime(job.getUuid(), job.getOwner(), searchTerm, relativeTimePhrase, job.getCreated(), shouldMatch);
    }

    protected void _doFindMatchingTime(String jobUuid, String jobOwner, String searchTerm, String searchPhrase, Date searchDate, boolean shouldMatch)
            throws Exception {

        // we generate the value here to ensure our query in the resulting SearchTerm
        // is relative to the actual job field date we're testing.
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        StringToTime searchDateTime = new StringToTime(searchPhrase, searchDate);
        Calendar cal = searchDateTime.getCal();

        String searchValue = formatter.format(cal.getTime());

        log.debug("Searching for " + searchTerm + "=" + searchValue + " (" + searchPhrase + " | " + formatter.format(searchDate) + ")");
        Map<String, String> map = new HashMap<String, String>();
        map.put("id.eq", jobUuid);
        map.put(searchTerm, searchValue);

        log.debug("Total active jobs at time of search: " + JobDao.countTotalActiveJobs());
        List<JobDTO> searchJobs = JobDao.findMatching(jobOwner, new JobSearchFilter().filterCriteria(map));
        Assert.assertNotNull(searchJobs, "findMatching should never return null");

        if (shouldMatch) {
            if (searchJobs.size() != 1) {
                Job unreturnedJob = JobDao.getByUuid(jobUuid);
                log.error("No job returned in match: \n\t" +
                        searchTerm + "=" + searchValue +
                        "(" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(unreturnedJob.getCreated()) + ")");
            }
            Assert.assertEquals(searchJobs.size(), 1, "findMatching returned the wrong number of jobs for search by " + searchTerm);
            Assert.assertEquals(searchJobs.get(0).getUuid(), jobUuid, "findMatching did not return the saved job when searching by " + searchTerm);
        } else {
            Assert.assertTrue(searchJobs.isEmpty(), "findMatching should not return any matches for queries out of range.");
        }
    }

    /**
     * Generates null, empty, and blank test cases for each {@link #DATETIME_FIELDS} and each
     * {@link SearchTerm.Operator#temporalValues()}. These should all fail to match any value
     * as null, blank, and empty should not resolve to anything.
     *
     * @return test case data
     */
    @DataProvider(parallel = false)
    protected Object[][] findBlankDateTimeProviderReturnsEmpty() throws Exception {

        ArrayList<Object[]> testCases = new ArrayList<Object[]>();

        for (String dateTimeField : DATETIME_FIELDS) {
            for (SearchTerm.Operator operator: SearchTerm.Operator.temporalValues()) {
                testCases.add(new Object[]{dateTimeField + "." + operator.name(), "", false});
                testCases.add(new Object[]{dateTimeField + "." + operator.name(), null, false});
                testCases.add(new Object[]{dateTimeField + "." + operator.name(), " ", false});
                testCases.add(new Object[]{dateTimeField + "." + operator.name(), "  ", false});
            }
        }

        return testCases.toArray(new Object[][]{});
    }

    /**
     * Generates daily, hourly and secondly tests of the {@link SearchTerm.Operator#ON} operator
     * for each {@link #DATETIME_FIELDS}. These should all match for a given data as they will all
     * be on the same day.
     * @return array of test case data
     */
    @DataProvider(parallel = false)
    protected Object[][] findExactDateTimeProvider() {
        ArrayList<Object[]> testCases = new ArrayList<Object[]>();

        for (String dateTimeField : DATETIME_FIELDS) {
            testCases.add(new Object[]{dateTimeField + "." + SearchTerm.Operator.ON.name(), "-1 hour", true});
            testCases.add(new Object[]{dateTimeField + "." + SearchTerm.Operator.ON.name(), "+1 hour", true});
            testCases.add(new Object[]{dateTimeField + "." + SearchTerm.Operator.ON.name(), "-1 second", true});
            testCases.add(new Object[]{dateTimeField + "." + SearchTerm.Operator.ON.name(), "+1 second", true});
            testCases.add(new Object[]{dateTimeField + "." + SearchTerm.Operator.ON.name(), "today", true});
        }

        return testCases.toArray(new Object[][]{});
    }


    @Test(dataProvider = "findExactDateTimeProvider")//, dependsOnMethods={"findMatchingCreatedTime"} )
    public void findExactCreatedTime(String searchTerm, String relativeTimePhrase, boolean shouldMatch) throws Exception {
        Software software = createSoftware();
        Job job = createJob(JobStatusType.FINISHED, software);
        Assert.assertNotNull(job.getId(), "Failed to generate a job ID.");

        StringToTime searchDate = new StringToTime(relativeTimePhrase, job.getCreated());

        _doFindExactTime(job.getUuid(), job.getOwner(), searchTerm, searchDate, job.getCreated(), shouldMatch);
    }

    protected void _doFindExactTime(String jobUuid, String jobOwner, String searchTerm, StringToTime searchDate, Date jobDate, boolean shouldMatch)
    throws Exception {
        // DB uses GMT, so we need to resolve this against GMT for our queries
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        Calendar cal = searchDate.getCal();
        String searchValue = formatter.format(cal.getTime());

        log.debug("Searching for " + searchTerm + "=" + searchValue + " (" + searchTerm + " | " + formatter.format(searchDate) + ")");

        Map<String, String> map = new HashMap<String, String>();
        map.put("id.eq", jobUuid);
        map.put(searchTerm, searchValue);

        List<JobDTO> searchJobs = JobDao.findMatching(jobOwner, new JobSearchFilter().filterCriteria(map));
        Assert.assertNotNull(searchJobs, "findMatching should never return null");

        if (shouldMatch) {
            Assert.assertEquals(searchJobs.size(), 1, "findMatching returned the wrong number of jobs for search by " + searchTerm);
            Assert.assertEquals(searchJobs.get(0).getUuid(), jobUuid, "findMatching did not return the saved job when searching by " + searchTerm);
        } else {
            if (!searchJobs.isEmpty()) {
                log.error("Incorrect result had created date: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(searchJobs.get(0).getCreated()));
            }
            Assert.assertTrue(searchJobs.isEmpty(), "findMatching should not return any matches for queries out of range.");
        }
    }

    @Test(dataProvider = "searchJobsProvider", dependsOnMethods = {"findMatchingCreatedTime"})
    public void findMatchingCaseInsensitive(String attribute, Object value) throws Exception {
        Software software = createSoftware();
        Job job = createJob(JobStatusType.PENDING, software);
        JobDao.persist(job);
        Assert.assertNotNull(job.getId(), "Failed to generate a job ID.");
        JobDTO jobDTO = new JobDTO(job);

        Map<String, String> map = new HashMap<String, String>();
        map.put(attribute.toUpperCase(), String.valueOf(value));

        List<JobDTO> searchJobs = JobDao.findMatching(job.getOwner(), new JobSearchFilter().filterCriteria(map));
        Assert.assertNotNull(searchJobs, "findMatching failed to find any jobs.");
        Assert.assertEquals(searchJobs.size(), 1, "findMatching returned the wrong number of jobs for search by " + attribute);
        Assert.assertTrue(searchJobs.contains(jobDTO), "findMatching did not return the saved job.");
    }

    @DataProvider
    protected Object[][] dateSearchExpressionTestProvider() {
        List<Object[]> testCases = new ArrayList<Object[]>();
        String[] timeFormats = new String[]{"Ka", "KKa", "K a", "KK a", "K:mm a", "KK:mm a", "Kmm a", "KKmm a", "H:mm", "HH:mm", "HH:mm:ss"};
        String[] sqlDateFormats = new String[]{"YYYY-MM-dd", "YYYY-MM"};
        String[] relativeDateFormats = new String[]{"yesterday", "today", "-1 day", "-1 month", "-1 year", "+1 day", "+1 month"};
        String[] calendarDateFormats = new String[]{"MMMM d", "MMM d", "YYYY-MM-dd", "MMMM d, Y",
                "MMM d, Y", "MMMM d, Y", "MMM d, Y",
                "M/d/Y", "M/dd/Y", "MM/dd/Y", "M/d/YYYY", "M/dd/YYYY", "MM/dd/YYYY"};

        DateTime dateTime = new DateTime().minusMonths(1);
        for (String field : new String[]{"created.before"}) {

            // milliseconds since epoch
            testCases.add(new Object[]{field, "" + dateTime.getMillis(), true});

            // ISO 8601
            testCases.add(new Object[]{field, dateTime.toString(), true});

            // SQL date and time format
            for (String date : sqlDateFormats) {
                for (String time : timeFormats) {
                    testCases.add(new Object[]{field, dateTime.toString(date + " " + time), date.contains("dd") && !time.contains("Kmm")});
                }
                testCases.add(new Object[]{field, dateTime.toString(date), true});
            }

            // Relative date formats
            for (String date : relativeDateFormats) {
                for (String time : timeFormats) {
                    testCases.add(new Object[]{field, date + " " + dateTime.toString(time), !(!(date.contains("month") || date.contains("year") || date.contains(" day")) && time.contains("Kmm"))});
                }
                testCases.add(new Object[]{field, date, true});
            }


            for (String date : calendarDateFormats) {
                for (String time : timeFormats) {
                    testCases.add(new Object[]{field, dateTime.toString(date + " " + time), !(date.contains("d") && time.contains("Kmm"))});
                }
                testCases.add(new Object[]{field, dateTime.toString(date), true});
            }

        }

        return testCases.toArray(new Object[][]{});
    }

    @Test(dataProvider = "dateSearchExpressionTestProvider", dependsOnMethods = {"findMatchingCaseInsensitive"}, enabled = true)
    public void dateSearchExpressionTest(String attribute, String dateFormattedString, boolean shouldSucceed) throws Exception {
        Software software = createSoftware();
        Job job = createJob(JobStatusType.ARCHIVING, software);
        job.setCreated(new DateTime().minusYears(5).toDate());
        JobDao.persist(job);
        Assert.assertNotNull(job.getId(), "Failed to generate a job ID.");

        Map<String, String> map = new HashMap<String, String>();
        map.put(attribute, dateFormattedString);

        try {
            List<JobDTO> searchJobs = JobDao.findMatching(job.getOwner(), new JobSearchFilter().filterCriteria(map));
            Assert.assertNotEquals(searchJobs == null, shouldSucceed, "Searching by date string of the format "
                    + dateFormattedString + " should " + (shouldSucceed ? "" : "not ") + "succeed");
            if (shouldSucceed) {
                Assert.assertEquals(searchJobs.size(), 1, "findMatching returned the wrong number of jobs for search by "
                        + attribute + "=" + dateFormattedString);
                JobDTO dto = new JobDTO(job);
                Assert.assertTrue(searchJobs.contains(dto), "findMatching did not return the saved job.");
            }
        } catch (Exception e) {
            if (shouldSucceed) {
                Assert.fail("Searching by date string of the format "
                        + dateFormattedString + " should " + (shouldSucceed ? "" : "not ") + "succeed", e);
            }
        }
    }

}
