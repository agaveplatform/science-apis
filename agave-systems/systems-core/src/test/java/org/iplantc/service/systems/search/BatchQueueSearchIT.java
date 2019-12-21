package org.iplantc.service.systems.search;

import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.systems.dao.BatchQueueDao;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.JSONTestDataUtil;
import org.iplantc.service.systems.model.PersistedSystemsModelTestCommon;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = {"integration", "broken"})
public class BatchQueueSearchIT extends PersistedSystemsModelTestCommon {

    private BatchQueueDao batchQueueDao = new BatchQueueDao();
    private SystemDao systemDao = new SystemDao();

    @BeforeClass
    @Override
    public void beforeClass() throws Exception {
        super.beforeClass();
    }

//	@AfterClass
//	@Override
//	public void afterClass() throws Exception
//	{
//		super.afterClass();
//	}

    @BeforeMethod
    public void setUp() throws Exception {

    }

    @AfterMethod
    public void afterMethod() throws Exception {
        clearSystems();
    }

    private ExecutionSystem createExecutionSystem() throws Exception {
        ExecutionSystem system = ExecutionSystem.fromJSON(jtd.getTestDataObject(JSONTestDataUtil.TEST_PROXY_EXECUTION_SYSTEM_FILE));
        system.setOwner(SYSTEM_OWNER);

        BatchQueue normalQueue = new BatchQueue("normal", 10L, 5L, 1L, 256d, 1L, "01:00:00", "", true);
        BatchQueue smallQueue = new BatchQueue("small", 5L, 1L, 1L, 128d, 1L, "00:15:00", "", false);
        BatchQueue largeQueue = new BatchQueue("large", 100L, 50L, 10L, 1024d, 16L, "12:00:00", "", false);
        system.setBatchQueues(Set.of(normalQueue, smallQueue, largeQueue));

        return system;
    }

    @DataProvider
    public Object[][] searchFieldProvider() throws Exception {
        ExecutionSystem system = createExecutionSystem();
        BatchQueue defaultQueue = system.getDefaultQueue();

        return new Object[][]{

//		        { "created", new DateTime(defaultQueue.getCreated()).toString(), defaultQueue.getName() },
                {"customdirectives", defaultQueue.getCustomDirectives(), defaultQueue.getName()},
                {"default", defaultQueue.isSystemDefault(), defaultQueue.getName()},
                {"description", defaultQueue.getDescription(), defaultQueue.getDescription()},
//                { "lastupdated", new DateTime(system.getLastUpdated()).toString(), defaultQueue.getName() },
                {"mappedName", defaultQueue.getMappedName(), defaultQueue.getMappedName()},
                {"maxjobs", defaultQueue.getMaxJobs(), defaultQueue.getName()},
                {"maxmemorypernode", defaultQueue.getMaxMemoryPerNode(), defaultQueue.getName()},
                {"maxnodes", defaultQueue.getMaxNodes(), defaultQueue.getName()},
                {"maxprocessorspernode", defaultQueue.getMaxProcessorsPerNode(), defaultQueue.getName()},
                {"maxrequestedtime", defaultQueue.getMaxRequestedTime(), defaultQueue.getName()},
                {"maxuserjobs", defaultQueue.getMaxUserJobs(), defaultQueue.getName()},
                {"name", defaultQueue.getName(), defaultQueue.getName()},
                {"uuid", defaultQueue.getUuid(), defaultQueue.getName()},
        };
    }

    @Test(dataProvider = "searchFieldProvider")
    public void findMatching(String attribute, Object value, String expectedQueueName) throws Exception {
        ExecutionSystem system = createExecutionSystem();
        systemDao.persist(system);
        Assert.assertNotNull(system.getId(), "Failed to generate a system ID.");

        Map<String, String> map = new HashMap<String, String>();
        if (StringUtils.equals(attribute, "uuid")) {
            map.put(attribute, system.getUuid());
        } else {
            map.put(attribute, String.valueOf(value));
        }

        List<BatchQueue> queues = batchQueueDao.findMatching(system.getId(), new BatchQueueSearchFilter().filterCriteria(map));
        Assert.assertNotNull(queues, "findMatching failed to find any system.");
        Assert.assertEquals(queues.size(), 1, "findMatching returned the wrong number of system for search by " + attribute);
        Assert.assertEquals(queues.get(0).getName(), expectedQueueName, "findMatching did not return the saved system.");
    }

    @Test
    public void findMatchingTime() throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

		ExecutionSystem system = createExecutionSystem();
		system.setCreated(new DateTime().minusYears(6).toDate());
		BatchQueue defaultQueue = system.getDefaultQueue();
		for (BatchQueue batchQueue : system.getBatchQueues()) {
			if (batchQueue.getName().equals(defaultQueue.getName())) {
				batchQueue.setCreated(new DateTime().minusYears(5).toDate());
				batchQueue.setLastUpdated(new DateTime().minusYears(5).toDate());
			} else {
				batchQueue.setCreated(new DateTime().minusDays(1).toDate());
				batchQueue.setLastUpdated(new DateTime().minusDays(1).toDate());
			}
		}
		systemDao.persist(system);

		Assert.assertNotNull(system.getId(), "Failed to generate a system ID.");

        Map<String, String> map = new HashMap<String, String>();
        map.put("lastupdated", formatter.format(system.getCreated()));

        List<BatchQueue> queues = batchQueueDao.findMatching(system.getId(), new BatchQueueSearchFilter().filterCriteria(map));
        Assert.assertNotNull(queues, "findMatching failed to find any systems matching lastupdated.");
        Assert.assertEquals(queues.size(), 1, "findMatching returned the wrong number of system records for search by lastupdated");
        Assert.assertEquals(queues.get(0).getName(), defaultQueue.getName(), "findMatching did not return the saved system when searching by lastupdated.");

        map.clear();
        map.put("created", formatter.format(system.getCreated()));

        queues = batchQueueDao.findMatching(system.getId(), new BatchQueueSearchFilter().filterCriteria(map));
        Assert.assertNotNull(queues, "findMatching failed to find any systems matching created.");
        Assert.assertEquals(queues.size(), 1, "findMatching returned the wrong number of system records for search by created");
        Assert.assertEquals(queues.get(0).getName(), defaultQueue.getName(), "findMatching did not return the saved system when searching by created.");
    }

    @Test(dataProvider = "searchFieldProvider")
    public void findMatchingCaseInsensitive(String attribute, Object value, String expectedQueueName) throws Exception {
        ExecutionSystem system = createExecutionSystem();
        if (StringUtils.equalsIgnoreCase(attribute, "privateonly")) {
            system.setPubliclyAvailable(false);
        } else if (StringUtils.equalsIgnoreCase(attribute, "publiconly")) {
            system.setPubliclyAvailable(true);
        }
        systemDao.persist(system);
        Assert.assertNotNull(system.getId(), "Failed to generate a system ID.");

        Map<String, String> map = new HashMap<String, String>();
        if (StringUtils.equals(attribute, "uuid")) {
            map.put(attribute, system.getUuid());
        } else {
            map.put(attribute, String.valueOf(value));
        }

        List<BatchQueue> queues = batchQueueDao.findMatching(system.getId(), new BatchQueueSearchFilter().filterCriteria(map));
        Assert.assertNotNull(queues, "findMatching failed to find any systems.");
        Assert.assertEquals(queues.size(), 1, "findMatching returned the wrong number of system records for search by " + attribute);
        Assert.assertEquals(queues.get(0).getName(), expectedQueueName, "findMatching did not return the saved system.");
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

    @Test(dataProvider = "dateSearchExpressionTestProvider")
    public void dateSearchExpressionTest(String attribute, String dateFormattedString, boolean shouldSucceed) throws Exception {
        ExecutionSystem system = createExecutionSystem();
        system.setCreated(new DateTime().minusYears(6).toDate());
        BatchQueue defaultQueue = system.getDefaultQueue();
        for (BatchQueue batchQueue : system.getBatchQueues()) {
            if (batchQueue.getName().equals(defaultQueue.getName())) {
                batchQueue.setCreated(new DateTime().minusYears(5).toDate());
                batchQueue.setLastUpdated(new DateTime().minusYears(5).toDate());
            } else {
                batchQueue.setCreated(new DateTime().minusDays(1).toDate());
                batchQueue.setLastUpdated(new DateTime().minusDays(1).toDate());
            }
        }
        systemDao.persist(system);

        Assert.assertTrue(system.getId() > 0, "Failed to generate a system ID.");

        Map<String, String> map = new HashMap<String, String>();
        map.put(attribute, dateFormattedString);

        try {
            List<BatchQueue> queues = batchQueueDao.findMatching(system.getId(),
                    new BatchQueueSearchFilter().filterCriteria(map));

            Assert.assertNotNull(queues, "findMatching should never return null");

            if (shouldSucceed) {
                Assert.assertEquals(queues.size(), 1,
                        "findMatching returned " + queues.size() + " software records for search by "
                                + attribute + "=" + dateFormattedString + " when 1 should have been returned.");
                Assert.assertEquals(queues.get(0).getName(), defaultQueue.getName(),
                        "findMatching did not return the test software record for search by " +
                                attribute + "=" + dateFormattedString);
            } else {
                Assert.assertEquals(queues.size(), 1,
                        "findMatching returned software records for search by "
                                + attribute + "=" + dateFormattedString +
                                " when none should have been returned.");
            }
        } catch (Exception e) {
            if (shouldSucceed) {
                Assert.fail("Searching by date string of the format "
                        + dateFormattedString + " should " + (shouldSucceed ? "" : "not ") + "succeed", e);
            }
        }
    }

}
