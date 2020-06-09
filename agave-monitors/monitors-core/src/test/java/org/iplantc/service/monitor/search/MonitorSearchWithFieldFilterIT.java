package org.iplantc.service.monitor.search;

import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.common.Settings;
import org.iplantc.service.monitor.AbstractMonitorIT;
import org.iplantc.service.monitor.dao.MonitorDao;
import org.iplantc.service.monitor.model.Monitor;
import org.iplantc.service.monitor.search.MonitorSearchFilter;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.*;

import java.text.SimpleDateFormat;
import java.util.*;

@Test(groups = {"integration"})
public class MonitorSearchWithFieldFilterIT extends AbstractMonitorIT {

    private MonitorDao monitorDao = new MonitorDao();

    @AfterMethod
    public void afterMethod() throws Exception {
        clearNotifications();
        clearMonitors();
        clearSystems();
    }

    @BeforeMethod
    public void beforeMethod() throws Exception {
        initSystems();
    }

    @DataProvider(name = "monitorsProvider")
    protected Object[][] monitorsProvider() throws Exception {
        Monitor monitor = createStorageMonitor();

        return new Object[][]{
                {"active", monitor.isActive()},
                {"frequency", monitor.getFrequency()},
                {"id", monitor.getUuid()},
                {"internalusername", monitor.getInternalUsername()},
                {"owner", monitor.getOwner()},
                {"target", monitor.getSystem().getSystemId()},
                {"updatesystemstatus", monitor.isUpdateSystemStatus()},
        };
    }

    @Test(dataProvider = "monitorsProvider")
    public void findMatching(String attribute, Object value) throws Exception {
        Monitor monitor = createStorageMonitor();
        monitorDao.persist(monitor);
        Assert.assertNotNull(monitor.getId(), "Failed to generate a monitor ID.");

        Map<String, String> map = new HashMap<String, String>();
        if (StringUtils.equals(attribute, "id")) {
            map.put(attribute, monitor.getUuid());
        } else if (StringUtils.equals(attribute, "target")) {
            map.put(attribute, monitor.getSystem().getSystemId());
        } else {
            map.put(attribute, String.valueOf(value));
        }

        List<Monitor> monitors = monitorDao.findMatching(monitor.getOwner(), new MonitorSearchFilter().filterCriteria(map), Settings.DEFAULT_PAGE_SIZE, 0, true);
        Assert.assertNotNull(monitors, "findMatching failed to find any monitor.");
        Assert.assertEquals(monitors.size(), 1, "findMatching returned the wrong number of monitor for search by " + attribute);
        Assert.assertEquals(monitors.get(0).getUuid(), monitor.getUuid(), "findMatching did not return the saved monitor.");
    }

    @Test(dataProvider = "monitorsProvider")//, dependsOnMethods = {"findMatching"})
    public void findMatchingTime(String attribute, Object value) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        Monitor monitor = createExecutionMonitor();
        monitorDao.persist(monitor);
        Assert.assertNotNull(monitor.getId(), "Failed to generate a monitor ID.");

        Map<String, String> map = new HashMap<String, String>();
        map.put("lastupdated.ON", formatter.format(monitor.getLastUpdated()));

        List<Monitor> monitors = monitorDao.findMatching(monitor.getOwner(), new MonitorSearchFilter().filterCriteria(map), Settings.DEFAULT_PAGE_SIZE, 0, true);
        Assert.assertNotNull(monitors, "findMatching failed to find any monitors matching starttime.");
        Assert.assertEquals(monitors.size(), 1, "findMatching returned the wrong number of monitor records for search by starttime");
        Assert.assertEquals(monitors.get(0).getUuid(), monitor.getUuid(), "findMatching did not return the saved monitor when searching by starttime.");

        map.clear();
        map.put("created.ON", formatter.format(monitor.getCreated()));

        monitors = monitorDao.findMatching(monitor.getOwner(), new MonitorSearchFilter().filterCriteria(map), Settings.DEFAULT_PAGE_SIZE, 0, true);
        Assert.assertNotNull(monitors, "findMatching failed to find any monitors matching created.");
        Assert.assertEquals(monitors.size(), 1, "findMatching returned the wrong number of monitor records for search by created");
        Assert.assertEquals(monitors.get(0).getUuid(), monitor.getUuid(), "findMatching did not return the saved monitor when searching by created.");
    }

    @Test(dataProvider = "monitorsProvider")//, dependsOnMethods = {"findMatchingTime"})
    public void findMatchingCaseInsensitive(String attribute, Object value) throws Exception {
        Monitor monitor = createStorageMonitor();

        monitorDao.persist(monitor);
        Assert.assertNotNull(monitor.getId(), "Failed to generate a monitor ID.");

        Map<String, String> map = new HashMap<String, String>();
        if (StringUtils.equals(attribute, "id")) {
            map.put(attribute, monitor.getUuid());
        } else if (StringUtils.equals(attribute, "target")) {
            map.put(attribute, monitor.getSystem().getSystemId());
        } else {
            map.put(attribute, String.valueOf(value));
        }

        List<Monitor> monitors = monitorDao.findMatching(monitor.getOwner(), new MonitorSearchFilter().filterCriteria(map), Settings.DEFAULT_PAGE_SIZE, 0, true);
        Assert.assertNotNull(monitors, "findMatching failed to find any monitors.");
        Assert.assertEquals(monitors.size(), 1, "findMatching returned the wrong number of monitor records for search by " + attribute);
        Assert.assertEquals(monitors.get(0).getUuid(), monitor.getUuid(), "findMatching did not return the saved monitor.");
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

    @Test(dataProvider = "dateSearchExpressionTestProvider")//, dependsOnMethods = {"findMatchingCaseInsensitive"})
    public void dateSearchExpressionTest(String attribute, String dateFormattedString, boolean shouldSucceed) throws Exception {
        Monitor monitor = createExecutionMonitor();
        monitor.setCreated(new DateTime().minusYears(5).toDate());
        monitorDao.persist(monitor);
        Assert.assertNotNull(monitor.getId(), "Failed to generate a monitor ID.");

        Map<String, String> map = new HashMap<String, String>();
        map.put(attribute, dateFormattedString);

        try {
            List<Monitor> monitors = monitorDao.findMatching(monitor.getOwner(), new MonitorSearchFilter().filterCriteria(map), Settings.DEFAULT_PAGE_SIZE, 0, true);
            Assert.assertNotEquals(monitors == null, shouldSucceed, "Searching by date string of the format "
                    + dateFormattedString + " should " + (shouldSucceed ? "" : "not ") + "succeed");
            if (shouldSucceed) {
                Assert.assertEquals(monitors.size(), 1, "findMatching returned the wrong number of monitor records for search by "
                        + attribute + "=" + dateFormattedString);
                Assert.assertEquals(monitors.get(0).getUuid(), monitor.getUuid(), "findMatching did not return the saved monitor.");
            }
        } catch (Exception e) {
            if (shouldSucceed) {
                Assert.fail("Searching by date string of the format "
                        + dateFormattedString + " should " + (shouldSucceed ? "" : "not ") + "succeed", e);
            }
        }
    }

}
