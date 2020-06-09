package org.iplantc.service.apps.search;

import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_OWNER;
import static org.iplantc.service.apps.model.JSONTestDataUtil.TEST_SOFTWARE_SYSTEM_FILE;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.apps.dao.AbstractDaoTest;
import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.search.SearchTerm;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups={"integration"}, singleThreaded = true)
public class SoftwareSearchIT extends AbstractDaoTest {

	@BeforeClass
	@Override
	public void beforeClass() throws Exception
	{
		HibernateUtil.getConfiguration().getProperties().setProperty("hibernate.show_sql", "true");
		super.beforeClass();
	}
	
	@AfterMethod
	public void afterMethod() throws Exception {
		clearSoftware();
		clearSystems();
	}

	@DataProvider
	public Object[][] findMatchingProvider() throws Exception
	{
	    Software software = createSoftware();
	    
		Object[][] testCases = new Object[][] {
	        { "available", software.isAvailable() },
	        { "checkpointable", software.isCheckpointable() },
	        { "checksum", software.getChecksum() },
	        { "defaultMaxRunTime", software.getDefaultMaxRunTime() },
	        { "defaultMemoryPerNode", software.getDefaultMemoryPerNode() },
	        { "defaultNodes", software.getDefaultNodes() },
	        { "defaultProcessorsPerNode", software.getDefaultProcessorsPerNode() },
	        { "defaultQueue", software.getDefaultQueue() },
	        { "deploymentPath", software.getDeploymentPath() },
			{ "deploymentSystem", software.getStorageSystem().getSystemId() },
	        { "executionSystem", software.getExecutionSystem().getSystemId() },
	        { "executionType", software.getExecutionType() },
	        { "helpURI", software.getHelpURI() },
	        { "icon", software.getIcon() },
	        { "id", software.getUniqueName() },
	        { "inputs.id", software.getInputs().get(0).getKey() },
	        { "label", software.getLabel() },
	        { "longDescription", software.getLongDescription() },
	        { "modules", software.getModulesAsList().get(0) },
	        { "name", software.getName() },
	        { "outputs.id", software.getOutputs().get(0).getKey() },
	        { "owner", software.getOwner() },
	        { "parallelism", software.getParallelism() },
	        { "parameters.id", software.getParameters().get(0).getKey() },
	        { "public", software.isPubliclyAvailable() },
	        { "publicOnly", Boolean.TRUE },
	        { "privateOnly", Boolean.FALSE },
	        { "revision", software.getRevisionCount() },
	        { "shortDescription", software.getShortDescription() },
	        { "storageSystem", software.getStorageSystem().getSystemId() },
	        { "templatePath", software.getExecutablePath() },
            { "testPath", software.getTestPath() },
	        { "uuid", software.getUuid() },
	        { "version", software.getVersion() },
		};

		SoftwareDao.delete(software);

		return testCases;
	}
	
	@Test(dataProvider="findMatchingProvider", enabled = false)
	public void findMatching(String attribute, Object value) throws Exception
	{
		Software software = createSoftware();
		if (StringUtils.equalsIgnoreCase(attribute, "privateonly")) {
		    software.setPubliclyAvailable(false);
		} else if (StringUtils.equalsIgnoreCase(attribute, "publiconly")) {
            software.setPubliclyAvailable(true);
        } 
		SoftwareDao.persist(software);
		Assert.assertNotNull(software.getId(), "Failed to generate a software ID.");
		
		Map<String, String> map = new HashMap<String, String>();
		if (StringUtils.equals(attribute, "uuid")) {
		    map.put(attribute, software.getUuid());
		} else if (StringUtils.equals(attribute, "executionSystem")) {
			map.put(attribute, software.getExecutionSystem().getSystemId());
		} else if (StringUtils.equals(attribute, "deploymentSystem")) {
			map.put(attribute, software.getStorageSystem().getSystemId());
		} else if (StringUtils.equals(attribute, "storageSystem")) {
			map.put(attribute, software.getStorageSystem().getSystemId());
		} else if (StringUtils.equals(attribute, "id")) {
			map.put(attribute, software.getUniqueName());
		} else if (StringUtils.equals(attribute, "name")) {
			map.put(attribute, software.getName());
		} else {
		    map.put(attribute, String.valueOf(value));
		}

		List<Software> softwares = SoftwareDao.findMatching(software.getOwner(), new SoftwareSearchFilter().filterCriteria(map), false);
		Assert.assertNotNull(softwares, "findMatching failed to find any software.");
		Assert.assertEquals(softwares.size(), 1, "findMatching returned the wrong number of software for search by " + attribute);
		Assert.assertTrue(softwares.contains(software), "findMatching did not return the saved software.");
	}

	/**
	 * Generates test cases using a list of ontologies, generating test cases to check that each can
	 * be searched via zero or more wildcards.
	 *
	 * @return test data
	 * @throws Exception
	 */
	@DataProvider
	public Object[][] findMatchingOntologyProvider() throws Exception
	{
		JsonArray ontologies = new JsonArray();
		ontologies.add(new JsonPrimitive("http://purl.org/ontology/mo/Lyrics"));
		ontologies.add(new JsonPrimitive("http://purl.org/ontology/mo/MusicArtist"));
		ontologies.add(new JsonPrimitive("http://purl.org/ontology/mo/MusicalExpression"));

		List<Object[]> testCases = new ArrayList<Object[]>();
		Object ontology = null;
		for (int i=0; i<ontologies.size(); i++) {
			ontology = ontologies.get(i);
			testCases.add(new Object[]{ontologies, "ontology.like", ontology.toString(), false});
			testCases.add(new Object[]{ontologies, "ontology.like", "*" + ontology.toString() + "*", true});
			testCases.add(new Object[]{ontologies, "ontology.like", "*" + ontology.toString(), false});
			testCases.add(new Object[]{ontologies, "ontology.like", ontology.toString() + "*", false});
		}
		return testCases.toArray(new Object[][]{});
	}

	@Test(dataProvider = "findMatchingOntologyProvider", enabled=false)
	public void findMatchingOntology(JsonArray ontologies, String attribute, String value, boolean shouldMatch) throws Exception
	{
		Software software = createSoftware();
		software.setOntology(ontologies.toString());
		software.setName(UUID.randomUUID().toString());
		SoftwareDao.persist(software);

		Assert.assertNotNull(software.getId(), "Failed to generate a software ID.");

		Map<String, String> map = new HashMap<String, String>();
		map.put(attribute, value);

		List<Software> softwares = SoftwareDao.findMatching(software.getOwner(), new SoftwareSearchFilter().filterCriteria(map), false);
		Assert.assertNotNull(softwares, "findMatching should never return null");

		if (shouldMatch) {
			Assert.assertEquals(softwares.size(), 1, "findMatching returned the wrong number of software for search by " + attribute);
			Assert.assertTrue(softwares.contains(software), "findMatching did not return the saved software.");
		}
		else {
			Assert.assertEquals(softwares.size(), 0, "findMatching should return empty list.");
		}
	}

	/**
	 * Generates test cases using a list of ontologies, generating test cases to check that each can
	 * be searched via zero or more wildcards.
	 *
	 * @return test data
	 * @throws Exception
	 */
	@DataProvider
	public Object[][] findMatchingTagProvider() throws Exception
	{
		JsonArray ontologies = new JsonArray();
		ontologies.add(new JsonPrimitive(createNonce()));
		ontologies.add(new JsonPrimitive(createNonce()));
		ontologies.add(new JsonPrimitive(createNonce()));

		List<Object[]> testCases = new ArrayList<Object[]>();
		Object ontology = null;
		for (int i=0; i<ontologies.size(); i++) {
			ontology = ontologies.get(i);
			testCases.add(new Object[]{ontologies, "tags.like", ontology.toString(), false});
			testCases.add(new Object[]{ontologies, "tags.like", "*" + ontology.toString() + "*", true});
			testCases.add(new Object[]{ontologies, "tags.like", "*" + ontology.toString(), false});
			testCases.add(new Object[]{ontologies, "tags.like", ontology.toString() + "*", false});
		}
		return testCases.toArray(new Object[][]{});
	}

	@Test(dataProvider = "findMatchingTagProvider",enabled=false)
	public void findMatchingTag(JsonArray ontologies, String attribute, String value, boolean shouldMatch) throws Exception
	{
		Software software = createSoftware();
		software.setTags(ontologies.toString());
		software.setName(UUID.randomUUID().toString());
		SoftwareDao.persist(software);

		Assert.assertNotNull(software.getId(), "Failed to generate a software ID.");

		Map<String, String> map = new HashMap<String, String>();
		map.put(attribute, value);

		List<Software> softwares = SoftwareDao.findMatching(software.getOwner(), new SoftwareSearchFilter().filterCriteria(map), false);
		Assert.assertNotNull(softwares, "findMatching should never return null");

		if (shouldMatch) {
			Assert.assertEquals(softwares.size(), 1, "findMatching returned the wrong number of software for search by " + attribute);
			Assert.assertTrue(softwares.contains(software), "findMatching did not return the saved software.");
		}
		else {
			Assert.assertEquals(softwares.size(), 0, "findMatching should return empty list.");
		}
	}
	
	@Test(enabled=false)
    public void findMatchingTime() throws Exception
    {
	    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        Software software = createSoftware();
        SoftwareDao.persist(software);
        Assert.assertNotNull(software.getId(), "Failed to generate a software ID.");
        
        Map<String, String> map = new HashMap<String, String>();
        map.put("lastupdated.ON", formatter.format(software.getLastUpdated()));
        
        List<Software> softwares = SoftwareDao.findMatching(software.getOwner(), new SoftwareSearchFilter().filterCriteria(map), false);
        Assert.assertNotNull(softwares, "findMatching failed to find any softwares matching lastupdated.");
        Assert.assertEquals(softwares.size(), 1, "findMatching returned the wrong number of software records for search by lastupdated");
        Assert.assertTrue(softwares.contains(software), "findMatching did not return the saved software when searching by lastupdated.");
        
        map.clear();
        map.put("created.ON", formatter.format(software.getCreated()));
        
        softwares = SoftwareDao.findMatching(software.getOwner(), new SoftwareSearchFilter().filterCriteria(map), false);
        Assert.assertNotNull(softwares, "findMatching failed to find any softwares matching created.");
        Assert.assertEquals(softwares.size(), 1, "findMatching returned the wrong number of software records for search by created");
        Assert.assertTrue(softwares.contains(software), "findMatching did not return the saved software when searching by created.");
    }
	
	@Test(dataProvider="findMatchingProvider")
	public void findMatchingCaseInsensitive(String attribute, Object value) throws Exception
	{
		Software software = createSoftware();
		if (StringUtils.equalsIgnoreCase(attribute, "privateonly")) {
            software.setPubliclyAvailable(false);
        } else if (StringUtils.equalsIgnoreCase(attribute, "publiconly")) {
            software.setPubliclyAvailable(true);
        } 
		SoftwareDao.persist(software);
		Assert.assertNotNull(software.getId(), "Failed to generate a software ID.");

		// The software object used to create the test was deleted prior to method invocation, so the
		// check for interanally assiged values such as uuid and system id will fail.
		Map<String, String> map = new HashMap<String, String>();
		if (StringUtils.equals(attribute, "uuid")) {
			map.put(attribute, software.getUuid());
		} else if (StringUtils.equals(attribute, "executionSystem")) {
			map.put(attribute, software.getExecutionSystem().getSystemId());
		} else if (StringUtils.equals(attribute, "deploymentSystem")) {
			map.put(attribute, software.getStorageSystem().getSystemId());
		} else if (StringUtils.equals(attribute, "storageSystem")) {
			map.put(attribute, software.getStorageSystem().getSystemId());
		} else if (StringUtils.equals(attribute, "id")) {
			map.put(attribute, software.getUniqueName());
		} else if (StringUtils.equals(attribute, "name")) {
			map.put(attribute, software.getName());
		} else {
			map.put(attribute, String.valueOf(value));
		}
		
		List<Software> softwares = SoftwareDao.findMatching(software.getOwner(), new SoftwareSearchFilter().filterCriteria(map), false);
		Assert.assertNotNull(softwares, "findMatching failed to find any softwares.");
		Assert.assertEquals(softwares.size(), 1, "findMatching returned the wrong number of software records for search by " + attribute);
		Assert.assertTrue(softwares.contains(software), "findMatching did not return the saved software.");
	}
	
	@DataProvider
	protected Object[][] dateSearchExpressionTestProvider() 
	{
	    List<Object[]> testCases = new ArrayList<Object[]>();
	    String[] timeFormats = new String[] { "Ka", "KKa", "K a", "KK a", "K:mm a", "KK:mm a", "Kmm a", "KKmm a", "H:mm", "HH:mm", "HH:mm:ss"};
	    String[] sqlDateFormats = new String[] { "YYYY-MM-dd", "YYYY-MM"};
	    String[] relativeDateFormats = new String[] { "yesterday", "today", "-1 day", "-1 month", "-1 year", "+1 day", "+1 month"};
	    String[] calendarDateFormats = new String[] {"MMMM d", "MMM d", "YYYY-MM-dd", "MMMM d, Y", 
	                                                "MMM d, Y", "MMMM d, Y", "MMM d, Y",
	                                                "M/d/Y", "M/dd/Y", "MM/dd/Y", "M/d/YYYY", "M/dd/YYYY", "MM/dd/YYYY"};



	    for(String searchField: new String[] {"created", "lastupdated"}) {
			for (SearchTerm.Operator operator : new SearchTerm.Operator[]{SearchTerm.Operator.BEFORE}) {

				String field = searchField + "." + operator.name();

				OffsetDateTime dateTime = OffsetDateTime.now(ZoneOffset.UTC);
				if (operator == SearchTerm.Operator.BEFORE) {
					dateTime = dateTime.minusMonths(1);
				} else {
					dateTime = dateTime.minusYears(2);
				}

				// milliseconds since epoch
				testCases.add(new Object[]{field, "" + dateTime.toEpochSecond(), true});

				// ISO 8601
				testCases.add(new Object[]{field, dateTime.toString(), true});

				// SQL date and time format
				for (String date : sqlDateFormats) {
					for (String time : timeFormats) {
						testCases.add(new Object[]{field, dateTime.format(DateTimeFormatter.ofPattern(date + " " + time)), date.contains("dd") && !time.contains("Kmm")});
					}
					testCases.add(new Object[]{field, dateTime.format(DateTimeFormatter.ofPattern(date)), true});
				}

				// Relative date formats
				for (String date : relativeDateFormats) {
					for (String time : timeFormats) {
						testCases.add(new Object[]{field, date + " " + dateTime.format(DateTimeFormatter.ofPattern(time)), !(!(date.contains("month") || date.contains("year") || date.contains(" day")) && time.contains("Kmm"))});
					}
					testCases.add(new Object[]{field, date, true});
				}

				for (String date : calendarDateFormats) {
					for (String time : timeFormats) {
						testCases.add(new Object[]{field, dateTime.format(DateTimeFormatter.ofPattern(date + " " + time)), !(date.contains("d") && time.contains("Kmm"))});
					}
					testCases.add(new Object[]{field, dateTime.format(DateTimeFormatter.ofPattern(date)), true});
				}
			}
		}
	    
	    return testCases.toArray(new Object[][] {});
	}
	
	@Test(dataProvider="dateSearchExpressionTestProvider", enabled = false)
	public void dateSearchExpressionTest(String attribute, String dateFormattedString, boolean shouldSucceed) throws Exception
    {
	    Software software = createSoftware();
	    software.setCreated(new DateTime().minusYears(5).toDate());
		software.setLastUpdated(new DateTime().minusYears(5).toDate());
	    software.setName(UUID.randomUUID().toString());
        SoftwareDao.persist(software);
        Assert.assertNotNull(software.getId(), "Failed to generate a software ID.");
        
        Map<String, String> map = new HashMap<String, String>();
        map.put(attribute, dateFormattedString);
        
        try
        {
            List<Software> softwares = SoftwareDao.findMatching(software.getOwner(),
					new SoftwareSearchFilter().filterCriteria(map), false);

            Assert.assertNotNull(softwares, "findMatching should never return null");

            if (shouldSucceed) {
                Assert.assertEquals(softwares.size(), 1,
						"findMatching returned " + softwares.size() + " software records for search by "
                        + attribute + "=" + dateFormattedString + " when 1 should have been returned.");
                Assert.assertTrue(softwares.contains(software),
						"findMatching did not return the test software record for search by " +
								attribute + "=" + dateFormattedString);
            }
            else {
				Assert.assertEquals(softwares.size(), 1,
						"findMatching returned software records for search by "
								+ attribute + "=" + dateFormattedString +
						" when none should have been returned.");
			}
        }
        catch (Exception e) {
            if (shouldSucceed) {
                Assert.fail("Searching by date string of the format " 
                    + dateFormattedString + " should " + (shouldSucceed ? "" : "not ") + "succeed", e);
            }
        }
    }
	
}
