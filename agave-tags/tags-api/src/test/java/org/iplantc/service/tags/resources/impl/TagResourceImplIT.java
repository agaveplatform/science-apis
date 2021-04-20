package org.iplantc.service.tags.resources.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.codec.binary.Base64;
import org.iplantc.service.common.auth.JWTClient;
import org.iplantc.service.common.exceptions.TenantException;
import org.iplantc.service.tags.AbstractTagIT;
import org.iplantc.service.tags.ServletJaxRsApplication;
import org.iplantc.service.tags.TestDataHelper;
import org.iplantc.service.tags.dao.TagDao;
import org.iplantc.service.tags.exceptions.TagException;
import org.iplantc.service.tags.model.Tag;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.restlet.Client;
import org.restlet.Component;
import org.restlet.Context;
import org.restlet.Server;
import org.restlet.data.MediaType;
import org.restlet.data.Protocol;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;
import org.testng.Assert;
import org.testng.annotations.*;

import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.iplantc.service.tags.TestDataHelper.TEST_USER;
import static org.testng.Assert.*;

@Test(groups={"integration"})
public class TagResourceImplIT extends AbstractTagIT
{	
	private final Component comp = new Component();
	private final TagDao dao = new TagDao();
	private final Client client = new Client(new Context(), Protocol.HTTP);
	private ServletJaxRsApplication application;
	private String testJWT;
	
	private void initRestletServer() throws Exception
	{
		// create Component (as ever for Restlet)
        Server server = comp.getServers().add(Protocol.HTTP, 8182);
		server.getContext().getParameters().add("maxTotalConnections", "512");

		// create JAX-RS runtime environment
        application = new ServletJaxRsApplication(comp.getContext());

        // Attach the application to the component and start it
        comp.getDefaultHost().attach(application);
        comp.start();
	}
	
	private void initJWT() throws Exception
	{
		JsonNode json = new ObjectMapper().createObjectNode()
				.put("iss", "wso2.org/products/am")
				.put("exp", new DateTime().plusHours(4).toDate().getTime())
				.put("http://wso2.org/claims/subscriber", System.getProperty("user.name"))
				.put("http://wso2.org/claims/applicationid", "5")
				.put("http://wso2.org/claims/applicationname", "DefaultApplication")
				.put("http://wso2.org/claims/applicationtier", "Unlimited")
				.put("http://wso2.org/claims/apicontext", "/apps")
				.put("http://wso2.org/claims/version", "2.0")
				.put("http://wso2.org/claims/tier", "Unlimited")
				.put("http://wso2.org/claims/keytype", "PRODUCTION")
				.put("http://wso2.org/claims/usertype", "APPLICATION_USER")
				.put("http://wso2.org/claims/enduser", System.getProperty("user.name"))
				.put("http://wso2.org/claims/enduserTenantId", "-9999")
				.put("http://wso2.org/claims/emailaddress", System.getProperty("user.name"))
				.put("http://wso2.org/claims/fullname", "Dev User")
				.put("http://wso2.org/claims/givenname", "Dev")
				.put("http://wso2.org/claims/lastname", "User")
				.put("http://wso2.org/claims/primaryChallengeQuestion", "N/A")
				.put("http://wso2.org/claims/role", "Internal/everyone")
				.put("http://wso2.org/claims/title", "N/A");
		StringBuilder builder = new StringBuilder();
		builder.append("eyJ0eXAiOiJKV1QiLCJhbGciOiJTSEEyNTZ3aXRoUlNBIiwieDV0IjoiTm1KbU9HVXhNelpsWWpNMlpEUmhOVFpsWVRBMVl6ZGhaVFJpT1dFME5XSTJNMkptT1RjMVpBPT0ifQ==.");
		builder.append(new String(Base64.encodeBase64(json.toString().getBytes())));
		builder.append(".FA6GZjrB6mOdpEkdIQL/p2Hcqdo2QRkg/ugBbal8wQt6DCBb1gC6wPDoAenLIOc+yDorHPAgRJeLyt2DutNrKRFv6czq1wz7008DrdLOtbT4EKI96+mXJNQuxrpuU9lDZmD4af/HJYZ7HXg3Hc05+qDJ+JdYHfxENMi54fXWrxs=");
				
		testJWT = builder.toString();
	}
	
	@BeforeClass
	public void beforeClass() throws Exception
	{
		initRestletServer();
		super.beforeClass();
		initJWT();
	}
	
	@AfterMethod
	public void afterMethod() throws Exception
	{
		clearTags();
		clearNotifications();
	}
	
	@AfterClass
	public void afterClass() throws TagException
	{
		super.afterClass();
		try { comp.stop(); } catch (Exception ignored) {}
	}

	/**
	 * Parses standard response stanza for the actual body and code
	 * 
	 * @param representation
	 * @param shouldSucceed
	 * @return
	 * @throws JSONException
	 * @throws IOException
	 */
	private JsonNode verifyResponse(Representation representation, boolean shouldSucceed) 
	throws JSONException, IOException 
	{
		String responseBody = representation.getText();
		
		assertNotNull(responseBody, "Null body returned");
		
		JsonNode json = mapper.readTree(responseBody);
		if (shouldSucceed) {
			assertEquals(json.get("result").asText().toLowerCase(), "success", "Error when success should have occurred");
		} else {
			assertEquals(json.get("result").asText().toLowerCase(), "error", "Success when error should have occurred");
		}
		
		return json.get("response");
	}

	/**
	 * Creates a client to call the api given by the url. This will reuse the connection
	 * multiple times rather than starting up a new client every time.
	 *
	 * @param url the url to point the client towards
	 * @return an initialized API client
	 */
	private ClientResource getService(String url) throws TenantException {
		String jwt = JWTClient.createJwtForTenantUser(TEST_USER, "agave.dev", false);
		ClientResource resource = new ClientResource(url);
		resource.setReferrerRef("http://test.example.com");
		resource.getRequest().getHeaders().add("X-JWT-ASSERTION-AGAVE_DEV", jwt);

		return resource;
	}

	@DataProvider(name="addTagProvider")
	public Object[][] addTagProvider() throws Exception
	{
//		ObjectNode jsonExecutionTagNoSystem = createTag();
//		ObjectNode jsonExecutionTagNoFrequency = (ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR);
//		ObjectNode jsonExecutionTagNoUpdateSystemStatus = (ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR);
//		ObjectNode jsonExecutionTagNoInternalUsername = (ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR);
//		jsonExecutionTagNoSystem.remove("target");
//		jsonExecutionTagNoFrequency.remove("system");
//		jsonExecutionTagNoUpdateSystemStatus.remove("updateSystemStatus");
//		jsonExecutionTagNoInternalUsername.remove("internalUsername");
//		
		return new Object[][] {
			{ dataHelper.getTestDataObject(TestDataHelper.TEST_TAG), "Valid tag json should parse", false },
//			{ jsonExecutionTagNoSystem, "Missing system should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", ""), "Empty target should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", mapper.createObjectNode()), "Object for target should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", mapper.createArrayNode()), "Array for target should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", 5), "Integer for target should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", 5.5), "Decimal for target should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", publicStorageSystem.getSystemId()), "Public storage system should not throw an exception", false },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", publicExecutionSystem.getSystemId()), "Public execution system should not throw an exception", false },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", privateExecutionSystem.getSystemId()), "Private execution system should not throw an exception", false },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", privateStorageSystem.getSystemId()), "Private storage system should not throw an exception", false },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", sharedExecutionSystem.getSystemId()), "Shared execution system should not throw an exception", false },
//
//
//			{ jsonExecutionTagNoFrequency, "Missing frequency should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", ""), "Empty frequency should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", mapper.createObjectNode()), "Object for frequency should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", mapper.createArrayNode()), "Array for frequency should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", 5), "Integer for frequency should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", 5.5), "Decimal for frequency should throw exception", true },
//
//			{ jsonExecutionTagNoUpdateSystemStatus, "Missing updateSystemStatus should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", ""), "Empty updateSystemStatus should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", mapper.createObjectNode()), "Object for updateSystemStatus should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", mapper.createArrayNode()), "Array for updateSystemStatus should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", 5), "Integer for updateSystemStatus should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", 5.5), "Decimal for updateSystemStatus should throw exception", true },
//
//			{ jsonExecutionTagNoInternalUsername, "Missing internalUsername should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("internalUsername", ""), "Empty internalUsername should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("internalUsername", mapper.createObjectNode()), "Object for internalUsername should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("internalUsername", mapper.createArrayNode()), "Array for internalUsername should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("internalUsername", 5), "Integer for internalUsername should throw exception", true },
//			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("internalUsername", 5.5), "Decimal for internalUsername should throw exception", true },
			
		};
	}
	
	@Test(dataProvider="addTagProvider")
	public void addSingleTag(JsonNode body, String errorMessage, boolean shouldThrowException)
	{
		JsonNode json = null;
		try 
		{
			ClientResource resource = getService("http://localhost:8182/");

			Representation response = resource.post(new StringRepresentation(mapper.writeValueAsString(body)));
			assertEquals(resource.getStatus().equals(Status.OK), !shouldThrowException, "Response failed when it should not have");
			assertNotNull(response, "Expected json tag object, instead received null");
			assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON,
					"Expected json media type returned, instead received " + response.getMediaType());
			
			json = verifyResponse(response, true);
			
			assertNotNull(json.get("id").asText().toLowerCase(), "No tag uuid given");
		}
		catch (Exception e) {
			Assert.fail("Unexpected exception thrown", e);
		}
	}

	@DataProvider(name="deleteTagProvider")
	public Object[][] deleteTagProvider() throws Exception
	{
		Tag validTag = createTag();
		dao.persist(validTag);
		
//		Tag invalidSystemTag = createStorageTag();
//		invalidSystemTag.setOwner(TestDataHelper.SYSTEM_SHARE_USER);
//		invalidSystemTag.setSystem(privateStorageSystem);
//		dao.persist(invalidSystemTag);
		
		Tag otherUserTag = createTag();
		otherUserTag.setOwner(TestDataHelper.TEST_SHAREUSER);
		dao.persist(otherUserTag);
		
		return new Object[][] {
			{ validTag.getUuid(), "Deleting valid tag should succeed", false },
			{ "", "Empty uuid should fail", true },
			{ "abcd", "Invalid uuid should fail", true },
			{ otherUserTag.getUuid(), "Deleting unowned tag should fail", true },
		};
	}
	
	@Test(dataProvider="deleteTagProvider")
	public void deleteTag(String uuid, String errorMessage, boolean shouldThrowException)
	{
		JsonNode json = null;
		try
		{
			ClientResource resource = getService("http://localhost:8182/" + uuid);

			Representation response = resource.delete();
			assertEquals(resource.getStatus().equals(Status.OK), !shouldThrowException, "Response failed when it should not have");
			assertNotNull(response, "Expected json tag object, instead received null");
			assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON,
					"Expected json media type returned, instead received " + response.getMediaType());
			
			json = verifyResponse(response, shouldThrowException);
			
			assertTrue(json.isNull(), "Message results attribute was not null");
		}
		catch (Exception e) {
			Assert.fail("Unexpected exception thrown", e);
		}
	}

	@Test
	public void getMissingTag()
	{
		JsonNode json = null;
		try
		{
			ClientResource resource = getService("http://localhost:8182/" + UUID.randomUUID());

			Representation response = resource.get();
		}
		catch (ResourceException e) {
			assertEquals(e.getStatus().getCode(), 404, "Unknown uuid response should be a 404");
		}
		catch (Exception e) {
			Assert.fail("Unexpected exception thrown", e);
		}
	}

	@DataProvider(name="getTagProvider")
	public Object[][] getTagProvider() throws Exception
	{
		Tag validTag = createTag();
		dao.persist(validTag);
		
		Tag otherUserTag = createTag();
		otherUserTag.setOwner(TestDataHelper.TEST_SHAREUSER);
		dao.persist(otherUserTag);
		
		return new Object[][] {
			{ validTag.getUuid(), "Requesting valid tag should succeed", false },
			{ otherUserTag.getUuid(), "Requesting unowned tag should fail", true },
		};
	}

	@Test(dataProvider="getTagProvider")
	public void getTag(String uuid, String errorMessage, boolean shouldThrowException)
	{
		JsonNode json = null;
		try
		{
			ClientResource resource = getService("http://localhost:8182/" + uuid);

			Representation response = resource.get();
			
			assertNotNull(response, "Expected json tag object, instead received null");
			assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON,
					"Expected json media type returned, instead received " + response.getMediaType());
			
			json = verifyResponse(response, shouldThrowException);
			assertEquals(resource.getStatus().equals(Status.OK), !shouldThrowException, "Response failed when it should not have");
			
		}
		catch (Exception e) {
			Assert.fail("Unexpected exception thrown", e);
		}
	}

	@Test
	public void listTags()
	{
		JsonNode json = null;
		try
		{
			ClientResource resource = getService("http://localhost:8182/");

			List<String> tagUuids = new ArrayList<>();
			for (int i=0;i<5; i++) {
				Tag tag = createTag();
				dao.persist(tag);
				tagUuids.add(tag.getUuid());
			}

			Representation response = resource.get();
			
			assertNotNull(response, "Expected json tag object, instead received null");
			assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON,
					"Expected json media type returned, instead received " + response.getMediaType());
			
			json = verifyResponse(response, true);
			assertEquals(resource.getStatus().getCode(), 200, "Response failed when it should not have");
			assertTrue(json instanceof ArrayNode, "Service returned object rather than array");
			assertEquals(json.size(), tagUuids.size(), "Invalid number of monitors returned.");

			List<String> responseUuids = json.findValuesAsText("uuid");
			assertTrue(tagUuids.containsAll(responseUuids), "All persisted tags should be returned in a list command");
		}
		catch (Exception e) {
			Assert.fail("Unexpected exception thrown", e);
		}
	}
}
