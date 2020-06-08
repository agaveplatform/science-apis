package org.iplantc.service.notification.resources.impl;

import java.io.IOException;

import javax.ws.rs.WebApplicationException;
//import javax.ws.rs.core.MediaType;
//import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.Status;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;
import org.iplantc.service.common.auth.JWTClient;
import org.iplantc.service.common.clients.RequestBin;
import org.iplantc.service.common.dao.TenantDao;
import org.iplantc.service.common.model.Tenant;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.notification.*;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.model.Notification;
import org.json.JSONException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.restlet.Component;
import org.restlet.Server;
import org.restlet.data.ChallengeScheme;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Protocol;
import org.restlet.ext.jaxrs.JaxRsApplication;
import org.restlet.ext.json.JsonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;
import org.testng.Assert;
import org.testng.annotations.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.restlet.data.Status.*;

@Test(groups={"integration"})
public class NotificationResourceImplIT extends AbstractNotificationTest {

	private static Logger log = Logger.getLogger(NotificationResourceImplIT.class);
	private Component comp = new Component();
	private ServletJaxRsApplication application = null;
	private NotificationResourceImpl service;
	private NotificationDao dao = new NotificationDao();
	private Notification referenceNotification;
	
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
	
	@BeforeClass
	public void beforeClass() throws Exception {
		initRestletServer();
		requestBin = RequestBin.getInstance();

		referenceNotification = new Notification("SENT", "help@agaveplatform.org");
		referenceNotification.setAssociatedUuid(referenceNotification.getUuid());
		referenceNotification.setOwner(TEST_USER);
		dao.persist(referenceNotification);
	}

	@AfterClass
	public void afterClass() throws Exception
	{
		for(Notification n: dao.getAll()) {
			dao.delete(n);
		}

		comp.stop();
	}
	
	private JsonNode verifyResponse(Representation representation, boolean shouldSucceed) throws IOException
	{
		String responseBody = representation.getText();
		
		Assert.assertNotNull(responseBody, "Null body returned");
		
		ObjectMapper mapper = new ObjectMapper();
		JsonNode json = mapper.readTree(responseBody);
		if (shouldSucceed) {
			Assert.assertEquals(json.get("status").asText().toLowerCase(), "success", "Error when success should have occurred");
		} else {
			Assert.assertEquals(json.get("status").asText().toLowerCase(), "error", "Success when error should have occurred");
		}
		
		return json.get("result");
	}

	@DataProvider(name="addNotificationFromJsonProvider", parallel = false)
	public Object[][] addNotificationFromJsonProvider() throws Exception
	{
		ObjectMapper mapper = new ObjectMapper();
		return new Object[][] {
			{ mapper.createObjectNode().put("url", requestBin.toString() + TEST_URL_QUERY).put("event","START").put("associatedUuid",referenceNotification.getUuid()), "Valid url should succeed", false },
			
			{ mapper.createObjectNode(), "Empty object should throw exception", true },
			{ mapper.createArrayNode(), "Array should throw exception", true },
			{ mapper.createObjectNode().put("event","START").put("associatedUuid",referenceNotification.getUuid()), "Missing url should throw exception", true },
			{ mapper.createObjectNode().put("url", "").put("event","START").put("associatedUuid",referenceNotification.getUuid()), "Empty url should throw exception", true },
			{ mapper.createObjectNode().putNull("url").put("event","START").put("associatedUuid",referenceNotification.getUuid()), "Null url should throw exception", true },
			{ mapper.createObjectNode().put("event","START").put("associatedUuid",referenceNotification.getUuid()).set("url", mapper.createArrayNode()), "Array for url should throw exception", true },
			{ mapper.createObjectNode().put("event","START").put("associatedUuid",referenceNotification.getUuid()).set("url", mapper.createObjectNode()), "Object for url should throw exception", true },
			{ mapper.createObjectNode().put("url", TEST_EMAIL).put("event","START").put("associatedUuid",referenceNotification.getUuid()), "Valid email should succeed", false },
			
			{ mapper.createObjectNode().put("url", TEST_EMAIL).put("associatedUuid",referenceNotification.getUuid()), "Missing event should throw exception", true },
			{ mapper.createObjectNode().put("event", "").put("url", TEST_EMAIL).put("associatedUuid",referenceNotification.getUuid()), "Empty event should throw exception", true },
			{ mapper.createObjectNode().putNull("event").put("url", TEST_EMAIL).put("associatedUuid",referenceNotification.getUuid()), "Null event should throw exception", true },
			{ mapper.createObjectNode().put("url", TEST_EMAIL).put("associatedUuid",referenceNotification.getUuid()).set("event", mapper.createArrayNode()), "Array for event should throw exception", true },
			{ mapper.createObjectNode().put("url", TEST_EMAIL).put("associatedUuid",referenceNotification.getUuid()).set("event", mapper.createObjectNode()), "Object for event should throw exception", true },
			
			{ mapper.createObjectNode().put("event","START").put("url", TEST_EMAIL), "Missing associatedUuid should throw exception", true },
			{ mapper.createObjectNode().put("associatedUuid", "").put("event","START").put("url", TEST_EMAIL), "Empty associatedUuid should throw exception", true },
			{ mapper.createObjectNode().putNull("associatedUuid").put("event","START").put("url", TEST_EMAIL), "Null associatedUuid should throw exception", true },
			{ mapper.createObjectNode().put("event","START").put("url", TEST_EMAIL).set("associatedUuid", mapper.createArrayNode()), "Array for associatedUuid should throw exception", true },
			{ mapper.createObjectNode().put("event","START").put("url", TEST_EMAIL).set("associatedUuid", mapper.createObjectNode()), "Object for associatedUuid should throw exception", true },
			{ mapper.createObjectNode().put("associatedUuid", WILDCARD_ASSOCIATED_UUID).put("event","START").put("url", TEST_EMAIL), "Wildcard associatedUuid should throw exception for non-admin", true },
		};
	}
	
	@Test(enabled = true, dataProvider="addNotificationFromJsonProvider", singleThreaded = true, priority = 1)
	public void addNotificationFromJson(JsonNode body, String errorMessage, boolean shouldThrowException)
	{
		JsonNode json = null;
		Representation response = null;
		ClientResource resource = null;
		try 
		{
			resource = new ClientResource("http://localhost:8182/");
			resource.setReferrerRef("http://test.example.com");
			resource.getRequest().getHeaders().add("X-JWT-ASSERTION-AGAVE_DEV", JWTClient.createJwtForTenantUser(TEST_USER, "agave.dev", false));

			response = resource.post(new JsonRepresentation(body.toString()));

			Assert.assertFalse(shouldThrowException, errorMessage);
			Assert.assertNotNull(response, "Expected json notification object, instead received null");
			Assert.assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON, 
					"Expected json media type returned, instead received " + response.getMediaType());
			
			json = verifyResponse(response, true);
			
			Assert.assertNotNull(json.get("id").asText().toLowerCase(), "No notification uuid given");
		}
		catch (ResourceException e) {
			if (!shouldThrowException) {
				Assert.fail(errorMessage, e);
			}
		}
		catch (Exception e) {
			Assert.fail("Unexpected exception thrown", e);
		}
		finally {
			try {
				if (json != null) {
					if (json.has("id")) {
						Notification n = dao.findByUuid(json.get("id").asText()); 
						dao.delete(n);
					}
					
				}
			} catch (Exception ignored) {}
		}
	}

	@DataProvider(name="addNotificationFromFormProvider", parallel = false)
	public Object[][] addNotificationFromFormProvider() throws Exception
	{
		ObjectMapper mapper = new ObjectMapper();
		return new Object[][] {
				{ mapper.createObjectNode().put("url", requestBin.toString() + TEST_URL_QUERY).put("event","START").put("associatedUuid",referenceNotification.getUuid()), "Valid url should succeed", false },

				{ mapper.createObjectNode().put("event","START").put("associatedUuid",referenceNotification.getUuid()), "Missing url should throw exception", true },
				{ mapper.createObjectNode().put("url", "").put("event","START").put("associatedUuid",referenceNotification.getUuid()), "Empty url should throw exception", true },
				{ mapper.createObjectNode().putNull("url").put("event","START").put("associatedUuid",referenceNotification.getUuid()), "Null url should throw exception", true },
				{ mapper.createObjectNode().put("event","START").put("associatedUuid",referenceNotification.getUuid()).set("url", mapper.createArrayNode()), "Array for url should throw exception", true },
				{ mapper.createObjectNode().put("event","START").put("associatedUuid",referenceNotification.getUuid()).set("url", mapper.createObjectNode()), "Object for url should throw exception", true },
				{ mapper.createObjectNode().put("url", TEST_EMAIL).put("event","START").put("associatedUuid",referenceNotification.getUuid()), "Valid email should succeed", false },

				{ mapper.createObjectNode().put("url", TEST_EMAIL).put("associatedUuid",referenceNotification.getUuid()), "Missing event should throw exception", true },
				{ mapper.createObjectNode().put("event", "").put("url", TEST_EMAIL).put("associatedUuid",referenceNotification.getUuid()), "Empty event should throw exception", true },
				{ mapper.createObjectNode().putNull("event").put("url", TEST_EMAIL).put("associatedUuid",referenceNotification.getUuid()), "Null event should throw exception", true },
//				{ mapper.createObjectNode().put("url", TEST_EMAIL).put("associatedUuid",referenceNotification.getUuid()).set("event", mapper.createArrayNode()), "Array for event should throw exception", true },
//				{ mapper.createObjectNode().put("url", TEST_EMAIL).put("associatedUuid",referenceNotification.getUuid()).set("event", mapper.createObjectNode()), "Object for event should throw exception", true },

				{ mapper.createObjectNode().put("event","START").put("url", TEST_EMAIL), "Missing associatedUuid should throw exception", true },
				{ mapper.createObjectNode().put("associatedUuid", "").put("event","START").put("url", TEST_EMAIL), "Empty associatedUuid should throw exception", true },
				{ mapper.createObjectNode().putNull("associatedUuid").put("event","START").put("url", TEST_EMAIL), "Null associatedUuid should throw exception", true },
				{ mapper.createObjectNode().put("event","START").put("url", TEST_EMAIL).set("associatedUuid", mapper.createArrayNode()), "Array for associatedUuid should throw exception", true },
				{ mapper.createObjectNode().put("event","START").put("url", TEST_EMAIL).set("associatedUuid", mapper.createObjectNode()), "Object for associatedUuid should throw exception", true },
				{ mapper.createObjectNode().put("associatedUuid", WILDCARD_ASSOCIATED_UUID).put("event","START").put("url", TEST_EMAIL), "Wildcard associatedUuid should throw exception for non-admin", true },
		};
	}

	@Test(dataProvider="addNotificationFromFormProvider", singleThreaded = true, priority = 2)
	public void addNotificationFromForm(JsonNode body, String errorMessage, boolean shouldThrowException)
	{
		JsonNode json = null;
		ClientResource resource = null;
		try
		{
			resource = new ClientResource("http://localhost:8182/");
			resource.setReferrerRef("http://test.example.com");
			resource.getRequest().getHeaders().add("X-JWT-ASSERTION-AGAVE_DEV", JWTClient.createJwtForTenantUser(TEST_USER, "agave.dev", false));

			Form form = new Form();
			if (body != null) 
			{
				if (body.has("event")) 
				{
					form.add("event", body.get("event").asText());
				}
				
				if (body.has("url")) 
				{
					form.add("url", body.get("url").asText());
				}
				
				if (body.has("associatedUuid")) 
				{
					form.add("associatedUuid", body.get("associatedUuid").asText());
				}
			}
			
			Representation response = resource.post(form.getWebRepresentation());
			Assert.assertFalse(shouldThrowException, errorMessage);
			Assert.assertNotNull(response, "Expected json notification object, instead received null");
			Assert.assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON, 
					"Expected json media type returned, instead received " + response.getMediaType());
			
			json = verifyResponse(response, true);
			
			Assert.assertNotNull(json.get("id").asText().toLowerCase(), "No notification uuid given");
		}
		catch (ResourceException e) {
			if (!shouldThrowException) {
				Assert.fail(errorMessage, e);
			}
		}
		catch (Exception e) {
			Assert.fail("Unexpected exception thrown", e);
		}
		finally {
			try {
				if (json != null) {
					if (json.has("id")) {
						Notification n = dao.findByUuid(json.get("id").asText());
						dao.delete(n);
					}

				}
			} catch (Exception ignored) {}
		}
	}

	@Test(singleThreaded = true, priority = 3)
	public void updateNotification()
	{
		JsonNode json = null;
		Representation response = null;
		ClientResource resource = null;
		Notification notification = null;
		try
		{
			notification = createWebhookNotification();
			notification.setAssociatedUuid(referenceNotification.getUuid());
			notification.setPersistent(false);
			notification.setOwner(TEST_USER);
			dao.persist(notification);

			ObjectMapper mapper = new ObjectMapper();

			ObjectNode updateNotification = (ObjectNode)mapper.readTree(notification.toJSON());
			updateNotification.remove("_links");
			updateNotification.remove("owner");
			updateNotification.remove("responseCode");
			updateNotification.remove("attempts");
			updateNotification.remove("success");
			updateNotification.remove("lastSent");
			updateNotification.put("persistent", true);

			resource = new ClientResource("http://localhost:8182/"+ notification.getUuid());
			resource.setReferrerRef("http://test.example.com");
			resource.getRequest().getHeaders().add("X-JWT-ASSERTION-AGAVE_DEV", JWTClient.createJwtForTenantUser(TEST_USER, "agave.dev", false));

			response = resource.post(new JsonRepresentation(updateNotification.toString()));

			Assert.assertNotNull(response, "Expected json notification object, instead received null");
			Assert.assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON,
					"Expected json media type returned, instead received " + response.getMediaType());

			json = verifyResponse(response, true);

			Assert.assertTrue(json.get("persistent").asBoolean(), "Persistent field should be updated in response from service");
		}
		catch (Exception e) {
			Assert.fail("Unexpected exception thrown during notification update", e);
		}
		finally {
			try {
				if (json != null) {
					if (json.has("id")) {
						Notification n = dao.findByUuid(json.get("id").asText());
						dao.delete(n);
					}
				}
			} catch (Exception ignored) {}
		}
	}

	@DataProvider(name="deleteNotificationProvider", parallel = false)
	public Object[][] deleteNotificationProvider() throws Exception
	{
		Notification n = new Notification("SENT", TEST_EMAIL);
		n.setOwner(TEST_USER);
		n.setAssociatedUuid(referenceNotification.getUuid());
		dao.persist(n);
		
		Notification n2 = new Notification("SENT", TEST_EMAIL);
		n2.setOwner(TEST_USER + "-test");
		n2.setAssociatedUuid(referenceNotification.getUuid());
		dao.persist(n2);
		
		return new Object[][] {
			{ n.getUuid(), "Valid url should succeed", false },
			{ "", "Empty uuid should fail", true },
			{ "abcd", "Invalid uuid should fail", true },
			{ n2.getUuid(), "Deleting by non-owner should fail", true },
		};
	}
	
	@Test(dataProvider="deleteNotificationProvider", dependsOnMethods={"addNotificationFromForm"}, priority = 3)
	public void deleteNotification(String uuid, String errorMessage, boolean shouldThrowException)
	{
		JsonNode json = null;
		try
		{
			ClientResource resource = new ClientResource("http://localhost:8182/" + uuid);
			resource.setReferrerRef("http://test.example.com");
			resource.getRequest().getHeaders().add("X-JWT-ASSERTION-AGAVE_DEV", JWTClient.createJwtForTenantUser(TEST_USER, "agave.dev", false));

			Representation response = resource.delete();
			Assert.assertFalse(shouldThrowException, errorMessage);
			Assert.assertNotNull(response, "Expected json notification object, instead received null");
			Assert.assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON, 
					"Expected json media type returned, instead received " + response.getMediaType());
			
			json = verifyResponse(response, true);
			
			Assert.assertNull(json, "Message results attribute was not null");
		}
		catch (ResourceException e) {
			if (!shouldThrowException) {
				Assert.fail(errorMessage, e);
			}
		}
		catch (Exception e) {
			Assert.fail("Unexpected exception thrown", e);
		}
		finally {
			try {
				Notification n = dao.findByUuid(uuid); 
				if (n != null) {
					dao.delete(n);
				}
			} catch (Exception ignored) {}
		}
	}

	@Test(dataProvider="deleteNotificationProvider", dependsOnMethods={"deleteNotification"}, singleThreaded = true)
	public void fireNotification(String uuid, String errorMessage, boolean shouldThrowException)
	{
		JsonNode json = null;
		try
		{
			ClientResource resource = new ClientResource("http://localhost:8182/" + uuid);
			resource.setReferrerRef("http://test.example.com");
			resource.getRequest().getHeaders().add("X-JWT-ASSERTION-AGAVE_DEV", JWTClient.createJwtForTenantUser(TEST_USER, "agave.dev", false));

			Representation response = resource.delete();
			
			Assert.assertNotNull(response, "Expected json notification object, instead received null");
			Assert.assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON, 
					"Expected json media type returned, instead received " + response.getMediaType());
			
			json = verifyResponse(response, true);
			Assert.assertFalse(shouldThrowException, "Response failed when it should not have");
			
		}
		catch (ResourceException e) {
			if (!shouldThrowException) {
				Assert.fail(errorMessage, e);
			}
		}
		catch (Throwable e) {
			Assert.fail("Unexpected exception thrown", e);
		}
		finally {
			try {
				Notification n = dao.findByUuid(uuid); 
				if (n != null) {
					dao.delete(n);
				}
			} catch (Exception ignored) {}
		}
	}

//	@Test
//	public void getNotification(String uuid, String errorMessage, boolean shouldThrowException)
//	{
//		try 
//		{
//			throw new RuntimeException("Test not implemented");
//		}
//		catch (WebApplicationException e) {
//			if (!shouldThrowException) 
//				Assert.fail(errorMessage, e);
//		}
//		catch (Throwable e) {
//			Assert.fail("Unexpected exception thrown", e);
//		}
//	}

//	@Test
//	public void getNotifications(String errorMessage, boolean shouldThrowException)
//	{
//		try 
//		{
//			throw new RuntimeException("Test not implemented");
//		}
//		catch (Throwable e) {
//			Assert.fail("Unexpected exception thrown", e);
//		}
//	}
}
