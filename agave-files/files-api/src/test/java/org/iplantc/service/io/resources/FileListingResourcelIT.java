package org.iplantc.service.io.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.AuthenticationStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.iplantc.service.common.auth.JWTClient;
import org.iplantc.service.io.BaseTestCase;
import org.iplantc.service.io.FilesApplication;
import org.iplantc.service.notification.Settings;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.http.HTTP;
import org.json.JSONException;
import org.restlet.Component;
import org.restlet.Server;
import org.restlet.data.*;
import org.restlet.engine.header.HeaderUtils;
import org.restlet.ext.jaxrs.JaxRsApplication;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ClientResource;
import org.restlet.util.Series;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

//import javax.ws.rs.core.MediaType;
//import javax.ws.rs.core.Response.Status;

@Test(groups={"integration"})
public class FileListingResourcelIT extends BaseTestCase {
	
	private FileListingResource service;
	private Component comp = new Component();
	private NotificationDao dao = new NotificationDao();
	private Notification referenceNotification;
	
	private void initRestletServer() throws Exception
	{
		// create Component (as ever for Restlet)
        Server server = comp.getServers().add(Protocol.HTTP, 8080);

        // create JAX-RS runtime environment
		FilesApplication application = new FilesApplication();

        // Attach the application to the component and start it
        comp.getDefaultHost().attach(application);
        comp.start();
	}
	
	@BeforeClass
	public void beforeClass() throws Exception
	{
		super.beforeClass();
		initRestletServer();
		initSystems();
	}

	private JsonNode verifyResponse(HttpResponse response, boolean shouldSucceed)
	throws JSONException, IOException 
	{
		try (InputStream in = response.getEntity().getContent()) {

			Assert.assertNotNull(in, "Null body returned");

			ObjectMapper mapper = new ObjectMapper();

			JsonNode json = mapper.getFactory().createParser(in).readValueAsTree();
			if (shouldSucceed) {
				Assert.assertEquals(json.get("result").asText().toLowerCase(), "success", "Error when success should have occurred");
			} else {
				Assert.assertEquals(json.get("result").asText().toLowerCase(), "error", "Success when error should have occurred");
			}
			return json.get("response");
		}
	}

	public void listRelativePathTest() {
		RemoteSystem system = new SystemDao().findBySystemId("sftp.example.com");

		String baseUrl = "http://localhost:8080/files/listings/system/" + system.getSystemId() + "/";


//		ClientResource resource = new ClientResource(baseUrl);
//
//		resource.setReferrerRef("http://test.example.com");
		JsonNode json = null;
		try
		{
			HTTP http = new HTTP(URI.create(baseUrl));
			// get naked response for quicker parsing.
//			resource.setQueryValue("naked", "true");

			String testJwt = JWTClient.createJwtForTenantUser(SYSTEM_OWNER, "agave.dev", false);
			HttpResponse response = http.doGet(baseUrl, Map.of("X-JWT-ASSERTION-AGAVE_DEV", testJwt));
//			Series<Header> headers = (Series<Header>)resource.getRequestAttributes().get(
//					"org.restlet.http.headers");
//			if (headers == null) {
//				headers = new Series<Header>(Header.class);
//
//			}
//			headers.set("X-JWT-ASSERTION-AGAVE_DEV", testJwt);
//			HeaderUtils.addEntityHeaders(resource.getRequestEntity(), headers);

//			resource.setAttribute("org.restlet.http.headers", headers);
//			Representation response = resource.get(MediaType.APPLICATION_ALL_JSON);
			Assert.assertEquals(response.getStatusLine().getStatusCode(), 200,
					"Listing home directory by empty path should succeed");
			Assert.assertNotNull(response, "Expected json notification object, instead received null");
			Assert.assertEquals(response.getEntity().getContentType().getValue(), MediaType.APPLICATION_JSON.toString(),
					"Expected json media type returned, instead received " + response.getEntity().getContentType().getValue());

			json = verifyResponse(response, true);

			// now check each response for valid paths and self ref links.
			((ArrayNode)json).forEach(jsonNode -> {
				try {
					String path = jsonNode.get("path").asText();
					Path absoluteHomeDir = Paths.get(system.getStorageConfig().getRootDir()).resolve(system.getStorageConfig().getHomeDir());
					Assert.assertTrue(path.startsWith(absoluteHomeDir.toString()),
							"Relative path file listing responses should all have a path beginning with teh home directory.");

					String selfLink = jsonNode.get("_links").get("self").get("href").asText();
					Assert.assertEquals(baseUrl + jsonNode.get("path").asText(), selfLink,
							"Response self href should mach base url + path value in response");
				} catch (Exception e) {
					Assert.fail("Failed to process response json", e);
				}

			});
		}
		catch (Exception e) {
			Assert.fail("Unexpected exception thrown", e);
		}
	}


//	@Test(dataProvider="addNotificationProvider")
//	public void addNotification(JsonNode body, String errorMessage, boolean shouldThrowException)
//	{
//		ClientResource resource = new ClientResource("http://localhost:8182/notifications");
//		resource.setReferrerRef("http://test.example.com");
//		JsonNode json = null;
//		try
//		{
//			Representation response = resource.post(new StringRepresentation(body.toString()));
//			Assert.assertEquals(resource.getStatus().equals(Status.OK), !shouldThrowException, "Response failed when it should not have");
//			Assert.assertNotNull(response, "Expected json notification object, instead received null");
//			Assert.assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON,
//					"Expected json media type returned, instead received " + response.getMediaType());
//
//			json = verifyResponse(response, true);
//
//			Assert.assertNotNull(json.get("id").asText().toLowerCase(), "No notification uuid given");
//		}
//		catch (Throwable e) {
//			Assert.fail("Unexpected exception thrown", e);
//		}
//		finally {
//			try {
//				if (json != null) {
//					if (json.has("id")) {
//						Notification n = dao.findByUuid(json.get("id").asText());
//						dao.delete(n);
//					}
//
//				}
//			} catch (Exception e) {}
//		}
//	}
//
//	@Test(dataProvider="addNotificationProvider", dependsOnMethods={"addNotification"})
//	public void addNotificationFromForm(JsonNode body, String errorMessage, boolean shouldThrowException)
//	{
//		ClientResource resource = new ClientResource("http://localhost:8182/notifications");
//		resource.setReferrerRef("http://test.example.com");
//		JsonNode json = null;
//		try
//		{
//			Form form = new Form();
//			if (body != null)
//			{
//				if (body.has("event"))
//				{
//					form.add("event", body.get("event").asText());
//				}
//
//				if (body.has("url"))
//				{
//					form.add("url", body.get("url").asText());
//				}
//
//				if (body.has("associatedUuid"))
//				{
//					form.add("associatedUuid", body.get("associatedUuid").asText());
//				}
//			}
//
//			Representation response = resource.post(form.getWebRepresentation());
//			Assert.assertEquals(resource.getStatus().equals(Status.OK), !shouldThrowException, "Response failed when it should not have");
//			Assert.assertNotNull(response, "Expected json notification object, instead received null");
//			Assert.assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON,
//					"Expected json media type returned, instead received " + response.getMediaType());
//
//			json = verifyResponse(response, true);
//
//			Assert.assertNotNull(json.get("id").asText().toLowerCase(), "No notification uuid given");
//		}
//		catch (Throwable e) {
//			Assert.fail("Unexpected exception thrown", e);
//		}
//		finally {
//			try {
//				if (json != null) {
//					if (json.has("id")) {
//						Notification n = dao.findByUuid(json.get("id").asText());
//						dao.delete(n);
//					}
//
//				}
//			} catch (Exception e) {}
//		}
//	}
//
//	@DataProvider(name="deleteNotificationProvider")
//	public Object[][] deleteNotificationProvider() throws Exception
//	{
//		Notification n = new Notification("SENT", TEST_EMAIL);
//		n.setOwner(TEST_USER);
//		n.setAssociatedUuid(referenceNotification.getUuid());
//		dao.persist(n);
//
//		Notification n2 = new Notification("SENT", TEST_EMAIL);
//		n2.setOwner(TEST_USER + "-test");
//		n2.setAssociatedUuid(referenceNotification.getUuid());
//		dao.persist(n2);
//
//		return new Object[][] {
//			{ n.getUuid(), "Valid url should succeed", false },
//			{ "", "Empty uuid should fail", true },
//			{ "abcd", "Invalid uuid should fail", true },
//			{ n2.getUuid(), "Deleting by non-owner should fail", true },
//		};
//	}
//
//	@Test(dataProvider="deleteNotificationProvider", dependsOnMethods={"addNotificationFromForm"})
//	public void deleteNotification(String uuid, String errorMessage, boolean shouldThrowException)
//	{
//		ClientResource resource = new ClientResource("http://localhost:8182/notifications/" + uuid);
//		resource.setReferrerRef("http://test.example.com");
//		resource.setChallengeResponse(ChallengeScheme.HTTP_BASIC, TEST_USER, Settings.IRODS_PASSWORD);
//		JsonNode json = null;
//		try
//		{
//			Representation response = resource.delete();
//			Assert.assertEquals(resource.getStatus().equals(Status.OK), !shouldThrowException, "Response failed when it should not have");
//			Assert.assertNotNull(response, "Expected json notification object, instead received null");
//			Assert.assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON,
//					"Expected json media type returned, instead received " + response.getMediaType());
//
//			json = verifyResponse(response, shouldThrowException);
//
//			Assert.assertTrue(json.isNull(), "Message results attribute was not null");
//		}
//		catch (Throwable e) {
//			Assert.fail("Unexpected exception thrown", e);
//		}
//		finally {
//			try {
//				Notification n = dao.findByUuid(uuid);
//				if (n != null) {
//					dao.delete(n);
//				}
//			} catch (Exception e) {}
//		}
//	}
//
//	@Test(dataProvider="deleteNotificationProvider", dependsOnMethods={"deleteNotification"})
//	public void fireNotification(String uuid, String errorMessage, boolean shouldThrowException)
//	{
//		ClientResource resource = new ClientResource("http://localhost:8182/notifications/" + uuid);
//		resource.setReferrerRef("http://test.example.com");
//		resource.setChallengeResponse(ChallengeScheme.HTTP_BASIC, TEST_USER, Settings.IRODS_PASSWORD);
//		JsonNode json = null;
//		try
//		{
//			Representation response = resource.delete();
//
//			Assert.assertNotNull(response, "Expected json notification object, instead received null");
//			Assert.assertEquals(response.getMediaType(), MediaType.APPLICATION_JSON,
//					"Expected json media type returned, instead received " + response.getMediaType());
//
//			json = verifyResponse(response, shouldThrowException);
//			Assert.assertEquals(resource.getStatus().equals(Status.OK), !shouldThrowException, "Response failed when it should not have");
//
//		}
//		catch (Throwable e) {
//			Assert.fail("Unexpected exception thrown", e);
//		}
//		finally {
//			try {
//				Notification n = dao.findByUuid(uuid);
//				if (n != null) {
//					dao.delete(n);
//				}
//			} catch (Exception e) {}
//		}
//	}

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
