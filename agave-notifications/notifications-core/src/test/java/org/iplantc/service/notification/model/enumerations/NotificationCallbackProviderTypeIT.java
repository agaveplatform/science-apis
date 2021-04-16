package org.iplantc.service.notification.model.enumerations;

import org.iplantc.service.common.Settings;
import org.iplantc.service.common.dao.TenantDao;
import org.iplantc.service.common.model.Tenant;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.notification.exceptions.BadCallbackException;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.iplantc.service.notification.model.enumerations.NotificationCallbackProviderType.*;

@Test(groups={"integration"})
public class NotificationCallbackProviderTypeIT {

	public NotificationCallbackProviderTypeIT() {}

	@ObjectFactory
	public IObjectFactory getObjectFactory() {
		return new org.powermock.modules.testng.PowerMockObjectFactory();
	}

	@DataProvider
	public Object[][] getInstanceForUriProvider() throws Exception {
		Object[][] testCases = null;
		try {
			Tenant tenant = new TenantDao().findByTenantId("agave.dev");
			URI tenantBaseUrl = URI.create(tenant.getBaseUrl());
			String realtimeHostname = "realtime." + tenantBaseUrl.getHost();

			testCases = new Object[][]{
					{"job@example.com", EMAIL, false, "Valid email should return NotificationCallbackProviderTypeTest.EMAIL"},
					{"mailto:job@example.com", null, true, "Valid mailto uri should throw exception"},
					{"${USERNAME}@example.com", EMAIL, false, "Valid email with template in handle should not throw exception."},
					{"job@${USERNAME}.example.com", null, true, "Valid email with template in handle should should throw exception"},
					{"job@${USERNAME}", null, true, "Valid email with template in hostname should throw exception"},
					{"@example.com", null, true, "Valid email address should throw exception"},
					{"job@", null, true, "Valid email address should throw exception"},
					{"@", null, true, "Valid email address should throw exception"},

					{"http://example.com", WEBHOOK, false, "Valid WEBHOOK should return NotificationCallbackProviderTypeTest.WEBHOOK"},
					{"http://example.com/", WEBHOOK, false, "Valid WEBHOOK should return NotificationCallbackProviderTypeTest.WEBHOOK"},
					{"https://example.com/", WEBHOOK, false, "Valid WEBHOOK should return NotificationCallbackProviderTypeTest.WEBHOOK"},
					{"https://example.com/", WEBHOOK, false, "Valid WEBHOOK should return NotificationCallbackProviderTypeTest.WEBHOOK"},


					{"http://example.com", WEBHOOK, false, "Valid url should be accepted"},
					{"https://example.com", WEBHOOK, false, "Valid url should be accepted"},
					{"example.com", null, true, "Hostname only should throw exception"},
					{"ftp://example.com", null, true, "FTP url protocol should throw exception"},
					{"sftp://example.com", null, true, "SFTP url protocol should throw exception"},
					{"agave://example.com", null, true, "AGAVE url protocol should throw exception"},
					{"gridftp://example.com", null, true, "GRIDFTP url protocol should throw exception"},
					{"file://example.com", null, true, "FILE url protocol should throw exception"},
					{"/example", null, true, "relative path should throw exception"},
					{"file:///", null, true, "FILE url protocol should throw exception"},
					{"///", null, true, "FILE url protocol should throw exception"},

//				 TODO: should recognize internal URL to the API for chaining calls via the custom data in the API. this will require a token being present in the mesage object
					{"https://" + tenantBaseUrl.getHost() + "/files/v2/listings/", AGAVE, false, "Valid AGAVE internal url should return NotificationCallbackProviderTypeTest.AGAVE"},
					{"agave://foo/bar", null, true, "Valid agave url with with agave scheme should throw exception"},

					{"https://hooks.slack.com/services/TTTTTTTTT/BBBBBBBBB/1234567890123456789012345", SLACK, false, "Valid SLACK should return NotificationCallbackProviderTypeTest.SLACK"},
					//{ "https://hooks.slack.com/services/", SLACK, false, "Invalid slack webhook should throw exception"},

					{"https://" + realtimeHostname + "/", REALTIME, false, "Valid REALTIME url with trailing slash should return NotificationCallbackProviderTypeTest.REALTIME"},
					{"https://" + realtimeHostname + "/${JOB_ID}", REALTIME, false, "Valid REALTIME url with variable channel path should return NotificationCallbackProviderTypeTest.REALTIME"},
					{"https://" + realtimeHostname + "/foo/bar", REALTIME, false, "Valid REALTIME url with static channel path should return NotificationCallbackProviderTypeTest.REALTIME"},
					{"https://" + realtimeHostname + "/?arn=agave:notification::${OWNER}:notifications/*/failures", REALTIME, false, "Valid REALTIME url with arn query should return NotificationCallbackProviderTypeTest.REALTIME"},
					{"https://" + realtimeHostname + "/", REALTIME, false, "Valid REALTIME should return NotificationCallbackProviderTypeTest.REALTIME"},
					{"wss://" + realtimeHostname + "/", null, true, "wss url should throw exception"},

					{"http://12345.fanoutcdn.com/fpp/", REALTIME, false, "fanout.io ffp realm subdomains with trailing slash should return REALTIME"},
					{"https://12345.fanoutcdn.com/fpp/", REALTIME, false, "fanout.io ffp ssl realm subdomains with trailing slash should return REALTIME"},
					{"http://12345.fanoutcdn.com/fpp", REALTIME, false, "fanout.io ffp realm subdomains without trailing slash should return REALTIME"},
					{"https://12345.fanoutcdn.com/fpp", REALTIME, false, "fanout.io ffp ssl realm subdomains without trailing slash should return REALTIME"},
					{"http://12345.fanoutcdn.com/bayoux/", REALTIME, false, "fanout.io bayoux realm subdomains with trailing slash should return REALTIME"},
					{"https://12345.fanoutcdn.com/bayoux/", REALTIME, false, "fanout.io bayoux ssl realm subdomains with trailing slashs hould return REALTIME"},
					{"http://12345.fanoutcdn.com/bayoux", REALTIME, false, "fanout.io bayoux realm subdomains without trailing slash should return REALTIME"},
					{"https://12345.fanoutcdn.com/bayoux", REALTIME, false, "fanout.io bayoux ssl realm subdomains without trailing slash should return REALTIME"},
					{"https://12345.fanoutcdn.com/ffp/", WEBHOOK, false, "fanout.io with trailing slash after fpp protocol should return WEBHOOK"},
					{"https://12345.fanoutcdn.com/", WEBHOOK, false, "fanout.io with no protocol in path should return WEBHOOK"},
					{"https://12345.fanoutcdn.com/bayou", WEBHOOK, false, "fanout.io with invalid protocol in path should return WEBHOOK"},
					{"https://12345.fanoutcdn.com/foo", WEBHOOK, false, "fanout.io with invalid protocol in path should return WEBHOOK"},
					{"https://12345.fanoutcdn.com/bayoux/hamilton", WEBHOOK, false, "fanout.io with channel after protocol should return WEBHOOK"},
					{"https://12345.fanoutcdn.com/bayouxs", WEBHOOK, false, "fanout.io with first path token prefixed by bayoux should return WEBHOOK"},
					{"https://12345.fanoutcdn.com/fpps", WEBHOOK, false, "fanout.io with first path token prefixed by ffp should return WEBHOOK"},
					{"http://fanoutcdn.com/bayoux", WEBHOOK, false, "fanout.io bayoux with no realm subdomain should return WEBHOOK"},
					{"https://fanoutcdn.com/bayoux", WEBHOOK, false, "fanout.io bayoux with ssl and no realm subdomain should return WEBHOOK"},
					{"http://fanoutcdn.com/fpp", WEBHOOK, false, "fanout.io ffp with no realm subdomain should return WEBHOOK"},
					{"https://fanoutcdn.com/fpp", WEBHOOK, false, "fanout.io ffp with ssl and no realm subdomain should return WEBHOOK"},
					{"http://fanoutcdn.com/fpp/", WEBHOOK, false, "fanout.io ffp with no realm subdomain with trailing slash should return WEBHOOK"},
					{"https://fanoutcdn.com/fpp/", WEBHOOK, false, "fanout.io ffp with ssl and no realm subdomain with trailing slash should return WEBHOOK"},


					{"http://foo.bar:5561/publish", REALTIME, false, "pushpin URL should return REALTIME"},
					{"https://foo.bar:5561/publish", REALTIME, false, "pushpin ssl URL should return REALTIME"},
					{"http://bar:5561/publish", REALTIME, false, "pushpin URL should return REALTIME"},
					{"https://bar:5561/publish", REALTIME, false, "pushpin ssl URL should return REALTIME"},
					{"http://foo.bar:55/publish", REALTIME, false, "pushpin URL with alternate port should return REALTIME"},
					{"https://foo.bar:55/publish", REALTIME, false, "pushpin ssl URL with alternate port should return REALTIME"},
					{"http://bar:55/publish", REALTIME, false, "pushpin URL with alternate port should return REALTIME"},
					{"https://bar:55/publish", REALTIME, false, "pushpin ssl URL with alternate port should return REALTIME"},
					{"http://foo.bar:5561/publish/", WEBHOOK, false, "pushpin URL should return WEBHOOK"},
					{"http://foo.bar:5561/publish/foo", WEBHOOK, false, "pushpin URL should return WEBHOOK"},
					{"http://foo.bar:5561/publishing", WEBHOOK, false, "pushpin URL with bad path should return WEBHOOK"},

					{"5555555555", SMS, false, "Valid unformatted phone number should return NotificationCallbackProviderType.SMS"},
					{"tel:5555555555", null, true, "Valid tel URL should throw exception"},
					{"+15555555555", null, true, "Valid US International phone number should throw exception"},
					{"+2225555555555", null, true, "Valid International phone number should throw exception"},
					{"5555555", null, true, "Missing area code phone number should throw exception."},
					{"15555555555", null, true, "Too many digits with + prefix should throw exception."},
					{"555555555", null, true, "Too few many digits should throw exception."},

					{"///", null, true, "Invalid relative path should throw exception"},
					{"/foo/bar/", null, true, "Invalid relative path should throw exception"},

					{"", null, true, "Empty url should throw exception"},
					{null, null, true, "null url should throw exception"},
			};
		} catch (NullPointerException e) {
			Assert.fail("Failed creating test cases", e);
		}

		return testCases;
	}

	@Test(dataProvider = "getInstanceForUriProvider")
	public void getInstanceForUri(String callbackUrl, NotificationCallbackProviderType expected, boolean shouldThrowException, String message) 
	{
		try {
			NotificationCallbackProviderType found = NotificationCallbackProviderType.getInstanceForUri(callbackUrl, "agave.dev");
			Assert.assertEquals(found, expected, message);
		}
		catch(NullPointerException e) {
			Assert.fail("Unexpected NPE", e);
		} catch (Throwable e) {
			Assert.assertTrue(shouldThrowException, message);
		}
	}
	
	@DataProvider
	public Object[][] getInstanceForUriFailsOnLoopackURLProvider() {
		String[] invalidHosts = {"127.0.0.1", "168.192.8.18", "172.1.1.1", "255.255.255.255", 
				"localhost", "localhost.local", "local.localhost", "local",
				Settings.getLocalHostname(), Settings.getIpLocalAddress()};
		
		List<Object[]> testCases = new ArrayList<Object[]>();
		for (String invalidHost: invalidHosts) {
			testCases.add(new Object[] { invalidHost });
		}
		
		for (String invalidHost: Settings.getIpAddressesFromNetInterface()) {
			testCases.add(new Object[] { invalidHost });
		}
		
		return testCases.toArray(new Object[][] {});
	}
	
	@Test(dataProvider = "getInstanceForUriFailsOnLoopackURLProvider")
	public void getInstanceForUriFailsOnLoopackURL(String callbackUrl) 
	{
		try {
			NotificationCallbackProviderType found = NotificationCallbackProviderType.getInstanceForUri(callbackUrl, "agave.dev");
			Assert.fail(callbackUrl + " should throw a BadCallbackException and never be allowed as a notification calback url");
		} catch (BadCallbackException e) {
			// this is what we want.
		}
	}
}
