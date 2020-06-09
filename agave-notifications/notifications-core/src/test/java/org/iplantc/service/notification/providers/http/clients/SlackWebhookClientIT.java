package org.iplantc.service.notification.providers.http.clients;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.model.NotificationAttempt;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Properties;

@Test(groups={"broken", "integration"}) // need to mock the slack call or provide a valid url for this to pass. Tests all work, though
public class SlackWebhookClientIT {

	private String slackWebhookUrl = null;

	@BeforeClass
	private void beforeClass() {
		Properties props = org.iplantc.service.common.Settings.loadRuntimeProperties();
		slackWebhookUrl = props.getProperty("test.slack.webhook.url");
		if (StringUtils.isEmpty(slackWebhookUrl)) {
			throw new SkipException("No Slack Incoming Webhook URL provided in the test service.properties file.");
		}
	}

	@DataProvider
	protected Object[][] getFilteredContentProvider() throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode simple = mapper.createObjectNode()
				.put("subject", "Test basic notification")
				.put("color", "good");
		
		JsonNode runtimeData = mapper.createObjectNode()
			.put("foo", "foo")
			.put("bar", simple.toString())
			.put("CUSTOM_USER_JOB_EVENT_NAME", "JOB_RUNTIME_CALLBACK_EVENT")
			.set("baz", simple);

			
		ObjectNode runtime = mapper.createObjectNode()
				.put("subject", "Test basic notification")
				.put("color", "red")
				.put("body", mapper.writer(new DefaultPrettyPrinter()).writeValueAsString(runtimeData));
		
		return new Object[][] {
				{ simple.toString(), slackWebhookUrl },
				{ runtime.toString(), slackWebhookUrl },
		};
	}
	
	@Test(dataProvider="getFilteredContentProvider")
	public void getFilteredContent(String content, String callbackUrl) {
		NotificationAttempt attempt = mock(NotificationAttempt.class);
		when(attempt.getContent()).thenReturn(content);
		when(attempt.getCallbackUrl()).thenReturn(callbackUrl);
		when(attempt.getNotificationId()).thenReturn(new AgaveUUID(UUIDType.NOTIFICATION).toString());
		when(attempt.getEventName()).thenReturn("UNIT_TEST_EVENT");
	  
		SlackWebhookClient client = spy(new SlackWebhookClient(attempt));
		try {
			client.publish();
		
			verify(client).publish();
			verify(client).getFilteredContent(content);
	    
		} catch (NotificationException e) {
			Assert.fail("Publishing slack message should not fail for valid test data", e);
		}
  	}
	
	@DataProvider
	protected Object[][] getFilteredContentHonorsColorsProvider() throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode simple = mapper.createObjectNode()
				.put("subject", "Test basic notification");
		
		Object[][] testCases = new Object[][] {
				{ simple.deepCopy().put("color", "good").toString(),    slackWebhookUrl },
				{ simple.deepCopy().put("color", "warning").toString(), slackWebhookUrl },
				{ simple.deepCopy().put("color", "danger").toString(),  slackWebhookUrl },
				{ simple.deepCopy().put("color", "#f3f3f3").toString(), slackWebhookUrl },
		};
		
		return testCases;
	}
	
	@Test(dataProvider="getFilteredContentHonorsColorsProvider")
	public void getFilteredContentHonorsColors(String content, String callbackUrl) {
		NotificationAttempt attempt = mock(NotificationAttempt.class);
		when(attempt.getContent()).thenReturn(content);
		when(attempt.getCallbackUrl()).thenReturn(callbackUrl);
		when(attempt.getNotificationId()).thenReturn(new AgaveUUID(UUIDType.NOTIFICATION).toString());
		when(attempt.getEventName()).thenReturn("UNIT_TEST_EVENT");
	  
		SlackWebhookClient client = spy(new SlackWebhookClient(attempt));
		try {
			client.publish();
		
			verify(client).publish();
			verify(client).getFilteredContent(content);
	    
		} catch (NotificationException e) {
			Assert.fail("Publishing slack message should not fail for valid test data", e);
		}
  	}
}
