package org.iplantc.service.jobs.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareInput;
import org.iplantc.service.apps.model.SoftwareParameter;
import org.iplantc.service.apps.model.SoftwareParameterEnumeratedValue;
import org.iplantc.service.apps.model.enumerations.SoftwareParameterType;
import org.iplantc.service.io.util.ServiceUtils;
import org.iplantc.service.jobs.dao.AbstractDaoTest;
import org.iplantc.service.jobs.exceptions.JobProcessingException;
import org.iplantc.service.jobs.model.JSONTestDataUtil;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.iplantc.service.systems.manager.SystemManager;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.json.JSONException;
import org.json.JSONObject;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static org.iplantc.service.jobs.model.JSONTestDataUtil.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(groups = {"integration"})
public class JobRequestProcessorIT extends AbstractDaoTest
{
	private boolean pass = false;
	private boolean fail = true;
	private ObjectMapper mapper = new ObjectMapper();
	
	private static final Answer<Boolean> ANSWER_TRUE = new Answer<Boolean>() {
		 public Boolean answer(InvocationOnMock invocation) throws Throwable {
	         return true;
	     }
	};
	
	@BeforeClass
	public void beforeClass() throws Exception {
		super.beforeClass();
		drainQueue();
	}

	@AfterClass
	public void afterClass() throws Exception {
		super.afterClass();
		drainQueue();
	}

	/**
	 * Adds or updates a value in a Restlet request form.
	 * 
	 * @param jobRequestMap the request form submission map
	 * @param field the field to add to the map
	 * @param value the value of the field being added
	 * @return the updated request map
	 */
	private Map<String, Object> updateJobRequestMap(Map<String, Object> jobRequestMap, String field, Object value) 
	{
		if (value == null) {
		    jobRequestMap.put(field, null);
		}
		else if (value instanceof String) {
		    jobRequestMap.put(field, (String)value);
		}
		else
		{
		    jobRequestMap.put(field, ObjectUtils.toString(value));
		}
		
		return jobRequestMap;
	}
	
	/**
	 * Sets an field in a ObjectNode object, determining the proper type on the fly.
	 * 
	 * @param json the object posted to the job reqeust
	 * @param field the field to add to the object
	 * @param value the value of the field
	 * @return the updated ObjectNode
	 */
	private ObjectNode updateObjectNode(ObjectNode json, String field, Object value)
	{
		if (value == null)
			json.putNull(field);
		else if (value instanceof ArrayNode)
			json.putArray(field).addAll((ArrayNode)value);
		else if (value instanceof ObjectNode)
			json.putObject(field);
		else if (value instanceof Long)
			json.put(field, (Long)value);
		else if (value instanceof Integer)
			json.put(field, (Integer)value);
		else if (value instanceof Float)
			json.put(field, (Float)value);
		else if (value instanceof Double)
			json.put(field, (Double)value);
		else if (value instanceof BigDecimal)
			json.put(field, (BigDecimal)value);
		else if (value instanceof Boolean)
			json.put(field, (Boolean)value);
		else if (value instanceof Collection) {
			ArrayNode arrayNode = new ObjectMapper().createArrayNode();
			for (Object o: (Collection)value) {
				if (o instanceof ArrayNode)
					arrayNode.addArray().addAll((ArrayNode)o);
				else if (o instanceof ObjectNode) {
					assert value instanceof ObjectNode;
					arrayNode.add((ObjectNode)value);
				}
				else if (o instanceof Long) {
					assert value instanceof Long;
					arrayNode.add((Long)value);
				}
				else if (o instanceof Integer) {
					assert value instanceof Long;
					arrayNode.add((Long)value);
				}
				else if (o instanceof Float) {
					assert value instanceof Long;
					arrayNode.add((Long)value);
				}
				else if (o instanceof Double) {
					assert value instanceof Long;
					arrayNode.add((Long)value);
				}
				else if (o instanceof Boolean) {
					assert value instanceof Boolean;
					arrayNode.add((Boolean)value);
				}
				else if (o instanceof String) {
					assert value instanceof String;
					arrayNode.add((String)value);
				}
				else
					arrayNode.addObject();
			}
			json.putArray(field).addAll(arrayNode);
		}
		else if (value instanceof Map) {
			for (String key: ((Map<String,Object>)value).keySet()) {
				json = updateObjectNode(json, key, ((Map<String,Object>)value).get(key));
			}
		}
		else if (value instanceof String)
			json.put(field, (String)value);
		else 
			json.putObject(field);
		
		return json;
	}
	
	/**
	 * Creates a JsonNode representation of a job notification that expires after first delivery
	 * @param url the url target of the notification
	 * @param event the name of the event to which to subscribe
	 * @return a json representation of the notification
	 */
	private JsonNode createJsonNotification(Object url, Object event) 
	{
		return createJsonNotification(url, event, false);
	}
	
	/**
	 * Creates a JsonNode representation of a job notification using the supplied values 
	 * and determining the types on the fly.
	 * 
	 * @param url the url target of the notification
	 * @param event the name of the event to which to subscribe
	 * @param persistent true if the notification should persist after firing once.
	 * @return a json representation of the notification
	 */
	private JsonNode createJsonNotification(Object url, Object event, boolean persistent) 
	{
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode json = updateObjectNode(mapper.createObjectNode(), "url", url);
		json = updateObjectNode(json, "event", event);
		json.put("persistent", persistent);
		
		return json;
	}
	
	/**
	 * Creates a bare bones ObjectNode representing a job submission.
	 * @return ObjectNode with minimal set of job attributes.
	 * @throws IOException 
	 * @throws JsonProcessingException 
	 */
	private ObjectNode createJobJsonNode(Software software)
	{
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode json = mapper.createObjectNode();
		
		try 
		{
			json.put("name", UUID.randomUUID().toString());
			json.put("appId", software.getUniqueName());
			ObjectNode jsonInput = mapper.createObjectNode();
			for (SoftwareInput input: software.getInputs()) {
				jsonInput.putArray(input.getKey()).addAll(input.getDefaultValueAsJsonArray());
			}
			json.set("inputs", jsonInput);
			
			ObjectNode jsonParameter = mapper.createObjectNode();
			for (SoftwareParameter parameter: software.getParameters()) {
				jsonParameter.putArray(parameter.getKey()).addAll(parameter.getDefaultValueAsJsonArray());
			}
			json.set("parameters", jsonParameter);
		} catch (Exception e) {
			Assert.fail("Failed to read in software description to create json job object", e);
		}
		
		return json;
	}
	
	/**
	 * Creates a bare bones Form representing a job submission.
	 * @return Map with minimal set of job attributes.
	 */
	private Map<String, Object> createJobRequestMap(Software software)
	{
	    Map<String, Object> jobRequestMap = new HashMap<String, Object>();
		try 
		{
			jobRequestMap.put("name", "processJsonJobWithNotifications");
			jobRequestMap.put("appId", software.getUniqueName());
			for (SoftwareInput input: software.getInputs()) {
				jobRequestMap.put(input.getKey(), input.getDefaultValueAsJsonArray().iterator().next().asText());
			}
			
			for (SoftwareParameter parameter: software.getParameters()) {
				jobRequestMap.put(parameter.getKey(), parameter.getDefaultValueAsJsonArray().iterator().next().asText());
			}
		} catch (Exception e) {
			Assert.fail("Failed to read in software description to create json job object", e);
		}
		
		return jobRequestMap;
	}

	/**
	 * Creates a {@link SoftwareParameter} with teh given constraints, validator, etc.
	 * @param key the key of the parameter
	 * @param type the type of the parameter
	 * @param defaultValue the default value of the parameter
	 * @param validator the validator to use on job requests with this parameter
	 * @param required whether the parameter is required
	 * @param visible whether the parameter should be visible
	 * @return a software parameter with the given constraints
	 */
	private SoftwareParameter createParameter(String key, String type, Object defaultValue, String validator, boolean required, boolean visible) throws JsonProcessingException, IOException
	{
		SoftwareParameter param = new SoftwareParameter();
		param.setKey(key);
		param.setType(type);
		param.setDefaultValue(defaultValue == null ? null : defaultValue.toString());
		param.setRequired(required);
		param.setVisible(visible);
		if (type.equalsIgnoreCase(SoftwareParameterType.enumeration.name())) {
			assert defaultValue != null;
			String val = ((ArrayNode)defaultValue).get(0).textValue();
			List<SoftwareParameterEnumeratedValue> enums = new ArrayList<SoftwareParameterEnumeratedValue>();
			enums.add(new SoftwareParameterEnumeratedValue(val, val, param));
			enums.add(new SoftwareParameterEnumeratedValue("BETA", "BETA", param));
			enums.add(new SoftwareParameterEnumeratedValue("GAMMA", "GAMMA", param));
			enums.add(new SoftwareParameterEnumeratedValue("DELTA", "DELTA", param));
			param.setEnumValues(enums);
		} else {
			param.setDefaultValue(defaultValue == null ? null : defaultValue.toString());
			param.setValidator(validator);
		}
		return param;
	}

	/**
	 * Creates a {@link SoftwareInput} with teh given constraints, validator, etc.
	 * @param key the key of the input
	 * @param defaultValue the default value of the input
	 * @param validator the validator to use on job requests with this input
	 * @param required whether the input is required
	 * @param visible whether the input should be visible
	 * @return a software input with the given constraints
	 */
	private SoftwareInput createInput(String key, String defaultValue, String validator, boolean required, boolean visible)
	{
		SoftwareInput input = new SoftwareInput();
		input.setKey(key);
		input.setDefaultValue(defaultValue);
		input.setRequired(required);
		input.setVisible(visible);
		input.setValidator(validator);
		input.setMaxCardinality(-1);
		input.setMinCardinality(required ? 1 : 0);
		
		return input;
	}
	
	@DataProvider
	public Object[][] validateBatchSubmitParametersProvider() {
		// name maxJobs userJobs nodes memory, procs, time, cstm, default
		BatchQueue queueUnbounded = new BatchQueue("queueMaximum", (long) -1,
				(long) -1, (long) -1, (double) -1.0, (long) -1,
				BatchQueue.DEFAULT_MAX_RUN_TIME, null, false);
		BatchQueue queueMinimal = new BatchQueue("queueMinimal", 2L,
				2L, 2L, (double) 2.0, 2L, "00:01:00", null,
				false);

		boolean pass = true;
		boolean fail = false;

		return new Object[][] {
				// fixed limit queue
				{ queueMinimal, queueMinimal.getMaxNodes(),
						queueMinimal.getMaxProcessorsPerNode(),
						queueMinimal.getMaxMemoryPerNode(),
						queueMinimal.getMaxRequestedTime(), pass,
						"Everything at limits should pass" },
				{ queueMinimal, (queueMinimal.getMaxNodes() - (long) 1),
						(queueMinimal.getMaxProcessorsPerNode() - (long) 1),
						(queueMinimal.getMaxMemoryPerNode() - (double) 1),
						"00:00:30", pass, 
						"Everything under limits should pass" },
				{ queueMinimal, (queueMinimal.getMaxNodes() + (long) 1),
						(queueMinimal.getMaxProcessorsPerNode() + (long) 1),
						(queueMinimal.getMaxMemoryPerNode() + (double) 1),
						"00:03:00", fail, 
						"Everything over limits should fail" },
				{ queueMinimal, null, null, null, null, fail,
						"Everything null should fail" },
				// node checks
				{ queueMinimal, (long) -1,
						queueMinimal.getMaxProcessorsPerNode(),
						queueMinimal.getMaxMemoryPerNode(),
						queueMinimal.getMaxRequestedTime(), pass,
						"Nodes unbounded, everything else at limits should pass" },
				{ queueMinimal, (long) -2,
						queueMinimal.getMaxProcessorsPerNode(),
						queueMinimal.getMaxMemoryPerNode(),
						queueMinimal.getMaxRequestedTime(), fail,
						"Nodes negative, everything else at limits should fail" },
				{ queueMinimal, 0L,
						queueMinimal.getMaxProcessorsPerNode(),
						queueMinimal.getMaxMemoryPerNode(),
						queueMinimal.getMaxRequestedTime(), fail,
						"Nodes zero, everything else at limits should fail" },
				{ queueMinimal, null, queueMinimal.getMaxProcessorsPerNode(),
						queueMinimal.getMaxMemoryPerNode(),
						queueMinimal.getMaxRequestedTime(), fail,
						"Nodes null, everything else at limits should fail" },
				{ queueMinimal, (queueMinimal.getMaxNodes() + (long) 1),
						queueMinimal.getMaxProcessorsPerNode(),
						queueMinimal.getMaxMemoryPerNode(),
						queueMinimal.getMaxRequestedTime(), fail,
						"Nodes over, everything else at limits should fail" },
				// proc checks
				{ queueMinimal, queueMinimal.getMaxNodes(), (long) -1,
						queueMinimal.getMaxMemoryPerNode(),
						queueMinimal.getMaxRequestedTime(), pass,
						"Procs unbounded, everything else at limits should pass" },
				{ queueMinimal, queueMinimal.getMaxNodes(), (long) -2,
						queueMinimal.getMaxMemoryPerNode(),
						queueMinimal.getMaxRequestedTime(), fail,
						"Procs negative, everything else at limits should fail" },
				{ queueMinimal, queueMinimal.getMaxNodes(), 0L,
						queueMinimal.getMaxMemoryPerNode(),
						queueMinimal.getMaxRequestedTime(), fail,
						"Procs zero, everything else at limits should fail" },
				{ queueMinimal, queueMinimal.getMaxNodes(), null,
						queueMinimal.getMaxMemoryPerNode(),
						queueMinimal.getMaxRequestedTime(), fail,
						"Procs null, everything else at limits should fail" },
				{ queueMinimal, queueMinimal.getMaxNodes(),
						(queueMinimal.getMaxNodes() + (long) 1),
						queueMinimal.getMaxMemoryPerNode(),
						queueMinimal.getMaxRequestedTime(), fail,
						"Procs over, everything else at limits should fail" },
				// memory checks
				{ queueMinimal, queueMinimal.getMaxNodes(),
						queueMinimal.getMaxProcessorsPerNode(), (double) -1,
						queueMinimal.getMaxRequestedTime(), pass,
						"Memory unbounded, everything else at limits should pass" },
				{ queueMinimal, queueMinimal.getMaxNodes(),
						queueMinimal.getMaxProcessorsPerNode(), (double) -2,
						queueMinimal.getMaxRequestedTime(), fail,
						"Memory negative, everything else at limits should fail" },
				{ queueMinimal, queueMinimal.getMaxNodes(),
						queueMinimal.getMaxProcessorsPerNode(), (double) 0,
						queueMinimal.getMaxRequestedTime(), fail,
						"Memory zero, everything else at limits should fail" },
				{ queueMinimal, queueMinimal.getMaxNodes(),
						queueMinimal.getMaxProcessorsPerNode(), null,
						queueMinimal.getMaxRequestedTime(), fail,
						"Memory null, everything else at limits should fail" },
				{ queueMinimal, queueMinimal.getMaxNodes(),
						queueMinimal.getMaxProcessorsPerNode(),
						(queueMinimal.getMaxMemoryPerNode() + (double) 1),
						queueMinimal.getMaxRequestedTime(), fail,
						"Memory over, everything else at limits should fail" },
				// time checks
				{ queueMinimal, 
						queueMinimal.getMaxNodes(),
						queueMinimal.getMaxProcessorsPerNode(),
						queueMinimal.getMaxMemoryPerNode(),
						"00:01:00", pass,
						"Time equal, everything else at limits should pass" },
				{ queueMinimal, 
						queueMinimal.getMaxNodes(),
						queueMinimal.getMaxProcessorsPerNode(), 
						queueMinimal.getMaxMemoryPerNode(),
						"00:00:30", pass,
						"Time under, everything else at limits should pass" },
				{ queueMinimal, 
						queueMinimal.getMaxNodes(),
						queueMinimal.getMaxProcessorsPerNode(), 
						queueMinimal.getMaxMemoryPerNode(),
						"00:03:00", fail,
						"Time over, everything else at limits should fail" },
				{ queueMinimal, 
						queueMinimal.getMaxNodes(),
						queueMinimal.getMaxProcessorsPerNode(), 
						queueMinimal.getMaxMemoryPerNode(),
						null, fail,
						"Time null, everything else at limits should fail" },

				// unbounded queue
				{ queueUnbounded, queueUnbounded.getMaxNodes(),
						queueUnbounded.getMaxProcessorsPerNode(),
						queueUnbounded.getMaxMemoryPerNode(),
						queueUnbounded.getMaxRequestedTime(), pass,
						"Everything at unbounded limits should pass" },
				{ queueUnbounded, 1L, 1L, 1d,
						"00:01:00", pass,
						"Everything under unbounded limits should pass" },
				{ queueUnbounded, null, null, null, null, fail,
						"Everything null should fail" },
				{ queueUnbounded, (long) -2, (long) -2, (double) -2,
						"00:01:00", fail, "Everything negative should fail" },
				// node checks
				{ queueUnbounded, 1L,
						queueUnbounded.getMaxProcessorsPerNode(),
						queueUnbounded.getMaxMemoryPerNode(),
						queueUnbounded.getMaxRequestedTime(), pass,
						"Nodes under, everything else unbounded should pass" },
				{ queueUnbounded, (long) -2,
						queueUnbounded.getMaxProcessorsPerNode(),
						queueUnbounded.getMaxMemoryPerNode(),
						queueUnbounded.getMaxRequestedTime(), fail,
						"Nodes negative, everything else unbounded should fail" },
				{ queueUnbounded, 0L,
						queueUnbounded.getMaxProcessorsPerNode(),
						queueUnbounded.getMaxMemoryPerNode(),
						queueUnbounded.getMaxRequestedTime(), fail,
						"Nodes zero, everything else unbounded should fail" },
				{ queueUnbounded, null,
						queueUnbounded.getMaxProcessorsPerNode(),
						queueUnbounded.getMaxMemoryPerNode(),
						queueUnbounded.getMaxRequestedTime(), fail,
						"Nodes null, everything else unbounded should fail" },
				// proc checks
				{ queueUnbounded, 
						queueUnbounded.getMaxNodes(),
						1L,
						queueUnbounded.getMaxMemoryPerNode(),
						queueUnbounded.getMaxRequestedTime(), pass,
						"Procs under, everything else unbounded should pass" },
				{ queueUnbounded, 
						queueUnbounded.getMaxNodes(),
						(long) -2,
						queueUnbounded.getMaxMemoryPerNode(),
						queueUnbounded.getMaxRequestedTime(), fail,
						"Procs negative, everything else unbounded should fail" },
				{ queueUnbounded, 
						queueUnbounded.getMaxNodes(),
						0L,
						queueUnbounded.getMaxMemoryPerNode(),
						queueUnbounded.getMaxRequestedTime(), fail,
						"Procs zero, everything else unbounded should fail" },
				{ queueUnbounded, 
						queueUnbounded.getMaxNodes(), 
						null,
						queueUnbounded.getMaxMemoryPerNode(),
						queueUnbounded.getMaxRequestedTime(), fail,
						"Procs null, everything else unbounded should fail" },
				// memory checks
				{ queueUnbounded, 
						queueUnbounded.getMaxNodes(),
						queueUnbounded.getMaxProcessorsPerNode(),
						1d,
						queueUnbounded.getMaxRequestedTime(), pass,
						"Memory under, everything else unbounded should fail" },
				{ queueUnbounded, 
						queueUnbounded.getMaxNodes(),
						queueUnbounded.getMaxProcessorsPerNode(),
						(double) -2,
						queueUnbounded.getMaxRequestedTime(), fail,
						"Memory negative, everything else unbounded should fail" },
				{ queueUnbounded, 
						queueUnbounded.getMaxNodes(),
						queueUnbounded.getMaxProcessorsPerNode(),
						(double) 0,
						queueUnbounded.getMaxRequestedTime(), fail,
						"Memory zero, everything else unbounded should fail" },
				{ queueUnbounded, 
						queueUnbounded.getMaxNodes(),
						queueUnbounded.getMaxProcessorsPerNode(), 
						null,
						queueUnbounded.getMaxRequestedTime(), fail,
						"Memory null, everything else unbounded should fail" },
				// time checks
				{ queueUnbounded, 
						queueUnbounded.getMaxNodes(),
						queueUnbounded.getMaxProcessorsPerNode(),
						queueUnbounded.getMaxMemoryPerNode(), 
						"00:01:00", pass,
						"Time equal, everything else unbounded should pass" },
				{ queueUnbounded, 
						queueUnbounded.getMaxNodes(),
						queueUnbounded.getMaxProcessorsPerNode(),
						queueUnbounded.getMaxMemoryPerNode(), 
						"00:00:00", fail,
						"Time zero, everything else unbounded should fail" },
				{ queueUnbounded, 
						queueUnbounded.getMaxNodes(),
						queueUnbounded.getMaxProcessorsPerNode(),
						queueUnbounded.getMaxMemoryPerNode(),  
						null, fail,
						"Time null, everything else unbounded should fail" }, };
	}

	/**
	 * Generic method to run the JobManager.processJob(JsonNode, String, String) method.
	 * @param json the json object representing the job request
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the assertion message to be returned if the test fails
	 */
	private Job _genericProcessJsonJob(ObjectNode json, boolean shouldThrowException, String message)
	{
		Job job = null;
		try 
		{
			JobRequestInputProcessor inputProcessor = mock(JobRequestInputProcessor.class);
			doNothing().when(inputProcessor).process(any());
			when(inputProcessor.getJobInputs()).thenReturn(mapper.createObjectNode());

			JobRequestParameterProcessor parameterProcessor = mock(JobRequestParameterProcessor.class);
			doNothing().when(parameterProcessor).process(any());
			when(parameterProcessor.getJobParameters()).thenReturn(mapper.createObjectNode());

			JobRequestNotificationProcessor notificationProcessor = mock(JobRequestNotificationProcessor.class);
			doNothing().when(notificationProcessor).process(any(JsonNode.class));
			doNothing().when(notificationProcessor).process(any(ArrayNode.class));
			doNothing().when(notificationProcessor).process(anyString());
			when(notificationProcessor.getNotifications()).thenReturn(new ArrayList<Notification>());

			JobRequestProcessor processor = mock(JobRequestProcessor.class, new Answer() {
				/**
				 * @param invocation the invocation on the mock.
				 * @return the value to be returned
				 * @throws Throwable the throwable to be thrown
				 */
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					if (invocation.getMethod().getName().equals("isSoftwareInvokableByUser")) {
						return true;
					} else if (invocation.getMethod().getName().equals("getInternalUsername")) {
						return null;
					} else if (invocation.getMethod().getName().equals("getParameterProcessor")) {
						return parameterProcessor;
					} else if (invocation.getMethod().getName().equals("getInputProcessor")) {
						return inputProcessor;
					} else if (invocation.getMethod().getName().equals("getNotificationProcessor")) {
						return notificationProcessor;
					} else if (invocation.getMethod().getName().equals("checkExecutionSystemLogin")) {
						return true;
					} else if (invocation.getMethod().getName().equals("checkExecutionSystemStorage")) {
						return true;
					} else if (invocation.getMethod().getName().equals("getUsername")) {
						return TEST_OWNER;
					} else {
						return invocation.callRealMethod();
					}
				}
			});

			when(processor.getUsername()).thenReturn(TEST_OWNER);
			when(processor.getInternalUsername()).thenReturn(null);
			when(processor.isSoftwareInvokableByUser(any(), eq(TEST_OWNER))).thenReturn(true);
			job = processor.processJob(json);
//
////			JobRequestProcessor jobRequestProcessor = new JobRequestProcessor(JSONTestDataUtil.TEST_OWNER, null);
////			job = JobManager.processJob(json, JSONTestDataUtil.TEST_OWNER, null);
//
//			JobRequestProcessor jobRequestProcessor = spy(JobRequestProcessor.class);
//			doReturn(true).when(jobRequestProcessor).checkExecutionSystemLogin(any(ExecutionSystem.class));
//			doReturn(true).when(jobRequestProcessor).checkExecutionSystemStorage(any(ExecutionSystem.class));
//			doReturn(true).when(jobRequestProcessor).createArchivePath(any(ExecutionSystem.class), any(String.class));
////			when(jobRequestProcessor.checkExecutionSystemLogin(any(ExecutionSystem.class)).thenReturn(true);
////			when(jobRequestProcessor.checkExecutionSystemStorage(any(ExecutionSystem.class))).thenReturn(true);
////			when(jobRequestProcessor.createArchivePath(any(RemoteSystem.class), any(String.class))).thenReturn(true);
//
//			jobRequestProcessor.setUsername(JSONTestDataUtil.TEST_OWNER);
//
//			job = jobRequestProcessor.processJob(json);
			
			assertNotNull(job.getId(), "Job was not saved after processing.");
		} 
		catch (JobProcessingException e) {
			if (!shouldThrowException) {
				Assert.fail(message, e);
			}
		} 
		catch (Exception e) {
			Assert.fail("Unexpected failed to process job", e);
		}
		
		return job;
	}
	
	/**
	 * Generic method to run the JobManager.processJob(Form, String, String) method.
	 * @param jobRequestMap the request form submission map
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the assertion message to be returned if the test fails
	 */
	private Job genericProcessFormJob(Map<String, Object> jobRequestMap, boolean shouldThrowException, String message)
	{
		Job job = null;
		
		try 
		{
			JobRequestInputProcessor inputProcessor = mock(JobRequestInputProcessor.class);
			doNothing().when(inputProcessor).process(any());
			when(inputProcessor.getJobInputs()).thenReturn(mapper.createObjectNode());

			JobRequestParameterProcessor parameterProcessor = mock(JobRequestParameterProcessor.class);
			doNothing().when(parameterProcessor).process(any());
			when(parameterProcessor.getJobParameters()).thenReturn(mapper.createObjectNode());

			JobRequestNotificationProcessor notificationProcessor = mock(JobRequestNotificationProcessor.class);
			doNothing().when(notificationProcessor).process(any(JsonNode.class));
			doNothing().when(notificationProcessor).process(any(ArrayNode.class));
			doNothing().when(notificationProcessor).process(anyString());
			when(notificationProcessor.getNotifications()).thenReturn(new ArrayList<Notification>());

			JobRequestProcessor processor = mock(JobRequestProcessor.class, new Answer() {
				/**
				 * @param invocation the invocation on the mock.
				 * @return the value to be returned
				 * @throws Throwable the throwable to be thrown
				 */
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					if (invocation.getMethod().getName().equals("isSoftwareInvokableByUser")) {
						return true;
					} else if (invocation.getMethod().getName().equals("getInternalUsername")) {
						return null;
					} else if (invocation.getMethod().getName().equals("getParameterProcessor")) {
						return parameterProcessor;
					} else if (invocation.getMethod().getName().equals("getInputProcessor")) {
						return inputProcessor;
					} else if (invocation.getMethod().getName().equals("getNotificationProcessor")) {
						return notificationProcessor;
					} else if (invocation.getMethod().getName().equals("checkExecutionSystemLogin")) {
						return true;
					} else if (invocation.getMethod().getName().equals("checkExecutionSystemStorage")) {
						return true;
					} else if (invocation.getMethod().getName().equals("getUsername")) {
						return TEST_OWNER;
					} else {
						return invocation.callRealMethod();
					}
				}
			});

			when(processor.getUsername()).thenReturn(TEST_OWNER);
			when(processor.getInternalUsername()).thenReturn(null);
			when(processor.isSoftwareInvokableByUser(any(), eq(TEST_OWNER))).thenReturn(true);
			job = processor.processJob(jobRequestMap);

//
////			job = JobManager.processJob(jobRequestMap, JSONTestDataUtil.TEST_OWNER, null);
////			Assert.assertNotNull(job.getId(), "Job was not saved after processing.");
//
//			JobRequestProcessor jobRequestProcessor = spy(JobRequestProcessor.class);
//			jobRequestProcessor.setUsername(JSONTestDataUtil.TEST_OWNER);
//
//			doReturn(true).when(jobRequestProcessor).checkExecutionSystemLogin(any(ExecutionSystem.class));
//			doReturn(true).when(jobRequestProcessor).checkExecutionSystemStorage(any(ExecutionSystem.class));
//			doReturn(true).when(jobRequestProcessor).createArchivePath(any(ExecutionSystem.class), any(String.class));
//
////			when(jobRequestProcessor.checkExecutionSystemLogin(any(ExecutionSystem.class))).thenReturn(true);
////			when(jobRequestProcessor.checkExecutionSystemStorage(any(ExecutionSystem.class))).thenReturn(true);
////			when(jobRequestProcessor.createArchivePath(any(RemoteSystem.class), any(String.class))).thenReturn(true);
//
//			job = jobRequestProcessor.processJob(jobRequestMap);
			
			assertNotNull(job.getId(), "Job was not saved after processing.");
		} catch (JobProcessingException e) {
			if (!shouldThrowException) {
				Assert.fail(message, e);
			}
		} catch (Exception e) {
			Assert.fail("Unexpected failed to process job", e);
		}
		
		return job;
	}
	
	/**
	 * Tests that batch submit parameters given in a job description are validated 
	 * properly against the limits of a given queue.
	 * 
	 * @param queue the queue to which to submit the job
	 * @param nodes the number of nodes to request
	 * @param processors the number of processors to request
	 * @param memory the amount of memory to request
	 * @param requestedTime the amount of time to request
	 * @param shouldPass true if the validation should succeed
	 * @param message the assertion message to be returned if the test fails
	 */
	@Test(dataProvider = "validateBatchSubmitParametersProvider")
	public void validateBatchSubmitParameters(BatchQueue queue, Long nodes, Long processors, Double memory, String requestedTime, boolean shouldPass, String message) 
	{
		assertEquals(shouldPass,
				JobManager.validateBatchSubmitParameters(queue, nodes, processors, memory, requestedTime), 
				message);
	}
	
	@DataProvider
	public Object[][] selectQueueLimitTestProvider() throws Exception {
		//TODO: isolate combinations of queues, systems, apps, etc to test
		
//		ExecutionSystem system = new ExecutionSystem();
		// name maxJobs userJobs nodes memory, procs, time, cstm, default
		
		BatchQueue queueDefault = new BatchQueue("queueDefault", 1L, 1L, 1L, (double) 1.0, 1L, "01:00:00", null, true);
		BatchQueue queueTwo = new BatchQueue("queueTwo", 2L, 2L, 2L, (double) 2.0, (long) -1, "02:00:00", null, false);
		BatchQueue queueTen = new BatchQueue("queueTen", 10L, 10L, 10L, (double) 10, 1L, "10:00:00", null, false);
		BatchQueue queueHundred = new BatchQueue("queueHundred", 100L, 100L, 100L, (double) 100, 100L, "100:00:00", null, false);
		BatchQueue queueUnbounded = new BatchQueue("queueMax", (long) -1, (long) -1, (long) -1, (double) -1.0, (long) -1, BatchQueue.DEFAULT_MAX_RUN_TIME, null, false);
		
		BatchQueue[] allQueues = {queueDefault, queueTwo, queueTen, queueHundred, queueUnbounded };
		return new Object[][] {
				{ new BatchQueue[] { queueDefault, queueUnbounded }, (long)-1, (double)-1, "00:00:01", queueDefault.getName(), "Default queue picked if specs fit." },
				
				{ allQueues, 1L, (double)-1, "00:00:01", queueDefault.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double) 1, "00:00:01", queueDefault.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)-1, "01:00:00", queueDefault.getName(), "First matching matching queue was not selected" },
				
				{ allQueues, 2L, (double)-1, "00:00:01", queueTwo.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double) 2, "00:00:01", queueTwo.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)-1, "02:00:00", queueTwo.getName(), "First matching matching queue was not selected" },
				
				{ allQueues, (long)10, (double)-1, "00:00:01", queueTen.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)10, "00:00:01", queueTen.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)-1, "10:00:00", queueTen.getName(), "First matching matching queue was not selected" },
				
				{ allQueues, (long)100, (double)-1, "00:00:01", queueHundred.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)100, "00:00:01", queueHundred.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)-1, "100:00:00", queueHundred.getName(), "First matching matching queue was not selected" },
				
				{ allQueues, 101L, (double)-1, "00:00:01", queueUnbounded.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, 101D, "00:00:01", queueUnbounded.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)-1, "101:00:00", queueUnbounded.getName(), "First matching matching queue was not selected" },
		};
	}

	/**
	 * Tests whether the JobManager.selectQueue method returns the expected queue
	 * given a set of inputs.
	 * 
	 * @param testQueues the queues to assign to the execution system for use in the test
	 * @param nodes the number of nodes to request
	 * @param memory the amount of memory to request
	 * @param requestedTime the amount of time to request
	 * @param expectedQueueName the name of the queue that should be selected
	 * @param message the assertion message to be returned if the test fails
	 */
	@Test(dataProvider = "selectQueueLimitTestProvider", dependsOnMethods = { "validateBatchSubmitParameters" })
	public void selectQueueLimitTest(BatchQueue[] testQueues, Long nodes, Double memory, String requestedTime, String expectedQueueName, String message) 
	{
		try 
		{
			ExecutionSystem testSystem = createExecutionSystem();
			testSystem.getBatchQueues().clear();

			for (BatchQueue testQueue: testQueues) {
				testSystem.addBatchQueue(testQueue);
			}
			
			BatchQueue selectedQueue = JobManager.selectQueue(testSystem, nodes, memory, requestedTime);
			String selectedQueueName = (selectedQueue == null ? null : selectedQueue.getName());
			
			assertEquals(selectedQueueName, expectedQueueName, message);
		} 
		catch (Exception e) {
			Assert.fail(message, e);
		}
	}
	
	@DataProvider
	public Object[][] processJsonJobProvider()
	{
		boolean pass = false;
		boolean fail = true;
		return new Object[][] {
				{ "name", null, fail, "Null name should throw exception" },
				{ "name", "", fail, "Empty name should throw exception" },
				{ "name", new Object(), fail, "Object for name should throw exception" },
				{ "name", new ArrayList<String>(), fail, "Array for name should throw exception" },
				{ "name", 1L, fail, "Long for name should throw exception" },
				{ "name", 1.0f, fail, "Float for name should throw exception" },
				{ "name", 1.0, fail, "Double for name should throw exception" },
				{ "name", 1, fail, "Integer for name should throw exception" },
				{ "name", new BigDecimal(1), fail, "BigDecimal  for name should throw exception" },
				{ "name", StringUtils.rightPad("h", 64, "h"), fail, "name must be under 64 characters" },
				{ "name", Boolean.FALSE, fail, "Boolean for name should throw exception" },
				{ "name", Boolean.TRUE, fail, "Boolean for name should throw exception" },
				// maybe add tests for special characters, chinese characters, etc.
				
				{ "jobName", null, fail, "Null jobName should throw exception" },
				{ "jobName", "", fail, "Empty jobName should throw exception" },
				{ "jobName", new Object(), fail, "Object for jobName should throw exception" },
				{ "jobName", new ArrayList<String>(), fail, "Array for jobName should throw exception" },
				{ "jobName", 1L, fail, "Long for jobName should throw exception" },
				{ "jobName", 1.0f, fail, "Float for jobName should throw exception" },
				{ "jobName", 1.0, fail, "Double for jobName should throw exception" },
				{ "jobName", 1, fail, "Integer for jobName should throw exception" },
				{ "jobName", new BigDecimal(1), fail, "BigDecimal  for jobName should throw exception" },
				{ "jobName", StringUtils.rightPad("h", 65, "h"), fail, "jobName must be under 64 characters" },
				{ "jobName", Boolean.FALSE, fail, "Boolean for jobName should throw exception" },
				{ "jobName", Boolean.TRUE, fail, "Boolean for jobName should throw exception" },
				
				{ "appId", null, fail, "Null appId should throw exception" },
				{ "appId", "", fail, "Empty appId should throw exception" },
				{ "appId", new Object(), fail, "Object for appId should throw exception" },
				{ "appId", new ArrayList<String>(), fail, "Array for appId should throw exception" },
				{ "appId", 1L, fail, "Long for appId should throw exception" },
				{ "appId", 1.0f, fail, "Float for appId should throw exception" },
				{ "appId", 1.0, fail, "Double for appId should throw exception" },
				{ "appId", 1, fail, "Integer for appId should throw exception" },
				{ "appId", new BigDecimal(1), fail, "BigDecimal  for appId should throw exception" },
				{ "appId", StringUtils.rightPad("h", 81, "h"), fail, "appId must be under 80 characters" },
				{ "appId", Boolean.FALSE, fail, "Boolean for appId should throw exception" },
				{ "appId", Boolean.TRUE, fail, "Boolean for appId should throw exception" },
				
				{ "softwareName", null, fail, "Null softwareName should throw exception" },
				{ "softwareName", "", fail, "Empty softwareName should throw exception" },
				{ "softwareName", new Object(), fail, "Object for softwareName should throw exception" },
				{ "softwareName", new ArrayList<String>(), fail, "Array for softwareName should throw exception" },
				{ "softwareName", 1L, fail, "Long for softwareName should throw exception" },
				{ "softwareName", 1.0f, fail, "Float for softwareName should throw exception" },
				{ "softwareName", 1.0, fail, "Double for softwareName should throw exception" },
				{ "softwareName", 1, fail, "Integer for softwareName should throw exception" },
				{ "softwareName", new BigDecimal(1), fail, "BigDecimal for softwareName should throw exception" },
				{ "softwareName", StringUtils.rightPad("h", 81, "h"), fail, "softwareName must be under 80 characters" },
				{ "softwareName", Boolean.FALSE, fail, "Boolean for softwareName should throw exception" },
				{ "softwareName", Boolean.TRUE, fail, "Boolean for softwareName should throw exception" },
				
				{ "executionSystem", null, fail, "Null executionSystem should throw exception" },
				{ "executionSystem", "", fail, "Empty executionSystem should throw exception" },
				{ "executionSystem", new Object(), fail, "Object for executionSystem should throw exception" },
				{ "executionSystem", new ArrayList<String>(), fail, "Array for executionSystem should throw exception" },
				{ "executionSystem", 1L, fail, "Long for executionSystem should throw exception" },
				{ "executionSystem", 1.0f, fail, "Float for executionSystem should throw exception" },
				{ "executionSystem", 1.0, fail, "Double for executionSystem should throw exception" },
				{ "executionSystem", 1, fail, "Integer for executionSystem should throw exception" },
				{ "executionSystem", new BigDecimal(1), fail, "BigDecimal for executionSystem should throw exception" },
				{ "executionSystem", StringUtils.rightPad("h", 81, "h"), fail, "executionSystem must be under 80 characters" },
				{ "executionSystem", Boolean.FALSE, fail, "Boolean for executionSystem should throw exception" },
				{ "executionSystem", Boolean.TRUE, fail, "Boolean for executionSystem should throw exception" },
				
				{ "executionHost", null, fail, "Null executionHost should throw exception" },
				{ "executionHost", "", fail, "Empty executionHost should throw exception" },
				{ "executionHost", new Object(), fail, "Object for executionHost should throw exception" },
				{ "executionHost", new ArrayList<String>(), fail, "Array for executionHost should throw exception" },
				{ "executionHost", 1L, fail, "Long for executionHost should throw exception" },
				{ "executionHost", 1.0f, fail, "Float for executionHost should throw exception" },
				{ "executionHost", 1.0, fail, "Double for executionHost should throw exception" },
				{ "executionHost", 1, fail, "Integer for executionHost should throw exception" },
				{ "executionHost", new BigDecimal(1), fail, "BigDecimal for executionHost should throw exception" },
				{ "executionHost", StringUtils.rightPad("h", 81, "h"), fail, "executionHost must be under 80 characters" },
				{ "executionHost", Boolean.FALSE, fail, "Boolean for executionHost should throw exception" },
				{ "executionHost", Boolean.TRUE, fail, "Boolean for executionHost should throw exception" },
				
				{ "batchQueue", null, fail, "Null batchQueue should throw exception" },
				{ "batchQueue", "", fail, "Empty batchQueue should throw exception" },
				{ "batchQueue", new Object(), fail, "Object for batchQueue should throw exception" },
				{ "batchQueue", new ArrayList<String>(), fail, "Array for batchQueue should throw exception" },
				{ "batchQueue", 1L, fail, "Long for batchQueue should throw exception" },
				{ "batchQueue", 1.0f, fail, "Float for batchQueue should throw exception" },
				{ "batchQueue", 1.0, fail, "Double for batchQueue should throw exception" },
				{ "batchQueue", 1, fail, "Integer for batchQueue should throw exception" },
				{ "batchQueue", new BigDecimal(1), fail, "BigDecimal for batchQueue should throw exception" },
				{ "batchQueue", Boolean.FALSE, fail, "Boolean for batchQueue should throw exception" },
				{ "batchQueue", Boolean.TRUE, fail, "Boolean for batchQueue should throw exception" },
				
				{ "queue", null, fail, "Null queue should throw exception" },
				{ "queue", "", fail, "Empty queue should throw exception" },
				{ "queue", new Object(), fail, "Object for queue should throw exception" },
				{ "queue", new ArrayList<String>(), fail, "Array for queue should throw exception" },
				{ "queue", 1L, fail, "Long for queue should throw exception" },
				{ "queue", 1.0f, fail, "Float for queue should throw exception" },
				{ "queue", 1.0, fail, "Double for queue should throw exception" },
				{ "queue", 1, fail, "Integer for queue should throw exception" },
				{ "queue", new BigDecimal(1), fail, "BigDecimal for queue should throw exception" },
				{ "queue", StringUtils.rightPad("h", 129, "h"),fail, "queue must be under 128 characters" },
				{ "queue", Boolean.FALSE, fail, "Boolean for queue should throw exception" },
				{ "queue", Boolean.TRUE, fail, "Boolean for queue should throw exception" },
				
				{ "nodeCount", null, fail, "Null nodeCount should throw exception" },
				{ "nodeCount", "", fail, "Empty nodeCount should throw exception" },
				{ "nodeCount", new Object(), fail, "Object for nodeCount should throw exception" },
				{ "nodeCount", new ArrayList<String>(), fail, "Array for nodeCount should throw exception" },
				{ "nodeCount", 1L, pass, "Long for nodeCount should pass" },
				{ "nodeCount", 1.0f, fail, "Float for nodeCount should fail" },
				{ "nodeCount", 1.0, fail, "Double for nodeCount should fail" },
				{ "nodeCount", 1, pass, "Integer for nodeCount should pass" },
				{ "nodeCount", new BigDecimal(1), pass, "BigDecimal for nodeCount should pass" },
				{ "nodeCount", Boolean.FALSE, fail, "Boolean for nodeCount should throw exception" },
				{ "nodeCount", Boolean.TRUE, fail, "Boolean for nodeCount should throw exception" },
				
				{ "processorsPerNode", null, fail, "Null processorsPerNode should throw exception" },
				{ "processorsPerNode", "", fail, "Empty processorsPerNode should throw exception" },
				{ "processorsPerNode", new Object(), fail, "Object for processorsPerNode should throw exception" },
				{ "processorsPerNode", new ArrayList<String>(), fail, "Array for processorsPerNode should throw exception" },
				{ "processorsPerNode", 1L, pass, "Long for processorsPerNode should throw exception" },
				{ "processorsPerNode", 1.0f, fail, "Float for processorsPerNode should fail" },
				{ "processorsPerNode", 1.0, fail, "Double for processorsPerNode should fail" },
				{ "processorsPerNode", 1, pass, "Integer for processorsPerNode should pass" },
				{ "processorsPerNode", new BigDecimal(1), pass, "BigDecimal for processorsPerNode should pass" },
				{ "processorsPerNode", Boolean.FALSE, fail, "Boolean for processorsPerNode should throw exception" },
				{ "processorsPerNode", Boolean.TRUE, fail, "Boolean for processorsPerNode should throw exception" },
				
				{ "processorCount", null, fail, "Null processorCount should throw exception" },
				{ "processorCount", "", fail, "Empty processorCount should throw exception" },
				{ "processorCount", new Object(), fail, "Object for processorCount should throw exception" },
				{ "processorCount", new ArrayList<String>(), fail, "Array for processorCount should throw exception" },
				{ "processorCount", 1L, pass, "Long for processorCount should throw exception" },
				{ "processorCount", 1.0f, fail, "Float for processorCount should fail" },
				{ "processorCount", 1.0, fail, "Double for processorCount should fail" },
				{ "processorCount", 1, pass, "Integer for processorCount should pass" },
				{ "processorCount", new BigDecimal(1), pass, "BigDecimal for processorCount should pass" },
				{ "processorCount", Boolean.FALSE, fail, "Boolean for processorCount should throw exception" },
				{ "processorCount", Boolean.TRUE, fail, "Boolean for processorCount should throw exception" },
				
				{ "memoryPerNode", null, fail, "Null memoryPerNode should throw exception" },
				{ "memoryPerNode", "", fail, "Empty memoryPerNode should throw exception" },
				{ "memoryPerNode", "abracadabra", fail, "Invalid memoryPerNode string should throw exception" },
				{ "memoryPerNode", "1GB", pass, "Vaid string memoryPerNode string should pass" },
				{ "memoryPerNode", new Object(), fail, "Object for memoryPerNode should throw exception" },
				{ "memoryPerNode", new ArrayList<String>(), fail, "Array for memoryPerNode should throw exception" },
				{ "memoryPerNode", 1L, pass, "Long for memoryPerNode should pass" },
				{ "memoryPerNode", 1.0f, pass, "Float for memoryPerNode should pass" },
				{ "memoryPerNode", 1.0, pass, "Double for memoryPerNode should pass" },
				{ "memoryPerNode", 1, pass, "Integer for memoryPerNode should pass" },
				{ "memoryPerNode", new BigDecimal(1), pass, "BigDecimal for memoryPerNode should pass" },
				{ "memoryPerNode", Boolean.FALSE, fail, "Boolean for memoryPerNode should throw exception" },
				{ "memoryPerNode", Boolean.TRUE, fail, "Boolean for memoryPerNode should throw exception" },
				
				{ "maxMemory", null, fail, "Null maxMemory should throw exception" },
				{ "maxMemory", "", fail, "Empty maxMemory should throw exception" },
				{ "maxMemory", "abracadabra", fail, "Invalid maxMemory string should throw exception" },
				{ "maxMemory", "1GB", pass, "Vaid string maxMemory string should oass" },
				{ "maxMemory", new Object(), fail, "Object for maxMemory should throw exception" },
				{ "maxMemory", new ArrayList<String>(), fail, "Array for maxMemory should throw exception" },
				{ "maxMemory", 1L, pass, "Long for maxMemory should pass" },
				{ "maxMemory", 1.0f, pass, "Float for maxMemory should pass" },
				{ "maxMemory", 1.0, pass, "Double for maxMemory should pass" },
				{ "maxMemory", 1, pass, "Integer for maxMemory should pass" },
				{ "maxMemory", new BigDecimal(1), pass, "BigDecimal for maxMemory pass" },
				{ "maxMemory", Boolean.FALSE, fail, "Boolean for maxMemory should throw exception" },
				{ "maxMemory", Boolean.TRUE, fail, "Boolean for maxMemory should throw exception" },
				
				{ "maxRunTime", null, fail, "Null maxRunTime should throw exception" },
				{ "maxRunTime", "", fail, "Empty maxRunTime should throw exception" },
				{ "maxRunTime", "asdfasdfasd", fail, "Invalid string maxRunTime should throw exception" },
				{ "maxRunTime", "00:00:01", pass, "Invalid string maxRunTime should pass" },
				{ "maxRunTime", new Object(), fail, "Object for maxRunTime should throw exception" },
				{ "maxRunTime", new ArrayList<String>(), fail, "Array for maxRunTime should throw exception" },
				{ "maxRunTime", 1L, fail, "Long for maxRunTime should throw exception" },
				{ "maxRunTime", 1.0f, fail, "Float for maxRunTime should throw exception" },
				{ "maxRunTime", 1.0, fail, "Double for maxRunTime should throw exception" },
				{ "maxRunTime", 1, fail, "Integer for maxRunTime should throw exception" },
				{ "maxRunTime", new BigDecimal(1), fail, "BigDecimal for maxRunTime should throw exception" },
				{ "maxRunTime", Boolean.FALSE, fail, "Boolean for maxRunTime should throw exception" },
				{ "maxRunTime", Boolean.TRUE, fail, "Boolean for maxRunTime should throw exception" },
				
				{ "requestedTime", null, fail, "Null requestedTime should throw exception" },
				{ "requestedTime", "", fail, "Empty requestedTime should throw exception" },
				{ "requestedTime", "asdfasdfasd", fail, "Invalid string requestedTime should throw exception" },
				{ "requestedTime", "00:00:01", pass, "Valid string requestedTime should pass" },
				{ "requestedTime", new Object(), fail, "Object for requestedTime should throw exception" },
				{ "requestedTime", new ArrayList<String>(), fail, "Array for requestedTime should throw exception" },
				{ "requestedTime", 1L, fail, "Long for requestedTime should throw exception" },
				{ "requestedTime", 1.0f, fail, "Float for requestedTime should throw exception" },
				{ "requestedTime", 1.0, fail, "Double for requestedTime should throw exception" },
				{ "requestedTime", 1, fail, "Integer for requestedTime should throw exception" },
				{ "requestedTime", new BigDecimal(1), fail, "BigDecimal for requestedTime should throw exception" },
				{ "requestedTime", Boolean.FALSE, fail, "Boolean for requestedTime should throw exception" },
				{ "requestedTime", Boolean.TRUE, fail, "Boolean for requestedTime should throw exception" },
				
				{ "dependencies", null, fail, "Null dependencies should throw exception" },
				{ "dependencies", "", fail, "Empty dependencies should throw exception" },
				{ "dependencies", "dependencies", fail, "String dependencies should throw exception" },
				{ "dependencies", new Object(), fail, "Object for dependencies should throw exception" },
				{ "dependencies", new ArrayList<String>(), fail, "Array for dependencies should throw exception" },
				{ "dependencies", 1L, fail, "Long for dependencies should throw exception" },
				{ "dependencies", 1.0f, fail, "Float for dependencies should throw exception" },
				{ "dependencies", 1.0, fail, "Double for dependencies should throw exception" },
				{ "dependencies", 1, fail, "Integer for dependencies should throw exception" },
				{ "dependencies", new BigDecimal(1), fail, "BigDecimal for dependencies should throw exception" },
				{ "dependencies", Boolean.FALSE, fail, "Boolean for dependencies should throw exception" },
				{ "dependencies", Boolean.TRUE, fail, "Boolean for dependencies should throw exception" },
				
				{ "archive", null, fail, "Null archive should throw exception" },
				{ "archive", "", fail, "Empty archive should throw exception" },
				{ "archive", "archive", fail, "String archive should throw exception" },
				{ "archive", new Object(), fail, "Object for archive should throw exception" },
				{ "archive", new ArrayList<String>(), fail, "Array for archive should throw exception" },
				{ "archive", 1L, fail, "Long for archive should throw exception" },
				{ "archive", 1.0f, fail, "Float for archive should throw exception" },
				{ "archive", 1.0, fail, "Double for archive should throw exception" },
				{ "archive", 1, fail, "Integer for archive should throw exception" },
				{ "archive", new BigDecimal(1), fail, "BigDecimal for archive should throw exception" },
				{ "archive", Boolean.FALSE, pass, "Boolean for archive should pass" },
				{ "archive", Boolean.TRUE, pass, "Boolean for archive should pass" },
				
				{ "archiveSystem", null, fail, "Null archiveSystem should throw exception" },
				{ "archiveSystem", "", fail, "Empty archiveSystem should throw exception" },
				{ "archiveSystem", new Object(), fail, "Object for archiveSystem should throw exception" },
				{ "archiveSystem", new ArrayList<String>(), fail, "Array for archiveSystem should throw exception" },
				{ "archiveSystem", 1L, fail, "Long for archiveSystem should throw exception" },
				{ "archiveSystem", 1.0f, fail, "Float for archiveSystem should throw exception" },
				{ "archiveSystem", 1.0, fail, "Double for archiveSystem should throw exception" },
				{ "archiveSystem", 1, fail, "Integer for archiveSystem should throw exception" },
				{ "archiveSystem", new BigDecimal(1), fail, "BigDecimal for archiveSystem should throw exception" },
				{ "archiveSystem", Boolean.FALSE, fail, "Boolean for archiveSystem should throw exception" },
				{ "archiveSystem", Boolean.TRUE, fail, "Boolean for archiveSystem should throw exception" },
				
				{ "archivePath", null, fail, "Null archivePath should throw exception" },
				{ "archivePath", "", fail, "Empty archivePath should throw exception" },
				{ "archivePath", new Object(), fail, "Object for archivePath should throw exception" },
				{ "archivePath", new ArrayList<String>(), fail, "Array for archivePath should throw exception" },
				{ "archivePath", 1L, fail, "Long for archivePath should throw exception" },
				{ "archivePath", 1.0f, fail, "Float for archivePath should throw exception" },
				{ "archivePath", 1.0, fail, "Double for archivePath should throw exception" },
				{ "archivePath", 1, fail, "Integer for archivePath should throw exception" },
				{ "archivePath", new BigDecimal(1), fail, "BigDecimal for archivePath should throw exception" },
				{ "archivePath", StringUtils.rightPad("h", 81, "h"), fail, "archivePath must be under 80 characters" },
				{ "archivePath", Boolean.FALSE, fail, "Boolean for archivePath should throw exception" },
				{ "archivePath", Boolean.TRUE, fail, "Boolean for archivePath should throw exception" },
				
				{ "inputs", null, fail, "Null inputs should throw exception" },
				{ "inputs", "", fail, "Empty inputs should throw exception" },
				{ "inputs", new ArrayList<String>(), fail, "Array for inputs should throw exception" },
				{ "inputs", 1L, fail, "Long for inputs should throw exception" },
				{ "inputs", 1.0f, fail, "Float for inputs should throw exception" },
				{ "inputs", 1.0, fail, "Double for inputs should throw exception" },
				{ "inputs", 1, fail, "Integer for inputs should throw exception" },
				{ "inputs", new BigDecimal(1), fail, "BigDecimal for inputs should throw exception" },
				{ "inputs", Boolean.FALSE, fail, "Boolean for inputs should throw exception" },
				{ "inputs", Boolean.TRUE, fail, "Boolean for inputs should throw exception" },
				
				{ "parameters", null, fail, "Null parameters should throw exception" },
				{ "parameters", "", fail, "Empty parameters should throw exception" },
				{ "parameters", new ArrayList<String>(), fail, "Array for parameters should throw exception" },
				{ "parameters", 1L, fail, "Long for parameters should throw exception" },
				{ "parameters", 1.0f, fail, "Float for parameters should throw exception" },
				{ "parameters", 1.0, fail, "Double for parameters should throw exception" },
				{ "parameters", 1, fail, "Integer for parameters should throw exception" },
				{ "parameters", new BigDecimal(1), fail, "BigDecimal for parameters should throw exception" },
				{ "parameters", Boolean.FALSE, fail, "Boolean for parameters should throw exception" },
				{ "parameters", Boolean.TRUE, fail, "Boolean for parameters should throw exception" },
				
		};
	}
	
	/**
	 * Tests basic field validation on jobs submitted as json
	 *
	 * @param field the field to add to the map
	 * @param value the value of the field being added
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the assertion message to be returned if the test fails
	 */
	@Test(dataProvider = "processJsonJobProvider")//, dependsOnMethods = { "selectQueueLimitTest" })
	public void processJsonJob(String field, Object value, boolean shouldThrowException, String message) 
	{
		try
		{
			Software software = createSoftware();
			ObjectNode json = createJobJsonNode(software);
			json = updateObjectNode(json, field, value);
			_genericProcessJsonJob(json, shouldThrowException, message);
		}
		catch (Exception e) {
			Assert.fail("Software creation should not fail", e);
		}
	}

	/**
	 * Creates an {@link ExecutionSystem} from the test JSON files in the test resources folder. The
	 * returned system is not persisted.
	 * @return a non-persisted execution system with random uuid and name.
	 */
	protected ExecutionSystem createMockExecutionSystem() {
		ExecutionSystem system = null;
		try {
			JSONObject json = JSONTestDataUtil.getInstance().getTestDataObject(TEST_EXECUTION_SYSTEM_FILE);
			json.put("id", UUID.randomUUID().toString());
			json.put("id", UUID.randomUUID().toString());
			system = ExecutionSystem.fromJSON(json);
			system.setOwner(SYSTEM_OWNER);
			system.setId(3L);
		} catch (IOException | JSONException | SystemArgumentException e) {
			Assert.fail("Unable create execution system", e);
		}

		return system;
	}

	protected StorageSystem createMockStorageSystem() {
		StorageSystem system = null;
		try {
			JSONObject json = JSONTestDataUtil.getInstance().getTestDataObject(TEST_STORAGE_SYSTEM_FILE);
			json.put("id", UUID.randomUUID().toString());
			system = StorageSystem.fromJSON(json);
			system.setOwner(SYSTEM_OWNER);
			system.getUsersUsingAsDefault().add(TEST_OWNER);
			system.setId(2L);
		} catch (IOException|JSONException e) {
			Assert.fail("Unable create storage system", e);
		}

		return system;
	}

	/**
	 * Creates a new {@link Software} resource and assigns the {@code executionSystem} and
	 * {@code storageSystem} to it. The {@link ExecutionSystem#getDefaultQueue()} will be set
	 * as the {@code Software#defaultQueue}.
	 * <strong><em>The returned instance will NOT be persisted. You must do this manually or
	 * use the {@link #createSoftware(ExecutionSystem, StorageSystem)} method.</em></strong>
	 * @param executionSystem the system on which the app should run
	 * @param deploymentSystem the system containing the remote asset deployment path
	 * @return a new {@link Software} instance
	 * @throws Exception if deserialization issues occur
	 * @see #createSoftware(ExecutionSystem, StorageSystem)
	 */
	protected Software createMockSoftware(ExecutionSystem executionSystem, StorageSystem deploymentSystem)
	{
		Software software = null;
		try {
			mockStatic(Software.class);
//			when(JobManager.updateStatus(job, JobStatusType.STAGED)).thenReturn(job);
//			StorageSystem storageSystem = createMockStorageSystem();
//			ExecutionSystem executionSystem = createMockExecutionSystem();
//
			SystemDao systemDao = mock(SystemDao.class);
			when(systemDao.findBySystemId(eq(deploymentSystem.getSystemId()))).thenReturn(deploymentSystem);
			when(systemDao.findBySystemId(eq(executionSystem.getSystemId()))).thenReturn(executionSystem);

			SystemManager systemManager = mock(SystemManager.class);
			when(systemManager.getUserDefaultStorageSystem(eq(TEST_OWNER))).thenReturn(deploymentSystem);
			when(systemManager.getDao()).thenReturn(systemDao);

			PowerMockito.when(Software.getSystemManager()).thenReturn(systemManager);
			PowerMockito.when(Software.fromJSON(any(), any())).thenCallRealMethod();

			JSONObject jsonSoftware = getDefaultSoftwareJson();
			jsonSoftware.put("executionSystem", executionSystem.getSystemId());
			jsonSoftware.put("deploymentSystem", deploymentSystem.getSystemId());
			BatchQueue queue = executionSystem.getDefaultQueue();
			jsonSoftware.put("defaultQueue", queue.getName());
			jsonSoftware.put("owner", executionSystem.getOwner());

			software = Software.fromJSON(jsonSoftware, SYSTEM_OWNER);
			software.setOwner(SYSTEM_OWNER);

		} catch (JSONException e) {
			Assert.fail("Failed to parse software template file: " + FORK_SOFTWARE_TEMPLATE_FILE, e);
		}

		return software;
	}
	
	/**
	 * Tests basic field validation on jobs submitted as form
	 * 
	 * @param field
	 * @param value
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	@Test(dataProvider = "processJsonJobProvider")//, dependsOnMethods={"processJsonJobWithNotifications"})
	public void processFormJob(String field, Object value, boolean shouldThrowException, String message) 
	{
		Software software = createSoftware();
		Map<String, Object> jobRequestMap = updateJobRequestMap(createJobRequestMap(software), field, value);
		genericProcessFormJob(jobRequestMap, shouldThrowException, message);
	}

	/**
	 * Tests empty job notifications validation on jobs submitted as form
	 */
	@Test
	public void processJsonJobWithMultipleNotifications()
	{
		Job job = null;
		try
		{
			Software software = createSoftware();
			ObjectNode json = createJobJsonNode(software);

			ObjectNode retryStrategy = mapper.createObjectNode()
					.put("retryStrategy", "DELAYED")
					.put("retryLimit", 3)
					.put("retryRate", 5)
					.put("retryDelay", 5)
					.put("saveOnFailure", true);

			ObjectNode defaultNotification = (ObjectNode)mapper.createObjectNode()
					.put("url", "test@example.com")
					.put("event", "FAILED")
					.put("persistent", true)
					.set("policy", retryStrategy);

			ArrayNode jsonNotifications = mapper.createArrayNode();
			for (String eventName: List.of("FAILED", "FINISHED", "STOPPED", "BLOCKED")) {
				jsonNotifications.add(defaultNotification.deepCopy().put("event", eventName));
			}

			JobRequestInputProcessor inputProcessor = mock(JobRequestInputProcessor.class);
			doNothing().when(inputProcessor).process(any());
			when(inputProcessor.getJobInputs()).thenReturn(mapper.createObjectNode());

			JobRequestParameterProcessor parameterProcessor = mock(JobRequestParameterProcessor.class);
			doNothing().when(parameterProcessor).process(any());
			when(parameterProcessor.getJobParameters()).thenReturn(mapper.createObjectNode());

//			JobRequestNotificationProcessor notificationProcessor = mock(JobRequestNotificationProcessor.class);
//			doNothing().when(notificationProcessor).process(any(JsonNode.class));
//			doNothing().when(notificationProcessor).process(any(ArrayNode.class));
//			doNothing().when(notificationProcessor).process(anyString());
//			when(notificationProcessor.getNotifications()).thenReturn(new ArrayList<Notification>());

			JobRequestProcessor processor = mock(JobRequestProcessor.class, new Answer() {
				/**
				 * @param invocation the invocation on the mock.
				 * @return the value to be returned
				 * @throws Throwable the throwable to be thrown
				 */
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					if (invocation.getMethod().getName().equals("isSoftwareInvokableByUser")) {
						return true;
					} else if (invocation.getMethod().getName().equals("getInternalUsername")) {
						return null;
					} else if (invocation.getMethod().getName().equals("getParameterProcessor")) {
						return parameterProcessor;
					} else if (invocation.getMethod().getName().equals("getInputProcessor")) {
						return inputProcessor;
//					} else if (invocation.getMethod().getName().equals("getNotificationProcessor")) {
//						return notificationProcessor;
					} else if (invocation.getMethod().getName().equals("checkExecutionSystemLogin")) {
						return true;
					} else if (invocation.getMethod().getName().equals("checkExecutionSystemStorage")) {
						return true;
					} else if (invocation.getMethod().getName().equals("getUsername")) {
						return TEST_OWNER;
					} else {
						return invocation.callRealMethod();
					}
				}
			});

			when(processor.getUsername()).thenReturn(TEST_OWNER);
			when(processor.getInternalUsername()).thenReturn(null);
			when(processor.isSoftwareInvokableByUser(eq(software), eq(TEST_OWNER))).thenReturn(true);
			json = updateObjectNode(json, "notifications", jsonNotifications);


			job = processor.processJob(json);

			assertNotNull(job.getId(), "Job was not saved after processing.");

			List<Notification> notifications = new NotificationDao().getActiveUserNotificationsForUuid(job.getOwner(), job.getUuid());
			Assert.assertEquals(notifications.size(), jsonNotifications.size(),
					"All notifications included with the job request shoudl be persistent after processing.");
		}
		catch (Exception e) {
			Assert.fail("Unexpected failed to process job", e);
		}
		finally {
			try { clearJobs(); } catch (Throwable ignored) {}
		}
	}

	
	/** 
	 * Tests empty job notifications validation on jobs submitted as form
	 */
	@Test
	public void processFormJobWithEmptyNotifications() 
	{
		Job job = null;
		Software software = createSoftware();

		for (String callbackUrl: new String[]{"", null}) 
		{
			Map<String, Object> jobRequestMap = updateJobRequestMap(createJobRequestMap(software), "notifications", callbackUrl);
			
			try 
			{
				JobRequestInputProcessor inputProcessor = mock(JobRequestInputProcessor.class);
				doNothing().when(inputProcessor).process(any());
				when(inputProcessor.getJobInputs()).thenReturn(mapper.createObjectNode());

				JobRequestParameterProcessor parameterProcessor = mock(JobRequestParameterProcessor.class);
				doNothing().when(parameterProcessor).process(any());
				when(parameterProcessor.getJobParameters()).thenReturn(mapper.createObjectNode());

				JobRequestNotificationProcessor notificationProcessor = mock(JobRequestNotificationProcessor.class);
				doNothing().when(notificationProcessor).process(any(JsonNode.class));
				doNothing().when(notificationProcessor).process(any(ArrayNode.class));
				doNothing().when(notificationProcessor).process(anyString());
				when(notificationProcessor.getNotifications()).thenReturn(new ArrayList<Notification>());

				JobRequestProcessor processor = mock(JobRequestProcessor.class, new Answer() {
					/**
					 * @param invocation the invocation on the mock.
					 * @return the value to be returned
					 * @throws Throwable the throwable to be thrown
					 */
					@Override
					public Object answer(InvocationOnMock invocation) throws Throwable {
						if (invocation.getMethod().getName().equals("isSoftwareInvokableByUser")) {
							return true;
						} else if (invocation.getMethod().getName().equals("getInternalUsername")) {
							return null;
						} else if (invocation.getMethod().getName().equals("getParameterProcessor")) {
							return parameterProcessor;
						} else if (invocation.getMethod().getName().equals("getInputProcessor")) {
							return inputProcessor;
						} else if (invocation.getMethod().getName().equals("getNotificationProcessor")) {
							return notificationProcessor;
						} else if (invocation.getMethod().getName().equals("checkExecutionSystemLogin")) {
							return true;
						} else if (invocation.getMethod().getName().equals("checkExecutionSystemStorage")) {
							return true;
						} else if (invocation.getMethod().getName().equals("getUsername")) {
							return TEST_OWNER;
						} else {
							return invocation.callRealMethod();
						}
					}
				});

				when(processor.getUsername()).thenReturn(TEST_OWNER);
				when(processor.getInternalUsername()).thenReturn(null);
				when(processor.isSoftwareInvokableByUser(eq(software), eq(TEST_OWNER))).thenReturn(true);
				job = processor.processJob(jobRequestMap);

				assertNotNull(job.getId(), "Job was not saved after processing.");
				
				List<Notification> notifications = new NotificationDao().getActiveUserNotificationsForUuid(job.getOwner(), job.getUuid());
				Assert.assertTrue(notifications.isEmpty(), "No notifications should be present if an empty value is provided");	
			} 
			catch (Exception e) {
				Assert.fail("Unexpected failed to process job", e);
			}
			finally {
				try { clearJobs(); } catch (Throwable ignored) {}
			}
		}	
	}

	
	@DataProvider
	public Object[][] processJsonJobBatchQueueParametersProvider()
	{
		ExecutionSystem system = new ExecutionSystem();
		// name maxJobs userJobs nodes memory, procs, time, cstm, default
		
		BatchQueue queueDefault = new BatchQueue("queueDefault", 1L, 1L, 1L, 1.0D, 1L, "01:00:00", null, true);
		BatchQueue queueTwo = new BatchQueue("queueTwo", 2L, 2L, 2L, 2.0D, 2L, "02:00:00", null, false);
		BatchQueue queueTen = new BatchQueue("queueTen", 10L, 10L, 10L, 10D, 10L, "10:00:00", null, false);
		BatchQueue queueHundred = new BatchQueue("queueHundred", 100L, 100L, 100L, 100D, 100L, "100:00:00", null, false);
		//BatchQueue queueUnbounded = new BatchQueue("queueMax", (long) -1, (long) -1, (long) -1, (double) -1.0, (long) -1, BatchQueue.DEFAULT_MAX_RUN_TIME, null, false);
		
		BatchQueue[] allQueues = {queueDefault, queueTwo, queueTen, queueHundred };
		List<Object[]> testData = new ArrayList<Object[]>();
		String[] jobQueues = new String[] { null };
		
		// no job queue specified, no app defaults
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, null, null, null, null, null, null, null, null, null, queueDefault.getName(), fail, "No specs and no default should fail" });
		
		// no app defaults, job exceeds default when defalut is only
													//												job specs								||||					app default specs
									// queues						//     q			nodes					mem			time		procs	  	q				nodes			mem			time		procs	   	expected					pass		message
		testData.add(new Object[]{ allQueues,   					 "thissinotaqueue", null, 					null, 		null, 		null,		null, 			null, 			null, 		null, 		null, 		null,						fail, "Non-existent queue fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			2L, 					null, 		null, 		null,     	null, 			null, 			null, 		null, 		null,     	null,						fail, "Default queue only, out of bounds job nodes fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					2D, 		null, 		null, 		null,			null, 			null, 		null, 		null, 		null,						fail, "Default queue only, out of bounds job memory fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					null, 		"02:00:00", null,  		null, 			null, 			null, 		null, 		null, 		null,						fail, "Default queue only, out of bounds job time fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					null,		null, 		2L,   		null, 			null, 			null, 		null, 		null, 		null,						fail, "Default queue only, out of bounds job procs fails" });
		
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					null, 		null, 		null, 		null, 			2L, 			null, 		null, 		null,     	null, 						fail, "Default queue only, out of bounds app default nodes fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					null, 		null, 		null,		null, 		 	null, 			2D, 		null, 		null, 		null, 						fail, "Default queue only, out of bounds app default memory fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					null,		null, 		null, 		null, 			null, 			null, 		"02:00:00", null,  		null, 						fail, "Default queue only, out of bounds app default time fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					null, 		null, 		null, 		null, 		 	null, 			null,		null, 		2L,    null, 						fail, "Default queue only, out of bounds app default procs fails" });
		
		// no app defaults, job exceeds max queue
		testData.add(new Object[]{ allQueues, 						  null, 			101L, 					null, 		null, 		null,     	null, 			null, 			null, 		null, 		null,     	null,						fail, "All queues, out of bounds nodes fails" });
		testData.add(new Object[]{ allQueues, 						  null, 			null, 					101D,		null, 		null, 		null,			null, 			null, 		null, 		null, 		null,						fail, "All queues, out of bounds memory fails" });
		testData.add(new Object[]{ allQueues, 						  null, 			null, 					null, 		"101:00:00",null,  		null, 			null, 			null, 		null, 		null, 		null,						fail, "All queues, out of bounds time fails" });
		testData.add(new Object[]{ allQueues, 						  null, 			null, 					null,		null, 		101L,  		null, 			null, 			null, 		null, 		null, 		null,						fail, "All queues, out of bounds procs fails" });
		
		// no job specs, app defaults exceeds max queue
		testData.add(new Object[]{ allQueues,  						  null, 			null,  					null,		null, 		null, 		null, 			101L, 			null, 		null, 		null,     	null,					 	fail, "All queues, out of bounds app default nodes fails" });
		testData.add(new Object[]{ allQueues,  						  null, 			null,  					null,		null, 		null, 		null, 			null, 			101D,		null, 		null, 		null, 						fail, "All queues, out of bounds app default memory fails" });
		testData.add(new Object[]{ allQueues,  						  null, 			null,  					null,		null, 		null, 		null, 			null, 			null, 		"101:00:00",null,  		null, 						fail, "All queues, out of bounds app default time fails" });
		testData.add(new Object[]{ allQueues,  						  null, 			null, 					null,		null, 		null,		null, 			null,			null, 		null, 		101L,  		null, 						fail, "All queues, out of bounds app default procs fails" });
		
		for (BatchQueue jobQueue: allQueues) {
			testData.add(new Object[]{ allQueues,  					  null, 			null,  					null,		null, 		null, 		jobQueue.getName(),	null, 					null, 							null, 							null, 									jobQueue.getName(),	pass, "No job specs and default app queue " + jobQueue.getName() + " did not select " + jobQueue.getName() });
			testData.add(new Object[]{ allQueues,   				  null, 			null,  					null,		null, 		null, 		null, 				jobQueue.getMaxNodes(), null, 							null, 							null,     								jobQueue.getName(),	pass, "No job specs and default queue nodes " + jobQueue.getMaxNodes() + " did not select " + jobQueue.getName() });
			testData.add(new Object[]{ allQueues,   				  null, 			null,  					null,		null, 		null, 		null, 				null, 					jobQueue.getMaxMemoryPerNode(), null, 							null, 									jobQueue.getName(),	pass, "No job specs and default queue memory " + jobQueue.getMaxMemoryPerNode() + " did not select " + jobQueue.getName() });
			testData.add(new Object[]{ allQueues,   				  null, 			null,  					null,		null, 		null, 		null, 				null, 					null, 							jobQueue.getMaxRequestedTime(), null,  									jobQueue.getName(),	pass, "No job specs and default queue run time " + jobQueue.getMaxRequestedTime() + " did not select " + jobQueue.getName() });
			testData.add(new Object[]{ allQueues,   				  null, 			null,  					null,		null, 		null, 		null, 				null, 					null,							null, 							jobQueue.getMaxProcessorsPerNode(),    	jobQueue.getName(),	pass, "No job specs and default queue procs " + jobQueue.getMaxProcessorsPerNode() + " did not select " + jobQueue.getName() });
			
			if (!jobQueue.equals(queueHundred)) 
			{				
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						null, 								null, 								null,									null, 					null, 						null, 								null, 								null,									jobQueue.getName(),		pass, "Specifying " + jobQueue.getName() + " did not select " + jobQueue.getName() });
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	queueHundred.getMaxNodes(), null,								null, 								null, 									null, 					null, 						null, 								null, 								null,									null,					fail, "Specifying " + jobQueue.getName() + " and trumping with " + queueHundred.getName() + " nodes should fail" });
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						queueHundred.getMaxMemoryPerNode(), null, 								null,									null, 					null, 						null, 								null, 								null,									null,					fail, "Specifying " + jobQueue.getName() + " and trumping with " + queueHundred.getName() + " memory should fail" });
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						null, 								queueHundred.getMaxRequestedTime(), null,									null, 					null, 						null, 								null, 								null,									null,					fail, "Specifying " + jobQueue.getName() + " and trumping with " + queueHundred.getName() + " run time should fail" });
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						null, 								null, 								queueHundred.getMaxProcessorsPerNode(),	null, 					null, 						null, 								null, 								null,									null,					fail, "Specifying " + jobQueue.getName() + " and trumping with " + queueHundred.getName() + " procs did should fail" });

				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						null, 								null, 								null,									queueHundred.getName(), null, 						null, 								null, 								null, 									jobQueue.getName(),		pass, "Specifying " + jobQueue.getName() + " and trumping with app default queue of " + queueHundred.getName() + " nodes did not select " + jobQueue.getName() });
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						null,								null, 								null, 									null,					queueHundred.getMaxNodes(), null, 								null, 								null, 									null,					fail, "Specifying default max nodes greater than " + jobQueue.getName() + " limit should fail" });
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						null,								null, 								null,									null, 					null, 						queueHundred.getMaxMemoryPerNode(), null, 								null, 									null,					fail, "Specifying default max memory greater than " + jobQueue.getName() + " limit should fail" });
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						null, 								null, 								null,									null, 					null, 						null, 								queueHundred.getMaxRequestedTime(), null, 									null,					fail, "Specifying default max run time greater than " + jobQueue.getName() + " limit should fail" });
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						null, 								null, 								null,									null, 					null, 						null, 								null, 								queueHundred.getMaxProcessorsPerNode(), null,					fail, "Specifying default max procs greater than " + jobQueue.getName() + " limit should fail" });
				
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	jobQueue.getMaxNodes(), 	null,								null, 								null, 									null,					queueHundred.getMaxNodes(), null, 								null, 								null, 									jobQueue.getName(),		pass, "Specifying user supplied node count value overrides app default" });
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						jobQueue.getMaxMemoryPerNode(), 	null, 								null,									null, 					null, 						queueHundred.getMaxMemoryPerNode(), null, 								null, 									jobQueue.getName(),		pass, "Specifying user supplied memory value overrides app default" });
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						null, 								jobQueue.getMaxRequestedTime(), 	null,									null, 					null, 						null, 								queueHundred.getMaxRequestedTime(), null, 									jobQueue.getName(),		pass, "Specifying user supplied run time value overrides app default" });
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						null, 								null, 								jobQueue.getMaxProcessorsPerNode(),		null, 					null, 						null, 								null, 								queueHundred.getMaxProcessorsPerNode(), jobQueue.getName(),		pass, "Specifying user supplied procs value overrides app default" });
			}
			else
			{
				testData.add(new Object[]{ allQueues,   jobQueue.getName(),	null, 						null, 								null, 								null,									null, 			null, 			null, 		null, 		null, 		jobQueue.getName(),		pass, "Specifying " + jobQueue.getName() + " did not select " + jobQueue.getName() });
				testData.add(new Object[]{ allQueues, 	null, 				jobQueue.getMaxNodes(),	 	null, 								null, 								null, 									null, 			null, 			null, 		null, 		null,     	jobQueue.getName(),		pass, "Specifying max nodes for " + jobQueue.getName() + " did not select that queue" });
				testData.add(new Object[]{ allQueues, 	null, 				null, 						jobQueue.getMaxMemoryPerNode(), 	null, 								null, 									null,			null, 			null, 		null, 		null, 		jobQueue.getName(),		pass, "Selecting max memory for " + jobQueue.getName() + " did not select that queue" });
				testData.add(new Object[]{ allQueues, 	null, 				null, 						null, 								jobQueue.getMaxRequestedTime(), 	null,  									null, 			null, 			null, 		null, 		null, 		jobQueue.getName(),		pass, "Selecting max run time for " + jobQueue.getName() + " did not select that queue" });
				testData.add(new Object[]{ allQueues, 	null, 				null, 						null,								null, 								jobQueue.getMaxProcessorsPerNode(),    	null, 			null, 			null, 		null, 		null, 		jobQueue.getName(),		pass, "Selecting max processors for " + jobQueue.getName() + " did not select that queue" });
			}
		}
		
		// add user setting overriding app settings
		
		return testData.toArray(new Object[][]{});
	}



	/**
	 * Tests job batch queue parameter validation on jobs submitted as json. This should provide coverage over
	 * all possible permutations of user parameters, app defaults, and batch queue limits.
	 *
	 * @param batchQueues the array of batch queues to process
	 * @param jobQueue the queue to which the job will be submitted
	 * @param jobNodes the number of nodes to request
	 * @param jobMemory the amount of memory to request
	 * @param jobRequestedTime the time to request
	 * @param jobProcessors the number of processors to request
	 * @param appQueue the default queue to assign to the app
	 * @param appNodes the default number of nodes to assign to the app
	 * @param appMemory the default amount of memory to assign to the app
	 * @param appRequestedTime  the default amount of time to assign to the app
	 * @param appProcessors the default number of processors to assign to the app
	 * @param expectedJobQueueName the name of the queue expected to be selected by the job
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	@Test(dataProvider = "processJsonJobBatchQueueParametersProvider", singleThreaded = true)//, dependsOnMethods={"processJsonJob"})
	public void processJsonJobBatchQueueParameters(BatchQueue[] batchQueues, 
			String jobQueue, Long jobNodes, Double jobMemory, String jobRequestedTime, Long jobProcessors, 
			String appQueue, Long appNodes, Double appMemory, String appRequestedTime, Long appProcessors,
			String expectedJobQueueName, boolean shouldThrowException, String message) 
	{
		ExecutionSystem testSystem = null;
		StorageSystem storageSystem = null;
		SystemDao systemDao = new SystemDao();
		try 
		{
			storageSystem = createStorageSystem();
			testSystem = createExecutionSystem(batchQueues);

			final Software testSoftware = createSoftware(testSystem, storageSystem);
			testSoftware.setName(UUID.randomUUID().toString());
			testSoftware.setDefaultQueue(appQueue);
			testSoftware.setDefaultNodes(appNodes);
			testSoftware.setDefaultMemoryPerNode(appMemory);
			testSoftware.setDefaultProcessorsPerNode(appProcessors);
			testSoftware.setDefaultMaxRunTime(appRequestedTime);
			SoftwareDao.persist(testSoftware);

			HashSet<BatchQueue> bq = new HashSet<>();
			BatchQueue defaultq = batchQueues[0];
			for (BatchQueue testQueue: batchQueues) {
				BatchQueue clonedTestQueue = testQueue.clone();
				clonedTestQueue.setMappedName(UUID.randomUUID().toString());
				clonedTestQueue.setExecutionSystem(testSystem);
				testSystem.addBatchQueue(clonedTestQueue);
				bq.add(clonedTestQueue);
				if (clonedTestQueue.isSystemDefault()) defaultq = clonedTestQueue;

			}

			ExecutionSystem mockExecutionSystem = mock(ExecutionSystem.class);
			when(mockExecutionSystem.getBatchQueues()).thenReturn(bq);
			when(mockExecutionSystem.getSystemId()).thenReturn(testSystem.getSystemId());
			when(mockExecutionSystem.getId()).thenReturn(testSystem.getId());
			when(mockExecutionSystem.getExecutionType()).thenReturn(testSystem.getExecutionType());
			when(mockExecutionSystem.getDefaultQueue()).thenReturn(defaultq);
			when(mockExecutionSystem.getQueue(any())).thenAnswer(new Answer<BatchQueue>() {

				/**
				 * @param invocation the invocation on the mock.
				 * @return the value to be returned
				 * @throws Throwable the throwable to be thrown
				 */
				@Override
				public BatchQueue answer(InvocationOnMock invocation) throws Throwable {
					Optional<BatchQueue> matchineQueue = bq.stream().filter(q -> q.getName().equals(invocation.getArgumentAt(0, String.class))).findFirst();
					return matchineQueue.orElse(null);
				}
			});
			when(mockExecutionSystem.getMaxSystemJobs()).thenReturn(testSystem.getMaxSystemJobs());
			when(mockExecutionSystem.getMaxSystemJobsPerUser()).thenReturn(testSystem.getMaxSystemJobsPerUser());
			when(mockExecutionSystem.getScheduler()).thenReturn(testSystem.getScheduler());
			when(mockExecutionSystem.getWorkDir()).thenReturn(testSystem.getWorkDir());
			when(mockExecutionSystem.getScratchDir()).thenReturn(testSystem.getScratchDir());
			when(mockExecutionSystem.getStatus()).thenReturn(testSystem.getStatus());
			when(mockExecutionSystem.getStartupScript()).thenReturn(testSystem.getStartupScript());
			when(mockExecutionSystem.getOwner()).thenReturn(testSystem.getOwner());
			when(mockExecutionSystem.getUuid()).thenReturn(testSystem.getUuid());
			when(mockExecutionSystem.getUsersUsingAsDefault()).thenReturn(Set.of(testSystem.getOwner()));

			// set up queue(s) on executionsystem
			// set up app defaults and map to the execution system
			// create job for the app with test fields
			ObjectNode json = createJobJsonNode(testSoftware);
			json = updateObjectNode(json, "appId", testSoftware.getUniqueName());
			if (!StringUtils.isEmpty(jobQueue))
				json = updateObjectNode(json, "batchQueue", jobQueue);
			if (jobNodes != null)
				json = updateObjectNode(json, "nodeCount", jobNodes);
			if (jobMemory != null)
				json = updateObjectNode(json, "memoryPerNode", jobMemory);
			if (jobProcessors != null)
				json = updateObjectNode(json, "processorsPerNode", jobProcessors);
			if (jobRequestedTime != null)
				json = updateObjectNode(json, "maxRunTime", jobRequestedTime);

			JobRequestInputProcessor inputProcessor = mock(JobRequestInputProcessor.class);
			doNothing().when(inputProcessor).process(any());
			when(inputProcessor.getJobInputs()).thenReturn(mapper.createObjectNode());

			JobRequestParameterProcessor parameterProcessor = mock(JobRequestParameterProcessor.class);
			doNothing().when(parameterProcessor).process(any());
			when(parameterProcessor.getJobParameters()).thenReturn(mapper.createObjectNode());

			JobRequestNotificationProcessor notificationProcessor = mock(JobRequestNotificationProcessor.class);
			doNothing().when(notificationProcessor).process(any(JsonNode.class));
			doNothing().when(notificationProcessor).process(any(ArrayNode.class));
			doNothing().when(notificationProcessor).process(anyString());
			when(notificationProcessor.getNotifications()).thenReturn(new ArrayList<Notification>());

			JobRequestProcessor processor = mock(JobRequestProcessor.class, new Answer() {
				/**
				 * @param invocation the invocation on the mock.
				 * @return the value to be returned
				 * @throws Throwable the throwable to be thrown
				 */
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					if (invocation.getMethod().getName().equals("isSoftwareInvokableByUser")) {
						return true;
					} else if (invocation.getMethod().getName().equals("getInternalUsername")) {
						return null;
					} else if (invocation.getMethod().getName().equals("getParameterProcessor")) {
						return parameterProcessor;
					} else if (invocation.getMethod().getName().equals("getInputProcessor")) {
						return inputProcessor;
					} else if (invocation.getMethod().getName().equals("getNotificationProcessor")) {
						return notificationProcessor;
					} else if (invocation.getMethod().getName().equals("checkExecutionSystemLogin")) {
						return true;
					} else if (invocation.getMethod().getName().equals("checkExecutionSystemStorage")) {
						return true;
					} else if (invocation.getMethod().getName().equals("getSoftware")) {
						return testSoftware;
					} else if (invocation.getMethod().getName().equals("getUsername")) {
						return TEST_OWNER;
					} else if (invocation.getMethod().getName().equals("getExecutionSystem")) {
						return mockExecutionSystem;
					} else {
						return invocation.callRealMethod();
					}
				}
			});

			Job job = processor.processJob(json);

			assertNotNull(job.getId(), "Job was not saved after processing.");

			System.out.println("Saved: " + ServiceUtils.explode(",", testSystem.getBatchQueues().stream().map(q -> String.format("%s (%s) -> %s", q.getName(), q.getMappedName(), q.getMaxRequestedTime())).collect(Collectors.toList())));
			System.out.println("Original: " + ServiceUtils.explode(",", Arrays.asList(batchQueues).stream().map(q -> String.format("%s (%s) -> %s", q.getName(), q.getMappedName(), q.getMaxRequestedTime())).collect(Collectors.toList())));
			assertEquals(job.getBatchQueue(), expectedJobQueueName,
							"Unexpected batchQueue found for job. Expected " + expectedJobQueueName + 
							" found " + job.getBatchQueue());
		} 
		catch (JobProcessingException e) 
		{
			if (!shouldThrowException) {
				Assert.fail(message, e);
			}
		} 
		catch (Exception e) 
		{
			Assert.fail("Failed to process job", e);
		}
	}
	
	@DataProvider
	public Object[][] updateStatusJobJobStatusTypeProvider()
	{
		List<Object[]> testData = new ArrayList<Object[]>();
	
		for (JobStatusType currentStatus: JobStatusType.values())
		{
			for (JobStatusType newStatus: JobStatusType.values()) {
			//JobStatusType newStatus = JobStatusType.RUNNING;
				testData.add(new Object[]{ currentStatus, newStatus, null, false, 
						String.format("Status update from %s to %s should not throw an exception", currentStatus.name(), newStatus.name()) } );
				testData.add(new Object[]{ currentStatus, newStatus, newStatus.name(), false, 
						String.format("Status update from %s to %s should not throw an exception", currentStatus.name(), newStatus.name()) } );
				testData.add(new Object[]{ currentStatus, newStatus, "NOTAREALEVENT", false, 
						String.format("Status update from %s to %s should not throw an exception", currentStatus.name(), newStatus.name()) } );
				testData.add(new Object[]{ currentStatus, newStatus, "*", false, 
						String.format("Status update from %s to %s should not throw an exception", currentStatus.name(), newStatus.name()) } );
				
			}	
			//break;
		}
		return testData.toArray(new Object[][]{});
	}
	
	
}
