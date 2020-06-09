package org.iplantc.service.jobs.managers;

import static org.iplantc.service.jobs.model.JSONTestDataUtil.TEST_EXECUTION_SYSTEM_FILE;
import static org.iplantc.service.jobs.model.JSONTestDataUtil.TEST_OWNER;
import static org.iplantc.service.jobs.model.JSONTestDataUtil.TEST_SOFTWARE_SYSTEM_FILE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareInput;
import org.iplantc.service.apps.model.SoftwareParameter;
import org.iplantc.service.apps.model.SoftwareParameterEnumeratedValue;
import org.iplantc.service.apps.model.enumerations.SoftwareParameterType;
import org.iplantc.service.jobs.dao.AbstractDaoTest;
import org.iplantc.service.jobs.exceptions.JobProcessingException;
import org.iplantc.service.jobs.model.JSONTestDataUtil;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.json.JSONException;
import org.json.JSONObject;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Test(groups={"broken", "integration"})
public class JobMangerRequestProcessingTest extends AbstractDaoTest 
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
			json.put("inputs", jsonInput);
			
			ObjectNode jsonParameter = mapper.createObjectNode();
			for (SoftwareParameter parameter: software.getParameters()) {
//				if (parameter.getType().equals(SoftwareParameterType.enumeration))
//				{
//					ArrayNode defaultEnumerations = (ArrayNode)mapper.readTree(parameter.getDefaultValueAsJsonArray().iterator().next().asText());
//					String[] defaults = new String[defaultEnumerations.size()];
//					for (int i=0; i<defaultEnumerations.size(); i++) {
//						defaults[i] = defaultEnumerations.get(i).textValue();
//					}
//					jsonParameter.put(parameter.getKey(), StringUtils.join(defaults, ';'));
//				} else {
					jsonParameter.putArray(parameter.getKey()).addAll(parameter.getDefaultValueAsJsonArray());
//				}
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
//				ArrayNode defaultEnumerations = parameter.getDefaultValueAsJsonArray();
//				String[] defaults = new String[defaultEnumerations.size()];
//				for (int i=0; i<defaultEnumerations.size(); i++) {
//					defaults[i] = defaultEnumerations.get(i).textValue();
//				}
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
		BatchQueue queueMinimal = new BatchQueue("queueMinimal", (long) 2,
				(long) 2, (long) 2, (double) 2.0, (long) 2, "00:01:00", null,
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
//			JobRequestProcessor jobRequestProcessor = new JobRequestProcessor(JSONTestDataUtil.TEST_OWNER, null);
//			job = JobManager.processJob(json, JSONTestDataUtil.TEST_OWNER, null);
			
			JobRequestProcessor jobRequestProcessor = spy(JobRequestProcessor.class);
			doReturn(true).when(jobRequestProcessor).checkExecutionSystemLogin(any(ExecutionSystem.class));
			doReturn(true).when(jobRequestProcessor).checkExecutionSystemStorage(any(ExecutionSystem.class));
			doReturn(true).when(jobRequestProcessor).createArchivePath(any(ExecutionSystem.class), any(String.class));
//			when(jobRequestProcessor.checkExecutionSystemLogin(any(ExecutionSystem.class)).thenReturn(true);
//			when(jobRequestProcessor.checkExecutionSystemStorage(any(ExecutionSystem.class))).thenReturn(true);
//			when(jobRequestProcessor.createArchivePath(any(RemoteSystem.class), any(String.class))).thenReturn(true);
			
			jobRequestProcessor.setUsername(JSONTestDataUtil.TEST_OWNER);
			
			job = jobRequestProcessor.processJob(json);
			
			Assert.assertNotNull(job.getId(), "Job was not saved after processing.");
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
//			job = JobManager.processJob(jobRequestMap, JSONTestDataUtil.TEST_OWNER, null);
//			Assert.assertNotNull(job.getId(), "Job was not saved after processing.");
			
			JobRequestProcessor jobRequestProcessor = spy(JobRequestProcessor.class);
			jobRequestProcessor.setUsername(JSONTestDataUtil.TEST_OWNER);
			
			doReturn(true).when(jobRequestProcessor).checkExecutionSystemLogin(any(ExecutionSystem.class));
			doReturn(true).when(jobRequestProcessor).checkExecutionSystemStorage(any(ExecutionSystem.class));
			doReturn(true).when(jobRequestProcessor).createArchivePath(any(ExecutionSystem.class), any(String.class));
			
//			when(jobRequestProcessor.checkExecutionSystemLogin(any(ExecutionSystem.class))).thenReturn(true);
//			when(jobRequestProcessor.checkExecutionSystemStorage(any(ExecutionSystem.class))).thenReturn(true);
//			when(jobRequestProcessor.createArchivePath(any(RemoteSystem.class), any(String.class))).thenReturn(true);
			
			job = jobRequestProcessor.processJob(jobRequestMap);
			
			Assert.assertNotNull(job.getId(), "Job was not saved after processing.");
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
		Assert.assertEquals(shouldPass, 
				JobManager.validateBatchSubmitParameters(queue, nodes, processors, memory, requestedTime), 
				message);
	}
	
	@DataProvider
	public Object[][] selectQueueLimitTestProvider() throws Exception {
		//TODO: isolate combinations of queues, systems, apps, etc to test
		
//		ExecutionSystem system = new ExecutionSystem();
		// name maxJobs userJobs nodes memory, procs, time, cstm, default
		
		BatchQueue queueDefault = new BatchQueue("queueDefault", (long) 1, (long) 1, (long) 1, (double) 1.0, (long) 1, "01:00:00", null, true);
		BatchQueue queueTwo = new BatchQueue("queueTwo", (long) 2, (long) 2, (long) 2, (double) 2.0, (long) -1, "02:00:00", null, false);
		BatchQueue queueTen = new BatchQueue("queueTen", (long) 10, (long) 10, (long) 10, (double) 10, (long) 1, "10:00:00", null, false);
		BatchQueue queueHundred = new BatchQueue("queueHundred", (long) 100, (long) 100, (long) 100, (double) 100, (long) 100, "100:00:00", null, false);
		BatchQueue queueUnbounded = new BatchQueue("queueMax", (long) -1, (long) -1, (long) -1, (double) -1.0, (long) -1, BatchQueue.DEFAULT_MAX_RUN_TIME, null, false);
		
		BatchQueue[] allQueues = {queueDefault, queueTwo, queueTen, queueHundred, queueUnbounded };
		return new Object[][] {
				{ new BatchQueue[] { queueDefault, queueUnbounded }, (long)-1, (double)-1, "00:00:01", queueDefault.getName(), "Default queue picked if specs fit." },
				
				{ allQueues, (long) 1, (double)-1, "00:00:01", queueDefault.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double) 1, "00:00:01", queueDefault.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)-1, "01:00:00", queueDefault.getName(), "First matching matching queue was not selected" },
				
				{ allQueues, (long) 2, (double)-1, "00:00:01", queueTwo.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double) 2, "00:00:01", queueTwo.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)-1, "02:00:00", queueTwo.getName(), "First matching matching queue was not selected" },
				
				{ allQueues, (long)10, (double)-1, "00:00:01", queueTen.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)10, "00:00:01", queueTen.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)-1, "10:00:00", queueTen.getName(), "First matching matching queue was not selected" },
				
				{ allQueues, (long)100, (double)-1, "00:00:01", queueHundred.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)100, "00:00:01", queueHundred.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)-1, "100:00:00", queueHundred.getName(), "First matching matching queue was not selected" },
				
				{ allQueues, (long)101, (double)-1, "00:00:01", queueUnbounded.getName(), "First matching matching queue was not selected" },
				{ allQueues, (long)-1, (double)101, "00:00:01", queueUnbounded.getName(), "First matching matching queue was not selected" },
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
			
			Assert.assertEquals(selectedQueueName, expectedQueueName, message);
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
	//@Test(dataProvider = "processJsonJobProvider")//, dependsOnMethods = { "selectQueueLimitTest" })
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
	
	@DataProvider
	private Object[][] processMultipleJobInputsProvider() throws Exception
	{
		ObjectMapper mapper = new ObjectMapper();

		JSONObject softwareJson = getDefaultSoftwareJson();
		JSONObject input = softwareJson.getJSONArray("inputs").getJSONObject(0);

//		SoftwareInput input = software.getInputs().iterator().next();
		String inputKey = input.getString("id");
		String inputDefaultValue = input.getJSONObject("value").getString("default");
		String inputDefaultValue2 = inputDefaultValue + "2";
		String inputDefaultValue3 = inputDefaultValue + "3";
		String inputDefaultValue4 = inputDefaultValue + "4";
		
		ObjectNode emptyInputObject = mapper.createObjectNode();
		
		ObjectNode singleValueObject = mapper.createObjectNode().put(inputKey, inputDefaultValue);
		ObjectNode mulipleRedundantDelimitedValuesObject = mapper.createObjectNode().put(inputKey, String.format("%1$s;%1$s", inputDefaultValue));
		
		ObjectNode singleValueArrayObject = mapper.createObjectNode();
		singleValueArrayObject.putArray(inputKey).add(inputDefaultValue);
		
		ObjectNode multipleRedundantValueArrayObject = mapper.createObjectNode();
		multipleRedundantValueArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue);
		
		ObjectNode multipleDelimitedValueObject = mapper.createObjectNode().put(inputKey, String.format("%1$s;%1$s", inputDefaultValue, inputDefaultValue2));
		
		ObjectNode multipleValueArrayObject = mapper.createObjectNode();
		multipleValueArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue2);
		
		ObjectNode tripleValueArrayObject = mapper.createObjectNode();
		tripleValueArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue2).add(inputDefaultValue3);
		
		ObjectNode tripleValueArrayWithRedundantValueObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue2);
		
		ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue2).add(inputDefaultValue3);
		
		ObjectNode quadValueArrayWithTwoRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithTwoRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue2).add(inputDefaultValue2);
		
		ObjectNode quadValueArrayWithSameRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithSameRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue);
		
		ObjectNode quadValueArrayObject = mapper.createObjectNode();
		quadValueArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue2).add(inputDefaultValue3).add(inputDefaultValue4);
		
		boolean pass = false;
		boolean fail = true;
		// need to verify cardinality
		return new Object[][] {
				{ 0, 1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input value with redundant values should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
				
				{ 0, -1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited input values with redundant values should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array with redundant values should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 0 and maxCard = -1" },
				
				{ 1, 1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 1" },
				{ 1, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 1" },
				{ 1, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 1" },
				
				{ 1, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited inputs with redundant values should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 1 and maxCard = -1" },
				
				{ 1, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited inputs with redundant values should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 2" },
				
				{ 2, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 2 and maxCard = 2" },
				{ 2, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 2 and maxCard = 2" },
				
				{ 2, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, multipleDelimitedValueObject, pass, "Multiple delimited input pass should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 2 and maxCard = -1" },				
				{ 2, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },
				
				{ 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 2 and maxCard = -1" },
				{ 3, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 3 and maxCard = -1" },
				{ 4, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 4 and maxCard = -1" },
				
				{ 2, -1, quadValueArrayWithTwoRedundantValueObject, pass, "Quad input value array with two redundant values should pass with minCard = 2 and maxCard = -1" },
				{ 3, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 3 and maxCard = -1" },
				{ 4, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 4 and maxCard = -1" },
				
				{ 0, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should fail with minCard = 0 and maxCard = -1" },
				{ 1, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should fail with minCard = 1 and maxCard = -1" },
				{ 2, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should fail with minCard = 2 and maxCard = -1" },
				{ 3, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 3 and maxCard = -1" },
				{ 4, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 4 and maxCard = -1" },
				
				{ 0, -1, quadValueArrayObject, pass, "Quad input value array with unique values should fail with minCard = 0 and maxCard = -1" },
				{ 1, -1, quadValueArrayObject, pass, "Quad input value array with unique values should fail with minCard = 1 and maxCard = -1" },
				{ 2, -1, quadValueArrayObject, pass, "Quad input value array with unique values should fail with minCard = 2 and maxCard = -1" },
				{ 3, -1, quadValueArrayObject, pass, "Quad input value array with unique values should fail with minCard = 3 and maxCard = -1" },
				{ 4, -1, quadValueArrayObject, pass, "Quad input value array with unique values should fail with minCard = 4 and maxCard = -1" },
		};
	}

	/**
	 * Tests job app input cardinality on jobs submitted as json
	 * 
	 * @param jobInputs test validation of multiple inputs in a job request
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processMultipleJobInputsProvider", dependsOnMethods={"processJsonJob"})
	public void processMultipleJobInputs(int minCardinality, int maxCardinality, ObjectNode jobInputs, boolean shouldThrowException, String message) 
	{
		try {
			Software software = createSoftware();
			software.getInputs().iterator().next().setValidator(null);
			software.getInputs().iterator().next().setMaxCardinality(maxCardinality);
			software.getInputs().iterator().next().setMinCardinality(minCardinality);
			SoftwareDao.persist(software);

			ObjectNode json = createJobJsonNode(software);
			json.put("appId", software.getUniqueName());
			json.set("inputs", jobInputs);
			_genericProcessJsonJob(json, shouldThrowException, message);
		} catch (Exception e) {
			Assert.fail("Test setup should not fail", e);
		}

	}
	
	@DataProvider
	private Object[][] processMultipleJobNullInputsProvider() throws Exception
	{
		ObjectMapper mapper = new ObjectMapper();

		JSONObject softwareJson = getDefaultSoftwareJson();
		JSONObject input = softwareJson.getJSONArray("inputs").getJSONObject(0);

//		SoftwareInput input = software.getInputs().iterator().next();
		String inputKey = input.getString("id");
		String inputDefaultValue = input.getJSONObject("value").getString("default");
		String inputDefaultValue2 = inputDefaultValue + "2";
		String inputDefaultValue3 = inputDefaultValue + "3";
		
		ObjectNode singleNullValueObject = mapper.createObjectNode().putNull(inputKey);
		
		ObjectNode singleNullValueArrayObject = mapper.createObjectNode();
		singleNullValueArrayObject.putArray(inputKey).addNull();
		
		ObjectNode multipleDelimitedNullValueObject = mapper.createObjectNode().put(inputKey, String.format("%1$s;", inputDefaultValue, inputDefaultValue2));
		
		ObjectNode multipleValueArrayWithNullObject = mapper.createObjectNode();
		multipleValueArrayWithNullObject.putArray(inputKey).add(inputDefaultValue).addNull();
		
		ObjectNode tripleValueNullArrayObject = mapper.createObjectNode();
		tripleValueNullArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue2).addNull();
		
		ObjectNode tripleValueArrayWithRedundantValueAndOneNullObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantValueAndOneNullObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).addNull();
		
		ObjectNode tripleValueArrayWithValueAndRedundantNullObject = mapper.createObjectNode();
		tripleValueArrayWithValueAndRedundantNullObject.putArray(inputKey).add(inputDefaultValue).addNull().addNull();
		
		ObjectNode tripleValueArrayWithRedundantNullObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantNullObject.putArray(inputKey).addNull().addNull().addNull();
		
		ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue2).addNull();
		
		boolean pass = false;
		boolean fail = true;
		// need to verify cardinality
		return new Object[][] {
				{ 0, 1, singleNullValueObject, pass, "Null input value should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, singleNullValueArrayObject, pass, "Single null input array should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited input values with null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, multipleValueArrayWithNullObject, fail, "Multiple input value array with null value should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueNullArrayObject, fail, "Triple input value array with one null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple input value array with redundant value and null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple input value array with one value and redundant null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple input value array with redundant null values should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values and null should fail with minCard = 0 and maxCard = 1" },
				
				{ 0, -1, singleNullValueObject, pass, "Null input value should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, singleNullValueArrayObject, pass, "Single null input array should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited input values with null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, multipleValueArrayWithNullObject, pass, "Multiple input value array with null value should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueNullArrayObject, pass, "Triple input value array with one null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple input value array with redundant value and null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple input value array with one value and redundant null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithRedundantNullObject, pass, "Triple input value array with redundant null values should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values and null should pass with minCard = 0 and maxCard = -1" },
				
				{ 1, 1, singleNullValueObject, fail, "Null input value should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, singleNullValueArrayObject, fail, "Single null input array should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited input values with null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, multipleValueArrayWithNullObject, fail, "Multiple input value array with null value should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueNullArrayObject, fail, "Triple input value array with one null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple input value array with redundant value and null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple input value array with one value and redundant null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple input value array with redundant null values should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values and null should fail with minCard = 1 and maxCard = 1" },
				
				{ 1, -1, singleNullValueObject, fail, "Null input value should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, singleNullValueArrayObject, fail, "Single null input array should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited input values with null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, multipleValueArrayWithNullObject, pass, "Multiple input value array with null value should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueNullArrayObject, pass, "Triple input value array with one null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple input value array with redundant value and null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple input value array with one value and redundant null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple input value array with redundant null values should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values and null should pass with minCard = 1 and maxCard = -1" },
				
				{ 1, 2, singleNullValueObject, fail, "Null input value should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, singleNullValueArrayObject, fail, "Single null input array should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, multipleDelimitedNullValueObject, pass, "Multiple delimited input values with null should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, multipleValueArrayWithNullObject, pass, "Multiple input value array with null value should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueNullArrayObject, fail, "Triple input value array with one null should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple input value array with redundant value and null should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple input value array with one value and redundant null should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple input value array with redundant null values should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values and null should fail with minCard = 1 and maxCard = 2" },
				
				{ 2, 2, singleNullValueObject, fail, "Null input value should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, singleNullValueArrayObject, fail, "Single null input array should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, multipleDelimitedNullValueObject, fail, "Multiple delimited input values with null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, multipleValueArrayWithNullObject, fail, "Multiple input value array with null value should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueNullArrayObject, fail, "Triple input value array with one null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple input value array with redundant value and null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple input value array with one value and redundant null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple input value array with redundant null values should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values and null should fail with minCard = 2 and maxCard = 2" },
				
				{ 2, -1, singleNullValueObject, fail, "Null input value should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, singleNullValueArrayObject, fail, "Single null input array should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited input pass should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, multipleValueArrayWithNullObject, fail, "Multiple input value array with null value should fail with minCard = 2 and maxCard = -1" },				
				{ 2, -1, tripleValueNullArrayObject, pass, "Triple input value array with one null should pass with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple input value array with redundant value and null should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple input value array with one value and redundant null should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple input value array with redundant null values should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values and null should pass with minCard = 2 and maxCard = -1" },	
		};
	}
	
	/**
	 * Tests job app input cardinality on jobs submitted as json
	 *
	 * @param minCardinality the min cardinality of the test input
	 * @param maxCardinality the max cardinality of the test input
	 * @param jobInputs test validation of multiple inputs in a job request
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processMultipleJobNullInputsProvider", dependsOnMethods={"processMultipleJobInputs"})
	public void processMultipleJobNullInputs(int minCardinality, int maxCardinality, ObjectNode jobInputs, boolean shouldThrowException, String message)
	{
		Software software;
		try {
			software = createSoftware();
			software.setName("processMultipleJobNullInputs");
			software.getInputs().iterator().next().setValidator(null);
			software.getInputs().iterator().next().setMaxCardinality(maxCardinality);
			software.getInputs().iterator().next().setMinCardinality(minCardinality);

			SoftwareDao.persist(software);

			ObjectNode json = createJobJsonNode(software);
			json.put("appId", software.getUniqueName());
			json.set("inputs", jobInputs);
			_genericProcessJsonJob(json, shouldThrowException, message);
		} catch (Exception e) {
			Assert.fail("Test setup should not fail", e);
		}
	}

	/**
	 * Default software takes a {@link SoftwareInput}. Here we generate tests for different values of that
	 * {@link SoftwareInput).
	 * @return test cases
	 * @throws IOException
	 * @throws JSONException
	 */
	@DataProvider
	public Object[][] processJsonJobInputsProvider() throws IOException, JSONException {

		ObjectMapper mapper = new ObjectMapper();
		JSONObject softwareJson = getDefaultSoftwareJson();
		String key = softwareJson.getJSONArray("inputs").getJSONObject(0).getString("id");
		
		boolean pass = false;
		boolean fail = true;
		// need to verify cardinality
		return new Object[][] {
				{ mapper.createObjectNode().put(key, "/path/to/folder"), pass, "absolute path should pass" },
				{ mapper.createObjectNode().put(key, "path/to/folder"), pass, "relative path should pass" },
				{ mapper.createObjectNode().put(key, "folder"), pass, "relative file should pass" },
				{ mapper.createObjectNode().put(key, "http://example.com"), pass, "HTTP uri schema should pass" },
				{ mapper.createObjectNode().put(key, "https://example.com"), pass, "HTTPS uri schema should pass" },
				{ mapper.createObjectNode().put(key, "http://foo:bar@example.com"), pass, "HTTP with basic auth uri schema should pass" },
				{ mapper.createObjectNode().put(key, "https://foo:bar@example.com"), pass, "HTTP with basic auth uri schema should pass" },
				{ mapper.createObjectNode().put(key, "sftp://asasfasdfasdfasdfasdgasdfasdf.asdfasdfa"), fail, "SFTP uri should fail without credentials" },
				{ mapper.createObjectNode().put(key, "sftp://foo:bar@asasfasdfasdfasdfasdgasdfasdf.asdfasdfa/"), fail, "SFTP uri should fail on invalid hostname" },
				{ mapper.createObjectNode().put(key, "sftp://testuser:testuser@docker.example.com:22"), fail, "SFTP uri should fail on invalid host and invalid port" },
				{ mapper.createObjectNode().put(key, "sftp://testuser:testuser@docker.example.com"), fail, "SFTP uri should fail on valid host and invalid port" },
				{ mapper.createObjectNode().put(key, "sftp://foo:bar@docker.example.com:10022"), fail, "SFTP with basic auth uri schema should fail on invalid credentials" },
				{ mapper.createObjectNode().put(key, "sftp://testuser:testuser@docker.example.com:10022"), pass, "SFTP with basic auth uri schema should fail on invalid credentials" },
				{ mapper.createObjectNode().put(key, "agave://example.com"), fail, "invalid system id in agave uri schema should fail" },
				
				{ mapper.createObjectNode().put(key, "HTTP://example.com"), pass, "HTTP uri schema is case insensitive and should pass" },
				{ mapper.createObjectNode().put(key, "HTTPS://example.com"), pass, "HTTPS uri schema should pass" },
				{ mapper.createObjectNode().put(key, "HTTP://foo:bar@example.com"), pass, "HTTP with basic auth uri schema is case insensitive and should pass" },
				{ mapper.createObjectNode().put(key, "HTTPS://foo:bar@example.com"), pass, "HTTP with basic auth uri schema is case insensitive and should pass" },
				{ mapper.createObjectNode().put(key, "SFTP://asasfasdfasdfasdfasdgasdfasdf.asdfasdfa"), fail, "SFTP uri should fail without credentials" },
				{ mapper.createObjectNode().put(key, "SFTP://foo:bar@asasfasdfasdfasdfasdgasdfasdf.asdfasdfa/"), fail, "SFTP uri should fail on invalid hostname" },
				{ mapper.createObjectNode().put(key, "SFTP://testuser:testuser@docker.example.com:22"), fail, "SFTP uri should fail on invalid host and invalid port" },
				{ mapper.createObjectNode().put(key, "SFTP://testuser:testuser@docker.example.com"), fail, "SFTP uri should fail on valid host and invalid port" },
				{ mapper.createObjectNode().put(key, "SFTP://foo:bar@docker.example.com:10022"), fail, "SFTP with basic auth uri schema should fail on invalid credentials" },
				{ mapper.createObjectNode().put(key, "SFTP://testuser:testuser@docker.example.com:10022"), pass, "SFTP with basic auth uri schema should fail on invalid credentials" },
				{ mapper.createObjectNode().put(key, "AGAVE://example.com"), fail, "invalid system id in agave uri schema should fail" },
				
				// unsupported schemas
				{ mapper.createObjectNode().put(key, "file:///path/to/folder"), fail, "FILE uri schema should fail" },
				{ mapper.createObjectNode().put(key, "ftp://example.com"), fail, "FTP uri schema should fail" },
				{ mapper.createObjectNode().put(key, "ftp://foo:bar@example.com/"), fail, "FTP with basic auth uri schema should fail" },
				{ mapper.createObjectNode().put(key, "ftps://example.com"), fail, "FTPS uri schema should fail" },
				{ mapper.createObjectNode().put(key, "ftps://foo:bar@example.com/"), fail, "FTPS with basic auth uri schema should fail" },
				{ mapper.createObjectNode().put(key, "gsiftp://example.com"), fail, "GSIFTP uri schema should fail" },
				{ mapper.createObjectNode().put(key, "gsiftp://foo:bar@example.com/"), fail, "GSIFTP with basic auth uri schema should fail" },
				{ mapper.createObjectNode().put(key, "gridftp://example.com"), fail, "gridftp uri schema should fail" },
				{ mapper.createObjectNode().put(key, "gridftp://foo:bar@example.com/"), fail, "GRIDFTP with basic auth uri schema should fail" },
				{ mapper.createObjectNode().put(key, "s3://s3.amazon.com/abced"), fail, "s3 uri schema should fail" },
				{ mapper.createObjectNode().put(key, "s3://foo:bar@s3.amazon.com/abced"), fail, "s3 with basic auth uri schema should fail" },
				
				{ mapper.createObjectNode().put(key, "FILE:///path/to/folder"), fail, "FILE uri schema should fail" },
				{ mapper.createObjectNode().put(key, "FTP://example.com"), fail, "FTP uri schema is case insensitive and should fail" },
				{ mapper.createObjectNode().put(key, "FTP://foo:bar@example.com/"), fail, "FTP with basic auth uri schema is case insensitive and should fail" },
				{ mapper.createObjectNode().put(key, "FTPS://example.com"), fail, "FTPS uri schema is case insensitive and should fail" },
				{ mapper.createObjectNode().put(key, "FTPS://foo:bar@example.com/"), fail, "FTPS with basic auth uri schema is case insensitive and should fail" },
				{ mapper.createObjectNode().put(key, "GSIFTP://example.com"), fail, "GSIFTP uri schema is case insensitive and should fail" },
				{ mapper.createObjectNode().put(key, "GSIFTP://foo:bar@example.com/"), fail, "GSIFTP with basic auth uri schema is case insensitive and should fail" },
				{ mapper.createObjectNode().put(key, "GRIDFTP://example.com"), fail, "gridftp uri schema is case insensitive and should fail" },
				{ mapper.createObjectNode().put(key, "GRIDFTP://foo:bar@example.com/"), fail, "GRIDFTP with basic auth uri schema is case insensitive and should fail" },
				{ mapper.createObjectNode().put(key, "S3://s3.amazon.com/abced"), fail, "s3 uri schema is case insensitive and should fail" },
				{ mapper.createObjectNode().put(key, "S3://foo:bar@s3.amazon.com/abced"), fail, "s3 with basic auth uri schema is case insensitive and should fail" },
				
				{ mapper.createObjectNode().put(key, "abba://example.com"), fail, "Unknown uri schema should fail" },
				{ mapper.createObjectNode().put(key, "ABBA://example.com"), fail, "Unknown uri schema is case insensitive and should fail" },
		};
		
	}
	
	/**
	 * Tests job app input validation on jobs submitted as json
	 *
	 * @param jobInputs test validation of multiple inputs in a job request
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processJsonJobInputsProvider", dependsOnMethods={"processJsonJob"})
	public void processJsonJobInputs(ObjectNode jobInputs, boolean shouldThrowException, String message) throws Exception
	{
		Software software = createSoftware();
		ObjectNode json = createJobJsonNode(software);
		json.set("inputs", jobInputs);
		_genericProcessJsonJob(json, shouldThrowException, message);
	}

	@DataProvider
	public Object[][] processJsonJobInputsAcceptsCaseInsensitiveAgaveURLProvider() {
		return new Object[][]{
				{"agave", "Lower case Agave uri schema should pass"},
				{"AGAVE", "Upper case Agave uri schema should pass"},
				{"AGaVe", "Mixed case Agave uri schema should pass"}
		};
	}

	/**
	 * Tests job app input validation on jobs submitted as json
	 *
	 * @param caseInsensitiveTestSchema permutation on case insensitive "agave" scheme
	 * @param message the message to assert for a failed test
	 */
	@Test(dataProvider = "processJsonJobInputsAcceptsCaseInsensitiveAgaveURLProvider", dependsOnMethods={"processJsonJob"})
	public void processJsonJobInputsAcceptsCaseInsensitiveAgaveURL(String caseInsensitiveTestSchema, String message)
	{
		ObjectMapper mapper = new ObjectMapper();
		try {
			Software software = createSoftware();

			// key of the first and only input for the software instance
			String key = software.getInputs().get(0).getKey();
			// agave url built from test schema used to test
			String inputUrl = String.format("%s://%s/", caseInsensitiveTestSchema, software.getStorageSystem().getSystemId());

			ObjectNode json = createJobJsonNode(software);
			json.set("inputs", mapper.createObjectNode().put(key, inputUrl));

			Job job = _genericProcessJsonJob(json, false, message);
		} catch (Exception e) {
			Assert.fail(message);	
		}
	}

	/**
	 * Tests job app input validation on jobs submitted as json
	 */
//	@Test(dependsOnMethods={"processJsonJobInputs"})
	public void processJsonJobInputDefaults() 
	{
		Software software = null;
		
		try {
			software = createSoftware();
			software.setOwner(JSONTestDataUtil.TEST_OWNER);
			software.setName("processJsonJobInputDefaults");
			software.getInputs().clear();
			SoftwareDao.persist(software);
			
//			SoftwareInput input1 = new SoftwareInput();
//			input1.setDefaultValue("/usr/bin/date");
//			input1.setKey("requiredKey");
//			input1.setRequired(true);
//			testSoftware.addInput(input1);
			
			SoftwareInput input2 = new SoftwareInput();
			input2.setDefaultValue("/usr/bin/time");
			input2.setKey("hiddenTime");
			input2.setVisible(false);
			input2.setRequired(true);
			software.addInput(input2);
			
			SoftwareInput input3 = new SoftwareInput();
			input3.setDefaultValue("/usr/bin/mkdir");
			input3.setKey("optionalKey1");
			input3.setRequired(false);
			software.addInput(input3);
			
			SoftwareInput input4 = new SoftwareInput();
			input4.setDefaultValue("/usr/bin/top");
			input4.setKey("requiredKey2");
			input4.setRequired(true);
			software.addInput(input4);
			
			SoftwareInput input5 = new SoftwareInput();
			input5.setDefaultValue("/usr/bin/ls");
			input5.setKey("optionalKey2");
			input5.setRequired(false);
			software.addInput(input5);
			
			SoftwareDao.persist(software);
			
			ObjectNode json = createJobJsonNode(software);
			json.put("appId", software.getUniqueName());
			json.set("inputs", new ObjectMapper().createObjectNode().put(input5.getKey(), "wazzup").put(input4.getKey(), "top").put("dummyfield", "something"));
			
			Job job = _genericProcessJsonJob(json, false, "Hidden and required inputs should be added by default.");
			
//			Assert.assertTrue(job.getInputsAsMap().containsKey(input1.getKey()), "Required fields should be added if not specified and a default exists");
//			Assert.assertEquals(job.getInputsAsMap().get(input1.getKey()), input1.getDefaultValue(), "Required fields should be added if not specified and a default exists");
			
			Assert.assertTrue(job.getInputsAsJsonObject().has(input2.getKey()), "Hidden fields should always be added");
			Assert.assertEquals(job.getInputsAsJsonObject().get(input2.getKey()), input2.getDefaultValueAsJsonArray(), "Hidden fields should always be added");
			
			Assert.assertFalse(job.getInputsAsJsonObject().has(input3.getKey()), "Optional fields should not be added if user does not supply value");
			Assert.assertFalse(job.getInputsAsJsonObject().has("dummyfield"), "User supplied fields not part of the job should not be persisted.");
			
			Assert.assertTrue(job.getInputsAsJsonObject().has(input4.getKey()), "User supplied required fields should be persisted");
			Assert.assertEquals(job.getInputsAsJsonObject().get(input4.getKey()).get(0).textValue(), "top", "Required field that user supplies as input should be the value persisted with the job");
			
			Assert.assertTrue(job.getInputsAsJsonObject().has(input5.getKey()), "User supplied optional fields should be persisted");
			Assert.assertEquals(job.getInputsAsJsonObject().get(input5.getKey()).get(0).textValue(), "wazzup", "Option field that user supplies as input should be the value persisted with the job");
			
		} catch (Exception e) {
			Assert.fail("Failed to process job", e);
		}
	}
	
	@DataProvider
	public Object[][] processJsonJobParametersProvider() throws JSONException, IOException
	{
		JSONObject softwareJson = getDefaultSoftwareJson();
		JSONObject parameter = softwareJson.getJSONArray("inputs").getJSONObject(0);
		String paramKey = parameter.getString("id");
		String parameterDefaultValue = parameter.getJSONObject("value").getString("default");

		// need to verify cardinality
		Map<SoftwareParameterType, Object> defaultValues = new HashMap<SoftwareParameterType, Object>();
		defaultValues.put(SoftwareParameterType.flag, Boolean.TRUE);
		defaultValues.put(SoftwareParameterType.bool, Boolean.TRUE);
		defaultValues.put(SoftwareParameterType.enumeration, new ObjectMapper().createArrayNode().add("ALPHA"));
		defaultValues.put(SoftwareParameterType.number, 512L);
		defaultValues.put(SoftwareParameterType.string, "somedefaultvalue");
		
		Map<SoftwareParameterType, Object> validTestValues = new HashMap<SoftwareParameterType, Object>();
		validTestValues.put(SoftwareParameterType.flag, Boolean.FALSE);
		validTestValues.put(SoftwareParameterType.bool, Boolean.FALSE);
		validTestValues.put(SoftwareParameterType.enumeration, new ObjectMapper().createArrayNode().add("BETA"));
		validTestValues.put(SoftwareParameterType.number, 215L);
		validTestValues.put(SoftwareParameterType.string, "anoteruservalue");
		
		Map<SoftwareParameterType, List<Object>> invalidTestValues = new HashMap<SoftwareParameterType, List<Object>>();
		
		for (SoftwareParameterType type: SoftwareParameterType.values()) {
			List<Object> invalidValues = new ArrayList<Object>();
			for (SoftwareParameterType validType: validTestValues.keySet()) {
				if (type.equals(validType) || 
						(type.equals(SoftwareParameterType.bool) && validType.equals(SoftwareParameterType.flag)) ||
						(type.equals(SoftwareParameterType.flag) && validType.equals(SoftwareParameterType.bool)))
					continue;
				else
					invalidValues.add(validTestValues.get(validType));
			}
			invalidTestValues.put(type, invalidValues);
		}
		
		List<Object[]> testData = new ArrayList<Object[]>();
		for (SoftwareParameterType type: SoftwareParameterType.values()) 
		{
			if (type.equals(SoftwareParameterType.flag) || type.equals(SoftwareParameterType.bool)) 
			{
														//	name			type		default					validator	required	visible
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		false), mapper.createObjectNode().put(paramKey, (Boolean)validTestValues.get(type)), null										  , fail, "Hidden param should fail if user provides value" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		false), mapper.createObjectNode().put(paramKey, (Boolean)validTestValues.get(type)), null										  , fail, "Hidden param should fail if user provides value" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		false), null, 														 		 mapper.createObjectNode().put(paramKey, (Boolean)defaultValues.get(type)), pass, "Hidden param should use default." });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		false), null, 																 mapper.createObjectNode().put(paramKey, (Boolean)defaultValues.get(type)), pass, "Hidden param should use default." });
				
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		true), null, 																 null, pass, "Visible param not required should not use default when no user data given" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		true), null, 																 mapper.createObjectNode().put(paramKey, (Boolean)defaultValues.get(type)), fail, "Required param should fail if not supplied" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		true), mapper.createObjectNode().put(paramKey, (Boolean)validTestValues.get(type)),  mapper.createObjectNode().put(paramKey, (Boolean)validTestValues.get(type)), pass, "User supplied value should be used for visible optional values of " + type.name() + " parameter" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		true), mapper.createObjectNode().put(paramKey, (Boolean)validTestValues.get(type)),  mapper.createObjectNode().put(paramKey, (Boolean)validTestValues.get(type)), pass, "User supplied value should be used for visible required values of " + type.name() + " parameter" });
				
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		true), mapper.createObjectNode().put(paramKey, Boolean.TRUE),  mapper.createObjectNode().put(paramKey, true), pass, "User supplied value TRUE should be used for visible optional values of " + type.name() + " parameter" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		true), mapper.createObjectNode().put(paramKey, Boolean.TRUE),  mapper.createObjectNode().put(paramKey, true), pass, "User supplied value TRUE should be used for visible required values of " + type.name() + " parameter" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		true), mapper.createObjectNode().put(paramKey, Boolean.FALSE),  mapper.createObjectNode().put(paramKey, false), pass, "User supplied value false should be used for visible optional values of " + type.name() + " parameter" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		true), mapper.createObjectNode().put(paramKey, Boolean.FALSE),  mapper.createObjectNode().put(paramKey, false), pass, "User supplied value false should be used for visible required values of " + type.name() + " parameter" });
			}
			else if (type.equals(SoftwareParameterType.enumeration))
			{
				ObjectNode expectedDefault = mapper.createObjectNode();
				expectedDefault.putArray(paramKey).add("ALPHA");
				
				ObjectNode expectedValid = mapper.createObjectNode();
				expectedValid.putArray(paramKey).add("BETA");
														//	name			type		default					validator	required	visible
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		false), expectedValid, null										  , fail, "Hidden param should fail if user provides value" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		false), expectedValid, null										  , fail, "Hidden param should fail if user provides value" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		false), null, 														 		 expectedDefault, pass, "Hidden param should use default." });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		false), null, 																 expectedDefault, pass, "Hidden param should use default." });
				
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		true), null, 																 null, pass, "Visible param not required should not use default when no user data given" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		true), null, 																 expectedDefault, fail, "Required param should fail if not supplied" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		true), expectedValid,  expectedValid, pass, "User supplied value should be used for visible optional values of " + type.name() + " parameter" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		true), expectedValid,  expectedValid, pass, "User supplied value should be used for visible required values of " + type.name() + " parameter" });
				
				// validate all enumerated values will work
				SoftwareParameter p = createParameter(paramKey, type.name(), defaultValues.get(type), null, 		true, 		true);
				for (String enumValue: p.getEnumeratedValuesAsList()) 
				{
					expectedValid = mapper.createObjectNode();
					expectedValid.putArray(paramKey).add(enumValue);
					testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), null, 		true, 		true), expectedValid, expectedValid, pass, "Valid required enumerated value of " + enumValue + " is within the available values of " + p.getEnumeratedValues().toString() + " and should pass" });
				}
			}
			else
			{
														//	name			type		default					validator	required	visible
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		false), mapper.createObjectNode().put(paramKey, validTestValues.get(type).toString()), null										  , fail, "Hidden param should fail if user provides value" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		false), mapper.createObjectNode().put(paramKey, validTestValues.get(type).toString()), null										  , fail, "Hidden param should fail if user provides value" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		false), null, 														 		 mapper.createObjectNode().put(paramKey, defaultValues.get(type).toString()), pass, "Hidden param should use default." });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		false), null, 																 mapper.createObjectNode().put(paramKey, defaultValues.get(type).toString()), pass, "Hidden param should use default." });
				
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		true), null, 																 null, pass, "Visible param not required should not use default when no user data given" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		true), null, 																 mapper.createObjectNode().put(paramKey, defaultValues.get(type).toString()), fail, "Required param should fail if not supplied" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		true), mapper.createObjectNode().put(paramKey, validTestValues.get(type).toString()),  mapper.createObjectNode().put(paramKey, validTestValues.get(type).toString()), pass, "User supplied value should be used for visible optional values of " + type.name() + " parameter" });
				testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		true), mapper.createObjectNode().put(paramKey, validTestValues.get(type).toString()),  mapper.createObjectNode().put(paramKey, validTestValues.get(type).toString()), pass, "User supplied value should be used for visible required values of " + type.name() + " parameter" });
			}
			
			if (!type.equals(SoftwareParameterType.string))
			{
				for (Object invalidValue: invalidTestValues.get(type)) {
					if (!type.equals(SoftwareParameterType.number)) {
						testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		true, 		true), mapper.createObjectNode().put(paramKey, invalidValue.toString()),  null, fail, "Invalid required value " + invalidValue + " should fail for " + type.name() + " parameter" });
						testData.add(new Object[] { createParameter(paramKey, type.name(), defaultValues.get(type), 	null, 		false, 		true), mapper.createObjectNode().put(paramKey, invalidValue.toString()),  null, fail, "Invalid optional value " + invalidValue + " should fail for " + type.name() + " parameter" });
					}
				}
			}
		}
		return testData.toArray(new Object[][] {});
	}
	
	/**
	 * Tests job app parameter validation on jobs submitted as json
	 *
	 * @param appParameter the app parameter to test
	 * @param jobParameters the job parameters under test
	 * @param expectedParameters the job parameters after processing
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processJsonJobParametersProvider", dependsOnMethods={"processJsonJobInputDefaults"})
	public void processJsonJobParameters(SoftwareParameter appParameter, ObjectNode jobParameters, ObjectNode expectedParameters, boolean shouldThrowException, String message) 
	{
		Software software = null;
		
		try {
			software = createSoftware();

			List<SoftwareParameter> params = new ArrayList<SoftwareParameter>();
			if (appParameter != null) {
				params.add(appParameter);
			}
			software.setParameters(params);
			
			SoftwareDao.persist(software);
			
			ObjectNode jobJson = createJobJsonNode(software);
			if (jobParameters == null) {
				jobJson.remove("parameters");
			} else {
				jobJson.set("parameters", jobParameters);
			}
			
			Job job = JobManager.processJob(jobJson, JSONTestDataUtil.TEST_OWNER, null);
			
			Assert.assertNotNull(job.getId(), "Job was not saved after processing.");
			
			if (expectedParameters != null) {
				Iterator<String> fieldNamesIterator = expectedParameters.fieldNames();
				while (fieldNamesIterator.hasNext())
				{
					String fieldName = fieldNamesIterator.next();
					Assert.assertTrue(job.getParametersAsJsonObject().has(fieldName), message);
					assert appParameter != null;
					JsonNode foundParameterJson = job.getParametersAsJsonObject().get(appParameter.getKey());
					String foundParameter;
					if (foundParameterJson.isArray()) {
						foundParameter = foundParameterJson.iterator().next().asText();
					} else {
						foundParameter = foundParameterJson.asText();
					}
					String expectedParameter = expectedParameters.get(fieldName).asText();
					if (appParameter.getType().equals(SoftwareParameterType.number)) {
						foundParameter = new Double(foundParameter).toString();
						expectedParameter = new Double(expectedParameter).toString();
					} else if (appParameter.getType().equals(SoftwareParameterType.bool) || 
							appParameter.getType().equals(SoftwareParameterType.flag)) {
//						foundParameter = StringUtils.equals("true", foundParameter) ? "" : "0";
					} else if (appParameter.getType().equals(SoftwareParameterType.enumeration)) {
						expectedParameter = ((ArrayNode)expectedParameters.get(fieldName)).get(0).asText();
					}
					Assert.assertEquals(foundParameter, expectedParameter, 
							"Unexpected value for field " + fieldName + " found. Expected " + expectedParameter + 
							" found " + foundParameter);
				}
			}
			else
			{
				Assert.assertEquals(job.getParametersAsJsonObject().size(), 0, message);
			}
		} catch (JobProcessingException e) {
			if (!shouldThrowException) {
				Assert.fail(message, e);
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Failed to process job", e);
		}
	}
	
	@DataProvider
	private Object[][] processMultipleStringJobParametersProvider() throws Exception
	{
		ObjectMapper mapper = new ObjectMapper();
		JSONObject softwareJson = getDefaultSoftwareJson();
		JSONObject parameter = softwareJson.getJSONArray("inputs").getJSONObject(0);
		String parameterKey = parameter.getString("id");
		String parameterDefaultValue = parameter.getJSONObject("value").getString("default");
		String parameterDefaultValue2 = parameterDefaultValue + "2";
		String parameterDefaultValue3 = parameterDefaultValue + "3";
		String parameterDefaultValue4 = parameterDefaultValue + "4";
		
		ObjectNode emptyInputObject = mapper.createObjectNode();
		
		ObjectNode singleValueObject = mapper.createObjectNode().put(parameterKey, parameterDefaultValue);
		
		ObjectNode mulipleRedundantDelimitedValuesObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", parameterDefaultValue));
		
		ObjectNode singleValueArrayObject = mapper.createObjectNode();
		singleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue);
		
		ObjectNode multipleRedundantValueArrayObject = mapper.createObjectNode();
		multipleRedundantValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue);
		
		ObjectNode multipleDelimitedValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", parameterDefaultValue, parameterDefaultValue2));
		
		ObjectNode multipleValueArrayObject = mapper.createObjectNode();
		multipleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2);
		
		ObjectNode tripleValueArrayObject = mapper.createObjectNode();
		tripleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3);
		
		ObjectNode tripleValueArrayWithRedundantValueObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2);
		
		ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3);
		
		ObjectNode quadValueArrayWithTwoRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithTwoRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue2);
		
		ObjectNode quadValueArrayWithSameRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithSameRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue);
		
		ObjectNode quadValueArrayObject = mapper.createObjectNode();
		quadValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3).add(parameterDefaultValue4);
		
		boolean pass = false;
		boolean fail = true;
		// need to verify cardinality
		return new Object[][] {
				{ SoftwareParameterType.string, 0, 1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.string, 0, 1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.string, 0, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.string, 0, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.string, 0, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input value with redundant values should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.string, 0, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.string, 0, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.string, 0, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.string, 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
				
				{ SoftwareParameterType.string, 0, -1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.string, 0, -1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.string, 0, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.string, 0, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.string, 0, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited input values with redundant values should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.string, 0, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.string, 0, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.string, 0, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array with redundant values should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.string, 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 0 and maxCard = -1" },
				
				{ SoftwareParameterType.string, 1, 1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.string, 1, 1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.string, 1, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.string, 1, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.string, 1, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.string, 1, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.string, 1, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.string, 1, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.string, 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 1" },
				
				{ SoftwareParameterType.string, 1, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.string, 1, -1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.string, 1, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.string, 1, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.string, 1, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited inputs with redundant values should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.string, 1, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.string, 1, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.string, 1, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.string, 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 1 and maxCard = -1" },
				
				{ SoftwareParameterType.string, 1, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.string, 1, 2, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.string, 1, 2, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.string, 1, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.string, 1, 2, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited inputs with redundant values should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.string, 1, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.string, 1, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.string, 1, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.string, 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 2" },
				
				{ SoftwareParameterType.string, 2, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.string, 2, 2, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.string, 2, 2, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.string, 2, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.string, 2, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.string, 2, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.string, 2, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.string, 2, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.string, 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 2 and maxCard = 2" },
				
				{ SoftwareParameterType.string, 2, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.string, 2, -1, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.string, 2, -1, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.string, 2, -1, multipleDelimitedValueObject, pass, "Multiple delimited input pass should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.string, 2, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 2 and maxCard = -1" },				
				{ SoftwareParameterType.string, 2, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.string, 2, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.string, 2, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },
				
				{ SoftwareParameterType.string, 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.string, 3, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.string, 4, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 4 and maxCard = -1" },
				
				{ SoftwareParameterType.string, 2, -1, quadValueArrayWithTwoRedundantValueObject, pass, "Quad input value array with two redundant values should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.string, 3, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.string, 4, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 4 and maxCard = -1" },
				
				{ SoftwareParameterType.string, 0, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.string, 1, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.string, 2, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.string, 3, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.string, 4, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 4 and maxCard = -1" },
				
				{ SoftwareParameterType.string, 0, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.string, 1, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.string, 2, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.string, 3, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.string, 4, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 4 and maxCard = -1" },
		};
	}
	
	/** 
	 * Test data for long numeric job parameter values
	 * @return long numeric test cases
	 * @throws Exception
	 */
	@DataProvider
	private Object[][] processMultipleLongJobParametersProvider() throws Exception
	{
		ObjectMapper mapper = new ObjectMapper();
		JSONObject softwareJson = getDefaultSoftwareJson();
		JSONObject parameter = softwareJson.getJSONArray("inputs").getJSONObject(0);
		String parameterKey = parameter.getString("id");
		Long parameterDefaultValue = 2L;
		Long parameterDefaultValue2 = 3L;
		Long parameterDefaultValue3 = 4L;
		Long parameterDefaultValue4 = 5L;
		
		ObjectNode emptyInputObject = mapper.createObjectNode();
		
		ObjectNode singleValueObject = mapper.createObjectNode().put(parameterKey, parameterDefaultValue);
		
		ObjectNode mulipleRedundantDelimitedValuesObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", parameterDefaultValue));
		
		ObjectNode singleValueArrayObject = mapper.createObjectNode();
		singleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue);
		
		ObjectNode multipleRedundantValueArrayObject = mapper.createObjectNode();
		multipleRedundantValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue);
		
		ObjectNode multipleDelimitedValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", parameterDefaultValue, parameterDefaultValue2));
		
		ObjectNode multipleValueArrayObject = mapper.createObjectNode();
		multipleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2);
		
		ObjectNode tripleValueArrayObject = mapper.createObjectNode();
		tripleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3);
		
		ObjectNode tripleValueArrayWithRedundantValueObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2);
		
		ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3);
		
		ObjectNode quadValueArrayWithTwoRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithTwoRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue2);
		
		ObjectNode quadValueArrayWithSameRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithSameRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue);
		
		ObjectNode quadValueArrayObject = mapper.createObjectNode();
		quadValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3).add(parameterDefaultValue4);
		
		boolean pass = false;
		boolean fail = true;
		
		// need to verify cardinality
		return new Object[][] {
				{ SoftwareParameterType.number, 0, 1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input value with redundant values should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
				
				{ SoftwareParameterType.number, 0, -1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited input values with redundant values should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array with redundant values should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 0 and maxCard = -1" },
				
				{ SoftwareParameterType.number, 1, 1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 1" },
				
				{ SoftwareParameterType.number, 1, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited inputs with redundant values should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 1 and maxCard = -1" },
				
				{ SoftwareParameterType.number, 1, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited inputs with redundant values should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 2" },
				
				{ SoftwareParameterType.number, 2, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 2 and maxCard = 2" },
				
				{ SoftwareParameterType.number, 2, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, multipleDelimitedValueObject, pass, "Multiple delimited input pass should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 2 and maxCard = -1" },				
				{ SoftwareParameterType.number, 2, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },
				
				{ SoftwareParameterType.number, 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 3, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.number, 4, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 4 and maxCard = -1" },
				
				{ SoftwareParameterType.number, 2, -1, quadValueArrayWithTwoRedundantValueObject, pass, "Quad input value array with two redundant values should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 3, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.number, 4, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 4 and maxCard = -1" },
				
				{ SoftwareParameterType.number, 0, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 3, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.number, 4, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 4 and maxCard = -1" },
				
				{ SoftwareParameterType.number, 0, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 3, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.number, 4, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 4 and maxCard = -1" },
		};
	}
	
	/**
	 * Test data for double numeric values
	 * @return
	 * @throws Exception
	 */
	@DataProvider
	private Object[][] processMultipleDoubleJobParametersProvider() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		JSONObject softwareJson = getDefaultSoftwareJson();
		JSONObject parameter = softwareJson.getJSONArray("inputs").getJSONObject(0);
		String parameterKey = parameter.getString("id");
		Double parameterDefaultValue = 1.1;
		Double parameterDefaultValue2 = 2.2;
		Double parameterDefaultValue3 = 3.3;
		Double parameterDefaultValue4 = 4.4;
		
		ObjectNode emptyInputObject = mapper.createObjectNode();
		
		ObjectNode singleValueObject = mapper.createObjectNode().put(parameterKey, parameterDefaultValue);
		
		ObjectNode mulipleRedundantDelimitedValuesObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", parameterDefaultValue));
		
		ObjectNode singleValueArrayObject = mapper.createObjectNode();
		singleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue);
		
		ObjectNode multipleRedundantValueArrayObject = mapper.createObjectNode();
		multipleRedundantValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue);
		
		ObjectNode multipleDelimitedValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", parameterDefaultValue, parameterDefaultValue2));
		
		ObjectNode multipleValueArrayObject = mapper.createObjectNode();
		multipleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2);
		
		ObjectNode tripleValueArrayObject = mapper.createObjectNode();
		tripleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3);
		
		ObjectNode tripleValueArrayWithRedundantValueObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2);
		
		ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3);
		
		ObjectNode quadValueArrayWithTwoRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithTwoRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue2);
		
		ObjectNode quadValueArrayWithSameRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithSameRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue);
		
		ObjectNode quadValueArrayObject = mapper.createObjectNode();
		quadValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3).add(parameterDefaultValue4);
		
		boolean pass = false;
		boolean fail = true;
		// need to verify cardinality
		return new Object[][] {
				{ SoftwareParameterType.number, 0, 1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input value with redundant values should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.number, 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
				
				{ SoftwareParameterType.number, 0, -1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited input values with redundant values should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array with redundant values should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 0 and maxCard = -1" },
				
				{ SoftwareParameterType.number, 1, 1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.number, 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 1" },
				
				{ SoftwareParameterType.number, 1, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited inputs with redundant values should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 1 and maxCard = -1" },
				
				{ SoftwareParameterType.number, 1, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited inputs with redundant values should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.number, 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 2" },
				
				{ SoftwareParameterType.number, 2, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.number, 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 2 and maxCard = 2" },
				
				{ SoftwareParameterType.number, 2, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, multipleDelimitedValueObject, pass, "Multiple delimited input pass should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 2 and maxCard = -1" },				
				{ SoftwareParameterType.number, 2, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },
				
				{ SoftwareParameterType.number, 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 3, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.number, 4, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 4 and maxCard = -1" },
				
				{ SoftwareParameterType.number, 2, -1, quadValueArrayWithTwoRedundantValueObject, pass, "Quad input value array with two redundant values should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 3, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.number, 4, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 4 and maxCard = -1" },
				
				{ SoftwareParameterType.number, 0, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 3, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.number, 4, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 4 and maxCard = -1" },
				
				{ SoftwareParameterType.number, 0, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.number, 1, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.number, 2, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.number, 3, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.number, 4, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 4 and maxCard = -1" },
		};
	}

	/**
	 * Test data for boolean numeric values
	 * @return
	 * @throws Exception
	 */
	@DataProvider
	private Object[][] processMultipleBooleanJobParametersProvider() throws Exception
	{
		ObjectMapper mapper = new ObjectMapper();
		JSONObject softwareJson = getDefaultSoftwareJson();
		JSONObject parameter = softwareJson.getJSONArray("inputs").getJSONObject(0);
		String parameterKey = parameter.getString("id");

		ObjectNode emptyInputObject = mapper.createObjectNode();
		
		ObjectNode singleValueObject = mapper.createObjectNode().put(parameterKey, Boolean.TRUE);
		
		ObjectNode mulipleRedundantDelimitedValuesObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", Boolean.TRUE));
		
		ObjectNode singleValueArrayObject = mapper.createObjectNode();
		singleValueArrayObject.putArray(parameterKey).add(Boolean.TRUE);
		
		ObjectNode multipleRedundantValueArrayObject = mapper.createObjectNode();
		multipleRedundantValueArrayObject.putArray(parameterKey).add(Boolean.TRUE).add(Boolean.TRUE);
		
		ObjectNode multipleDelimitedValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", Boolean.TRUE, Boolean.FALSE));
		
		ObjectNode multipleValueArrayObject = mapper.createObjectNode();
		multipleValueArrayObject.putArray(parameterKey).add(Boolean.TRUE).add(Boolean.FALSE);
		
		ObjectNode tripleValueArrayWithRedundantValueObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantValueObject.putArray(parameterKey).add(Boolean.TRUE).add(Boolean.TRUE).add(Boolean.FALSE);
		
		ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(Boolean.TRUE).add(Boolean.TRUE).add(Boolean.FALSE);
		
		ObjectNode quadValueArrayWithTwoRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithTwoRedundantValueObject.putArray(parameterKey).add(Boolean.TRUE).add(Boolean.TRUE).add(Boolean.FALSE).add(Boolean.FALSE);
		
		ObjectNode quadValueArrayWithSameRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithSameRedundantValueObject.putArray(parameterKey).add(Boolean.TRUE).add(Boolean.TRUE).add(Boolean.TRUE).add(Boolean.TRUE);
		
		boolean pass = false;
		boolean fail = true;
		// need to verify cardinality
		return new Object[][] {
				{ SoftwareParameterType.bool, 0, 1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.bool, 0, 1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.bool, 0, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.bool, 0, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.bool, 0, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input value with redundant values should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.bool, 0, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.bool, 0, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
				{ SoftwareParameterType.bool, 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
				
				{ SoftwareParameterType.bool, 0, -1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.bool, 0, -1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.bool, 0, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.bool, 0, -1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.bool, 0, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.bool, 0, -1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.bool, 0, -1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array with redundant values should fail with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.bool, 0, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 0 and maxCard = -1" },
				
				{ SoftwareParameterType.bool, 1, 1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.bool, 1, 1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.bool, 1, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.bool, 1, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.bool, 1, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.bool, 1, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.bool, 1, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
				{ SoftwareParameterType.bool, 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 1" },
				
				{ SoftwareParameterType.bool, 1, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.bool, 1, -1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.bool, 1, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.bool, 1, -1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.bool, 1, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.bool, 1, -1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.bool, 1, -1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.bool, 1, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = -1" },
				
				{ SoftwareParameterType.bool, 1, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.bool, 1, 2, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.bool, 1, 2, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.bool, 1, 2, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.bool, 1, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.bool, 1, 2, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.bool, 1, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
				{ SoftwareParameterType.bool, 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 2" },
				
				{ SoftwareParameterType.bool, 2, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.bool, 2, 2, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.bool, 2, 2, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.bool, 2, 2, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.bool, 2, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.bool, 2, 2, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.bool, 2, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
				{ SoftwareParameterType.bool, 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 2 and maxCard = 2" },
				
				{ SoftwareParameterType.bool, 2, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.bool, 2, -1, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.bool, 2, -1, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.bool, 2, -1, multipleDelimitedValueObject, fail, "Multiple delimited input pass should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.bool, 2, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 2 and maxCard = -1" },				
				{ SoftwareParameterType.bool, 2, -1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.bool, 2, -1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = -1" },
				
				{ SoftwareParameterType.bool, 2, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.bool, 3, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.bool, 4, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 4 and maxCard = -1" },
				
				{ SoftwareParameterType.bool, 2, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.bool, 3, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.bool, 4, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 4 and maxCard = -1" },
				
				{ SoftwareParameterType.bool, 0, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 0 and maxCard = -1" },
				{ SoftwareParameterType.bool, 1, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 1 and maxCard = -1" },
				{ SoftwareParameterType.bool, 2, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 2 and maxCard = -1" },
				{ SoftwareParameterType.bool, 3, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 3 and maxCard = -1" },
				{ SoftwareParameterType.bool, 4, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 4 and maxCard = -1" },
		};
	}
	
	/** 
	 * Private utility class to process multiple job parameters of all types
	 * @param type the parameter type under test
	 * @param minCardinality the min parameter cardinality
	 * @param maxCardinality the max parameter cardinality
	 * @param jobParameters the parameters to submit to the job processor
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	private void _processMultipleJobParameters(SoftwareParameterType type, int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message)
	{
		Software software = createSoftware();
		software.setName("processMultipleJobParametersProvider");

		SoftwareParameter parameter = null;
		for (SoftwareParameter p : software.getParameters()) {
			parameter = p.clone(software);

			parameter.setValidator(null);
			parameter.setMaxCardinality(maxCardinality);
			parameter.setMinCardinality(minCardinality);
			parameter.setType(type);
			software.getParameters().remove(p);
			software.addParameter(parameter);
			break;
		}

		try {
			SoftwareDao.persist(software);
			ObjectNode json = createJobJsonNode(software);
			json.put("appId", software.getUniqueName());
			assert parameter != null;
			JsonNode jobParamValue = jobParameters.path(parameter.getKey());
			((ObjectNode)json.path("parameters")).put(parameter.getKey(), jobParamValue);
			_genericProcessJsonJob(json, shouldThrowException, message);
		}
		finally {
			try { SoftwareDao.delete(software);} catch (Exception ignored) {}
		}
	}
	
	/**
	 * Tests string job app parameter cardinality on jobs submitted as json
	 *
	 * @param minCardinality the min parameter cardinality
	 * @param maxCardinality the max parameter cardinality
	 * @param jobParameters the parameters to submit to the job processor
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processMultipleStringJobParametersProvider", dependsOnMethods={"processJsonJobParameters"})
	public void processMultipleStringJobParameters(SoftwareParameterType type, int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message) 
	{
		_processMultipleJobParameters(SoftwareParameterType.string, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
	}
	
	/**
	 * Tests integer job app parameter cardinality on jobs submitted as json
	 *
	 * @param minCardinality the min parameter cardinality
	 * @param maxCardinality the max parameter cardinality
	 * @param jobParameters the parameters to submit to the job processor
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processMultipleLongJobParametersProvider", dependsOnMethods={"processMultipleStringJobParameters"})
	public void processMultipleLongJobParameters(SoftwareParameterType type, int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message) 
	{
		_processMultipleJobParameters(SoftwareParameterType.number, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
	}
	
	/**
	 * Tests decimal job app parameter cardinality on jobs submitted as json
	 *
	 * @param minCardinality the min parameter cardinality
	 * @param maxCardinality the max parameter cardinality
	 * @param jobParameters the parameters to submit to the job processor
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processMultipleDoubleJobParametersProvider", dependsOnMethods={"processMultipleLongJobParameters"})
	public void processMultipleDoubleJobParameters(SoftwareParameterType type, int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message) 
	{
		_processMultipleJobParameters(SoftwareParameterType.number, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
	}
	
	/**
	 * Tests boolean job app parameter cardinality on jobs submitted as json
	 *
	 * @param minCardinality the min parameter cardinality
	 * @param maxCardinality the max parameter cardinality
	 * @param jobParameters the parameters to submit to the job processor
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processMultipleBooleanJobParametersProvider", dependsOnMethods={"processMultipleDoubleJobParameters"})
	public void processMultipleBooleanJobParameters(SoftwareParameterType type, int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message) 
	{
		_processMultipleJobParameters(SoftwareParameterType.bool, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
	}
	
	/**
	 * Tests flag job app parameter cardinality on jobs submitted as json
	 *
	 * @param minCardinality the min parameter cardinality
	 * @param maxCardinality the max parameter cardinality
	 * @param jobParameters the parameters to submit to the job processor
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processMultipleBooleanJobParametersProvider", dependsOnMethods={"processMultipleBooleanJobParameters"})
	public void processMultipleFlagJobParameters(SoftwareParameterType type, int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message) 
	{
		_processMultipleJobParameters(SoftwareParameterType.flag, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
	}

	/**
	 * Test data for null string numeric values
	 * @return
	 * @throws Exception
	 */
	@DataProvider
	private Object[][] processMultipleNullStringJobParametersProvider() throws Exception
	{
		ObjectMapper mapper = new ObjectMapper();
		JSONObject softwareJson = getDefaultSoftwareJson();
		JSONObject parameter = softwareJson.getJSONArray("inputs").getJSONObject(0);
		String parameterKey = parameter.getString("id");
		String parameterDefaultValue = parameter.getJSONObject("value").getString("default");
		String parameterDefaultValue2 = parameterDefaultValue + "2";
		
		ObjectNode singleNullValueObject = mapper.createObjectNode().putNull(parameterKey);
		
		ObjectNode singleNullValueArrayObject = mapper.createObjectNode();
		singleNullValueArrayObject.putArray(parameterKey).addNull();
		
		ObjectNode multipleDelimitedNullValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;", parameterDefaultValue, parameterDefaultValue2));
		
		ObjectNode multipleValueArrayWithNullObject = mapper.createObjectNode();
		multipleValueArrayWithNullObject.putArray(parameterKey).add(parameterDefaultValue).addNull();
		
		ObjectNode tripleValueNullArrayObject = mapper.createObjectNode();
		tripleValueNullArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).addNull();
		
		ObjectNode tripleValueArrayWithRedundantValueAndOneNullObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantValueAndOneNullObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).addNull();
		
		ObjectNode tripleValueArrayWithValueAndRedundantNullObject = mapper.createObjectNode();
		tripleValueArrayWithValueAndRedundantNullObject.putArray(parameterKey).add(parameterDefaultValue).addNull().addNull();
		
		ObjectNode tripleValueArrayWithRedundantNullObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantNullObject.putArray(parameterKey).addNull().addNull().addNull();
		
		ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).addNull();
		
		boolean pass = false;
		boolean fail = true;
		// need to verify cardinality
		return new Object[][] {
				{ 0, 1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 0 and maxCard = 1" },
				
				{ 0, -1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple parameter value array with redundant value and null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple parameter value array with one value and redundant null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithRedundantNullObject, pass, "Triple parameter value array with redundant null values should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 0 and maxCard = -1" },
				
				{ 1, 1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 1" },
				
				{ 1, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple parameter value array with redundant value and null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple parameter value array with one value and redundant null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 1 and maxCard = -1" },
				
				{ 1, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 2" },
				
				{ 2, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 2 and maxCard = 2" },
				
				{ 2, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter pass should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = -1" },				
				{ 2, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 2 and maxCard = -1" },	
		};
	}
	
	/**
	 * Test data for null long numeric values
	 * @return
	 * @throws Exception
	 */
	@DataProvider
	private Object[][] processMultipleNullLongJobParametersProvider() throws Exception
	{
		ObjectMapper mapper = new ObjectMapper();
		JSONObject softwareJson = getDefaultSoftwareJson();
		JSONObject parameter = softwareJson.getJSONArray("inputs").getJSONObject(0);
		String parameterKey = parameter.getString("id");
		Long parameterDefaultValue = 2L;
		Long parameterDefaultValue2 = 3L;
		
		ObjectNode singleNullValueObject = mapper.createObjectNode().putNull(parameterKey);
		
		ObjectNode singleNullValueArrayObject = mapper.createObjectNode();
		singleNullValueArrayObject.putArray(parameterKey).addNull();
		
		ObjectNode multipleDelimitedNullValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;", parameterDefaultValue, parameterDefaultValue2));
		
		ObjectNode multipleValueArrayWithNullObject = mapper.createObjectNode();
		multipleValueArrayWithNullObject.putArray(parameterKey).add(parameterDefaultValue).addNull();
		
		ObjectNode tripleValueNullArrayObject = mapper.createObjectNode();
		tripleValueNullArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).addNull();
		
		ObjectNode tripleValueArrayWithRedundantValueAndOneNullObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantValueAndOneNullObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).addNull();
		
		ObjectNode tripleValueArrayWithValueAndRedundantNullObject = mapper.createObjectNode();
		tripleValueArrayWithValueAndRedundantNullObject.putArray(parameterKey).add(parameterDefaultValue).addNull().addNull();
		
		ObjectNode tripleValueArrayWithRedundantNullObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantNullObject.putArray(parameterKey).addNull().addNull().addNull();
		
		ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).addNull();
		
		boolean pass = false;
		boolean fail = true;
		// need to verify cardinality
		return new Object[][] {
				{ 0, 1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 0 and maxCard = 1" },
				
				{ 0, -1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple parameter value array with redundant value and null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple parameter value array with one value and redundant null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithRedundantNullObject, pass, "Triple parameter value array with redundant null values should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 0 and maxCard = -1" },
				
				{ 1, 1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 1" },
				
				{ 1, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple parameter value array with redundant value and null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple parameter value array with one value and redundant null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 1 and maxCard = -1" },
				
				{ 1, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 2" },
				
				{ 2, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 2 and maxCard = 2" },
				
				{ 2, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter pass should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = -1" },				
				{ 2, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 2 and maxCard = -1" },	
		};
	}
	
	/**
	 * Test data for null double numeric values
	 * @return
	 * @throws Exception
	 */
	@DataProvider
	private Object[][] processMultipleNullDoubleJobParametersProvider() throws Exception
	{
		ObjectMapper mapper = new ObjectMapper();
		JSONObject softwareJson = getDefaultSoftwareJson();
		JSONObject parameter = softwareJson.getJSONArray("inputs").getJSONObject(0);
		String parameterKey = parameter.getString("id");
		Double parameterDefaultValue = 2.2;
		Double parameterDefaultValue2 = 3.3;
		
		ObjectNode singleNullValueObject = mapper.createObjectNode().putNull(parameterKey);
		
		ObjectNode singleNullValueArrayObject = mapper.createObjectNode();
		singleNullValueArrayObject.putArray(parameterKey).addNull();
		
		ObjectNode multipleDelimitedNullValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;", parameterDefaultValue, parameterDefaultValue2));
		
		ObjectNode multipleValueArrayWithNullObject = mapper.createObjectNode();
		multipleValueArrayWithNullObject.putArray(parameterKey).add(parameterDefaultValue).addNull();
		
		ObjectNode tripleValueNullArrayObject = mapper.createObjectNode();
		tripleValueNullArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).addNull();
		
		ObjectNode tripleValueArrayWithRedundantValueAndOneNullObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantValueAndOneNullObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).addNull();
		
		ObjectNode tripleValueArrayWithValueAndRedundantNullObject = mapper.createObjectNode();
		tripleValueArrayWithValueAndRedundantNullObject.putArray(parameterKey).add(parameterDefaultValue).addNull().addNull();
		
		ObjectNode tripleValueArrayWithRedundantNullObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantNullObject.putArray(parameterKey).addNull().addNull().addNull();
		
		ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).addNull();
		
		boolean pass = false;
		boolean fail = true;
		// need to verify cardinality
		return new Object[][] {
				{ 0, 1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 0 and maxCard = 1" },
				
				{ 0, -1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple parameter value array with redundant value and null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple parameter value array with one value and redundant null should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithRedundantNullObject, pass, "Triple parameter value array with redundant null values should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 0 and maxCard = -1" },
				
				{ 1, 1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 1" },
				
				{ 1, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple parameter value array with redundant value and null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple parameter value array with one value and redundant null should pass with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 1 and maxCard = -1" },
				
				{ 1, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 2" },
				
				{ 2, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 2 and maxCard = 2" },
				
				{ 2, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter pass should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = -1" },				
				{ 2, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 2 and maxCard = -1" },	
		};
	}
	
	/**
	 * Test data for null boolean numeric values
	 * @return
	 * @throws Exception
	 */
	@DataProvider
	private Object[][] processMultipleNullBooleanJobParametersProvider() throws Exception
	{
		ObjectMapper mapper = new ObjectMapper();
		JSONObject softwareJson = getDefaultSoftwareJson();
		JSONObject parameter = softwareJson.getJSONArray("inputs").getJSONObject(0);
		String parameterKey = parameter.getString("id");
		
		ObjectNode singleNullValueObject = mapper.createObjectNode().putNull(parameterKey);
		
		ObjectNode singleNullValueArrayObject = mapper.createObjectNode();
		singleNullValueArrayObject.putArray(parameterKey).addNull();
		
		ObjectNode multipleDelimitedNullValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;", Boolean.TRUE));
		
		ObjectNode multipleValueArrayWithNullObject = mapper.createObjectNode();
		multipleValueArrayWithNullObject.putArray(parameterKey).add(Boolean.TRUE).addNull();
		
		ObjectNode tripleValueNullArrayObject = mapper.createObjectNode();
		tripleValueNullArrayObject.putArray(parameterKey).add(Boolean.TRUE).add(Boolean.FALSE).addNull();
		
		ObjectNode tripleValueArrayWithRedundantValueAndOneNullObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantValueAndOneNullObject.putArray(parameterKey).add(Boolean.TRUE).add(Boolean.TRUE).addNull();
		
		ObjectNode tripleValueArrayWithValueAndRedundantNullObject = mapper.createObjectNode();
		tripleValueArrayWithValueAndRedundantNullObject.putArray(parameterKey).add(Boolean.TRUE).addNull().addNull();
		
		ObjectNode tripleValueArrayWithRedundantNullObject = mapper.createObjectNode();
		tripleValueArrayWithRedundantNullObject.putArray(parameterKey).addNull().addNull().addNull();
		
		ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
		quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(Boolean.TRUE).add(Boolean.TRUE).add(Boolean.FALSE).addNull();
		
		boolean pass = false;
		boolean fail = true;
		// need to verify cardinality
		return new Object[][] {
				{ 0, 1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 0 and maxCard = 1" },
				{ 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 0 and maxCard = 1" },
				
				{ 0, -1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = -1" },
				{ 0, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 0 and maxCard = -1" },
				{ 0, -1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 0 and maxCard = -1" },
				{ 0, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 0 and maxCard = -1" },
				{ 0, -1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 0 and maxCard = -1" },
				
				{ 1, 1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 1" },
				{ 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 1" },
				
				{ 1, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = -1" },
				{ 1, -1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = -1" },
				
				{ 1, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 2" },
				{ 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 2" },
				
				{ 2, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = 2" },
				{ 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 2 and maxCard = 2" },
				
				{ 2, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter pass should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = -1" },				
				{ 2, -1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = -1" },
				{ 2, -1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 2 and maxCard = -1" },	
		};
	}
	
	/**
	 * Tests null parameter values on jobs submitted as json
	 *
	 * @param minCardinality the min parameter cardinality
	 * @param maxCardinality the max parameter cardinality
	 * @param jobParameters the parameters to submit to the job processor
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processMultipleNullStringJobParametersProvider", dependsOnMethods={"processMultipleFlagJobParameters"})
	public void processMultipleNullStringJobParameters(int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message) 
	{
		_processMultipleJobParameters(SoftwareParameterType.string, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
	}
	
	/**
	 * Tests integer job app parameter cardinality on jobs submitted as json
	 *
	 * @param minCardinality the min parameter cardinality
	 * @param maxCardinality the max parameter cardinality
	 * @param jobParameters the parameters to submit to the job processor
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processMultipleNullLongJobParametersProvider", dependsOnMethods={"processMultipleNullStringJobParameters"})
	public void processMultipleNullLongJobParameters(int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message) 
	{
		_processMultipleJobParameters(SoftwareParameterType.number, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
	}
	
	/**
	 * Tests decimal job app parameter cardinality on jobs submitted as json
	 *
	 * @param minCardinality the min parameter cardinality
	 * @param maxCardinality the max parameter cardinality
	 * @param jobParameters the parameters to submit to the job processor
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processMultipleNullDoubleJobParametersProvider", dependsOnMethods={"processMultipleNullLongJobParameters"})
	public void processMultipleNullDoubleJobParameters(int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message) 
	{
		_processMultipleJobParameters(SoftwareParameterType.number, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
	}
	
	/**
	 * Tests boolean job app parameter cardinality on jobs submitted as json
	 *
	 * @param minCardinality the min parameter cardinality
	 * @param maxCardinality the max parameter cardinality
	 * @param jobParameters the parameters to submit to the job processor
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processMultipleNullBooleanJobParametersProvider", dependsOnMethods={"processMultipleNullDoubleJobParameters"})
	public void processMultipleNullBooleanJobParameters(int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message) 
	{
		_processMultipleJobParameters(SoftwareParameterType.bool, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
	}

	/**
	 * Tests flag job app parameter cardinality on jobs submitted as json
	 *
	 * @param minCardinality the min parameter cardinality
	 * @param maxCardinality the max parameter cardinality
	 * @param jobParameters the parameters to submit to the job processor
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processMultipleNullBooleanJobParametersProvider", dependsOnMethods={"processMultipleNullBooleanJobParameters"})
	public void processMultipleNullFlagJobParameters(int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message) 
	{
		_processMultipleJobParameters(SoftwareParameterType.flag, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
	}

	/**
	 * Tests hidden {@link SoftwareParameter} throws an exception when included in the job request
	 */
	//@Test(dependsOnMethods={"processMultipleNullFlagJobParameters"})
	public void processJsonJobHiddenParametersThrowExceptionWhenProvided() 
	{
		Software testSoftware = null;
		
		try {
			testSoftware = createSoftware();
			testSoftware.setName("processJsonJobHiddenParameters");
			testSoftware.getParameters().clear();
			SoftwareDao.persist(testSoftware);
			SoftwareParameter param = createParameter("hiddenparam", SoftwareParameterType.bool.name(), "true", 	null, 		true, 		false);
			testSoftware.addParameter(param);
			SoftwareDao.persist(testSoftware);
			
			ObjectNode json = createJobJsonNode(testSoftware);
			json.set("parameters", mapper.createObjectNode().put(param.getKey(), true));
			
			JobManager.processJob(json, testSoftware.getOwner(), null);
					
			Assert.fail("User supplied value for a hidden parameter should fail.");
		}
		catch (JobProcessingException e) {
			// this is expected;
		} catch (Exception e) {
			Assert.fail("Unexpected exception parsing job", e);
		}
		finally {
			try { SoftwareDao.delete(testSoftware); } catch (Throwable ignored) {}
			try { clearJobs(); } catch (Throwable ignored) {}
		}
	}
	
	/**
	 * Test data for job notification test values
	 * @return job notification test cases
	 */
	@DataProvider
	public Object[][] processJsonJobWithNotificationsProvider()
	{
		ObjectMapper mapper = new ObjectMapper();
		Object[] validUrls = { "http://example.com", "http://foo@example.com", "http://foo:bar@example.com", "http://example.com/job/${JOB_ID}/${JOB_STATUS}", "http://example.com?foo=${JOB_ID}", "foo@example.com"};
		Object[] invalidWebhookUrls = { "example.com", "example", "", null, 1L, mapper.createArrayNode(), mapper.createObjectNode() };
		Object[] invalidEmailAddresses = { "@example.com", "@example", "foo@example", "foo@", "@.com", "foo@.com" };
		Object[] validEvents = { JobStatusType.RUNNING.name(), JobStatusType.RUNNING.name().toLowerCase() };
		Object[] invalidEvents = { "", null, 1L,  mapper.createArrayNode(), mapper.createObjectNode() };
		
		boolean pass = false;
		boolean fail = true;
		
		JsonNode validNotificationJsonNode = createJsonNotification(validUrls[0], JobStatusType.FINISHED.name(), false);
		
		List<Object[]> testCases = new ArrayList<Object[]>();
		for (Object url: validUrls) {
			for (Object event: validEvents) {
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, event, false)), pass, "Valid notifications should pass" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, event, true)), pass, "Valid notifications should pass" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, event)), pass, "Valid notifications without persistence field should pass" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, event)), pass, "Valid notifications without persistence field should pass" });
				
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, event, false)).add(validNotificationJsonNode), pass, "Valid multiple notifications should pass" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, event, true)).add(validNotificationJsonNode), pass, "Valid multiple notifications should pass" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, event)).add(validNotificationJsonNode), pass, "Valid multiple notifications without persistence field should pass" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, event)).add(validNotificationJsonNode), pass, "Valid multiple notifications without persistence field should pass" });
			}
			
			for (Object invalidEvent: invalidEvents) {
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, invalidEvent, false)), fail, "Invalid notifications event " + invalidEvent + " should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, invalidEvent, true)), fail, "Invalid notifications event " + invalidEvent + " should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, invalidEvent)), fail, "Invalid notifications event " + invalidEvent + " without persistence field should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, invalidEvent)), fail, "Invalid notifications event " + invalidEvent + " without persistence field should fail" });

				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, invalidEvent, false)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalid event " + invalidEvent + " should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, invalidEvent, true)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalid event " + invalidEvent + " should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, invalidEvent)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalid event " + invalidEvent + " without persistence field should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(url, invalidEvent)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalid event " + invalidEvent + " without persistence field should fail" });
			}
		}
		
		for (Object invalidWebhookUrl: invalidWebhookUrls) {
			for (Object event: validEvents) {
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event, false)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " notifications should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event, true)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " notifications should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " notifications without persistence field should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " notifications without persistence field should fail" });

				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event, false)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " notifications should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event, true)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " notifications should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " notifications without persistence field should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, event)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " notifications without persistence field should fail" });
			}
			
			for (Object invalidEvent: invalidEvents) {
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent, false)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent, true)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " without persistence field should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent)), fail, "Invalid notifications invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " without persistence field should fail" });
				
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent, false)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent, true)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " without persistence field should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidWebhookUrl, invalidEvent)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidWebhookUrl " + invalidWebhookUrl + " and event " + invalidEvent + " without persistence field should fail" });
			}
		}
		
		for (Object invalidEmailAddress: invalidEmailAddresses) {
			for (Object event: validEvents) {
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event, false)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " notifications should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event, true)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " notifications should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " notifications without persistence field should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " notifications without persistence field should fail" });
				
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event, false)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " notifications should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event, true)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " notifications should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " notifications without persistence field should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, event)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " notifications without persistence field should fail" });
			}
			
			for (Object invalidEvent: invalidEvents) {
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent, false)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent, true)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " without persistence field should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent)), fail, "Invalid notifications invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " without persistence field should fail" });
				
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent, false)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent, true)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " without persistence field should fail" });
				testCases.add(new Object[]{ mapper.createArrayNode().add(createJsonNotification(invalidEmailAddress, invalidEvent)).add(validNotificationJsonNode), fail, "One valid notification and a second with an invalidEmailAddress " + invalidEmailAddress + " and event " + invalidEvent + " without persistence field should fail" });
			}
		}
		
		return testCases.toArray(new Object[][] {});
	}
	
	/**
	 * Tests job notifications validation on jobs submitted as json
	 * 
	 * @param notificationsJsonArray the array of json objects to be added to the job
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processJsonJobWithNotificationsProvider", dependsOnMethods={"processJsonJobHiddenParametersThrowExceptionWhenProvided"})
	public void processJsonJobWithNotifications(ArrayNode notificationsJsonArray, boolean shouldThrowException, String message) 
	{
		Software software = createSoftware();
		ObjectNode json = createJobJsonNode(software);
		json.putArray("notifications").addAll(notificationsJsonArray);
		Job job = null;
		try {
			try 
			{
				job = JobManager.processJob(json, JSONTestDataUtil.TEST_OWNER, null);
				Assert.assertNotNull(job.getId(), "Job was not saved after processing.");
			} catch (JobProcessingException e) {
				if (!shouldThrowException) {
					Assert.fail(message, e);
				}
			} catch (Exception e) {
				Assert.fail("Unexpected failed to process job", e);
			}
			
			try {
				if (job != null) {
					List<Notification> notifications = new NotificationDao().getActiveUserNotificationsForUuid(job.getOwner(), job.getUuid());
					Assert.assertEquals(notifications.size(), notificationsJsonArray.size(), "Unexpected notification count. Found " + notifications.size() + " expected " + notificationsJsonArray.size());
					
					// this won't correctly check for multiple notifications to the same event
					for (int i=0; i<notificationsJsonArray.size();i++)
					{
						JsonNode notificationJson = notificationsJsonArray.get(i);
						
						notifications = new NotificationDao().getActiveForAssociatedUuidAndEvent(job.getUuid(), notificationJson.get("event").textValue());
						Assert.assertEquals(notifications.size(), 1, "Provided " + notificationJson.get("event").textValue() + " notification not found for job.");
						Assert.assertEquals(notifications.get(0).getCallbackUrl(), notificationJson.get("url").textValue(), 
								"Saved " + notificationJson.get("event").textValue() + " notification had the wrong callback url. Expected " + 
								notificationJson.get("url").textValue() + " found " + notifications.get(0).getCallbackUrl());
						
						if (notificationJson.has("persistent")) {
							Assert.assertEquals(notifications.get(0).isPersistent(), notificationJson.get("persistent").asBoolean(), 
									"Saved " + notificationJson.get("event").textValue() + " notification had the wrong persistent value. Expected " + 
									notificationJson.get("persistent").asBoolean() + " found " + notifications.get(0).isPersistent());
						} else {
							Assert.assertEquals(notifications.get(0).isPersistent(), false, 
									"Saved " + notificationJson.get("event").textValue() + " notification defaulted to the wrong persistent value. Expected " + 
									Boolean.FALSE + " found " + notifications.get(0).isPersistent());
						}
					}	
					
				}
			} catch (Exception e) {
				Assert.fail("Error verifying notifications for job: " + e.getMessage(), e);
			}
		}
		finally {
			try { clearJobs(); } catch (Throwable ignored) {}
		}
		
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
	
	@DataProvider
	public Object[][] processFormJobWithNotificationsProvider()
	{
		ObjectMapper mapper = new ObjectMapper();
		Object[] validUrls = { "http://example.com", "http://foo@example.com", "http://foo:bar@example.com", "http://example.com/job/${JOB_ID}/${JOB_STATUS}", "http://example.com?foo=${JOB_ID}"};
		Object[] invalidWebhookUrls = { "example.com", "example", 1L, mapper.createArrayNode(), mapper.createObjectNode() };
		Object[] validEmailAddresses = { "foo@example.com", "foo+bar@example.com", "foo.bar@example.com" };
		Object[] invalidEmailAddresses = { "@example.com", "@example", "foo@example", "foo@", "@.com", "foo@.com" };
		Object[] emptyUrls = { "", null };
		
		List<Object[]> testCases = new ArrayList<Object[]>();
		for (Object url: validUrls) {
			testCases.add(new Object[] { url, pass, "Valid url " + url + " should pass" });
		}
		
		for (Object url: invalidWebhookUrls) {
			
			testCases.add(new Object[] { url, fail, "Invalid url " + url + " should fail;" });
		}
		
		for (Object url: validEmailAddresses) {
			testCases.add(new Object[] { url, pass, "Valid email address " + url + " should pass" });
		}
		
		for (Object url: invalidEmailAddresses) {
			testCases.add(new Object[] { url, fail, "Invalid email address " + url + " should fail" });
		}
		
		return testCases.toArray(new Object[][] {});
	}
	
	/** 
	 * Tests job notifications validation on jobs submitted as form
	 * 
	 * @param webhookUrlOrEmail the target of the notification
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processFormJobWithNotificationsProvider", dependsOnMethods={"processFormJob"})
	public void processFormJobWithNotifications(Object webhookUrlOrEmail, boolean shouldThrowException, String message) 
	{
		Software software = createSoftware();
		Map<String, Object> jobRequestMap = updateJobRequestMap(createJobRequestMap(software), "notifications", webhookUrlOrEmail);

		Job job = null;
		try {
			try 
			{
				job = JobManager.processJob(jobRequestMap, JSONTestDataUtil.TEST_OWNER, null);
				Assert.assertNotNull(job.getId(), "Job was not saved after processing.");
			} catch (JobProcessingException e) {
				if (!shouldThrowException) {
					Assert.fail(message, e);
				}
			} catch (Throwable e) {
				Assert.fail("Unexpected failed to process job", e);
			}
			
			try {
				if (job != null) 
				{
					List<Notification> notifications = new NotificationDao().getActiveUserNotificationsForUuid(job.getOwner(), job.getUuid());
					Assert.assertEquals(notifications.size(), 2, "Unexpected notification count. Found " + notifications.size() + " expected 2");
					
					notifications = new NotificationDao().getActiveForAssociatedUuidAndEvent(job.getUuid(), JobStatusType.FINISHED.name());
					Assert.assertEquals(notifications.size(), 1, "No FINISHED notification found for job.");
					Assert.assertEquals(notifications.get(0).getCallbackUrl(), webhookUrlOrEmail, 
							"Persisted FINISHED notification had the wrong callback url. Expected " + 
							webhookUrlOrEmail + " found " + notifications.get(0).getCallbackUrl());
					Assert.assertFalse(notifications.get(0).isPersistent(), "Saved FINISHED notification defaulted to the wrong persistent value. "
							+ "Expected " + Boolean.FALSE + " found " + notifications.get(0).isPersistent());
					
					notifications = new NotificationDao().getActiveForAssociatedUuidAndEvent(job.getUuid(), JobStatusType.FAILED.name());
					Assert.assertEquals(notifications.size(), 1, "No FAILED notification found for job.");
					Assert.assertEquals(notifications.get(0).getCallbackUrl(), webhookUrlOrEmail, 
							"Persisted FAILED notification had the wrong callback url. Expected " + 
							webhookUrlOrEmail + " found " + notifications.get(0).getCallbackUrl());
					Assert.assertFalse(notifications.get(0).isPersistent(), "Saved FAILED notification defaulted to the wrong persistent value. "
							+ "Expected " + Boolean.FALSE + " found " + notifications.get(0).isPersistent());
				}
			} catch (Exception e) {
				Assert.fail("Error verifying notifications for job", e);
			}
		}
		finally {
			try { clearJobs(); } catch (Throwable ignored) {}
		}
	}
	
	/** 
	 * Tests empty job notifications validation on jobs submitted as form
	 */
	//@Test(dependsOnMethods={"processFormJobWithNotifications"})
	public void processFormJobWithEmptyNotifications() 
	{
		Job job = null;
		Software software = createSoftware();

		for (String callbackUrl: new String[]{"", null}) 
		{
			Map<String, Object> jobRequestMap = updateJobRequestMap(createJobRequestMap(software), "notifications", callbackUrl);
			
			try 
			{
				job = JobManager.processJob(jobRequestMap, JSONTestDataUtil.TEST_OWNER, null);
				Assert.assertNotNull(job.getId(), "Job was not saved after processing.");
				
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
	
	/** 
	 * Tests job notifications validation on jobs submitted as form
	 * 
	 * @param webhookUrlOrEmail the target of the notification
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processFormJobWithNotificationsProvider", dependsOnMethods={"processFormJobWithEmptyNotifications"})
	public void processFormJobCallbackUrlTest(Object webhookUrlOrEmail, boolean shouldThrowException, String message) 
	{
		Software software = createSoftware();
	    Map<String, Object> jobRequestMap = updateJobRequestMap(createJobRequestMap(software), "notifications", webhookUrlOrEmail);
		genericProcessFormJob(jobRequestMap, fail, "Use of callbackUrl in a job description should fail.");
	}
	
	@DataProvider
	public Object[][] processFormJobInputsProvider()
	{
		Object[][] validTestValues = { 
				{ "path/to/folder", "relative path should pass" },
				{ "folder", "relative file should pass" },
				{ "http://example.com", "HTTP uri schema should pass" },
				{ "https://example.com", "HTTPS uri schema should pass" },
				{ "http://foo:bar@example.com", "HTTP with basic auth uri schema should pass" },
				{ "https://foo:bar@example.com", "HTTP with basic auth uri schema should pass" },
				{ "sftp://testuser:testuser@docker.example.com:10022", "SFTP with basic auth and port should pass" },
				{ "http://example.com;http://example.com", "Semicolon separated lists of inputs are allowed" },
				{ "http://example.com;;http://example.com", "Double semicolons should be ignored and treated as a single semicolon" },
				
				{ "HTTP://example.com", "HTTP uri schema is case insensitive and should pass" },
				{ "HTTPS://example.com", "HTTPS uri schema should pass" },
				{ "HTTP://foo:bar@example.com", "HTTP with basic auth uri schema is case insensitive and should pass" },
				{ "HTTPS://foo:bar@example.com", "HTTP with basic auth uri schema is case insensitive and should pass" },
				{ "SFTP://testuser:testuser@docker.example.com:10022/", "SFTP with basic auth and port should pass" },
		};
		
		Object[][] invalidTestValues = { 		
				// unsupported schemas
				{ "file:///path/to/folder", "FILE uri schema should fail" },
				{ "ftp://example.com", "FTP uri schema should pass" },
				{ "ftp://foo:bar@example.com/", "FTP with basic auth uri schema should pass" },
				{ "ftps://example.com", "FTPS uri schema should pass" },
				{ "ftps://foo:bar@example.com/", "FTPS with basic auth uri schema should pass" },
				{ "gsiftp://example.com", "GSIFTP uri schema should fail" },
				{ "gsiftp://foo:bar@example.com/", "GSIFTP with basic auth uri schema should fail" },
				{ "gridftp://example.com", "gridftp uri schema should fail" },
				{ "gridftp://foo:bar@example.com/", "GRIDFTP with basic auth uri schema should fail" },
				{ "sftp://asasfasdfasdfasdfasdgasdfasdf.asdfasdfa", "SFTP uri should fail without credentials" },
				{ "sftp://foo:bar@asasfasdfasdfasdfasdgasdfasdf.asdfasdfa/", "SFTP uri should fail on invalid hostname" },
				{ "sftp://testuser:testuser@docker.example.com:22", "SFTP uri should fail on invalid host and invalid port" },
				{ "sftp://testuser:testuser@docker.example.com", "SFTP uri should fail on valid host and invalid port" },
				{ "sftp://foo:bar@docker.example.com:10022", "SFTP with basic auth uri schema should fail on invalid credentials" },
				{ "s3://s3.amazon.com/abced", "s3 uri schema should fail" },
				{ "s3://foo:bar@s3.amazon.com/abced", "s3 with basic auth uri schema should fail" },
				{ "agave://example.com", "agave uri schema is case insensitive, but invalid system id and should pass" },
				
				{ "FILE:///path/to/folder", "FILE uri schema should fail" },
				{ "FTP://example.com", "FTP uri schema is case insensitive and should pass" },
				{ "FTP://foo:bar@example.com/", "FTP with basic auth uri schema is case insensitive and should pass" },
				{ "FTPS://example.com", "FTPS uri schema is case insensitive and should pass" },
				{ "FTPS://foo:bar@example.com/", "FTPS with basic auth uri schema is case insensitive and should pass" },
				{ "GSIFTP://example.com", "GSIFTP uri schema is case insensitive and should fail" },
				{ "GSIFTP://foo:bar@example.com/", "GSIFTP with basic auth uri schema is case insensitive and should fail" },
				{ "GRIDFTP://example.com", "gridftp uri schema is case insensitive and should fail" },
				{ "GRIDFTP://foo:bar@example.com/", "GRIDFTP with basic auth uri schema is case insensitive and should fail" },
				{ "SFTP://asasfasdfasdfasdfasdgasdfasdf.asdfasdfa", "SFTP uri should fail without credentials" },
				{ "SFTP://foo:bar@asasfasdfasdfasdfasdgasdfasdf.asdfasdfa/", "SFTP uri should fail on invalid hostname" },
				{ "SFTP://testuser:testuser@docker.example.com:22", "SFTP uri should fail on invalid host and invalid port" },
				{ "SFTP://testuser:testuser@docker.example.com", "SFTP uri should fail on valid host and invalid port" },
				{ "SFTP://foo:bar@docker.example.com:10022", "SFTP with basic auth uri schema should fail on invalid credentials" },
				{ "S3://s3.amazon.com/abced", "s3 uri schema is case insensitive and should fail" },
				{ "S3://foo:bar@s3.amazon.com/abced", "s3 with basic auth uri schema is case insensitive and should fail" },
				{ "AGAVE://example.com", "agave uri schema is case insensitive, but invalid system id and should pass" },
				{ "abba://example.com", "Unknown uri schema should fail" },
				{ "ABBA://example.com", "Unknown uri schema is case insensitive and should fail" },
		};
		
		List<Object[]> testData = new ArrayList<Object[]>();
		String inputKey = "testinputkey";
		String defaultValue = "http://example.com";
		
		testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		false, 		false), mapper.createObjectNode().put(inputKey, "path/to/folder"), 	null										  							, fail, "Hidden input should fail if user provides value" });
		testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		true, 		false), mapper.createObjectNode().put(inputKey, "path/to/folder"), 	null										  							, fail, "Hidden input should fail if user provides value" });
		testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		true, 		false), null, 													  				mapper.createObjectNode().put(inputKey, defaultValue)						, pass, "Hidden input should use default." });
		testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		false, 		false), null, 													  				mapper.createObjectNode().put(inputKey, defaultValue)						, pass, "Hidden input should use default." });
		
		testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		false, 		true), null, 													  				null																		, pass, "Visible input not required should not use default when no user data given" });
		testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		true, 		true), null, 													  				mapper.createObjectNode().put(inputKey, defaultValue)						, fail, "Required input should fail if not supplied" });
		testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		false, 		true), mapper.createObjectNode().put(inputKey, "path/to/folder"),  	mapper.createObjectNode().put(inputKey, "path/to/folder")	, pass, "User supplied value should be used for visible optional input values" });
		testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		true, 		true), mapper.createObjectNode().put(inputKey, "path/to/folder"),  	mapper.createObjectNode().put(inputKey, "path/to/folder")	, pass, "User supplied value should be used for visible required input values" });
		
		testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		true, 		true), mapper.createObjectNode().putNull(inputKey),  							null																		, fail, "Visible required input should fail when null user input value given" });
		testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		false, 		true), mapper.createObjectNode().putNull(inputKey),  							null																		, fail, "Visible optional input should fail when null user input value given" });
		
		for (Object[] validTestValue: validTestValues) 
		{
			testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		false, 		true), mapper.createObjectNode().put(inputKey, (String)validTestValue[0]),  	mapper.createObjectNode().put(inputKey, (String)validTestValue[0])	, pass, (String)validTestValue[1] });
			testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		true, 		true), mapper.createObjectNode().put(inputKey, (String)validTestValue[0]),  	mapper.createObjectNode().put(inputKey, (String)validTestValue[0])	, pass, (String)validTestValue[1] });
		}
		
		for (Object[] invalidTestValue: invalidTestValues) 
		{
			testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		false, 		true), mapper.createObjectNode().put(inputKey, (String)invalidTestValue[0]),  	null	, fail, (String)invalidTestValue[1] });
			testData.add(new Object[] { createInput(inputKey, defaultValue, 	null, 		true, 		true), mapper.createObjectNode().put(inputKey, (String)invalidTestValue[0]),  	null	, fail, (String)invalidTestValue[1] });
		}
		
		return testData.toArray(new Object[][] {});
	}
	
	/**
	 * Tests job app input validation on jobs submitted as form
	 * 
	 * @param jobInputs
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processFormJobInputsProvider", dependsOnMethods={"processFormJobCallbackUrlTest"})
	public void processFormJobInputs(SoftwareInput softwareInput, ObjectNode jobInputs, ObjectNode expectedInputs, boolean shouldThrowException, String message) 
	{
	    Software software = null;
		try {
		    software = createSoftware();
		    software.setInputs(new ArrayList<SoftwareInput>());
			
		    if (softwareInput != null) {
			    software.addInput(softwareInput);
			}
			
			SoftwareDao.persist(software);
			
			Map<String, Object> jobRequestMap = createJobRequestMap(software);
	        
//			for (SoftwareInput input: software.getInputs()) {
//			    jobRequestMap.remove(input.getKey());
//			}
			jobRequestMap = updateJobRequestMap(jobRequestMap, "appId", software.getUniqueName());
			
			if (jobInputs != null) {
				for (Iterator<String> inputKeys = jobInputs.fieldNames(); inputKeys.hasNext();){
					String key = inputKeys.next();
					jobRequestMap = updateJobRequestMap(jobRequestMap, key, jobInputs.get(key).textValue());
				}
			}
			
			Job job = JobManager.processJob(jobRequestMap, JSONTestDataUtil.TEST_OWNER, null);
			
			Assert.assertNotNull(job.getId(), "Job was not saved after processing.");
			
			if (expectedInputs != null) {
				for (Iterator<String> fieldNamesIterator = expectedInputs.fieldNames(); fieldNamesIterator.hasNext();)
				{
					String fieldName = fieldNamesIterator.next();
					Assert.assertTrue(job.getInputsAsJsonObject().has(fieldName), message);
					String foundValue = job.getInputsAsJsonObject().get(fieldName).asText();
					String expectedValue = expectedInputs.get(fieldName).textValue();
					Assert.assertEquals(foundValue, expectedValue, 
							"Unexpected value for field " + fieldName + " found. Expected " + expectedValue + 
							" found " + foundValue);
				}
			}
			else
			{
				Assert.assertEquals(job.getInputsAsJsonObject().size(), 0, message);
			}
		} catch (JobProcessingException e) {
			if (!shouldThrowException) {
				Assert.fail(message, e);
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("Failed to process job", e);
		}
	}

	@DataProvider
	public Object[][] processFormJobInputsAcceptsCaseInsensitiveAgaveURLProvider() {
		return new Object[][]{
				{"agave", "Lower case Agave uri schema should pass"},
				{"AGAVE", "Upper case Agave uri schema should pass"},
				{"AGaVe", "Mixed case Agave uri schema should pass"}
		};
	}

	/**
	 * Tests job app input validation on jobs submitted as form
	 *
	 * @param jobInputs
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processFormJobInputsAcceptsCaseInsensitiveAgaveURLProvider", dependsOnMethods={"processFormJobInputs"})
	public void processFormJobInputsAcceptsCaseInsensitiveAgaveURL(SoftwareInput softwareInput, ObjectNode jobInputs, ObjectNode expectedInputs, boolean shouldThrowException, String message)
	{
		Software software = null;
		try {
			software = createSoftware();
			software.setInputs(new ArrayList<SoftwareInput>());

			if (softwareInput != null) {
				software.addInput(softwareInput);
			}

			SoftwareDao.persist(software);

			Map<String, Object> jobRequestMap = createJobRequestMap(software);

//			for (SoftwareInput input: software.getInputs()) {
//			    jobRequestMap.remove(input.getKey());
//			}
			jobRequestMap = updateJobRequestMap(jobRequestMap, "appId", software.getUniqueName());

			if (jobInputs != null) {
				for (Iterator<String> inputKeys = jobInputs.fieldNames(); inputKeys.hasNext();){
					String key = inputKeys.next();
					jobRequestMap = updateJobRequestMap(jobRequestMap, key, jobInputs.get(key).textValue());
				}
			}

			Job job = JobManager.processJob(jobRequestMap, JSONTestDataUtil.TEST_OWNER, null);

			Assert.assertNotNull(job.getId(), "Job was not saved after processing.");

			if (expectedInputs != null) {
				for (Iterator<String> fieldNamesIterator = expectedInputs.fieldNames(); fieldNamesIterator.hasNext();)
				{
					String fieldName = fieldNamesIterator.next();
					Assert.assertTrue(job.getInputsAsJsonObject().has(fieldName), message);
					String foundValue = job.getInputsAsJsonObject().get(fieldName).asText();
					String expectedValue = expectedInputs.get(fieldName).textValue();
					Assert.assertEquals(foundValue, expectedValue,
							"Unexpected value for field " + fieldName + " found. Expected " + expectedValue +
									" found " + foundValue);
				}
			}
			else
			{
				Assert.assertEquals(job.getInputsAsJsonObject().size(), 0, message);
			}
		} catch (Exception e) {
			Assert.fail("Failed to process job", e);
		}
	}

	/**
	 * Tests job app parameter validation on jobs submitted as form
	 * @param appParameter the software parameter under test
	 * @param jobParameters the parameter to assign to the job
	 * @param expectedParameters the expected parameter to be present in the processed job
	 * @param shouldThrowException true if processing should throw an exception
	 * @param message the message to assert for a failed test
	 */
	//@Test(dataProvider = "processJsonJobParametersProvider", dependsOnMethods={"processFormJobInputs"})
	public void processFormJobParameters(SoftwareParameter appParameter, ObjectNode jobParameters, ObjectNode expectedParameters, boolean shouldThrowException, String message) 
	{
		Software testSoftware = null;
		
		try {
			testSoftware = createSoftware();
			testSoftware.getParameters().clear();
			SoftwareDao.persist(testSoftware);
			if (appParameter != null) {
				testSoftware.addParameter(appParameter);
			}
			SoftwareDao.persist(testSoftware);

			Map<String, Object> jobRequestMap = createJobRequestMap(testSoftware);
//			for (SoftwareParameter parameter: software.getParameters()) {
//			    jobRequestMap.remove(parameter.getKey());
//			}
			
			if (jobParameters != null) {
				for (Iterator<String> inputKeys = jobParameters.fieldNames(); inputKeys.hasNext();){
					String key = inputKeys.next();
					jobRequestMap = updateJobRequestMap(jobRequestMap, key, jobParameters.get(key).textValue());
				}
			}
			
			Job job = JobManager.processJob(jobRequestMap, JSONTestDataUtil.TEST_OWNER, null);
			
			Assert.assertNotNull(job.getId(), "Job was not saved after processing.");
			
			if (expectedParameters != null) {
				Iterator<String> fieldNamesIterator = expectedParameters.fieldNames();
				while (fieldNamesIterator.hasNext()) {
					String fieldName = fieldNamesIterator.next();
					Assert.assertTrue(job.getParametersAsJsonObject().has(fieldName), message);
					assert appParameter != null;
					String foundParameter = job.getParametersAsJsonObject().get(appParameter.getKey()).asText();
					String expectedParameter = expectedParameters.get(fieldName).asText();
					if (appParameter.getType().equals(SoftwareParameterType.number)) {
						foundParameter = new Double(foundParameter).toString();
						expectedParameter = new Double(expectedParameter).toString();
					} 
					Assert.assertEquals(foundParameter, expectedParameter, 
							"Unexpected value for field " + fieldName + " found. Expected " + expectedParameter + 
							" found " + foundParameter);
				}
			} else {
				Assert.assertEquals(job.getParametersAsJsonObject().size(), 0, message);
			}
		} catch (JobProcessingException e) {
			if (!shouldThrowException) {
				Assert.fail(message, e);
			}
		} catch (Exception e) {
			Assert.fail("Failed to process job", e);
		}
		finally {
			try { SoftwareDao.delete(testSoftware); } catch (Throwable ignored) {}
			try { clearJobs(); } catch (Throwable ignored) {}
		}
	}
	
	@DataProvider
	public Object[][] processJsonJobBatchQueueParametersProvider()
	{
		ExecutionSystem system = new ExecutionSystem();
		// name maxJobs userJobs nodes memory, procs, time, cstm, default
		
		BatchQueue queueDefault = new BatchQueue("queueDefault", (long) 1, (long) 1, (long) 1, (double) 1.0, (long) 1, "01:00:00", null, true);
		BatchQueue queueTwo = new BatchQueue("queueTwo", (long) 2, (long) 2, (long) 2, (double) 2.0, (long) 2, "02:00:00", null, false);
		BatchQueue queueTen = new BatchQueue("queueTen", (long) 10, (long) 10, (long) 10, (double) 10, (long) 10, "10:00:00", null, false);
		BatchQueue queueHundred = new BatchQueue("queueHundred", (long) 100, (long) 100, (long) 100, (double) 100, (long) 100, "100:00:00", null, false);
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
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			(long)2, 				null, 		null, 		null,     	null, 			null, 			null, 		null, 		null,     	null,						fail, "Default queue only, out of bounds job nodes fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					(double)2, 	null, 		null, 		null,			null, 			null, 		null, 		null, 		null,						fail, "Default queue only, out of bounds job memory fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					null, 		"02:00:00", null,  		null, 			null, 			null, 		null, 		null, 		null,						fail, "Default queue only, out of bounds job time fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					null,		null, 		(long)2,    null, 			null, 			null, 		null, 		null, 		null,						fail, "Default queue only, out of bounds job procs fails" });
		
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					null, 		null, 		null, 		null, 			(long)2, 		null, 		null, 		null,     	null, 						fail, "Default queue only, out of bounds app default nodes fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					null, 		null, 		null,		null, 		 	null, 			(double)2, 	null, 		null, 		null, 						fail, "Default queue only, out of bounds app default memory fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					null,		null, 		null, 		null, 			null, 			null, 		"02:00:00", null,  		null, 						fail, "Default queue only, out of bounds app default time fails" });
		testData.add(new Object[]{ new BatchQueue[] { queueDefault }, null, 			null, 					null, 		null, 		null, 		null, 		 	null, 			null,		null, 		(long)2,    null, 						fail, "Default queue only, out of bounds app default procs fails" });
		
		// no app defaults, job exceeds max queue
		testData.add(new Object[]{ allQueues, 						  null, 			(long)101, 				null, 		null, 		null,     	null, 			null, 			null, 		null, 		null,     	null,						fail, "All queues, out of bounds nodes fails" });
		testData.add(new Object[]{ allQueues, 						  null, 			null, 					(double)101,null, 		null, 		null,			null, 			null, 		null, 		null, 		null,						fail, "All queues, out of bounds memory fails" });
		testData.add(new Object[]{ allQueues, 						  null, 			null, 					null, 		"101:00:00",null,  		null, 			null, 			null, 		null, 		null, 		null,						fail, "All queues, out of bounds time fails" });
		testData.add(new Object[]{ allQueues, 						  null, 			null, 					null,		null, 		(long)101,  null, 			null, 			null, 		null, 		null, 		null,						fail, "All queues, out of bounds procs fails" });
		
		// no job specs, app defaults exceeds max queue
		testData.add(new Object[]{ allQueues,  						  null, 			null,  					null,		null, 		null, 		null, 			(long)101, 		null, 		null, 		null,     	null,					 	fail, "All queues, out of bounds app default nodes fails" });
		testData.add(new Object[]{ allQueues,  						  null, 			null,  					null,		null, 		null, 		null, 			null, 			(double)101,null, 		null, 		null, 						fail, "All queues, out of bounds app default memory fails" });
		testData.add(new Object[]{ allQueues,  						  null, 			null,  					null,		null, 		null, 		null, 			null, 			null, 		"101:00:00",null,  		null, 						fail, "All queues, out of bounds app default time fails" });
		testData.add(new Object[]{ allQueues,  						  null, 			null, 					null,		null, 		null,		null, 			null,			null, 		null, 		(long)101,  null, 						fail, "All queues, out of bounds app default procs fails" });
		
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
	//@Test(dataProvider = "processJsonJobBatchQueueParametersProvider", dependsOnMethods={"processFormJobInputs"})
	public void processJsonJobBatchQueueParameters(BatchQueue[] batchQueues, 
			String jobQueue, Long jobNodes, Double jobMemory, String jobRequestedTime, Long jobProcessors, 
			String appQueue, Long appNodes, Double appMemory, String appRequestedTime, Long appProcessors,
			String expectedJobQueueName, boolean shouldThrowException, String message) 
	{
		Software testSoftware = null;
		ExecutionSystem testSystem = null;
		SystemDao systemDao = new SystemDao();
		try 
		{
			testSoftware = createSoftware();
			testSoftware.setDefaultQueue(appQueue);
			testSoftware.setDefaultNodes(appNodes);
			testSoftware.setDefaultMemoryPerNode(appMemory);
			testSoftware.setDefaultProcessorsPerNode(appProcessors);
			testSoftware.setDefaultMaxRunTime(appRequestedTime);
			SoftwareDao.persist(testSoftware);

			testSystem = testSoftware.getExecutionSystem();
			testSystem.getUsersUsingAsDefault().add(testSystem.getOwner());
			testSystem.getBatchQueues().clear();
			systemDao.persist(testSystem);
			for (BatchQueue testQueue: batchQueues) {
				testSystem.addBatchQueue(testQueue);
			}
			systemDao.persist(testSystem);

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
			
			Job job = JobManager.processJob(json, JSONTestDataUtil.TEST_OWNER, null);
			
			Assert.assertNotNull(job.getId(), "Job was not saved after processing.");
			
			Assert.assertEquals(expectedJobQueueName, job.getBatchQueue(), 
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
