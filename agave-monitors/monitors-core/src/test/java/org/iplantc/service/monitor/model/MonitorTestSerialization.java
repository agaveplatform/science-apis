package org.iplantc.service.monitor.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iplantc.service.monitor.TestDataHelper;
import org.iplantc.service.monitor.dao.MonitorCheckDao;
import org.iplantc.service.monitor.dao.MonitorDao;
import org.iplantc.service.monitor.exceptions.MonitorException;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.SystemRole;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.json.JSONException;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.iplantc.service.monitor.TestDataHelper.*;

@Test(groups = {"integration"})
public class MonitorTestSerialization {

	protected static final String TEST_USER = "systest";
	protected static final String TEST_EMAIL = "help@agaveplatform.org";
	protected static final String TEST_URL = "http://requestb.in/11pbi6m1?username=${USERNAME}&status=${STATUS}";

	protected ObjectMapper mapper = new ObjectMapper();
	protected SystemDao systemDao = new SystemDao();
	protected MonitorDao dao = new MonitorDao();
	protected MonitorCheckDao checkDao = new MonitorCheckDao();
	protected TestDataHelper dataHelper;
	protected StorageSystem publicStorageSystem;
	protected StorageSystem privateStorageSystem;
	protected StorageSystem sharedStorageSystem;
	protected ExecutionSystem publicExecutionSystem;
	protected ExecutionSystem privateExecutionSystem;
	protected ExecutionSystem sharedExecutionSystem;

	@ObjectFactory
	public IObjectFactory getObjectFactory() {
		return new org.powermock.modules.testng.PowerMockObjectFactory();
	}

	@BeforeClass
	protected void beforeClass() throws Exception
	{
		dataHelper = TestDataHelper.getInstance();

//		super.beforeClass();
		privateStorageSystem = StorageSystem.fromJSON( dataHelper.getTestDataObjectAsJSONObject(
				TEST_STORAGE_SYSTEM_FILE));
		privateStorageSystem.setOwner(TEST_USER);
		privateStorageSystem.setId((long)1);

		publicStorageSystem = StorageSystem.fromJSON( dataHelper.getTestDataObjectAsJSONObject(
				TEST_STORAGE_SYSTEM_FILE));
		publicStorageSystem.setOwner(TEST_USER);
		publicStorageSystem.setPubliclyAvailable(true);
		publicStorageSystem.setGlobalDefault(true);
		publicStorageSystem.setSystemId(publicStorageSystem.getSystemId() + ".public");
		publicStorageSystem.setId((long)2);

		privateExecutionSystem = ExecutionSystem.fromJSON( dataHelper.getTestDataObjectAsJSONObject(
				TEST_EXECUTION_SYSTEM_FILE));
		privateExecutionSystem.setOwner(TEST_USER);
		privateExecutionSystem.setId((long)3);

		publicExecutionSystem = ExecutionSystem.fromJSON( dataHelper.getTestDataObjectAsJSONObject(
				TEST_EXECUTION_SYSTEM_FILE));
		publicExecutionSystem.setOwner(TEST_USER);
		publicExecutionSystem.setPubliclyAvailable(true);
		publicExecutionSystem.setGlobalDefault(true);
		publicExecutionSystem.setSystemId(publicExecutionSystem.getSystemId() + ".public");
		publicExecutionSystem.setId((long)4);

		sharedExecutionSystem = ExecutionSystem.fromJSON( dataHelper.getTestDataObjectAsJSONObject(
				TEST_EXECUTION_SYSTEM_FILE));
		sharedExecutionSystem.setOwner(TEST_USER);
		sharedExecutionSystem.getRoles().add(new SystemRole(SYSTEM_SHARE_USER, RoleType.ADMIN));
		sharedExecutionSystem.setSystemId(sharedExecutionSystem.getSystemId() + ".shared");
		sharedExecutionSystem.setId((long)5);

		sharedStorageSystem = StorageSystem.fromJSON( dataHelper.getTestDataObjectAsJSONObject(
				TEST_STORAGE_SYSTEM_FILE));
		sharedStorageSystem.setOwner(TEST_USER);
		sharedStorageSystem.getRoles().add(new SystemRole(SYSTEM_SHARE_USER, RoleType.ADMIN));
		sharedStorageSystem.setSystemId(sharedStorageSystem.getSystemId() + ".shared");
		sharedStorageSystem.setId((long)6);
	}
	
	@DataProvider(name="fromJSONProvider")
	private Object[][] fromJSONProvider() throws JSONException, IOException
	{
		ObjectNode jsonExecutionMonitorNoSystem = (ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR);
		ObjectNode jsonExecutionMonitorNoFrequency = (ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR);
		ObjectNode jsonExecutionMonitorNoUpdateSystemStatus = (ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR);
		ObjectNode jsonExecutionMonitorNoInternalUsername = (ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR);
		jsonExecutionMonitorNoSystem.remove("target");
		jsonExecutionMonitorNoFrequency.remove("system");
		jsonExecutionMonitorNoUpdateSystemStatus.remove("updateSystemStatus");
		jsonExecutionMonitorNoInternalUsername.remove("internalUsername");
		RemoteSystem nullSystem = null;

		return new Object[][] {
			{ dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR), privateExecutionSystem, "Valid monitor json should parse", false },
			{ jsonExecutionMonitorNoSystem, nullSystem, "Missing system should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", ""), nullSystem, "Empty target should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", mapper.createObjectNode()), nullSystem, "Object for target should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", mapper.createArrayNode()), nullSystem, "Array for target should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", 5), nullSystem, "Integer for target should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", 5.5), nullSystem, "Decimal for target should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", publicStorageSystem.getSystemId()), publicStorageSystem, "Public storage system should not throw an exception", false },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", publicExecutionSystem.getSystemId()), publicExecutionSystem, "Public execution system should not throw an exception", false },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", privateExecutionSystem.getSystemId()), privateExecutionSystem, "Private execution system should not throw an exception", false },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", privateStorageSystem.getSystemId()), privateStorageSystem, "Private storage system should not throw an exception", false },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", sharedExecutionSystem.getSystemId()), sharedExecutionSystem, "Shared execution system should not throw an exception", false },


			{ jsonExecutionMonitorNoFrequency, nullSystem, "Missing frequency should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", ""), nullSystem, "Empty frequency should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", mapper.createObjectNode()), nullSystem, "Object for frequency should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", mapper.createArrayNode()), nullSystem, "Array for frequency should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", 5), nullSystem, "Integer for frequency should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", 5.5), nullSystem, "Decimal for frequency should throw exception", true },

			{ jsonExecutionMonitorNoUpdateSystemStatus, nullSystem, "Missing updateSystemStatus should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", ""), nullSystem, "Empty updateSystemStatus should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", mapper.createObjectNode()), nullSystem, "Object for updateSystemStatus should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", mapper.createArrayNode()), nullSystem, "Array for updateSystemStatus should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", 5), nullSystem, "Integer for updateSystemStatus should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", 5.5), nullSystem, "Decimal for updateSystemStatus should throw exception", true },

			{ jsonExecutionMonitorNoInternalUsername, nullSystem, "Missing internalUsername should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("internalUsername", ""), nullSystem, "Empty internalUsername should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("internalUsername", mapper.createObjectNode()), nullSystem, "Object for internalUsername should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("internalUsername", mapper.createArrayNode()), nullSystem, "Array for internalUsername should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("internalUsername", 5), nullSystem, "Integer for internalUsername should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("internalUsername", 5.5), nullSystem, "Decimal for internalUsername should throw exception", true },

		};
	}

	@Test(dataProvider="fromJSONProvider")
	public void fromJSON(JsonNode json, RemoteSystem system, String message, boolean shouldThrowException)
	{
		try
		{
			Monitor.fromJSON(json, null, TEST_USER);
		}
		catch (MonitorException e)
		{
			if (!shouldThrowException) {
				Assert.fail(message, e);
			}
		}
		catch (Exception e)
		{
			Assert.fail(message, e);
		}
	}

	@DataProvider(name="permissionTestProvider")
	private Object[][] permissionTestProvider() throws JSONException, IOException
	{
		return new Object[][] {
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", publicStorageSystem.getSystemId()), publicStorageSystem, "Public storage system should not throw an exception", false },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", publicExecutionSystem.getSystemId()), publicExecutionSystem, "Public execution system should not throw an exception", false },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", privateExecutionSystem.getSystemId()), privateExecutionSystem, "Private execution system user does not have a role on should not throw an exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", privateStorageSystem.getSystemId()), privateStorageSystem, "Private storage system user does not have a role on should throw an exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", sharedExecutionSystem.getSystemId()), sharedExecutionSystem, "Shared storage system user has a role on should not throw an exception", false },
		};
	}

	@Test(dataProvider="permissionTestProvider")
	public void permissionTest(JsonNode json, RemoteSystem system, String message, boolean shouldThrowException)
	{
		try
		{
			Monitor.fromJSON(json, null, TestDataHelper.SYSTEM_SHARE_USER);
		}
		catch (MonitorException e)
		{
			if (!shouldThrowException) {
				Assert.fail(message, e);
			}
		}
		catch (Exception e)
		{
			Assert.fail(message, e);
		}
	}
}
