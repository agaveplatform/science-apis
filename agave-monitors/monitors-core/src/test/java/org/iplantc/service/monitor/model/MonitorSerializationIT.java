package org.iplantc.service.monitor.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iplantc.service.monitor.TestDataHelper;
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
public class MonitorSerializationIT {

	protected static final String TEST_USER = "systest";

	protected ObjectMapper mapper = new ObjectMapper();
	protected MonitorDao dao = new MonitorDao();
	protected TestDataHelper dataHelper;
	protected SystemDao systemDao = new SystemDao();
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
//		SystemDao dao = Mockito.mock(SystemDao.class);
//		Mockito.when(dao.findBySystemId(Mockito.anyString()))
//				.thenReturn(system);

		dataHelper = TestDataHelper.getInstance();

//		super.beforeClass();
		privateStorageSystem = StorageSystem.fromJSON( dataHelper.getTestDataObjectAsJSONObject(
				TEST_STORAGE_SYSTEM_FILE));
		privateStorageSystem.setOwner(TEST_USER);
		systemDao.persist(privateStorageSystem);

		publicStorageSystem = StorageSystem.fromJSON( dataHelper.getTestDataObjectAsJSONObject(
				TEST_STORAGE_SYSTEM_FILE));
		publicStorageSystem.setOwner(TEST_USER);
		publicStorageSystem.setPubliclyAvailable(true);
		publicStorageSystem.setGlobalDefault(true);
		publicStorageSystem.setSystemId(publicStorageSystem.getSystemId() + ".public");
		systemDao.persist(publicStorageSystem);

		privateExecutionSystem = ExecutionSystem.fromJSON( dataHelper.getTestDataObjectAsJSONObject(
				TEST_EXECUTION_SYSTEM_FILE));
		privateExecutionSystem.setOwner(TEST_USER);
		systemDao.persist(privateExecutionSystem);

		publicExecutionSystem = ExecutionSystem.fromJSON( dataHelper.getTestDataObjectAsJSONObject(
				TEST_EXECUTION_SYSTEM_FILE));
		publicExecutionSystem.setOwner(TEST_USER);
		publicExecutionSystem.setPubliclyAvailable(true);
		publicExecutionSystem.setGlobalDefault(true);
		publicExecutionSystem.setSystemId(publicExecutionSystem.getSystemId() + ".public");
		systemDao.persist(publicExecutionSystem);

		sharedExecutionSystem = ExecutionSystem.fromJSON( dataHelper.getTestDataObjectAsJSONObject(
				TEST_EXECUTION_SYSTEM_FILE));
		sharedExecutionSystem.setOwner(TEST_USER);
		sharedExecutionSystem.setSystemId(sharedExecutionSystem.getSystemId() + ".shared");
		systemDao.persist(sharedExecutionSystem);
		sharedExecutionSystem.addRole(new SystemRole(SYSTEM_SHARE_USER, RoleType.ADMIN));
		systemDao.persist(sharedExecutionSystem);

		sharedStorageSystem = StorageSystem.fromJSON( dataHelper.getTestDataObjectAsJSONObject(
				TEST_STORAGE_SYSTEM_FILE));
		sharedStorageSystem.setOwner(TEST_USER);
		sharedStorageSystem.setSystemId(sharedStorageSystem.getSystemId() + ".shared");
		systemDao.persist(sharedStorageSystem);
		sharedStorageSystem.addRole(new SystemRole(SYSTEM_SHARE_USER, RoleType.ADMIN));
		systemDao.persist(sharedStorageSystem);
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
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).set("target", mapper.createObjectNode()), nullSystem, "Object for target should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).set("target", mapper.createArrayNode()), nullSystem, "Array for target should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", 5), nullSystem, "Integer for target should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", 5.5), nullSystem, "Decimal for target should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", publicStorageSystem.getSystemId()), publicStorageSystem, "Public storage system should not throw an exception", false },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", publicExecutionSystem.getSystemId()), publicExecutionSystem, "Public execution system should not throw an exception", false },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", privateExecutionSystem.getSystemId()), privateExecutionSystem, "Private execution system should not throw an exception", false },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", privateStorageSystem.getSystemId()), privateStorageSystem, "Private storage system should not throw an exception", false },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("target", sharedExecutionSystem.getSystemId()), sharedExecutionSystem, "Shared execution system should not throw an exception", false },


			{ jsonExecutionMonitorNoFrequency, nullSystem, "Missing frequency should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", ""), nullSystem, "Empty frequency should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).set("frequency", mapper.createObjectNode()), nullSystem, "Object for frequency should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).set("frequency", mapper.createArrayNode()), nullSystem, "Array for frequency should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", 5), nullSystem, "Integer for frequency should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("frequency", 5.5), nullSystem, "Decimal for frequency should throw exception", true },

			{ jsonExecutionMonitorNoUpdateSystemStatus, nullSystem, "Missing updateSystemStatus should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", ""), nullSystem, "Empty updateSystemStatus should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).set("updateSystemStatus", mapper.createObjectNode()), nullSystem, "Object for updateSystemStatus should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).set("updateSystemStatus", mapper.createArrayNode()), nullSystem, "Array for updateSystemStatus should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", 5), nullSystem, "Integer for updateSystemStatus should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("updateSystemStatus", 5.5), nullSystem, "Decimal for updateSystemStatus should throw exception", true },

			{ jsonExecutionMonitorNoInternalUsername, nullSystem, "Missing internalUsername should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).put("internalUsername", ""), nullSystem, "Empty internalUsername should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).set("internalUsername", mapper.createObjectNode()), nullSystem, "Object for internalUsername should throw exception", true },
			{ ((ObjectNode)dataHelper.getTestDataObject(TestDataHelper.TEST_EXECUTION_MONITOR)).set("internalUsername", mapper.createArrayNode()), nullSystem, "Array for internalUsername should throw exception", true },
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
