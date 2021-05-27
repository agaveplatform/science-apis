package org.iplantc.service.monitor.model;

import org.iplantc.service.monitor.TestDataHelper;
import org.iplantc.service.monitor.dao.MonitorDao;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.SystemRole;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

import static org.iplantc.service.monitor.TestDataHelper.*;

@Test(groups={"unit"})
public class MonitorTest {

	protected static final String TEST_USER = "systest";

	protected MonitorDao dao = new MonitorDao();
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
	
	@Test(groups={"unit"})
	public void constructMonitor()
	{
		Monitor monitor = new Monitor();
		Assert.assertNotNull(monitor.getUuid(), "UUID not set on instantiation.");
		Assert.assertNotNull(monitor.getTenantId(), "Tenant id not set on instantiation.");
		Assert.assertNotNull(monitor.getCreated(), "Creation date not set on instantiation.");
		Assert.assertNotNull(monitor.getLastUpdated(), "Last updated date not set on instantiation.");
		Assert.assertNotNull(monitor.getNextUpdateTime(), "Next updated date not set on instantiation.");
	}

	@DataProvider(name="initMonitorStringIntStringProvider")
	private Object[][] initMonitorStringIntStringProvider()
	{
		return new Object[][] {
				{ privateStorageSystem, "Valid private storage system should be accepted", false },
				{ publicStorageSystem, "Valid public storage system should be accepted", false },
				{ privateExecutionSystem, "Valid private execution system should be accepted", false },
				{ publicExecutionSystem, "Valid public execution system should be accepted", false },
				{ sharedExecutionSystem, "Valid shared execution system should be accepted", false },
				{ null, "null system should throw an exception", true }
		};
	}
	
	@Test(dataProvider="initMonitorStringIntStringProvider")
	public void initMonitorStringIntString(RemoteSystem system, String message, boolean shouldThrowException) 
	{
		try 
		{
			Monitor notif = new Monitor(system, 5, TEST_USER);
			Assert.assertNotNull(notif.getUuid(), "UUID not set on instantiation.");
			Assert.assertNotNull(notif.getCreated(), "Creation date not set on instantiation.");
			Assert.assertNotNull(notif.getLastUpdated(), "Last updated date not set on instantiation.");
		} 
		catch (Exception e) 
		{
			if (!shouldThrowException) {
				Assert.fail(message, e);
			}
		}
	}
	
	@DataProvider(name="setMonitorFrequencyTestProvider")
	private Object[][] setMonitorFrequencyTestProvider()
	{
		return new Object[][] {
				{ 0, "0 frequency should throw exception", true },
				{ 4, "4 frequency should throw exception", true },
				{ -1, "-1 frequency should throw exception", true },
				{ 5, "5 or greater frequency should be accepted", false },
				{ 6, "5 or greater frequency should throw exception", false }
		};
	}
	
	@Test(dataProvider="setMonitorFrequencyTestProvider")
	public void setMonitorFrequencyTest(int frequency, String message, boolean shouldThrowException) 
	{
		try 
		{
			new Monitor(publicStorageSystem, frequency, TEST_USER);
		} 
		catch (Exception e) 
		{
			if (!shouldThrowException) {
				Assert.fail(message, e);
			}
		}
	}
}
