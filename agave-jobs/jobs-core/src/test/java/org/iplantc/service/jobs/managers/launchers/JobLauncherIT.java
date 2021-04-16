package org.iplantc.service.jobs.managers.launchers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareInput;
import org.iplantc.service.apps.model.SoftwareParameter;
import org.iplantc.service.common.Settings;
import org.iplantc.service.jobs.dao.JobDao;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.jobs.submission.AbstractJobSubmissionTest;
import org.iplantc.service.jobs.util.Slug;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.enumerations.ExecutionType;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.systems.model.enumerations.SchedulerType;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

@Test(groups={"integration"})
public class JobLauncherIT extends AbstractJobSubmissionTest {

	private final ExecutionType executionType;
	private String remoteFilePath;
	protected Software software;
	protected SchedulerType schedulerType;
	protected ObjectMapper mapper = new ObjectMapper();
	
	public JobLauncherIT(ExecutionType executionType, SchedulerType schedulerType) {
		this.schedulerType = schedulerType;
		this.executionType = executionType;
	}

	@BeforeClass
	@Override
	protected void beforeClass() throws Exception {
		super.beforeClass();
	}

	@AfterClass
	@Override
	protected void afterClass() throws Exception {
		super.afterClass();
	}

	@Override
	protected void initSystems() throws Exception {
	    storageSystem = (StorageSystem) getNewInstanceOfRemoteSystem(RemoteSystemType.STORAGE, 
	    		getStorageSystemProtocolType().name().toLowerCase());
        storageSystem.setOwner(SYSTEM_OWNER);
        storageSystem.setPubliclyAvailable(true);
        storageSystem.setGlobalDefault(true);
        storageSystem.getUsersUsingAsDefault().add(SYSTEM_OWNER);
        systemDao.persist(storageSystem);
        
        executionSystem = (ExecutionSystem)getNewInstanceOfRemoteSystem(RemoteSystemType.EXECUTION, 
        		getSchedulerType().name().toLowerCase());
        executionSystem.setOwner(SYSTEM_OWNER);
        executionSystem.setPubliclyAvailable(true);
        executionSystem.setType(RemoteSystemType.EXECUTION);
        systemDao.persist(executionSystem);
    }

	/**
	 * The type of scheduler to we'll use in this test. The 
	 * {@link SchedulerType} maps to a config file named 
	 * as systems/execution/{@link SchedulerType#name()}.example.com.json
	 * @return the scheduler to use for the storage system
	 */
	protected SchedulerType getSchedulerType() {
		return this.schedulerType;
	}
	
	/**
	 * Specifies the type of storage system we'll use in this test. 
	 * The {@link StorageProtocolType} maps to a config file named 
	 * as systems/storage/{@link StorageProtocolType#name()}.example.com.json
	 * 
	 * @return the protocol to use for the storage system
	 */
	protected StorageProtocolType getStorageSystemProtocolType() {
		return StorageProtocolType.SFTP;
	}

	@Override
	protected void initSoftware() throws Exception {
		JSONObject json = jtd.getTestDataObject(SOFTWARE_SYSTEM_TEMPLATE_DIR + File.separator + 
				executionSystem.getExecutionType().name().toLowerCase() + File.separator +
				getSchedulerType().name().toLowerCase() + ".json");
		
		json.put("deploymentSystem", storageSystem.getSystemId());

		software = Software.fromJSON(json, SYSTEM_OWNER);
		software.setOwner(SYSTEM_OWNER);
		software.setExecutablePath("bin/" + software.getExecutablePath());
		
		SoftwareDao.persist(software);
	}

	protected void stageSoftwareDeploymentDirectory(Software software)
	throws Exception {
		Path tempTestSoftwareDeploymentDir = Files.createDirectories(
				Paths.get(Settings.TEMP_DIRECTORY)
					.resolve(UUID.randomUUID().toString())
					.resolve(FilenameUtils.getName(software.getDeploymentPath())));

		for (String subdir: new String[]{"bin", "lib", "etc", "test"}) {
			Path tempSubDirPath = Files.createDirectory(tempTestSoftwareDeploymentDir.resolve(subdir));
			Path tempSubDirFilePath = Files.createFile(tempSubDirPath.resolve(UUID.randomUUID().toString()));
			Files.write(tempSubDirFilePath, ("This is test file " + tempSubDirFilePath.toString()).getBytes());
		}
		Files.copy(Paths.get(SOFTWARE_WRAPPER_FILE),
				tempTestSoftwareDeploymentDir.resolve("bin").resolve(FilenameUtils.getName(SOFTWARE_WRAPPER_FILE)),
				StandardCopyOption.REPLACE_EXISTING);

		RemoteDataClient remoteDataClient = null;
		try 
		{
			Path deploymentPath = Paths.get(software.getDeploymentPath());
			remoteDataClient = software.getStorageSystem().getRemoteDataClient();
			remoteDataClient.authenticate();
			remoteDataClient.mkdirs(deploymentPath.toString());

			Path remoteTemplatePath = deploymentPath.resolve(software.getExecutablePath());

			// copy the directory to the parent directory on the software storage system so it has the expected
			// deployment path name
			remoteDataClient.put(tempTestSoftwareDeploymentDir.toString(), deploymentPath.getParent().toString());

			Assert.assertTrue(remoteDataClient.doesExist(remoteTemplatePath.toString()),
					"Failed to copy software assets to deployment system " + software.getStorageSystem().getSystemId());
		} catch (RemoteDataException e) {
			Assert.fail("Failed to authenticate to the storage system " + job.getSoftwareName(), e);
		} catch (Exception e) {
			Assert.fail("Failed to copy input file to remote system", e);
		} 
		finally {
			if (remoteDataClient != null) {
				remoteDataClient.disconnect();
			}
			FileUtils.deleteQuietly(tempTestSoftwareDeploymentDir.toFile());
		}
	}

	/**
	 * Quick job setup method to create a job we can submit
	 * @param software the app for which to create a job
	 * @return the persisted job
	 * @throws Exception if job cannot be created and saved
	 */
	protected Job createAndPersistJob(Software software) throws Exception {
		Job job = new Job();
		job.setName( software.getExecutionSystem().getScheduler().name() + " test");
		job.setArchiveOutput(false);
		job.setArchivePath("/");
		job.setArchiveSystem(storageSystem);
		job.setCreated(new Date());
		job.setExecutionType(software.getExecutionType());
		job.setSchedulerType(software.getExecutionSystem().getScheduler());
		job.setMemoryPerNode((double).5);
		job.setOwner(software.getOwner());
		job.setProcessorsPerNode((long)1);
		job.setMaxRunTime("1:00:00");
		job.setSoftwareName(software.getUniqueName());
		job.setStatus(JobStatusType.PENDING, job.getErrorMessage());
		job.setSystem(software.getExecutionSystem().getSystemId());
		job.setBatchQueue(software.getExecutionSystem().getDefaultQueue().getName());
		
		ObjectNode jobInputs = mapper.createObjectNode();
		for(SoftwareInput input: software.getInputs()) {
			jobInputs.put(input.getKey(), String.format("agave://%s/%s/%s", 
					software.getStorageSystem().getSystemId(),
					software.getDeploymentPath(),
					software.getExecutablePath()));
		}
		job.setInputsAsJsonObject(jobInputs);
		
		ObjectNode jobParameters = mapper.createObjectNode();
		for (SoftwareParameter parameter: software.getParameters()) {
			jobParameters.set(parameter.getKey(), parameter.getDefaultValueAsJsonArray());
		}
		job.setParametersAsJsonObject(jobParameters);
		
		JobDao.persist(job);
		
		String remoteWorkPath = null;
		if (StringUtils.isEmpty(software.getExecutionSystem().getScratchDir())) {
			remoteWorkPath = job.getOwner() +
				"/job-" + job.getId() + "-" + Slug.toSlug(job.getName()) + 
				"/" + FilenameUtils.getName(software.getDeploymentPath());
		} else {
			remoteWorkPath = software.getExecutionSystem().getScratchDir() + 
					job.getOwner() + 
					"/job-" + job.getId() + "-" + Slug.toSlug(job.getName()) +
					"/" + FilenameUtils.getName(software.getDeploymentPath());
		}
		
		job.setWorkPath(remoteWorkPath);
		
		JobDao.persist(job);
		
		return job;
	}

	@DataProvider(name = "submitJobProvider")
	protected Object[][] submitJobProvider() {
		List<Object[]> testData = new ArrayList<>();
		for (Software app: List.of(software)) {
			testData.add(new Object[] { app, getSchedulerType().name() + " submission to " + getExecutionType().name() + " system should succeed.", false });
		}

		return testData.toArray(new Object[][]{});
	}

	@Test(dataProvider = "submitJobProvider")
	public void testRun(Software software, String message, boolean shouldThrowException)
	throws Exception {
		Job job = null;
		try {
			job = createAndPersistJob(software);
		
			stageSoftwareDeploymentDirectory(software);
		
			stageJobInputs(job);
			
			this.genericRemoteSubmissionTestCase(job, true, "Condor job submission failed", false);
		}
		finally {
			try { JobDao.delete(job); } catch (Exception ignored) {}
			
		}
	}

	protected ExecutionType getExecutionType() {
		return executionType;
	}
}