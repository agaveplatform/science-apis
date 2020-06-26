/**
 * 
 */
package org.iplantc.service.jobs.dao;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.JsonParseException;
import com.surftools.BeanstalkClientImpl.ClientImpl;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.StringUtils;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.iplantc.service.apps.dao.SoftwareDao;
import org.iplantc.service.apps.exceptions.SoftwareException;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareInput;
import org.iplantc.service.apps.model.SoftwareParameter;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.util.Slug;
import org.iplantc.service.jobs.exceptions.JobException;
import org.iplantc.service.jobs.model.JSONTestDataUtil;
import org.iplantc.service.jobs.model.Job;
import org.iplantc.service.jobs.model.JobEvent;
import org.iplantc.service.jobs.model.JobPermission;
import org.iplantc.service.jobs.model.enumerations.JobStatusType;
import org.iplantc.service.notification.model.Notification;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.iplantc.service.systems.model.BatchQueue;
import org.iplantc.service.systems.model.ExecutionSystem;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.dao.TransferTaskDao;
import org.iplantc.service.transfer.model.TransferTask;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.iplantc.service.jobs.model.JSONTestDataUtil.*;

/**
 * @author dooley
 *
 */
@Test(groups={"integration"})
public class AbstractDaoTest 
{	
    private static final Logger log = Logger.getLogger(AbstractDaoTest.class);
            
    public static final BatchQueue SHORT_QUEUE = new BatchQueue("short", (long)1000, (long)10, (long)1, 16.0, (long)16, "01:00:00", null, true);
    public static final BatchQueue MEDIUM_QUEUE = new BatchQueue("medium", (long)100, (long)10, (long)1, 16.0, (long)16, "12:00:00", null, false);
    public static final BatchQueue LONG_QUEUE = new BatchQueue("long", (long)10, (long)4, (long)1, 16.0, (long)16, "48:00:00", null, false);
    public static final BatchQueue DEDICATED_QUEUE = new BatchQueue("dedicated", (long)1, (long)1, (long)1, 16.0, (long)16, "144:00:00", null, false);
    public static final BatchQueue UNLIMITED_QUEUE = new BatchQueue("dedicated", (long)-1, 2048.0);
    public String thingToPrint = null;

    public static final String ADMIN_USER = "testadmin";
    public static final String TENANT_ADMIN = "testtenantadmin";
    public static final String SYSTEM_OWNER = "testuser";
    public static final String SYSTEM_SHARE_USER = "testshare";
    public static final String SYSTEM_PUBLIC_USER = "public";
    public static final String SYSTEM_UNSHARED_USER = "testother";
    public static final String SYSTEM_INTERNAL_USERNAME = "test_user";
    public static final String EXECUTION_SYSTEM_TEMPLATE_DIR = "src/test/resources/systems/execution";
    public static final String STORAGE_SYSTEM_TEMPLATE_DIR = "target/test-classes/systems/storage";
    public static final String SOFTWARE_SYSTEM_TEMPLATE_DIR = "src/test/resources/software";
    public static final String FORK_SOFTWARE_TEMPLATE_FILE = SOFTWARE_SYSTEM_TEMPLATE_DIR + "/fork-1.0.0/app.json";
    public static final String INTERNAL_USER_TEMPLATE_DIR = "src/test/resources/internal_users";
    public static final String CREDENTIALS_TEMPLATE_DIR = "src/test/resources/credentials";
	
	protected JSONTestDataUtil jtd;
	protected SystemDao systemDao = new SystemDao();
	
	/**
	 * Clears database of all {@link RemoteSystem}, {@link Software}, {@link Job}, 
	 * {@link TransferTask}, and {@link Notification}. Creates new test instances 
	 * of each type as needed.
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	protected void beforeClass() throws Exception
	{
		systemDao = new SystemDao();
		
		jtd = JSONTestDataUtil.getInstance();
		
        clearSoftware();
        clearSystems();
        clearJobs();
        drainQueue();
	}

    /**
     * Clears database of all {@link RemoteSystem}, {@link Software}, {@link Job},
     * {@link TransferTask}, and {@link Notification}.
     *
     * @throws Exception
     */
    @AfterClass
    protected void afterClass() throws Exception
    {
        clearJobs();
        clearSoftware();
        clearSystems();
        drainQueue();
    }

    /**
     * Clears database of all {@link RemoteSystem}, {@link Software}, {@link Job},
     * {@link TransferTask}, and {@link Notification}.
     *
     * @throws Exception
     */
    @AfterMethod
    protected void afterMethod() throws Exception
    {
        clearJobs();
        clearSoftware();
        clearSystems();
    }

    /**
     * Creates private execution system with a random uuid as the id.
     * Asserts that the system is created and valid prior to returning.
     * @return a persisted {@link ExecutionSystem}
     */
    protected ExecutionSystem createExecutionSystem() {
        ExecutionSystem system = null;
        try {
            JSONObject json = JSONTestDataUtil.getInstance().getTestDataObject(TEST_EXECUTION_SYSTEM_FILE);
            json.put("id", UUID.randomUUID().toString());
            system = ExecutionSystem.fromJSON(json);
            system.setOwner(SYSTEM_OWNER);
            new SystemDao().persist(system);
        } catch (IOException | JSONException | SystemArgumentException e) {
            log.error("Unable create execution system", e);
            Assert.fail("Unable create execution system", e);
        }

        return system;
    }

    /**
     * Creates private execution system with a random uuid as the id.
     * Asserts that the system is created and valid prior to returning.
     * @return a persisted {@link StorageSystem}
     */
    protected StorageSystem createStorageSystem() {
        StorageSystem system = null;
        try {
            JSONObject json = JSONTestDataUtil.getInstance().getTestDataObject(TEST_STORAGE_SYSTEM_FILE);
            json.put("id", UUID.randomUUID().toString());
            system = StorageSystem.fromJSON(json);
            system.setOwner(SYSTEM_OWNER);
            system.getUsersUsingAsDefault().add(TEST_OWNER);
            new SystemDao().persist(system);
        } catch (IOException|JSONException e) {
            log.error("Unable create storage system", e);
            Assert.fail("Unable create storage system", e);
        }

        return system;
    }

    /**
     * Removes all storage and execution systems from the db
     */
    protected void clearSystems()
    {
        Session session = null;
        try
        {
            HibernateUtil.beginTransaction();
            session = HibernateUtil.getSession();
            HibernateUtil.disableAllFilters();

            session.createQuery("DELETE ExecutionSystem").executeUpdate();
            session.createQuery("DELETE BatchQueue").executeUpdate();
            session.createQuery("DELETE AuthConfig").executeUpdate();
            session.createQuery("DELETE LoginConfig").executeUpdate();
            session.createQuery("DELETE CredentialServer").executeUpdate();
            session.createQuery("DELETE TransferTask").executeUpdate();
            session.createQuery("DELETE RemoteConfig").executeUpdate();
            session.createQuery("DELETE StorageSystem").executeUpdate();
            session.createQuery("DELETE StorageConfig").executeUpdate();
            session.createQuery("DELETE SystemRole").executeUpdate();
            session.createQuery("DELETE SystemPermission").executeUpdate();
            session.createSQLQuery("delete from userdefaultsystems").executeUpdate();
            session.flush();
        }
        finally
        {
            try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
        }
    }

    protected JSONObject getDefaultSoftwareJson() {
        JSONObject jsonSoftware = null;
        try {
            jsonSoftware = JSONTestDataUtil.getInstance().getTestDataObject(FORK_SOFTWARE_TEMPLATE_FILE);
            jsonSoftware.put("name", UUID.randomUUID().toString());

            return jsonSoftware;
        } catch (Exception e) {
            Assert.fail("Failed to read default software template file: " + FORK_SOFTWARE_TEMPLATE_FILE, e);
        }

        return jsonSoftware;
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
    protected Software createDetachedSoftware(ExecutionSystem executionSystem, StorageSystem deploymentSystem)
    {
        Software software = null;
        try {
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
     * Creates a new persisted {@link Software} resource and assigns the {@link ExecutionSystem} and
     * {@link StorageSystem} to it.
     * @param executionSystem the system to which the new {@link Software} resource will be assigned
     * @return a persisted {@link Software} resource
     */
    protected Software createSoftware(ExecutionSystem executionSystem, StorageSystem storageSystem)
    {
        Software software = createDetachedSoftware(executionSystem, storageSystem);
        SoftwareDao.persist(software);
        return software;
    }

    /**
     * Creates a new persisted {@link Software} resource and assigns it to a new {@link ExecutionSystem}
     * and {@link StorageSystem}.
     * @return a persisted {@link Software} resource
     * @see #createSoftware(ExecutionSystem, StorageSystem)
     */
    protected Software createSoftware() {
        ExecutionSystem executionSystem = createExecutionSystem();
        StorageSystem storageSystem = createStorageSystem();
        return createSoftware(executionSystem, storageSystem);
    }

    /**
     * Removes all software records from the db
     */
    protected void clearSoftware()
    {
        Session session = null;
        try
        {
            HibernateUtil.beginTransaction();
            session = HibernateUtil.getSession();
            HibernateUtil.disableAllFilters();

            session.createQuery("DELETE SoftwareParameter").executeUpdate();
            session.createQuery("DELETE SoftwareInput").executeUpdate();
            session.createQuery("DELETE SoftwareOutput").executeUpdate();
            session.createSQLQuery("delete from softwares_parameters").executeUpdate();
            session.createSQLQuery("delete from softwares_inputs").executeUpdate();
            session.createSQLQuery("delete from softwares_outputs").executeUpdate();
            session.createQuery("DELETE SoftwarePermission").executeUpdate();
            session.createQuery("DELETE SoftwareParameterEnumeratedValue").executeUpdate();
            session.createQuery("DELETE SoftwareEvent").executeUpdate();
            session.createQuery("DELETE Software").executeUpdate();
            session.flush();
        }
        finally
        {
            try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
        }
    }

	/**
     * Delete all {@link JobEvent}, {@link TransferTask}, {@link JobPermission}, {@link Notification},
     * and {@link Job}s from the database.
     * @throws Exception
     */
	protected void clearJobs() throws Exception
	{
		Session session = null;
		try
		{
			HibernateUtil.beginTransaction();
			session = HibernateUtil.getSession();
			session.clear();
			HibernateUtil.disableAllFilters();
			
            session.createQuery("delete JobEvent").executeUpdate();
			session.createQuery("delete TransferTask").executeUpdate();
			session.createQuery("delete JobPermission").executeUpdate();
            session.createQuery("delete Notification").executeUpdate();
			session.createQuery("delete Job").executeUpdate();
			
			session.flush();
		}
		catch (Throwable ex) {
			throw new JobException(ex);
		}
		finally {
			try { HibernateUtil.commitTransaction();} catch (Throwable ignored) {}
		}
	}
	
	/**
	 * Delete all {@link Notification}s from the database
	 */
	protected void clearJobNotifications()
	{
	    Session session = null;
        try
        {
            HibernateUtil.beginTransaction();
            session = HibernateUtil.getSession();
            session.clear();
            HibernateUtil.disableAllFilters();
            session.createQuery("delete Notification").executeUpdate();
            session.flush();
        }
        finally {
            try { HibernateUtil.commitTransaction();} catch (Throwable ignored) {}
        }
	}

	/**
	 * Creates and persists a {@link Job} for the given {@code software} and {@code queue}. The
	 * job will be attributed to {@code username} and archived to {@code archiveSystem}. All lifecycle
     * {@link JobEvent}s will be created and attributed to the returned job, though notifications
     * will not be sent.
	 * 
	 * @param status the status of the new job
	 * @param software the software to run
     * @param archiveSystem the system to which the job will be archived. defaults to default storage system
     * @param queue the queue on which to submit the job. defaults to the software default queue
	 * @param username the job owner
	 * @return a new custom job persisted to the db
	 * @throws Exception if anything goes wrong
	 */
	public Job createJob(JobStatusType status, Software software, RemoteSystem archiveSystem, BatchQueue queue, String username)
    throws Exception 
	{
        ObjectMapper mapper = new ObjectMapper();
        Job job = new Job();
	    try {

            job.setName(UUID.randomUUID().toString());
            job.setOutputPath(software.getExecutionSystem().getScratchDir() + username + "job-" + Slug.toSlug(job.getName()));
            job.setOwner(username);
            job.setInternalUsername(null);
            
            job.setArchiveOutput(true);
            job.setArchiveSystem(archiveSystem);
            job.setArchivePath(username + "/archive/test-job-999");
            job.setExecutionType(software.getExecutionType());
            job.setSchedulerType(software.getExecutionSystem().getScheduler());
            job.setSoftwareName(software.getUniqueName());
            job.setSystem(software.getExecutionSystem().getSystemId());
            job.setBatchQueue(queue.getName());
            job.setMaxRunTime(StringUtils.isEmpty(software.getDefaultMaxRunTime()) ? "00:30:00" : software.getDefaultMaxRunTime());
            job.setMemoryPerNode((software.getDefaultMemoryPerNode() == null) ? (double)1 : software.getDefaultMemoryPerNode());
            job.setNodeCount((software.getDefaultNodes() == null) ? (long)1 : software.getDefaultNodes());
            job.setProcessorsPerNode((software.getDefaultProcessorsPerNode() == null) ? (long)1 : software.getDefaultProcessorsPerNode());
            
            ObjectNode inputs = mapper.createObjectNode();
            for(SoftwareInput swInput: software.getInputs()) {
                inputs.set(swInput.getKey(), swInput.getDefaultValueAsJsonArray());
            }
            job.setInputsAsJsonObject(inputs);
            
            ObjectNode parameters = mapper.createObjectNode();
            for(SoftwareParameter swParameter: software.getParameters()) {
                parameters.set(swParameter.getKey(), swParameter.getDefaultValueAsJsonArray());
            }
            job.setParametersAsJsonObject(parameters);
            int minutesAgoJobWasCreated = RandomUtils.nextInt(360)+1;
            DateTime created = new DateTime().minusMinutes(minutesAgoJobWasCreated+20);
            job.setCreated(created.toDate());
            
            if (JobStatusType.isExecuting(status) || status == JobStatusType.CLEANING_UP) {
                job.setLocalJobId("q." + System.currentTimeMillis());
                job.setSchedulerJobId(job.getUuid());
                job.setStatus(status, status.getDescription());
                
                int minutesAgoJobWasSubmitted = RandomUtils.nextInt(minutesAgoJobWasCreated)+1;
                int minutesAgoJobWasStarted = minutesAgoJobWasSubmitted + RandomUtils.nextInt(minutesAgoJobWasSubmitted);
                job.setSubmitTime(created.plusMinutes(minutesAgoJobWasSubmitted).toDate());
                job.setStartTime(created.plusMinutes(minutesAgoJobWasStarted).toDate());
                
                if (status == JobStatusType.CLEANING_UP) {
                    int minutesAgoJobWasEnded =  minutesAgoJobWasStarted + RandomUtils.nextInt(minutesAgoJobWasStarted);
                    job.setEndTime(created.plusMinutes(minutesAgoJobWasEnded).toDate());
                }
            } else if (JobStatusType.isFinished(status) || JobStatusType.isArchived(status)) {
                job.setLocalJobId("q." + System.currentTimeMillis());
                job.setSchedulerJobId(job.getUuid());
                job.setStatus(status, status.getDescription());
                
                int minutesAgoJobWasSubmitted = RandomUtils.nextInt(minutesAgoJobWasCreated)+1;
                int minutesAgoJobWasStarted = minutesAgoJobWasSubmitted + RandomUtils.nextInt(minutesAgoJobWasSubmitted);
                int minutesAgoJobWasEnded = minutesAgoJobWasStarted + RandomUtils.nextInt(minutesAgoJobWasStarted);
                job.setSubmitTime(created.plusMinutes(minutesAgoJobWasSubmitted).toDate());
                job.setStartTime(created.plusMinutes(minutesAgoJobWasStarted).toDate());
                job.setEndTime(created.plusMinutes(minutesAgoJobWasEnded).toDate());
                
            } else if (status == JobStatusType.STAGING_INPUTS) {
                
                int minutesAgoJobStartedStaging = RandomUtils.nextInt(minutesAgoJobWasCreated)+1;
                DateTime stagingTime = created.plusMinutes(minutesAgoJobStartedStaging);
                
                for(SoftwareInput input: software.getInputs())
                {
                    for (JsonNode jsonNode : input.getDefaultValueAsJsonArray()) {
                        String val = jsonNode.asText();

                        TransferTask stagingTransferTask = new TransferTask(
                                val,
                                "agave://" + job.getSystem() + "/" + job.getWorkPath() + "/" + FilenameUtils.getName(URI.create(val).getPath()),
                                job.getOwner(),
                                null,
                                null);
                        stagingTransferTask.setStatus(TransferStatusType.TRANSFERRING);
                        stagingTransferTask.setCreated(stagingTime.toDate());
                        stagingTransferTask.setLastUpdated(stagingTime.toDate());

                        TransferTaskDao.persist(stagingTransferTask);

                        JobEvent event = new JobEvent(
                                JobStatusType.STAGING_INPUTS,
                                "Copy in progress",
                                stagingTransferTask,
                                job.getOwner());
                        event.setCreated(stagingTime.toDate());

                        job.setStatus(JobStatusType.STAGING_INPUTS, event);
                    }
                }
                
                job.setLastUpdated(stagingTime.toDate());
                
            } else if (status == JobStatusType.ARCHIVING 
                        || status == JobStatusType.ARCHIVING_FAILED 
                        || status == JobStatusType.ARCHIVING_FINISHED ) {
                
                DateTime stagingTime = created.plusMinutes(5);
                DateTime stagingEnded = stagingTime.plusMinutes(1);
                DateTime startTime = stagingEnded.plusMinutes(1);
                DateTime endTime = startTime.plusMinutes(1);
                DateTime archiveTime = endTime.plusMinutes(1);
                
                for(SoftwareInput input: software.getInputs())
                {
                    for (JsonNode jsonNode : input.getDefaultValueAsJsonArray()) {
                        String val = jsonNode.asText();

                        TransferTask stagingTransferTask = new TransferTask(
                                val,
                                "agave://" + job.getSystem() + "/" + job.getWorkPath() + "/" + FilenameUtils.getName(URI.create(val).getPath()),
                                job.getOwner(),
                                null,
                                null);

                        stagingTransferTask.setStatus(TransferStatusType.COMPLETED);
                        stagingTransferTask.setCreated(stagingTime.toDate());
                        stagingTransferTask.setStartTime(stagingTime.toDate());
                        stagingTransferTask.setEndTime(stagingEnded.toDate());
                        stagingTransferTask.setLastUpdated(stagingEnded.toDate());

                        TransferTaskDao.persist(stagingTransferTask);

                        JobEvent event = new JobEvent(
                                JobStatusType.STAGING_INPUTS,
                                "Staging completed",
                                stagingTransferTask,
                                job.getOwner());
                        event.setCreated(stagingTime.toDate());

                        job.setStatus(JobStatusType.STAGING_INPUTS, event);
                    }
                }
                
                job.setStatus(JobStatusType.STAGED, JobStatusType.STAGED.getDescription());
                job.setLocalJobId("q." + System.currentTimeMillis());
                job.setSchedulerJobId(job.getUuid());
                job.setStatus(status, status.getDescription());
                
                job.setSubmitTime(stagingEnded.toDate());
                job.setStartTime(startTime.toDate());
                job.setEndTime(endTime.toDate());
                
                TransferTask archivingTransferTask = new TransferTask(
                        "agave://" + job.getSystem() + "/" + job.getWorkPath(),
                        job.getArchiveCanonicalUrl(), 
                        job.getOwner(), 
                        null, 
                        null);
                
                archivingTransferTask.setCreated(archiveTime.toDate());
                archivingTransferTask.setStartTime(archiveTime.toDate());
                TransferTaskDao.persist(archivingTransferTask);
                
                JobEvent event = new JobEvent(
                        JobStatusType.ARCHIVING ,
                        JobStatusType.ARCHIVING.getDescription(), 
                        archivingTransferTask, 
                        job.getOwner());
                event.setCreated(archiveTime.toDate());
                job.setStatus(status, event);
                
                if (status != JobStatusType.ARCHIVING) {
                    JobEvent event2 = new JobEvent(
                            status,
                            status.getDescription(),
                            job.getOwner());
                    event2.setCreated(archiveTime.toDate());
                    job.setStatus(status, event2);
                }
                
                job.setLastUpdated(archiveTime.toDate());
            }
            else if (JobStatusType.isFailed(status)) {
                job.setEndTime(new Date());
            }
            else {
                job.setStatus(status, status.getDescription());
            }
            
            log.debug("Adding job " + job.getId() + " - " + job.getUuid());
            JobDao.persist(job, false);
            
            return job;
	    } catch (Exception e) {
	        log.error("Failed to create test job", e);
	        throw e;
	    }
    }

    /**
     * Creates a {@link Job} using the default {@link Software#getExecutionSystem()} as teh execution system,
     * {@link ExecutionSystem#getDefaultQueue()} as the queue, and {@link Software#getOwner()} as the job owner.
     * @param status the status of the created job
     * @param software the app to run
     * @return a persisted job
     * @throws Exception if job creation goes south
     * @see #createJob(JobStatusType, Software, RemoteSystem, BatchQueue, String)
     */
	public Job createJob(JobStatusType status, Software software) throws Exception
	{
	    return createJob(status, software, software.getExecutionSystem(), software.getExecutionSystem().getDefaultQueue(), software.getOwner());
	}
	
	/**
	 * Removes the current tenant id and end user from the current thread.
	 */
	public void clearCurrentTenancyInfo() {
	    TenancyHelper.setCurrentEndUser(null);
        TenancyHelper.setCurrentTenantId(null);
	}

    /**
     * Copies job assets to a remote {@link StorageSystem} using the {@code software.getDeploymentPath()}.
     *
     * @param software the app for which to stage the assets
     * @throws Exception if the copy fails
     */
	protected void stageRemoteSoftwareAssets(Software software) throws Exception
    {
        RemoteSystem storageSystem = software.getStorageSystem();
        RemoteDataClient storageDataClient = null;
        try 
        {
            storageDataClient = storageSystem.getRemoteDataClient();
            storageDataClient.authenticate();
            if (!storageDataClient.doesExist(software.getDeploymentPath())) {
                storageDataClient.mkdirs(FilenameUtils.getPath(software.getDeploymentPath()));
                storageDataClient.put(SOFTWARE_SYSTEM_TEMPLATE_DIR + File.separator + software.getUniqueName(), FilenameUtils.getPath(software.getDeploymentPath()));
            }
            else
            {
                for (File localSoftwareAssetPath: new File(SOFTWARE_SYSTEM_TEMPLATE_DIR + File.separator + software.getUniqueName()).listFiles()) {
                    if (!storageDataClient.doesExist(software.getDeploymentPath() + File.separator + localSoftwareAssetPath.getName())) {
                        storageDataClient.put(localSoftwareAssetPath.getAbsolutePath(), FilenameUtils.getPath(software.getDeploymentPath()) + File.separator + localSoftwareAssetPath.getName());
                    }
                }
            }
        }
        finally {
            try {
                if (storageDataClient != null) {
                    storageDataClient.disconnect();
                }
            } catch (Exception ignored) {}
        }
        
    }

    /**
     * Cleans up job assets at the {@code software.getDeploymentPath()} of the {@link StorageSystem}.
     *
     * @param software the app for which to stage the assets
     * @throws Exception if the copy fails
     */
    protected void deleteRemoteSoftwareAssets(Software software) throws Exception
    {
        RemoteSystem storageSystem = software.getStorageSystem();
        RemoteDataClient storageDataClient = null;
        try 
        {
            storageDataClient = storageSystem.getRemoteDataClient();
            storageDataClient.authenticate();
            storageDataClient.delete(software.getDeploymentPath());
        }
        finally {
            try {
                if (storageDataClient != null) {
                    storageDataClient.disconnect();
                }
            } catch (Exception ignored) {}
        }
        
    }

    /**
     * Creates a nonce for use as the token by generating an md5 hash of the
     * salt, current timestamp, and a random number.
     *
     * @param salt the salt prepended to semi-random data when calculating the nonce digest
     * @return md5 hash of the adjusted salt
     */
    public String createNonce(String salt) {
        String digestMessage = salt + System.currentTimeMillis() + new Random().nextInt();
        return DigestUtils.md5Hex(digestMessage);
    }

    /**
     * Creates a nonce for use as the token by generating an md5 hash of the
     * a random uuid, current timestamp, and a random number.
     *
     * @return md5 hash of the adjusted salt
     * @see #createNonce(String)
     */
    public String createNonce() {
        return createNonce(UUID.randomUUID().toString());
    }

    /**
     * Flushes the messaging tube of any and all existing jobs.
     */
    public void drainQueue()
    {
        ClientImpl client = null;

        try {
            // drain the message queue
            client = new ClientImpl(Settings.MESSAGING_SERVICE_HOST,
                    Settings.MESSAGING_SERVICE_PORT);
            client.watch(Settings.NOTIFICATION_QUEUE);
            client.useTube(Settings.NOTIFICATION_QUEUE);
            client.kick(Integer.MAX_VALUE);

            com.surftools.BeanstalkClient.Job beanstalkJob = null;
            do {
                try {
                    beanstalkJob = client.peekReady();
                    if (beanstalkJob != null)
                        client.delete(beanstalkJob.getJobId());
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            } while (beanstalkJob != null);
            do {
                try {
                    beanstalkJob = client.peekBuried();
                    if (beanstalkJob != null)
                        client.delete(beanstalkJob.getJobId());
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            } while (beanstalkJob != null);
            do {
                try {
                    beanstalkJob = client.peekDelayed();

                    if (beanstalkJob != null)
                        client.delete(beanstalkJob.getJobId());
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            } while (beanstalkJob != null);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        finally {
            try { client.ignore(Settings.NOTIFICATION_QUEUE); } catch (Throwable e) {}
            try { client.close(); } catch (Throwable e) {}
            client = null;
        }
    }

    /**
     * Counts number of messages in the queue.
     *
     * @param queueName
     * @return int totoal message count
     */
    public int getMessageCount(String queueName) throws MessagingException
    {
        ClientImpl client = null;

        try {
            // drain the message queue
            client = new ClientImpl(Settings.MESSAGING_SERVICE_HOST,
                    Settings.MESSAGING_SERVICE_PORT);
            client.watch(queueName);
            client.useTube(queueName);
            Map<String,String> stats = client.statsTube(queueName);
            String totalJobs = stats.get("current-jobs-ready");
            if (NumberUtils.isNumber(totalJobs)) {
                return NumberUtils.toInt(totalJobs);
            } else {
                throw new MessagingException("Failed to find total job count for queue " + queueName);
            }
        } catch (MessagingException e) {
            throw e;
        } catch (Throwable e) {
            throw new MessagingException("Failed to read jobs from queue " + queueName, e);
        }
        finally {
            try { client.ignore(Settings.NOTIFICATION_QUEUE); } catch (Throwable e) {}
            try { client.close(); } catch (Throwable e) {}
            client = null;
        }
    }
}
