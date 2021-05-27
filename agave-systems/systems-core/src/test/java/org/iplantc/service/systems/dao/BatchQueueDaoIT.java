package org.iplantc.service.systems.dao;

import com.google.common.collect.Collections2;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.systems.model.*;
import org.json.JSONObject;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.*;

public class BatchQueueDaoIT extends PersistedSystemsModelTestCommon {

    @InjectMocks
    BatchQueueDao batchQueueDao;

    @InjectMocks
    SystemDao systemDao;

    @BeforeClass
    @Override
    public void beforeClass() throws Exception {
        super.beforeClass();
        clearSystems();
    }

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @AfterMethod
    public void afterMethod() throws Exception {
        clearSystems();
    }

    /**
     * Generates a test execution for with no
     * @return test exectuion system
     */
    private ExecutionSystem createExecutionSystem() {
        ExecutionSystem system = null;

        try {
            JSONObject systemJson = jtd.getTestDataObject(JSONTestDataUtil.TEST_PROXY_EXECUTION_SYSTEM_FILE)
                    .put("id", UUID.randomUUID().toString());
            system = ExecutionSystem.fromJSON(systemJson);
            system.getBatchQueues().clear();
            system.setOwner(SYSTEM_OWNER);
        }
        catch (Exception e) {
            Assert.fail("Test system should be created without exceptions", e);
        }

        return system;
    }

    /**
     * Returns set with 3 {@link BatchQueue} representing teh normal, small, and large default queues for testing
     * @return
     */
    private Set<BatchQueue> createStandardBatchQueues() {
        BatchQueue normalQueue = new BatchQueue("normal", 10L, 5L, 1L, 256d, 1L, "01:00:00", "", true);
        BatchQueue smallQueue = new BatchQueue("small", 5L, 1L, 1L, 128d, 1L, "00:15:00", "", false);
        BatchQueue largeQueue = new BatchQueue("large", 100L, 50L, 10L, 1024d, 16L, "12:00:00", "", false);
        return Set.of(normalQueue, smallQueue, largeQueue);
    }

    /**
     * Generates a test execution for with predictable queues
     * @return test exectuion with 3 batch queues
     */
    private BatchQueue createBatchQueue() {
        return new BatchQueue(UUID.randomUUID().toString(), 10L, 1L, 1L, 128d, 1L, "01:30:00", "", false);
    }

    @Test
    public void testPersist() {
        ExecutionSystem system = createExecutionSystem();
        systemDao.persist(system);

        BatchQueue q = createBatchQueue();
        q.setExecutionSystem(system);
        batchQueueDao.persist(q);

        Session session = HibernateUtil.getSession();
        BatchQueue queue = (BatchQueue)session
                .createCriteria(BatchQueue.class)
                .add(Restrictions.eq("name", q.getName()))
                .uniqueResult();

        assertEquals(queue.getExecutionSystem().getSystemId(), system.getSystemId(),
                "System should be persisted when adding BatchQueue through BatchQueueDao");

        system = (ExecutionSystem) systemDao.findById(system.getId());
        assertNotNull(system.getBatchQueues(), "ExecutionSystem should return queues after additions through BatchQueueDao");
    }

    @Test
    public void testGetAll() {
        int testQueueCount = 10;
        for (int i = 0; i < testQueueCount; i++) {
            ExecutionSystem system = createExecutionSystem();
            system.addBatchQueue(createBatchQueue());
            systemDao.persist(system);
        }

        List<BatchQueue> queues = batchQueueDao.getAll();
        assertEquals(queues.size(), testQueueCount,
                "Incorrect number of batch queues returned from getAll");
    }

    @Test
    public void testDelete() throws HibernateException {
        ExecutionSystem system = createExecutionSystem();
        Set<BatchQueue> standardQueues = createStandardBatchQueues();
        for (BatchQueue q: standardQueues) {
            system.addBatchQueue(q);
        }
        systemDao.persist(system);

        // persist the queue with the system
        BatchQueue q = createBatchQueue();
        q.setExecutionSystem(system);
        batchQueueDao.persist(q);

        // delete the queue
        batchQueueDao.delete(q);

        Session session = HibernateUtil.getSession();
        List queues = session
                .createCriteria(BatchQueue.class)
                .add(Restrictions.idEq(q.getId()))
                .list();

        assertEquals(queues.size(), 0,
                "BatchQueue should not be present after deletion through BatchQueueDao");
    }

    @Test
    public void testFindById() {
        ExecutionSystem system = createExecutionSystem();
        Set<BatchQueue> standardQueues = createStandardBatchQueues();
        for (BatchQueue q: standardQueues) {
            system.addBatchQueue(q);
        }
        systemDao.persist(system);

        // persist the queue with the system
        BatchQueue q = createBatchQueue();
        q.setExecutionSystem(system);
        batchQueueDao.persist(q);

        BatchQueue result = batchQueueDao.findById(q.getId());

        assertEquals(result.getId(), q.getId(),
                "Queue with the correct id should be returned from findById");
        assertEquals(result.getExecutionSystem().getUuid(), system.getUuid(),
                "Queue with the correct systemId should be returned from findById");
        assertEquals(result.getName(), q.getName(),
                "Queue with the correct name should be returned from findById");
    }

    @Test
    public void testFindByUuid() {
        ExecutionSystem system = createExecutionSystem();
        Set<BatchQueue> standardQueues = createStandardBatchQueues();
        for (BatchQueue q: standardQueues) {
            system.addBatchQueue(q);
        }
        systemDao.persist(system);

        // persist the queue with the system
        BatchQueue q = createBatchQueue();
        q.setExecutionSystem(system);
        batchQueueDao.persist(q);

        BatchQueue result = batchQueueDao.findByUuid(q.getUuid());

        assertEquals(result.getId(), q.getId(),
                "Queue with the correct id should be returned from findByUuid");
        assertEquals(result.getExecutionSystem().getUuid(), system.getUuid(),
                "Queue with the correct systemId should be returned from findByUuid");
        assertEquals(result.getName(), q.getName(),
                "Queue with the correct name should be returned from findByUuid");
    }

    @Test
    public void testFindBySystemIdAndName() {
        ExecutionSystem system = createExecutionSystem();
        Set<BatchQueue> standardQueues = createStandardBatchQueues();
        for (BatchQueue q: standardQueues) {
            system.addBatchQueue(q);
        }
        systemDao.persist(system);

        // persist the queue with the system
        BatchQueue q = createBatchQueue();
        q.setExecutionSystem(system);
        batchQueueDao.persist(q);

        BatchQueue result = batchQueueDao.findBySystemIdAndName(system.getSystemId(), q.getName());

        assertEquals(result.getId(), q.getId(),
                "Queue with the correct id should be returned from findBySystemIdAndName");
        assertEquals(result.getExecutionSystem().getUuid(), system.getUuid(),
                "Queue with the correct systemId should be returned from findBySystemIdAndName");
        assertEquals(result.getName(), q.getName(),
                "Queue with the correct name should be returned from findBySystemIdAndName");
    }

    @Test
    public void testFindBySystemId() {
        ExecutionSystem system = createExecutionSystem();
        Set<BatchQueue> standardQueues = createStandardBatchQueues();
        for (BatchQueue q: standardQueues) {
            system.addBatchQueue(q);
        }
        systemDao.persist(system);

        List<BatchQueue> result = batchQueueDao.findBySystemId(system.getSystemId(), 0, 99999);

        assertEquals(result.size(), 3,
                "All test systems batch queues should be returned from findBySystemId");

        for (BatchQueue q: result) {
            assertEquals(q.getExecutionSystem().getUuid(), system.getUuid(),
                    "Queues with the correct systemId should be returned from findBySystemId");
            assertEquals(Collections2.filter(standardQueues, input -> input.getUuid().equals(q.getUuid())).size(), 1,
                "All test systems should be present in the response from findBySystemId");
        }
    }

    @Test
    public void testFindBySystemIdHonorsOffset() {
        ExecutionSystem system = createExecutionSystem();
        Set<BatchQueue> standardQueues = createStandardBatchQueues();
        for (BatchQueue q: standardQueues) {
            system.addBatchQueue(q);
        }
        systemDao.persist(system);

        // delete the first of the original queues to predict the results after offset
        List<BatchQueue> offsetList = Lists.newArrayList(standardQueues);
        offsetList.remove(0);

        List<BatchQueue> result = batchQueueDao.findBySystemId(system.getSystemId(), 1, 9999);

        assertEquals(result.size(), 2,
                "All test systems batch queues should be returned from findBySystemId");

        for (BatchQueue q: result) {
            assertEquals(q.getExecutionSystem().getUuid(), system.getUuid(),
                    "Queues with the correct systemId should be returned from findBySystemId");

            boolean found = false;
            for (BatchQueue systemQueue: standardQueues) {
                if (systemQueue.getName().equals(q.getName())) {
                    found = true;
                    break;
                }
            }
            assertTrue(found,
                    "All test systems should be present in the response from findBySystemId");
        }
    }

    @Test
    public void testGetCurrentLoad() {
        ExecutionSystem system = createExecutionSystem();
        Set<BatchQueue> standardQueues = createStandardBatchQueues();
        for (BatchQueue q: standardQueues) {
            system.addBatchQueue(q);
        }
        systemDao.persist(system);

        // delete the first of the original queues to predict the results after offset
        List<BatchQueue> offsetList = Lists.newArrayList(standardQueues);
        offsetList.remove(0);

        BatchQueueLoad result = batchQueueDao.getCurrentLoad(system.getSystemId(), system.getDefaultQueue().getName());

        assertEquals(result.getName(), system.getDefaultQueue().getName(),
                "BatchQueueLoad should have the queried queue name");

        assertEquals(result.getActive(), 0,
                "BatchQueueLoad#getActive() should be zero for unused queues");
        assertEquals(result.getArchiving(), 0,
                "BatchQueueLoad#getArchiving() should be zero for unused queues");
        assertEquals(result.getBacklogged(), 0,
                "BatchQueueLoad#getBacklogged() should be zero for unused queues");
        assertEquals(result.getCleaningUp(), 0,
                "BatchQueueLoad#getCleaningUp() should be zero for unused queues");
        assertEquals(result.getPaused(), 0,
                "BatchQueueLoad#getPaused() should be zero for unused queues");
        assertEquals(result.getPending(), 0,
                "BatchQueueLoad#getPending() should be zero for unused queues");
        assertEquals(result.getProcessingInputs(), 0,
                "BatchQueueLoad#getProcessingInputs() should be zero for unused queues");
        assertEquals(result.getQueued(), 0,
                "BatchQueueLoad#getQueued() should be zero for unused queues");
        assertEquals(result.getRunning(), 0,
                "BatchQueueLoad#getRunning() should be zero for unused queues");
        assertEquals(result.getStaging(), 0,
                "BatchQueueLoad#getStaging() should be zero for unused queues");
        assertEquals(result.getStagingInputs(), 0,
                "BatchQueueLoad#getStagingInputs() should be zero for unused queues");
        assertEquals(result.getSubmitting(), 0,
                "BatchQueueLoad#getSubmitting() should be zero for unused queues");

    }

}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme