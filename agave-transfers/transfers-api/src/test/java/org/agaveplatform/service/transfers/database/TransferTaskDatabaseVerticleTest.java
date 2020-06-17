package org.agaveplatform.service.transfers.database;

import com.google.common.base.CaseFormat;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers completed task listener integration tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled
public class TransferTaskDatabaseVerticleTest extends BaseTestCase {

    private TransferTaskDatabaseService service;

    @BeforeAll
    @Override
    public void setUpService(Vertx vertx, VertxTestContext ctx) throws IOException {
        Checkpoint authCheckpoint = ctx.checkpoint();
        Checkpoint deployCheckpoint = ctx.checkpoint();
        Checkpoint serviceCheckpoint = ctx.checkpoint();

        // init the jwt auth used in the api calls
        initAuth(vertx, reply -> {
            authCheckpoint.flag();
            if (reply.succeeded()) {
                jwtAuth = reply.result();

                DeploymentOptions options = new DeploymentOptions()
                        .setConfig(config)
                        .setWorker(true)
                        .setMaxWorkerExecuteTime(3600);

                vertx.deployVerticle(new TransferTaskDatabaseVerticle(), options, ctx.succeeding(id -> {
                    deployCheckpoint.flag();
                    service = TransferTaskDatabaseService.createProxy(vertx, config.getString(CONFIG_TRANSFERTASK_DB_QUEUE));

                    ctx.verify(() -> {
                        serviceCheckpoint.flag();
                        assertNotNull(jwtAuth);
                        assertNotNull(config);
                        ctx.completeNow();
                    });
                }));
            } else {
                ctx.failNow(new Exception("Failed to initialize jwtAuth"));
            }
        });
    }

    @Test
    public void crudTest(Vertx vertx, VertxTestContext context) {
        TransferTask testTransferTask = _createTestTransferTask();

        service.create(TENANT_ID, testTransferTask, context.succeeding(createdJsonTransferTask -> {
            assertNotNull(createdJsonTransferTask.getLong("id"), "Object returned from create should have not null id");
            assertNotNull(createdJsonTransferTask.getInstant("created"), "Object returned from create should have not null created");
            assertNotNull(createdJsonTransferTask.getInstant("last_updated"), "Object returned from create should have not null last_updated");
            assertEquals(TENANT_ID, createdJsonTransferTask.getString("tenant_id"),"Object returned from create should have same tenant_id as sent");
            assertEquals(testTransferTask.getUuid(), createdJsonTransferTask.getString("uuid"),"Object returned from create should have same uuid as sent");
            assertEquals(testTransferTask.getSource(), createdJsonTransferTask.getString("source"),"Object returned from create should have same source value as sent");
            assertEquals(testTransferTask.getDest(), createdJsonTransferTask.getString("dest"),"Object returned from create should have same dest value as sent");

            service.getById(TENANT_ID, testTransferTask.getUuid(), context.succeeding(getByIdJsonTransferTask -> {
                assertEquals(TENANT_ID, getByIdJsonTransferTask.getString("tenant_id"),"Object returned from create should have same tenant_id as original");
                assertEquals(testTransferTask.getUuid(), getByIdJsonTransferTask.getString("uuid"),"Object returned from create should have same uuid as original");
                assertEquals(testTransferTask.getSource(), getByIdJsonTransferTask.getString("source"),"Object returned from create should have same source value as original");
                assertEquals(testTransferTask.getDest(), getByIdJsonTransferTask.getString("dest"),"Object returned from create should have same dest value as original");

                service.updateStatus(TENANT_ID, testTransferTask.getUuid(), TransferStatusType.TRANSFERRING.name(), context.succeeding(updateStatusJsonTransferTask -> {
                    assertEquals(TransferStatusType.TRANSFERRING.name(), updateStatusJsonTransferTask.getString("status"),
                            "Object returned from updateStatus should have updated status");
                    assertTrue(getByIdJsonTransferTask.getInstant("last_updated").isBefore(updateStatusJsonTransferTask.getInstant("last_updated")),
                            "Object returned from udpate have last_updated value more recent than original");
                    assertEquals(createdJsonTransferTask.getLong("id"), updateStatusJsonTransferTask.getLong("id"),
                            "Object returned from create should have same id as original");

                    service.getAll(TENANT_ID, 100, 0, context.succeeding(getAllJsonTransferTaskArray -> {
                        assertEquals(1, getAllJsonTransferTaskArray.size(),
                                "getAll should only have a single record after updating status of an existing object." +
                                        "New object was likely added to the db.");

                        JsonObject updatedJsonObject = new JsonObject()
                                .put("attempts", 1) // attempts
                                .put("bytesTransferred", (long)Math.pow(2, 19)) // getBytesTransferred())
//                                .add("created should not update") // getCreated())
//                                .add("dest should not update") // getDest())
                                .put("endTime", Instant.now()) // getEndTime())
//                                .addNull() // getEventId())
//                                .add(Instant.now()) // getLastUpdated())
//                                .add("owner should not update") // getOwner())
//                                .add("source should not update") // getSource())
                                .put("startTime", Instant.now()) // getStartTime())
                                .put("status", TransferStatusType.PAUSED.name()) // getStatus())
//                                .add("tenant_id should not update")
                                .put("totalSize", (long)Math.pow(2, 20)) // getTotalSize())
                                .put("transferRate", 9999.0) // getTransferRate())
//                                .addNull() // getParentTaskId())
//                                .addNull() // getRootTaskId())
//                                .add("this should not change") // getUuid())
                                .put("totalFiles", 10L) // getTotalFiles())
                                .put("totalSkippedFiles", 9L); // getTotalSkippedFiles());

                        TransferTask updatedTransferTask = new TransferTask(updatedJsonObject);

                        service.update(TENANT_ID, testTransferTask.getUuid(), updatedTransferTask, context.succeeding(updateJsonTransferTask -> {

                            service.getAll(TENANT_ID, 100, 0, context.succeeding(getAllJsonTransferTaskArray2 -> {
                                assertEquals(1, getAllJsonTransferTaskArray2.size(),
                                        "getAll should only have a single record after updating an existing object." +
                                                "New object was likely added to the db.");
                                assertEquals(updateJsonTransferTask.getString("uuid"), getAllJsonTransferTaskArray2.getJsonObject(0).getString("uuid"),
                                        "Object returned by getAll should have the same uuid as the updated object.");


                                service.getById(TENANT_ID, testTransferTask.getUuid(), context.succeeding(getByIdJsonTransferTask2 -> {

                                    assertEquals(TENANT_ID, getByIdJsonTransferTask2.getString("tenant_id"),"Object returned from create should have same tenant_id as original");
                                    assertEquals(createdJsonTransferTask.getLong("id"), getByIdJsonTransferTask2.getLong("id"),"Object returned from create should have same id as original");
                                    assertEquals(testTransferTask.getUuid(), getByIdJsonTransferTask2.getString("uuid"),"Object returned from create should have same uuid as original");
                                    assertEquals(testTransferTask.getSource(), getByIdJsonTransferTask2.getString("source"),"Object returned from create should have same source value as original");
                                    assertEquals(testTransferTask.getDest(), getByIdJsonTransferTask2.getString("dest"),"Object returned from create should have same dest value as original");
                                    //Assertions.assertTrue(updateStatusJsonTransferTask.getInstant("last_updated").isBefore(getByIdJsonTransferTask2.getInstant("last_updated")),"Object returned from udpate have last_updated value more recent than original");
                                    // verify the updated values are present in the response
                                    updatedJsonObject
                                            .stream()
                                            .forEach(entry -> {
                                                if (entry.getKey().equalsIgnoreCase("startTime") || entry.getKey().equalsIgnoreCase("endTime")) {
                                                    assertEquals(Instant.parse(((String)entry.getValue())).getEpochSecond(), getByIdJsonTransferTask2.getInstant(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey())).getEpochSecond(),
                                                            entry.getKey() + " should be updated in the response from the service");
                                                } else {
                                                    assertEquals(entry.getValue(), getByIdJsonTransferTask2.getValue(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey())),
                                                            entry.getKey() + " should be updated in the response from the service" );
                                                }
                                            });

//                                    Assertions.assertEquals(TENANT_ID, getByIdJsonTransferTask2.getString("tenant_id"),"Object returned from create should have same tenant_id as original");
//                                    Assertions.assertEquals(createdJsonTransferTask.getLong("id"), getByIdJsonTransferTask2.getLong("id"),"Object returned from create should have same id as original");
//                                    Assertions.assertEquals(testTransferTask.getUuid(), getByIdJsonTransferTask2.getString("uuid"),"Object returned from create should have same uuid as original");
//                                    Assertions.assertEquals(testTransferTask.getSource(), getByIdJsonTransferTask2.getString("source"),"Object returned from create should have same source value as original");
//                                    Assertions.assertEquals(testTransferTask.getDest(), getByIdJsonTransferTask2.getString("dest"),"Object returned from create should have same dest value as original");


                                    service.delete(TENANT_ID, testTransferTask.getUuid(), context.succeeding(v3 -> {

                                        service.getAll(TENANT_ID, 100, 0, context.succeeding( getAllJsonTransferTaskArray3 -> {
                                            assertTrue(getAllJsonTransferTaskArray3.isEmpty(), "No records should be returned from getAll after deleting the test object");
                                            context.completeNow();
                                        }));
                                    }));
                                }));
                            }));
                        }));
                    }));
                }));
            }));
        }));
    }

    @Test
    public void crudChildTransferTaskTest(Vertx vertx, VertxTestContext context) {
        final TransferTask rootTransferTask = _createTestTransferTask();
        rootTransferTask.setSource("agave://source/" + UUID.randomUUID().toString());
        rootTransferTask.setDest("agave://dest/" + UUID.randomUUID().toString());
        rootTransferTask.setStatus(TransferStatusType.QUEUED);

        // create the task
        final JsonObject[] testTT = new JsonObject[4];
        service.create(rootTransferTask.getTenantId(), _createChildTestTransferTask(rootTransferTask), t0 -> {
            testTT[0] = t0.result();
            service.create(rootTransferTask.getTenantId(), _createChildTestTransferTask(t0.result()), t1 -> {
                testTT[1] = t1.result();
                service.create(rootTransferTask.getTenantId(), _createChildTestTransferTask(t1.result()), t2 -> {
                    testTT[2] = t2.result();
                    service.create(rootTransferTask.getTenantId(), _createChildTestTransferTask(t1.result()), t3 -> {
                        testTT[3] = t3.result();

                        // everything is now saved, we can run our queries against the saved test data
                        service.findChildTransferTask(rootTransferTask.getTenantId(), testTT[2].getString("rootTask"), testTT[2].getString("source"), testTT[2].getString("dest"), context.succeeding(fetch -> {
                            context.verify(() -> {
                                assertEquals(testTT[2], fetch, "Child returned from the db should be identical to the one returned when the record was saved.");
                            });
                        }));
                    });
                });
            });
        });
    }



    /**
     * Generates a TransferTask configured with the {@code parentTransferTask} as the parent. The parent's root task id is
     * set as the child's root unless it is null, in which case, the parent is set as the child's root. Random
     * source and dest subpaths are generated for the client based on the source and dest of the parent.
     * @param parentTransferTask the parent {@code TransferTask} of the child
     * @return a new {@link TransferTask} instance initialized wiht the given parent.
     */
    private TransferTask _createChildTestTransferTask(TransferTask parentTransferTask) {
        TransferTask child = _createTestTransferTask();

        if (parentTransferTask != null) {
            child.setParentTaskId(parentTransferTask.getUuid());
            child.setRootTaskId(parentTransferTask.getRootTaskId() == null ? parentTransferTask.getUuid() : parentTransferTask.getRootTaskId());
            child.setSource(parentTransferTask.getSource() + "/" + UUID.randomUUID().toString());
            child.setDest(parentTransferTask.getDest() + "/" + UUID.randomUUID().toString());
            child.setOwner(parentTransferTask.getOwner());
            child.setStatus(parentTransferTask.getStatus());
        }

        return child;

    }

    /**
     * Generates a TransferTask configured with the {@code parentTransferTask} as the parent. The parent's root task id is
     * set as the child's root unless it is null, in which case, the parent is set as the child's root. Random
     * source and dest subpaths are generated for the client based on the source and dest of the parent.
     * @param parentTransferTask the parent {@code TransferTask} serialized as {@link JsonObject} of the child
     * @return a new {@link TransferTask} instance initialized wiht the given parent.
     */
    private TransferTask _createChildTestTransferTask(JsonObject parentTransferTask) {
        return _createChildTestTransferTask(new TransferTask(parentTransferTask));
    }


    /**
     * Async creates {@code count} transfer tasks by calling {@link #addTransferTask(TransferTask)} using a {@link CompositeFuture}.
     * The saved tasks are returned as a {@link JsonArray} to the callback once complete
     * @param count number of tasks to create
     * @param handler the callback with the saved tasks
     */
    protected void addTransferTasks(int count, TransferTask parentTask, Handler<AsyncResult<JsonArray>> handler) {
        JsonArray savedTasks = new JsonArray();

        // generate a future for each task to save
        List<Future> futureTasks = new ArrayList<>();
        for (int i=0; i<count; i++) {
            futureTasks.add(addTransferTask(parentTask));
        }
        // collect them all into a composite future so we wait until they are all complete.
        CompositeFuture.all(futureTasks).setHandler(rh -> {
            if (rh.succeeded()) {
                CompositeFuture composite = rh.result();
                // iterate over the completed futures adding the result from the db call into our saved tasks array
                for (int index = 0; index < composite.size(); index++) {
                    if (composite.succeeded(index) && composite.resultAt(index) != null) {
                        savedTasks.add(composite.<JsonObject>resultAt(index));
                    }
                }
                // resolve the callback handler with the saved tasks
                handler.handle(Future.succeededFuture(savedTasks));
            } else {
                // fail now. this didn't work
                handler.handle(Future.failedFuture(rh.cause()));
            }
        });
    }

    /**
     * Creates a promise that resolves when a new {@link TransferTask} is inserted into the db.
     * @return a future that resolves the saved task.
     */
    protected Future<JsonObject> addTransferTask(TransferTask parentTransferTask) {

        Promise<JsonObject> promise = Promise.promise();

        TransferTask tt = _createChildTestTransferTask(parentTransferTask);

        service.create(tt.getTenantId(), tt, resp -> {
            if (resp.failed()) {
                promise.fail(resp.cause());
            } else {
                promise.complete(resp.result());
            }
        });
        return promise.future();
    }

}
