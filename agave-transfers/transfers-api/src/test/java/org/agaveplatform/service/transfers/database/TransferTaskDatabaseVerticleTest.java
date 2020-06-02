package org.agaveplatform.service.transfers.database;

import com.google.common.base.CaseFormat;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
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
import java.util.concurrent.TimeUnit;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers completed task listener integration tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
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
    @Timeout(value = 10, timeUnit = TimeUnit.SECONDS)
    public void crudTest(Vertx vertx, VertxTestContext context) {
        TransferTask testTransferTask = _createTestTransferTask();

        service.create(TENANT_ID, testTransferTask, context.succeeding(createdJsonTransferTask -> {
            Assertions.assertNotNull(createdJsonTransferTask.getLong("id"), "Object returned from create should have not null id");
            Assertions.assertNotNull(createdJsonTransferTask.getInstant("created"), "Object returned from create should have not null created");
            Assertions.assertNotNull(createdJsonTransferTask.getInstant("last_updated"), "Object returned from create should have not null last_updated");
            Assertions.assertEquals(TENANT_ID, createdJsonTransferTask.getString("tenant_id"),"Object returned from create should have same tenant_id as sent");
            Assertions.assertEquals(testTransferTask.getUuid(), createdJsonTransferTask.getString("uuid"),"Object returned from create should have same uuid as sent");
            Assertions.assertEquals(testTransferTask.getSource(), createdJsonTransferTask.getString("source"),"Object returned from create should have same source value as sent");
            Assertions.assertEquals(testTransferTask.getDest(), createdJsonTransferTask.getString("dest"),"Object returned from create should have same dest value as sent");

            service.getById(TENANT_ID, testTransferTask.getUuid(), context.succeeding(getByIdJsonTransferTask -> {
                Assertions.assertEquals(TENANT_ID, getByIdJsonTransferTask.getString("tenant_id"),"Object returned from create should have same tenant_id as original");
                Assertions.assertEquals(testTransferTask.getUuid(), getByIdJsonTransferTask.getString("uuid"),"Object returned from create should have same uuid as original");
                Assertions.assertEquals(testTransferTask.getSource(), getByIdJsonTransferTask.getString("source"),"Object returned from create should have same source value as original");
                Assertions.assertEquals(testTransferTask.getDest(), getByIdJsonTransferTask.getString("dest"),"Object returned from create should have same dest value as original");

                service.updateStatus(TENANT_ID, testTransferTask.getUuid(), TransferStatusType.TRANSFERRING.name(), context.succeeding(updateStatusJsonTransferTask -> {
                    Assertions.assertEquals(TransferStatusType.TRANSFERRING.name(), updateStatusJsonTransferTask.getString("status"),
                            "Object returned from updateStatus should have updated status");
                    Assertions.assertTrue(getByIdJsonTransferTask.getInstant("last_updated").isBefore(updateStatusJsonTransferTask.getInstant("last_updated")),
                            "Object returned from udpate have last_updated value more recent than original");
                    Assertions.assertEquals(createdJsonTransferTask.getLong("id"), updateStatusJsonTransferTask.getLong("id"),
                            "Object returned from create should have same id as original");

                    service.getAll(TENANT_ID, 100, 0, context.succeeding(getAllJsonTransferTaskArray -> {
                        Assertions.assertEquals(1, getAllJsonTransferTaskArray.size(),
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
                                Assertions.assertEquals(1, getAllJsonTransferTaskArray2.size(),
                                        "getAll should only have a single record after updating an existing object." +
                                                "New object was likely added to the db.");
                                Assertions.assertEquals(updateJsonTransferTask.getString("uuid"), getAllJsonTransferTaskArray2.getJsonObject(0).getString("uuid"),
                                        "Object returned by getAll should have the same uuid as the updated object.");


                                service.getById(TENANT_ID, testTransferTask.getUuid(), context.succeeding(getByIdJsonTransferTask2 -> {

                                    Assertions.assertEquals(TENANT_ID, getByIdJsonTransferTask2.getString("tenant_id"),"Object returned from create should have same tenant_id as original");
                                    Assertions.assertEquals(createdJsonTransferTask.getLong("id"), getByIdJsonTransferTask2.getLong("id"),"Object returned from create should have same id as original");
                                    Assertions.assertEquals(testTransferTask.getUuid(), getByIdJsonTransferTask2.getString("uuid"),"Object returned from create should have same uuid as original");
                                    Assertions.assertEquals(testTransferTask.getSource(), getByIdJsonTransferTask2.getString("source"),"Object returned from create should have same source value as original");
                                    Assertions.assertEquals(testTransferTask.getDest(), getByIdJsonTransferTask2.getString("dest"),"Object returned from create should have same dest value as original");
                                    Assertions.assertTrue(updateStatusJsonTransferTask.getInstant("last_updated").isBefore(getByIdJsonTransferTask2.getInstant("last_updated")),"Object returned from udpate have last_updated value more recent than original");
                                    // verify the updated values are present in the response
                                    updatedJsonObject
                                            .stream()
                                            .forEach(entry -> {
                                                if (entry.getKey().equalsIgnoreCase("startTime") || entry.getKey().equalsIgnoreCase("endTime")) {
                                                    Assertions.assertEquals(Instant.parse(((String)entry.getValue())).getEpochSecond(), getByIdJsonTransferTask2.getInstant(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey())).getEpochSecond(),
                                                            entry.getKey() + " should be updated in the response from the service");
                                                } else {
                                                    Assertions.assertEquals(entry.getValue(), getByIdJsonTransferTask2.getValue(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey())),
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
                                            Assertions.assertTrue(getAllJsonTransferTaskArray3.isEmpty(), "No records should be returned from getAll after deleting the test object");
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

}
