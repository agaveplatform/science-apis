package org.agaveplatform.service.transfers;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.resources.TransferAPIVertical;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static io.restassured.RestAssured.given;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers API integration tests")
//@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
public class TransferApplicationIT extends BaseTestCase {

    private static final Logger log = LoggerFactory.getLogger(TransferApplicationTest.class);
    private TransferTaskDatabaseService dbService;
    private static RequestSpecification requestSpecification;

    @Test
    @DisplayName("Create new transfer task")
    void create(Vertx vertx, VertxTestContext ctx) {

        TransferTask tt = _createTestTransferTask();
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();

        vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
            dbDeploymentCheckpoint.flag();
//            TransferAPIVertical apiVert = new TransferAPIVertical(vertx);
            vertx.deployVerticle(TransferAPIVertical.class.getName(), options, ctx.succeeding(apiId -> {
                apiDeploymentCheckpoint.flag();

                RequestSpecification requestSpecification = new RequestSpecBuilder()
                        //.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
                        .setBaseUri("http://localhost:" + port + "/")
                        .build();

                String token = this.makeTestJwt(TEST_USERNAME);
                SoftAssertions softly = new SoftAssertions();
                ctx.verify(() -> {
                    String response = given()
                            .spec(requestSpecification)
                            .header("X-JWT-ASSERTION-AGAVE_DEV", token)
                            .contentType(ContentType.JSON)
                            .queryParam("naked", true)
                            .body(tt.toJSON())
                            .when()
                            .post("api/transfers")
                            .then()
                            .assertThat()
                            .statusCode(201)
                            .extract()
                            .asString();
                    TransferTask respTask = new TransferTask(new JsonObject(response));
                    softly.assertThat(respTask).as("Returned task is not null").isNotNull();
                    softly.assertThat(respTask.getSource()).as("Returned task source is equivalent to the original source").isEqualTo(tt.getSource());
                    softly.assertThat(respTask.getDest()).as("Returned task dest is equivalent to the original dest").isEqualTo(tt.getDest());
                    softly.assertThat(respTask.getOwner()).as("Returned task owner is equivalent to the jwt user").isEqualTo(TEST_USERNAME);
                    softly.assertThat(respTask.getTenantId()).as("Returned task tenant id is equivalent to the jwt tenant id").isEqualTo(TENANT_ID);
                    softly.assertAll();
                    requestCheckpoint.flag();
                });
            }));
        }));

    }


    /**
     * Async creates {@code count} transfer tasks by calling {@link #addTransferTask()} using a {@link CompositeFuture}.
     * The saved tasks are returned as a {@link JsonArray} to the callback once complete
     * @param count number of tasks to create
     * @param handler the callback with the saved tasks
     */
    protected void addTransferTasks(int count, Handler<AsyncResult<JsonArray>> handler) {
        JsonArray savedTasks = new JsonArray();

        // generate a future for each task to save
        List<Future> futureTasks = new ArrayList<>();
        for (int i=0; i<count; i++) {
            futureTasks.add(addTransferTask());
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
                log.error("Failed to insert tests records into the db", rh.cause());
                handler.handle(Future.failedFuture(rh.cause()));
            }
        });
    }

    /**
     * Creates a promise that resolves when a new {@link TransferTask} is inserted into the db.
     * @return a future that resolves the saved task.
     */
    protected Future<JsonObject> addTransferTask() {

        Promise<JsonObject> promise = Promise.promise();

        TransferTask tt = _createTestTransferTask();

        dbService.create(tt.getTenantId(), tt, resp -> {
            if (resp.failed()) {
                promise.fail(resp.cause());
            } else {
                promise.complete(resp.result());
            }
        });
        return promise.future();
    }

}