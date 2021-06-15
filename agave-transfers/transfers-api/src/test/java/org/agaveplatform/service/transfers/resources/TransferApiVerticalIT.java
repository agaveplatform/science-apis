package org.agaveplatform.service.transfers.resources;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers API integration tests")
//@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
public class TransferApiVerticalIT extends BaseTestCase {

    private static final Logger log = LoggerFactory.getLogger(TransferApiVerticalIT.class);
    private TransferTaskDatabaseService dbService;
    private static RequestSpecification requestSpecification;


    /**
     * Async creates {@code count} transfer tasks by calling {@link #addTransferTask()} using a {@link CompositeFuture}.
     * The saved tasks are returned as a {@link JsonArray} to the callback once complete
     *
     * @param count   number of tasks to create
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

    @Test
    @Disabled
    @DisplayName("List web root says hello")
    void register(Vertx vertx, VertxTestContext ctx) {
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();

        dbService = TransferTaskDatabaseService.createProxy(vertx, config.getString(CONFIG_TRANSFERTASK_DB_QUEUE));

        dbService.deleteAll(TENANT_ID, ctx.succeeding(deleteAllTransferTask -> {
            TransferAPIVertical apiVert = null;
            try {
                apiVert = new TransferAPIVertical(vertx);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            vertx.deployVerticle(apiVert, options, ctx.succeeding(apiId -> {

                apiDeploymentCheckpoint.flag();

                RequestSpecification requestSpecification = new RequestSpecBuilder()
                        //.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
                        .setBaseUri("http://localhost:" + port + "/")
                        .build();

                //            String token = this.makeJwtToken(TEST_USERNAME);
                ctx.verify(() -> {
                    String response = given()
                            .spec(requestSpecification)
                            //                        .auth().oauth2(token)
                            //                        .header(TEST_JWT_HEADER, token)
                            .get("/")
                            .then()
                            .assertThat()
                            .statusCode(200)
                            .extract()
                            .asString();

                    assertThat(response).contains("Hello");
                    requestCheckpoint.flag();
                });
            }));
        }));
    }

    @Test
    @DisplayName("Create new transfer task")
    @Disabled
    void create(Vertx vertx, VertxTestContext ctx) {
        TransferTask tt = _createTestTransferTask();
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();

        vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
            dbDeploymentCheckpoint.flag();

            dbService = TransferTaskDatabaseService.createProxy(vertx, config.getString(CONFIG_TRANSFERTASK_DB_QUEUE));

            dbService.deleteAll(TENANT_ID, ctx.succeeding(deleteAllTransferTask -> {
                TransferAPIVertical apiVert = null;
                try {
                    apiVert = new TransferAPIVertical(vertx);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                vertx.deployVerticle(apiVert, options, ctx.succeeding(apiId -> {
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
                                .header(TEST_JWT_HEADER, token)
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
        }));
    }

    @Test
    @DisplayName("List transfer tasks by admin")
    @Disabled
    void getAllForAdmin(Vertx vertx, VertxTestContext ctx) {

        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint testDataCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();


        vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
            dbDeploymentCheckpoint.flag();

            dbService = TransferTaskDatabaseService.createProxy(vertx, config.getString(CONFIG_TRANSFERTASK_DB_QUEUE));

            dbService.deleteAll(TENANT_ID, ctx.succeeding(deleteAllTransferTask -> {
                // fetch saved transfer tasks to request from the service in our test
                addTransferTasks(10, taskReply -> {
                testDataCheckpoint.flag();
                if (taskReply.failed()) {
                    ctx.failNow(taskReply.cause());
                } else {

                    // result should have our tasks
                    JsonArray testTransferTasks = taskReply.result();

                    vertx.deployVerticle(TransferAPIVertical.class, options, ctx.succeeding(apiId -> {
                        apiDeploymentCheckpoint.flag();

                        RequestSpecification requestSpecification = new RequestSpecBuilder()
                                //.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
                                .setBaseUri("http://localhost:" + port + "/")
                                .build();

                        String adminToken = this.makeTestJwt(TEST_ADMIN_USERNAME, "Internal/" + TENANT_ID.replace(".", "_") + "-services-admin");
                        SoftAssertions softly = new SoftAssertions();
                        ctx.verify(() -> {
                            String response = given()
                                    .spec(requestSpecification)
                                    .header(TEST_JWT_HEADER, adminToken)
                                    .contentType(ContentType.JSON)
                                    .queryParam("naked", true)
                                    .when()
                                    .get("api/transfers")
                                    .then()
                                    .assertThat()
                                    .statusCode(200)
                                    .extract()
                                    .asString();
                            JsonArray responseJson = new JsonArray(response);

                            // same transfer tasks ids should be in the response
                            List<String> responseUuids = responseJson.stream().map(x -> ((JsonObject) x).getString("uuid")).collect(Collectors.toList());
                            List<String> testTransferTaskUuids = responseJson.stream().map(x -> ((JsonObject) x).getString("uuid")).collect(Collectors.toList());

                            softly.assertThat(responseJson.size()).as("Owner listing result size").isEqualTo(testTransferTasks.size());
                            softly.assertThat(responseUuids).as("Owner listing result contents").containsAll(testTransferTaskUuids);
                            softly.assertAll();

                            requestCheckpoint.flag();

                        });
                    }));
                }
            });
            }));
        }));
    }

    @Test
    @DisplayName("List transfer tasks by user")
    @Disabled
    void getAllForUser(Vertx vertx, VertxTestContext ctx) {

        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint testDataCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();

        vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
            dbDeploymentCheckpoint.flag();

            dbService = TransferTaskDatabaseService.createProxy(vertx, config.getString(CONFIG_TRANSFERTASK_DB_QUEUE));

            dbService.deleteAll(TENANT_ID, ctx.succeeding(deleteAllTransferTask -> {
                // fetch saved transfer tasks to request from the service in our test
                addTransferTasks(10, taskReply -> {
                    testDataCheckpoint.flag();
                    if (taskReply.failed()) {
                        ctx.failNow(taskReply.cause());
                    } else {

                        // result should have our tasks
                        JsonArray testTransferTasks = taskReply.result();

                        vertx.deployVerticle(TransferAPIVertical.class, options, ctx.succeeding(apiId -> {
                            apiDeploymentCheckpoint.flag();

                            RequestSpecification requestSpecification = new RequestSpecBuilder()
                                    //.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
                                    .setBaseUri("http://localhost:" + port + "/")
                                    .build();

                            String otherUserToken = this.makeTestJwt(TEST_OTHER_USERNAME);
                            String ownerToken = this.makeTestJwt(TEST_USERNAME);
                            SoftAssertions softly = new SoftAssertions();
                            ctx.verify(() -> {
                                String response = given()
                                        .spec(requestSpecification)
                                        .header(TEST_JWT_HEADER, otherUserToken)
                                        .contentType(ContentType.JSON)
                                        .queryParam("naked", true)
                                        .when()
                                        .get("api/transfers")
                                        .then()
                                        .assertThat()
                                        .statusCode(200)
                                        .extract()
                                        .asString();
                                JsonArray responseJson = new JsonArray(response);

                                softly.assertThat(responseJson.size()).as("Unshared user listing response size").isEqualTo(0);

                                response = given()
                                        .spec(requestSpecification)
                                        .header(TEST_JWT_HEADER, ownerToken)
                                        .contentType(ContentType.JSON)
                                        .queryParam("naked", true)
                                        .when()
                                        .get("api/transfers")
                                        .then()
                                        .assertThat()
                                        .statusCode(200)
                                        .extract()
                                        .asString();
                                responseJson = new JsonArray(response);

                                softly.assertThat(responseJson.size()).as("Owner listing response size").isEqualTo(testTransferTasks.size());
                                List<String> responseUuids = responseJson.stream().map(x -> ((JsonObject) x).getString("uuid")).collect(Collectors.toList());
                                List<String> testTransferTaskUuids = responseJson.stream().map(x -> ((JsonObject) x).getString("uuid")).collect(Collectors.toList());

                                // same transfer tasks ids should be in the response
                                softly.assertThat(responseUuids).as("Owner listing response contents").containsAll(testTransferTaskUuids);
                                softly.assertAll();

                                requestCheckpoint.flag();
                            });
                        }));
                    }
                });
            }));
        }));
    }

    @Test
    @DisplayName("Delete transfer task by admin")
    @Disabled
    void deleteForAdmin(Vertx vertx, VertxTestContext ctx) {

        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint testDataCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();

        vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
            dbDeploymentCheckpoint.flag();

            dbService = TransferTaskDatabaseService.createProxy(vertx, config.getString(CONFIG_TRANSFERTASK_DB_QUEUE));

            dbService.deleteAll(TENANT_ID, ctx.succeeding(deleteAllTransferTask -> {
                // fetch saved transfer tasks to request from the service in our test
                addTransferTasks(10, taskReply -> {
                    testDataCheckpoint.flag();
                    if (taskReply.failed()) {
                        ctx.failNow(taskReply.cause());
                    } else {

                        // result should have our tasks
                        JsonArray testTransferTasks = taskReply.result();

                        vertx.deployVerticle(TransferAPIVertical.class, options, ctx.succeeding(apiId -> {
                            apiDeploymentCheckpoint.flag();

                            RequestSpecification requestSpecification = new RequestSpecBuilder()
                                    //.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
                                    .setBaseUri("http://localhost:" + port + "/")
                                    .build();

                            String adminTestUuid = testTransferTasks.getJsonObject(0).getString("uuid");
                            String adminUserToken = this.makeTestJwt(TEST_ADMIN_USERNAME, "Internal/" + TENANT_ID.replace(".", "_") + "-services-admin");

                            ctx.verify(() -> {
                                String response = given()
                                        .spec(requestSpecification)
                                        .header(TEST_JWT_HEADER, adminUserToken)
                                        .contentType(ContentType.JSON)
                                        .when()
                                        .delete("api/transfers/" + adminTestUuid)
                                        .then()
                                        .assertThat()
                                        .statusCode(203)
                                        .extract()
                                        .asString();
//                            JsonObject responseJson = new JsonObject(response);

                                response = given()
                                        .spec(requestSpecification)
                                        .header(TEST_JWT_HEADER, adminUserToken)
                                        .contentType(ContentType.JSON)
                                        .when()
                                        .get("api/transfers/" + adminTestUuid)
                                        .then()
                                        .assertThat()
                                        .statusCode(404)
                                        .extract()
                                        .asString();
//                            responseJson = new JsonObject(response);

                                String ownerTestUuid = testTransferTasks.getJsonObject(1).getString("uuid");
                                String ownerToken = this.makeTestJwt(TEST_USERNAME);
                                response = given()
                                        .spec(requestSpecification)
                                        .header(TEST_JWT_HEADER, ownerToken)
                                        .contentType(ContentType.JSON)
                                        .when()
                                        .delete("api/transfers/" + ownerTestUuid)
                                        .then()
                                        .assertThat()
                                        .statusCode(203)
                                        .extract()
                                        .asString();
//                            responseJson = new JsonObject(response);

                                response = given()
                                        .spec(requestSpecification)
                                        .header(TEST_JWT_HEADER, ownerToken)
                                        .contentType(ContentType.JSON)
                                        .when()
                                        .delete("api/transfers/" + ownerTestUuid)
                                        .then()
                                        .assertThat()
                                        .statusCode(404)
                                        .extract()
                                        .asString();
//                            responseJson = new JsonObject(response);

                                String otherTestUuid = testTransferTasks.getJsonObject(2).getString("uuid");
                                String otherUserToken = this.makeTestJwt(TEST_OTHER_USERNAME);
                                response = given()
                                        .spec(requestSpecification)
                                        .header(TEST_JWT_HEADER, otherUserToken)
                                        .contentType(ContentType.JSON)
                                        .when()
                                        .delete("api/transfers/" + otherTestUuid)
                                        .then()
                                        .assertThat()
                                        .statusCode(403)
                                        .extract()
                                        .asString();
//                            responseJson = new JsonObject(response);

                                response = given()
                                        .spec(requestSpecification)
                                        .header(TEST_JWT_HEADER, otherUserToken)
                                        .contentType(ContentType.JSON)
                                        .when()
                                        .get("api/transfers/" + otherTestUuid)
                                        .then()
                                        .assertThat()
                                        .statusCode(403)
                                        .extract()
                                        .asString();
//                            responseJson = new JsonObject(response);


//                            softly.assertThat(responseJson.size()).as("Owner listing response size").isEqualTo(testTransferTasks.size());
//                            List<String> responseUuids = responseJson.stream().map(x -> ((JsonObject) x).getString("uuid")).collect(Collectors.toList());
//                            List<String> testTransferTaskUuids = responseJson.stream().map(x -> ((JsonObject) x).getString("uuid")).collect(Collectors.toList());
//
//                            // same transfer tasks ids should be in the response
//                            softly.assertThat(responseUuids).as("Owner listing response contents").containsAll(testTransferTaskUuids);
                                requestCheckpoint.flag();

                            });
                        }));
                    }
                });
            }));
        }));
    }

    @Test
    @DisplayName("Cancel transfer task by admin")
    @Disabled
    void cancelTaskForAdmin(Vertx vertx, VertxTestContext ctx) {

        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint testDataCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();

        vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
            dbDeploymentCheckpoint.flag();

            dbService = TransferTaskDatabaseService.createProxy(vertx, config.getString(CONFIG_TRANSFERTASK_DB_QUEUE));

            dbService.deleteAll(TENANT_ID, ctx.succeeding(deleteAllTransferTask -> {
                // fetch saved transfer tasks to request from the service in our test
                addTransferTasks(10, taskReply -> {
                    testDataCheckpoint.flag();
                    if (taskReply.failed()) {
                        ctx.failNow(taskReply.cause());
                    } else {

                        // result should have our tasks
                        JsonArray testTransferTasks = taskReply.result();

                        vertx.deployVerticle(TransferAPIVertical.class, options, ctx.succeeding(apiId -> {
                            apiDeploymentCheckpoint.flag();

                            RequestSpecification requestSpecification = new RequestSpecBuilder()
                                    //.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
                                    .setBaseUri("http://localhost:" + port + "/")
                                    .build();

                            String adminTestUuid = testTransferTasks.getJsonObject(0).getString("uuid");
                            String adminUserToken = this.makeTestJwt(TEST_ADMIN_USERNAME, "Internal/" + TENANT_ID.replace(".", "_") + "-services-admin");
                            SoftAssertions softly = new SoftAssertions();

                            ctx.verify(() -> {
                                String response = given()
                                        .spec(requestSpecification)
                                        .header(TEST_JWT_HEADER, adminUserToken)
                                        .contentType(ContentType.JSON)
                                        .when()
                                        .post("api/transfers/" + adminTestUuid + "/cancel")
                                        .then()
                                        .assertThat()
                                        .statusCode(203)
                                        .extract()
                                        .asString();

                                String afterCancelResponse = given()
                                        .spec(requestSpecification)
                                        .header(TEST_JWT_HEADER, adminUserToken)
                                        .contentType(ContentType.JSON)
                                        .when()
                                        .get("api/transfers/" + adminTestUuid)
                                        .then()
                                        .assertThat()
                                        .statusCode(200)
                                        .extract()
                                        .asString();
                                JsonObject responseJson = new JsonObject(afterCancelResponse);
                                softly.assertThat(responseJson.getJsonObject("result").getString("uuid")).as("Admin cancel task").isEqualTo(adminTestUuid);
                                softly.assertThat(responseJson.getJsonObject("result").getString("status")).as("Admin cancel task status is updated to CANCELLED").isEqualTo(TransferStatusType.CANCELLED.name());
                                softly.assertAll();

                                requestCheckpoint.flag();

                            });
                        }));
                    }
                });
            }));
        }));
    }

}