package org.agaveplatform.service.transfers.resources;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.impl.AgaveJWTAuthProviderImpl;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.assertj.core.api.SoftAssertions;
import org.hibernate.Session;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemException;
import org.iplantc.service.systems.manager.SystemManager;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageSystem;
import org.json.JSONObject;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;
import static java.util.Arrays.asList;
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
     * Switches to use the priamry config pointing at an active mysql server
     * @return the name of the config file. This can be overridden for integration tests
     */
    protected String getConfigFileName() {
        return "config.json";
    }

    @BeforeAll
    public void setUpService(Vertx vertx, VertxTestContext ctx) throws IOException {
        Checkpoint authCheckpoint = ctx.checkpoint();
        Checkpoint systemsCheckpoint = ctx.checkpoint();
        // init the jwt auth used in the api calls
        initAuth(vertx, resp -> {
            authCheckpoint.flag();
            if (resp.succeeded()) {
                jwtAuth = resp.result();
                ctx.verify(() -> {
                    assertNotNull(jwtAuth);
                    assertNotNull(config);

                    // remove existing systems prior to tests starting
                    clearSystems(clearSystemsResp -> {
                        if (clearSystemsResp.succeeded()) {
                            log.debug("Finished removing remote systems during cleanup.");
                            // now load new systems
                            initSystems(initSystemsResp -> {
                                systemsCheckpoint.flag();;
                                if (initSystemsResp.succeeded()) {
                                    log.debug("Finished loading test systems definitions");
                                    ctx.completeNow();
                                } else {
                                    log.error("Failed to load test systems.", initSystemsResp.cause());
                                    ctx.failNow(initSystemsResp.cause());
                                }
                            });
                        } else {
                            log.error("Failed removing remote systems during cleanup.", clearSystemsResp.cause());
                            ctx.failNow(clearSystemsResp.cause());
                        }
                    });

                });
            } else {
                log.error("Unable to init auth.", resp.cause());
                ctx.failNow(resp.cause());
            }
        });
    }

    @AfterAll
    public void tearDown(Vertx vertx, VertxTestContext ctx) {
        clearSystems(clearSystemsResp -> {
            if (clearSystemsResp.failed()) {
                log.error("Failed removing remote systems during cleanup.", clearSystemsResp.cause());
            }
            vertx.close(ctx.completing());
        });
    }

    /**
     * Deletes all {@link RemoteSystem} definitions from the db
     * @param handler the callback to recieve the results of the system load
     */
    public void clearSystems(Handler<AsyncResult<Boolean>> handler) {
        Session session = null;
        try {
            HibernateUtil.beginTransaction();
            session = HibernateUtil.getSession();
            session.clear();
            HibernateUtil.disableAllFilters();

            session.createQuery("delete RemoteSystem").executeUpdate();
            session.createQuery("delete BatchQueue").executeUpdate();
            session.createQuery("delete StorageConfig").executeUpdate();
            session.createQuery("delete LoginConfig").executeUpdate();
            session.createQuery("delete AuthConfig").executeUpdate();
            session.createQuery("delete SystemRole").executeUpdate();
            session.createQuery("delete CredentialServer").executeUpdate();
            session.createQuery("delete SystemHistoryEvent").executeUpdate();
            session.flush();
            handler.handle(Future.succeededFuture(true));
        } catch (Throwable t) {
            handler.handle(Future.failedFuture(t));
        } finally {
            try { HibernateUtil.commitTransaction(); } catch (Exception ignored) {}
        }
    }

    /**
     * Loads the system definition templates from the src/test/resouces/storage folder and adds them to the db.
     * @param handler the callback to recieve the results of the system load
     * @throws IOException when the system template folder cannot be found
     * @throws URISyntaxException when the system template folder is not present in the classpath
     */
    protected void initSystems(Handler<AsyncResult<List<RemoteSystem>>> handler) {
        try {
            Path systemTemplatePath = Paths.get(TransferApiVerticalIT.class.getClassLoader().getResource("storage").toURI());
            SystemDao systemDao = new SystemDao();
            if (Files.exists(systemTemplatePath)) {
                Files.list(systemTemplatePath).forEach(child -> {
                    if (child.toString().endsWith(".json")) {
                        try (InputStream in = Files.newInputStream(child)) {
                            String systemJson = new String(Files.readAllBytes(child));
                            JSONObject json = new JSONObject(systemJson);
                            StorageSystem system = StorageSystem.fromJSON(json);
                            system.setOwner(TEST_USER);
                            system.setTenantId(TENANT_ID);
                            systemDao.persist(system);
                        } catch (Throwable e) {
                            log.error("Failed to parse system template for {}", child.toString(), e);
                            handler.handle(Future.failedFuture(e));
                        }
                    }
                });

                List<RemoteSystem> systems = systemDao.getAll();
                if (systems.isEmpty()) {
                    handler.handle(Future.failedFuture(new SystemException(
                            "All system templates should have been added to the database. None were found")));
                } else {
                    handler.handle(Future.succeededFuture(systems));
                }
            } else {
                handler.handle(Future.failedFuture(new IOException(
                        "System template folder not found within the current classloader classpath.")));
            }
        } catch (IOException | URISyntaxException e) {
            handler.handle(Future.failedFuture(e));
        }
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

    @Test
    @DisplayName("List web root says hello")
    void register(Vertx vertx, VertxTestContext ctx) {
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
//        Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();

        TransferAPIVertical apiVert = new TransferAPIVertical(vertx);
        vertx.deployVerticle(apiVert, options, ctx.succeeding(apiId -> {

            apiDeploymentCheckpoint.flag();

            RequestSpecification requestSpecification = new RequestSpecBuilder()
                    //.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
                    .setBaseUri("http://localhost:" + port + "/")
                    .build();

            ctx.verify(() -> {
                String response = given()
                        .spec(requestSpecification)
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
    }

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
            TransferAPIVertical apiVert = new TransferAPIVertical(vertx);
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

    @Test
    @DisplayName("List transfer tasks by admin")
    void getAllForAdmin(Vertx vertx, VertxTestContext ctx) {

        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint testDataCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();


        vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
            dbDeploymentCheckpoint.flag();

            dbService = TransferTaskDatabaseService.createProxy(vertx, config.getString(CONFIG_TRANSFERTASK_DB_QUEUE));

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
                                    .header("X-JWT-ASSERTION-AGAVE_DEV", adminToken)
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
    }

    @Test
    @DisplayName("List transfer tasks by user")
    void getAllForUser(Vertx vertx, VertxTestContext ctx) {

        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint testDataCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();


        vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
            dbDeploymentCheckpoint.flag();

            dbService = TransferTaskDatabaseService.createProxy(vertx, config.getString(CONFIG_TRANSFERTASK_DB_QUEUE));

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
                                    .header("X-JWT-ASSERTION-AGAVE_DEV", otherUserToken)
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
                                    .header("X-JWT-ASSERTION-AGAVE_DEV", ownerToken)
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
    }

    @Test
    @DisplayName("Delete transfer task by admin")
    void deleteForAdmin(Vertx vertx, VertxTestContext ctx) {

        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint testDataCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();

        vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
            dbDeploymentCheckpoint.flag();

            dbService = TransferTaskDatabaseService.createProxy(vertx, config.getString(CONFIG_TRANSFERTASK_DB_QUEUE));

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
                                    .header("X-JWT-ASSERTION-AGAVE_DEV", adminUserToken)
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
                                    .header("X-JWT-ASSERTION-AGAVE_DEV", adminUserToken)
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
                                    .header("X-JWT-ASSERTION-AGAVE_DEV", ownerToken)
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
                                    .header("X-JWT-ASSERTION-AGAVE_DEV", ownerToken)
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
                                    .header("X-JWT-ASSERTION-AGAVE_DEV", otherUserToken)
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
                                    .header("X-JWT-ASSERTION-AGAVE_DEV", otherUserToken)
                                    .contentType(ContentType.JSON)
                                    .when()
                                    .get("api/transfers/" + otherTestUuid)
                                    .then()
                                    .assertThat()
                                    .statusCode(403)
                                    .extract()
                                    .asString();
//                            responseJson = new JsonObject(response);

                            requestCheckpoint.flag();
                        });
                    }));
                }
            });
        }));
    }

}