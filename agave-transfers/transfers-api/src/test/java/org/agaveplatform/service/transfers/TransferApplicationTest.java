package org.agaveplatform.service.transfers;

import com.google.common.io.Files;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.listener.TransferTaskAssignedListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Assert;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.StorageSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.InstanceOfAssertFactories.PATH;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static java.util.Arrays.asList;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.*;

import org.iplantc.service.*;
import org.iplantc.service.systems.*;
import org.iplantc.service.systems.Settings;
import org.iplantc.service.systems.exceptions.SystemArgumentException;
import org.iplantc.service.systems.exceptions.SystemException;
import org.iplantc.service.systems.model.enumerations.AuthConfigType;
import org.iplantc.service.systems.model.enumerations.RemoteSystemType;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.iplantc.service.systems.util.ServiceUtils;


@ExtendWith(VertxExtension.class)
@DisplayName("TransferApplication Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TransferApplicationTest extends BaseTestCase {
	private static final Logger log = LoggerFactory.getLogger(TransferApplicationTest.class);
	private TransferTaskDatabaseService dbService;

	@Override
	public int getPort() {
		return 38085;
	}

	@BeforeAll
	@Override
	public void setUpService(Vertx vertx, VertxTestContext ctx) throws IOException {


		Checkpoint authCheckpoint = ctx.checkpoint();

		// init the jwt auth used in the api calls
		initAuth(vertx, resp -> {
			authCheckpoint.flag();
			if (resp.succeeded()) {
				jwtAuth = resp.result();

				initVerticles(vertx, ctx, vrt -> {
					if (vrt.succeeded()) {
						ctx.verify(() -> {
							assertNotNull(jwtAuth);
							assertNotNull(config);
							assertTrue(vrt.succeeded());
							ctx.completeNow();
						});
					} else {
						log.error("Application deployment failed: {}", vrt.cause().getMessage(), vrt.cause());
						ctx.failNow(vrt.cause());
					}
				});
			} else {
				ctx.failNow(resp.cause());
			}
		});

	}

	protected void initVerticles(Vertx vertx, VertxTestContext ctx, Handler<AsyncResult<Boolean>> handler) {

		System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
		Logger log = LoggerFactory.getLogger(TransferApplication.class);

		int poolSize = 1;
		int instanceSize = 1;

		log.debug("Starting the app with config: " + config.encodePrettily());

		vertx.deployVerticle("org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle", new DeploymentOptions().setConfig(config), ctx.succeeding(dbId -> {

			vertx.deployVerticle("org.agaveplatform.service.transfers.resources.TransferAPIVertical", new DeploymentOptions().setConfig(config), ctx.succeeding(apiId -> {

				if (apiId != null) {
					log.info("TransferAPIVertical ({}) started on port {}", apiId, getPort());

					DeploymentOptions localOptions = new DeploymentOptions().setConfig(config)
							.setWorkerPoolName("streaming-task-worker-pool")
							.setWorkerPoolSize(poolSize)
							.setInstances(instanceSize)
							.setWorker(true);

					//Deploy the TransferTaskAssignedListener vertical
					vertx.deployVerticle(TransferTaskAssignedListener.class.getName(),
							localOptions, res0 -> {
								if (res0.succeeded()) {
									// Deployment TransferTaskCreatedListener verticle
									vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferTaskCreatedListener",
											localOptions, res1 -> {
												if (res1.succeeded()) {
													// Deploy the TransferRetryListener vertical
													vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferRetryListener",
															localOptions, res3 -> {
																if (res3.succeeded()) {
																	// Deploy the TransferAllProtocolVertical vertical
																	vertx.deployVerticle("org.agaveplatform.service.transfers.protocol.TransferAllProtocolVertical",
																			localOptions, res4 -> {
																				if (res4.succeeded()) {
																					// Deploy the TransferCompleteTaskListener vertical
																					vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferCompleteTaskListener",
																							localOptions, res5 -> {
																								if (res5.succeeded()) {
																									// Deploy the TransferErrorTaskListener vertical
																									vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferErrorListener",
																											localOptions, res6 -> {
																												if (res6.succeeded()) {
																													// Deploy the TransferFailureHandler vertical
																													vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferFailureHandler",
																															localOptions, res7 -> {
																																if (res7.succeeded()) {
																																	// Deploy the TransferTaskCancelListener vertical
																																	vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferTaskCancelListener",
																																			localOptions, res8 -> {
																																				if (res8.succeeded()) {
																																					// Deploy the TransferTaskPausedListener vertical
																																					vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferTaskPausedListener",
																																							localOptions, res9 -> {
																																								if (res9.succeeded()) {
																																									// Deploy the NotificationListener vertical
																																									vertx.deployVerticle("org.agaveplatform.service.transfers.listener.NotificationListener",
																																											localOptions, res10 -> {
																																												if (res10.succeeded()) {
																																													// Deploy the TransferErrorListener vertical
																																													vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferErrorListener",
																																															localOptions, res11 -> {
																																																if (res11.succeeded()) {
																																																	// Deploy the TransferHealthcheckListener vertical
																																																	vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferHealthcheckListener",
																																																			localOptions, res12 -> {
																																																				if (res12.succeeded()) {
																																																					// Deploy the TransferWatchListener vertical
																																																					vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferWatchListener",
																																																							localOptions, res13 -> {
																																																								if (res13.succeeded()) {
																																																									System.out.println("TransferWatchListener Deployment id is " + res13.result());
																																																									handler.handle(Future.succeededFuture(true));
																																																								} else {
																																																									System.out.println("TransferWatchListener Deployment failed !");
																																																									handler.handle(Future.failedFuture(res13.cause()));
																																																								}
																																																							});
																																																					System.out.println("TransferHealthcheckListener Deployment id is " + res12.result());
																																																				} else {
																																																					System.out.println("TransferHealthcheckListener Deployment failed !");
																																																					handler.handle(Future.failedFuture(res12.cause()));
																																																				}
																																																			});
																																																	System.out.println("TransferErrorListener Deployment id is " + res11.result());
																																																} else {
																																																	System.out.println("TransferErrorListener Deployment failed !");
																																																	handler.handle(Future.failedFuture(res11.cause()));
																																																}
																																															});
																																													System.out.println("NotificationListener Deployment id is " + res10.result());
																																												} else {
																																													System.out.println("NotificationListener Deployment failed !");
																																													handler.handle(Future.failedFuture(res10.cause()));
																																												}
																																											});
																																									System.out.println("TransferTaskPausedListener Deployment id is " + res9.result());
																																								} else {
																																									System.out.println("TransferTaskPausedListener Deployment failed !");
																																									handler.handle(Future.failedFuture(res9.cause()));
																																								}
																																							});
																																					System.out.println("TransferTaskCancelListener Deployment id is " + res8.result());
																																				} else {
																																					System.out.println("TransferTaskCancelListener Deployment failed !");
																																					handler.handle(Future.failedFuture(res8.cause()));
																																				}
																																			});
																																	System.out.println("TransferFailureHandler Deployment id is " + res7.result());
																																} else {
																																	System.out.println("TransferFailureHandler Deployment failed !");
																																	handler.handle(Future.failedFuture(res7.cause()));
																																}
																															});
																													System.out.println("ErrorTaskListener Deployment id is " + res6.result());
																												} else {
																													System.out.println("ErrorTaskListener Deployment failed !");
																													handler.handle(Future.failedFuture(res6.cause()));
																												}
																											});
																									System.out.println("TransferCompleteTaskListener Deployment id is " + res5.result());
																								} else {
																									System.out.println("TransferCompleteTaskListener Deployment failed !");
																									handler.handle(Future.failedFuture(res5.cause()));
																								}
																							});
																					System.out.println("TransferAllProtocolVertical Deployment id is " + res4.result());
																				} else {
																					System.out.println("TransferAllProtocolVertical Deployment failed !");
																					handler.handle(Future.failedFuture(res4.cause()));
																				}
																			});
																	System.out.println("TransferRetryListener Deployment id is " + res3.result());
																} else {
																	System.out.println("TransferRetryListener Deployment failed !");
																	handler.handle(Future.failedFuture(res3.cause()));
																}
															});
													System.out.println("TransferTaskCreatedListener Deployment id is " + res1.result());
												} else {
													System.out.println("TransferTaskCreatedListener Deployment failed !");
													handler.handle(Future.failedFuture(res1.cause()));
												}
											});
									System.out.println("TransferTaskAssignedListener Deployment id is " + res0.result());
								} else {
									System.out.println("TransferTaskAssignedListener Deployment failed !");
									handler.handle(Future.failedFuture(res0.cause()));
								}
							});


				} else {
					System.out.println("TransferApiVertical deployment failed !\n");
					ctx.verify(() -> {
						ctx.failNow(new Exception("Why?!?!?!?!"));
					});
				}
			}));
		}));
	}

	@AfterAll
	public void afterEach(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	@Test
	@DisplayName("testEndToEndTaskAssignmentSmoke")
	@Disabled
	void testEndToEndTaskAssignmentSmoke(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = _createTestTransferTask();

		RequestSpecification requestSpecification = new RequestSpecBuilder()
				//.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
				.setBaseUri("http://localhost:" + getPort() + "/")
				.build();

		Checkpoint requestCheckpoint = ctx.checkpoint();
		Checkpoint assignedListenerCheckpoint = ctx.checkpoint();

		String token = this.makeTestJwt(TEST_USERNAME);

		String response = given()
				.spec(requestSpecification)
//				.auth().oauth2(token)
				.header("X-JWT-ASSERTION-AGAVE_DEV", token)
                .contentType(ContentType.JSON)
                .body(tt.toJSON())
                .post("api/transfers")
				.then()
				.assertThat()
				.statusCode(201)
				.extract()
				.asString();

		vertx.eventBus().consumer(TRANSFERTASK_NOTIFICATION, m -> {
			JsonObject json = (JsonObject) m.body();
			ctx.verify(() -> {
				if (json.getString("event").equalsIgnoreCase(TRANSFERTASK_NOTIFICATION)) {
					assertTrue(true, "The call succeeded.");
				}
				ctx.completeNow();
			});
		});
		ctx.completeNow();
	}

	@Test
	@DisplayName("testEndToEndTaskAssignment_CancelTest")
	@Disabled
	void testEndToEndTaskAssignment_CancelTest(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = _createTestTransferTask();

		RequestSpecification requestSpecification = new RequestSpecBuilder()
				//.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
				.setBaseUri("http://localhost:" + getPort() + "/")
				.build();

		Checkpoint requestCheckpoint = ctx.checkpoint();
		Checkpoint assignedListenerCheckpoint = ctx.checkpoint();

		String token = this.makeTestJwt(TEST_USERNAME);

		String response = given()
				.spec(requestSpecification)
//				.auth().oauth2(token)
				.header("X-JWT-ASSERTION-AGAVE_DEV", token)
				.contentType(ContentType.JSON)
				.body(tt.toJSON())
				.post("api/transfers")
				.then()
				.assertThat()
				.statusCode(201)
				.extract()
				.asString();

		vertx.eventBus().send(TRANSFERTASK_CANCELLED, tt.toJson());

		vertx.eventBus().consumer(TRANSFERTASK_CANCELED_COMPLETED, m -> {
			JsonObject json = (JsonObject) m.body();
			ctx.verify(() -> {
				if (json.getString("event").equalsIgnoreCase(TRANSFERTASK_CANCELED_COMPLETED)) {
					assertTrue(true, "The call succeeded.");
				}
				ctx.completeNow();
			});
		});
		ctx.completeNow();
	}

	@Test
	@DisplayName("testEndToEndTaskAssignment_PauseTest")
	@Disabled
	void testEndToEndTaskAssignment_PauseTest(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = _createTestTransferTask();

		RequestSpecification requestSpecification = new RequestSpecBuilder()
				//.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
				.setBaseUri("http://localhost:" + getPort() + "/")
				.build();

		Checkpoint requestCheckpoint = ctx.checkpoint();
		Checkpoint assignedListenerCheckpoint = ctx.checkpoint();

		String token = this.makeTestJwt(TEST_USERNAME);

		String response = given()
				.spec(requestSpecification)
//				.auth().oauth2(token)
				.header("X-JWT-ASSERTION-AGAVE_DEV", token)
				.contentType(ContentType.JSON)
				.body(tt.toJSON())
				.post("api/transfers")
				.then()
				.assertThat()
				.statusCode(201)
				.extract()
				.asString();

		vertx.eventBus().send(TRANSFERTASK_PAUSED, tt.toJson());

		vertx.eventBus().consumer(TRANSFERTASK_PAUSED_COMPLETED, m -> {
			JsonObject json = (JsonObject) m.body();
			ctx.verify(() -> {
				if (json.getString("event").equalsIgnoreCase(TRANSFERTASK_PAUSED_COMPLETED)) {
					assertTrue(true, "The call succeeded.");
				}
				ctx.completeNow();
			});
		});
		ctx.completeNow();
	}

	@Test
	@DisplayName("testEndToEndTaskAssignment_PauseTest Fail")
	@Disabled
	void testEndToEndTaskAssignment_PauseTestFail(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = _createTestTransferTask();

		RequestSpecification requestSpecification = new RequestSpecBuilder()
				//.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
				.setBaseUri("http://localhost:" + getPort() + "/")
				.build();

		Checkpoint requestCheckpoint = ctx.checkpoint();
		Checkpoint assignedListenerCheckpoint = ctx.checkpoint();

		String token = this.makeTestJwt(TEST_USERNAME);

		String response = given()
				.spec(requestSpecification)
//				.auth().oauth2(token)
				.header("X-JWT-ASSERTION-AGAVE_DEV", token)
				.contentType(ContentType.JSON)
				.body(tt.toJSON())
				.post("api/transfers")
				.then()
				.assertThat()
				.statusCode(201)
				.extract()
				.asString();

		TransferTask tt2 = _createTestTransferTask();
		vertx.eventBus().send(TRANSFERTASK_PAUSED, tt2.toJson());

		vertx.eventBus().consumer(TRANSFERTASK_PAUSED_COMPLETED, m -> {
			JsonObject json = (JsonObject) m.body();
			ctx.verify(() -> {
				if (json.getString("event").equalsIgnoreCase(TRANSFERTASK_PAUSED_COMPLETED)) {
					assertFalse(false, "The call should fail.");
					log.info("Pause here");
				}
				ctx.completeNow();
			});
		});
		ctx.completeNow();
	}

	@Test
	@DisplayName("testEndToEndTransferTaskFileMovement")
	void testEndToEndTransferTaskFileMovement(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = _createTestTransferTask();
//		StorageSystem system = new StorageSystem();
//		system.getStorageConfig().setRootDir("/");
//		system.getStorageConfig().setHomeDir(Files.createTempDir().getAbsolutePath());


		RequestSpecification requestSpecification = new RequestSpecBuilder()
				//.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
				.setBaseUri("http://localhost:" + getPort() + "/")
				.build();

		Checkpoint requestCheckpoint = ctx.checkpoint();
		Checkpoint assignedListenerCheckpoint = ctx.checkpoint();

		String token = this.makeTestJwt(TEST_USERNAME);

		String response = given()
				.spec(requestSpecification)
//				.auth().oauth2(token)
				.header("X-JWT-ASSERTION-AGAVE_DEV", token)
				.contentType(ContentType.JSON)
				.body(tt.toJSON())
				.post("api/transfers")
				.then()
				.assertThat()
				.statusCode(201)
				.extract()
				.asString();

		vertx.eventBus().consumer(TRANSFERTASK_ASSIGNED, m -> {
			JsonObject json = (JsonObject) m.body();
			ctx.verify(() -> {
				if (json.getString("event").equalsIgnoreCase(TRANSFERTASK_ASSIGNED)) {
					assertTrue(true, "The call succeeded.");
				}
				ctx.completeNow();
			});
		});
		Path destPath = Paths.get(tt.getDest());

		assertTrue(java.nio.file.Files.exists(destPath), "Downloaded file should be present at " + destPath.toString() );

		ctx.completeNow();
	}
}

