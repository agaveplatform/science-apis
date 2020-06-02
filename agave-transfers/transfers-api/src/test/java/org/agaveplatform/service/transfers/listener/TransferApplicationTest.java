package org.agaveplatform.service.transfers.listener;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.jwt.JWTOptions;
import io.vertx.ext.web.handler.impl.AgaveJWTAuthProviderImpl;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.ext.web.client.WebClient;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.TransferApplication;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.resources.TransferAPIVertical;
import org.agaveplatform.service.transfers.resources.TransferApiVerticalTest;
import org.agaveplatform.service.transfers.util.CryptoHelper;
import org.aspectj.lang.annotation.Before;
import org.iplantc.service.common.Settings;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.restassured.RestAssured.given;
import static java.util.Arrays.asList;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(VertxExtension.class)
@DisplayName("TransferApplication Tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
class TransferApplicationTest extends BaseTestCase {
	private static final Logger log = LoggerFactory.getLogger(TransferApplicationTest.class);
	private TransferTaskDatabaseService dbService;

	@Override
	public int getPort() {
		return 8085;
	}

	@BeforeEach
	protected void beforeEach(Vertx vertx, VertxTestContext ctx) {
		System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
		Logger log = LoggerFactory.getLogger(TransferApplication.class);

		int poolSize = 1;
		int instanceSize = 1;

		log.debug("Starting the app with config: " + config.encodePrettily());

		vertx.deployVerticle("org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle", new DeploymentOptions().setConfig(config), ctx.succeeding(dbId -> {

			vertx.deployVerticle("org.agaveplatform.service.transfers.resources.TransferAPIVertical", new DeploymentOptions().setConfig(config), ctx.succeeding(apiId -> {

				if (apiId != null) {
					log.info("TransferAPIVertical ({}) started on port {}", apiId, getPort());

					DeploymentOptions localOptions = new DeploymentOptions()
							.setWorkerPoolName("streaming-task-worker-pool")
							.setWorkerPoolSize(poolSize)
							.setInstances(instanceSize)
							.setWorker(true);

					//Deploy the TransferTaskAssignedListener vertical
					vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferTaskAssignedListener",
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
																									vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferErrorTaskListener",
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
																																																								} else {
																																																									System.out.println("TransferWatchListener Deployment failed !");
																																																								}
																																																							});
																																																					System.out.println("TransferHealthcheckListener Deployment id is " + res12.result());
																																																				} else {
																																																					System.out.println("TransferHealthcheckListener Deployment failed !");
																																																				}
																																																			});
																																																	System.out.println("TransferErrorListener Deployment id is " + res11.result());
																																																} else {
																																																	System.out.println("TransferErrorListener Deployment failed !");
																																																}
																																															});
																																													System.out.println("NotificationListener Deployment id is " + res10.result());
																																												} else {
																																													System.out.println("NotificationListener Deployment failed !");
																																												}
																																											});
																																									System.out.println("TransferTaskPausedListener Deployment id is " + res9.result());
																																								} else {
																																									System.out.println("TransferTaskPausedListener Deployment failed !");
																																								}
																																							});
																																					System.out.println("TransferTaskCancelListener Deployment id is " + res8.result());
																																				} else {
																																					System.out.println("TransferTaskCancelListener Deployment failed !");
																																				}
																																			});
																																	System.out.println("TransferFailureHandler Deployment id is " + res7.result());
																																} else {
																																	System.out.println("TransferFailureHandler Deployment failed !");
																																}
																															});
																													System.out.println("ErrorTaskListener Deployment id is " + res6.result());
																												} else {
																													System.out.println("ErrorTaskListener Deployment failed !");
																												}
																											});
																									System.out.println("TransferCompleteTaskListener Deployment id is " + res5.result());
																								} else {
																									System.out.println("TransferCompleteTaskListener Deployment failed !");
																								}
																							});
																					System.out.println("TransferAllProtocolVertical Deployment id is " + res4.result());
																				} else {
																					System.out.println("TransferAllProtocolVertical Deployment failed !");
																				}
																			});
																	System.out.println("TransferRetryListener Deployment id is " + res3.result());
																} else {
																	System.out.println("TransferRetryListener Deployment failed !");
																}
															});
													System.out.println("TransferTaskCreatedListener Deployment id is " + res1.result());
												} else {
													System.out.println("TransferTaskCreatedListener Deployment failed !");
												}
											});
									System.out.println("TransferTaskAssignedListener Deployment id is " + res0.result());
								} else {
									System.out.println("TransferTaskAssignedListener Deployment failed !");
								}
							});


					ctx.verify(() -> {
						ctx.completeNow();
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

	@AfterEach
	public void afterEach(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	@Test
	//@Disabled
	@DisplayName("testEndToEndTaskAssignmentSmoke")
	void testEndToEndTaskAssignmentSmoke(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = _createTestTransferTask();

		RequestSpecification requestSpecification = new RequestSpecBuilder()
				.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
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

		// TODO: fix this check as it cannot work as written
		vertx.eventBus().consumer(TRANSFERTASK_NOTIFICATION, m -> {
			JsonObject json = (JsonObject) m.body();
			ctx.verify(() -> {
				if (json.getString("event").equalsIgnoreCase(TRANSFERTASK_COMPLETED)) {
					ctx.completeNow();
				}
			});
		});
	}
}

