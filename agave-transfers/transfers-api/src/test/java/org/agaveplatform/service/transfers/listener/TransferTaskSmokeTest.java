package org.agaveplatform.service.transfers.listener;


import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.jwt.JWTOptions;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.protocol.TransferHttpVertical;
import org.agaveplatform.service.transfers.protocol.TransferSftpVertical;
//import org.agaveplatform.service.transfers.resources.FileTransferCreateServiceImpl;
import org.agaveplatform.service.transfers.resources.TransferAPIVertical;
//import org.agaveplatform.service.transfers.resources.TransferTaskUnaryImpl;
//import org.agaveplatform.service.transfers.streaming.StreamingFileTaskImpl;
import org.agaveplatform.service.transfers.util.CryptoHelper;
import org.hibernate.annotations.Check;
import org.iplantc.service.common.Settings;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


@ExtendWith(VertxExtension.class)
@DisplayName("Transfers tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
public class TransferTaskSmokeTest extends BaseTestCase {
	private static final Logger log = LoggerFactory.getLogger(TransferTaskSmokeTest.class);

	private Vertx vertx;
	private JWTAuth jwtAuth;
	private static RequestSpecification requestSpecification;
	private TransferTaskDatabaseService dbService;
	public static final String HOST = "foo.bar";
	public static final String PROTOCOL = "http";

	TransferErrorListener getMockErrListenerInstance(Vertx vertx) {
		TransferErrorListener listener = spy(new TransferErrorListener(vertx));
		when(listener.getEventChannel()).thenCallRealMethod();
		when(listener.getVertx()).thenReturn(vertx);
		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}

	TransferFailureHandler getMockFailListenerInstance(Vertx vertx) {
		TransferFailureHandler listener = spy(new TransferFailureHandler(vertx));
		when(listener.getEventChannel()).thenCallRealMethod();
		when(listener.getVertx()).thenReturn(vertx);
		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}

	InteruptEventListener getMockInteruptListenerInstance(Vertx vertx) {
		InteruptEventListener listener = spy(new InteruptEventListener(vertx));
		return listener;
	}
	NotificationListener getMockNotificationListenerInstance(Vertx vertx) {
		NotificationListener listener = Mockito.spy(new NotificationListener(vertx));
		return listener;
	}
	TransferCompleteTaskListener getMockTCTListenerInstance(Vertx vertx) {
		TransferCompleteTaskListener listener = spy(new TransferCompleteTaskListener(vertx));
		return listener;
	}
	TransferTaskAssignedListener getMockTTAListenerInstance(Vertx vertx) {
		TransferTaskAssignedListener listener = spy(new TransferTaskAssignedListener(vertx));
		return listener;
	}
	TransferTaskCancelListener getMockTTCancelListenerInstance(Vertx vertx) {
		TransferTaskCancelListener listener = spy(new TransferTaskCancelListener(vertx));
		return listener;
	}
	TransferTaskCreatedListener getMockTTCListenerInstance(Vertx vertx) {
		TransferTaskCreatedListener listener = spy(new TransferTaskCreatedListener(vertx));
		return listener;
	}
	TransferTaskPausedListener getMockTTPausedListenerInstance(Vertx vertx) {
		TransferTaskPausedListener listener = spy(new TransferTaskPausedListener(vertx));
		return listener;
	}
	TransferSftpVertical getMockSFTPVerticalInstance(Vertx vertx) {
		TransferSftpVertical listener = spy(new TransferSftpVertical(vertx));
		return listener;
	}

	TransferHttpVertical getMockHTTPVerticalInstance(Vertx vertx) {
		TransferHttpVertical listener = spy(new TransferHttpVertical(vertx));
		return listener;
	}

	TransferWatchListener getMockWatchListenerInstance(Vertx vertx) {
		TransferWatchListener listener = spy(new TransferWatchListener(vertx));
		return listener;
	}
	TransferHealthcheckListener getMockHealthcheckListenerInstance(Vertx vertx) {
		TransferHealthcheckListener listener = spy(new TransferHealthcheckListener(vertx));
		return listener;
	}

	TransferAPIVertical getMockTransServUIVertInstance(Vertx vertx) {
		TransferAPIVertical listener = spy(new TransferAPIVertical(vertx));
		return listener;
	}
	TransferRetryListener getMockRetryListenerInstance(Vertx vertx) {
		TransferRetryListener listener = spy(new TransferRetryListener(vertx));
		return listener;
	}

	/**
	 * Generates a JWT token to authenticate to the service. Token is signed using the
	 * test private_key.pem and public_key.pem files in the resources directory.
	 *
	 * @param username Name of the test user
	 * @return signed jwt token
	 */
	private String makeJwtToken(String username) {
		// Add wso2 claims set
		JsonObject claims = new JsonObject()
				.put("http://wso2.org/claims/subscriber", username)
				.put("http://wso2.org/claims/applicationid", "-9999")
				.put("http://wso2.org/claims/applicationname", "agaveops")
				.put("http://wso2.org/claims/applicationtier", "Unlimited")
				.put("http://wso2.org/claims/apicontext", "/internal")
				.put("http://wso2.org/claims/version", Settings.SERVICE_VERSION)
				.put("http://wso2.org/claims/tier", "Unlimited")
				.put("http://wso2.org/claims/keytype", "PRODUCTION")
				.put("http://wso2.org/claims/usertype", "APPLICATION_USER")
				.put("http://wso2.org/claims/enduser", username)
				.put("http://wso2.org/claims/enduserTenantId", "-9999")
				.put("http://wso2.org/claims/emailaddress", "testuser@example.com")
				.put("http://wso2.org/claims/fullname", "Test User")
				.put("http://wso2.org/claims/givenname", "Test")
				.put("http://wso2.org/claims/lastname", "User")
				.put("http://wso2.org/claims/primaryChallengeQuestion", "N/A")
				.put("http://wso2.org/claims/role", "Internal/everyone,Internal/subscriber")
				.put("http://wso2.org/claims/title", "N/A");

		JWTOptions jwtOptions = new JWTOptions()
				.setAlgorithm("RS256")
				.setExpiresInMinutes(10_080) // 7 days
				.setIssuer("transfers-api-integration-tests")
				.setSubject(username);
		return jwtAuth.generateToken(claims, jwtOptions);
	}


	/**
	 * Initializes the jwt auth options and the
	 * @throws IOException when the key cannot be read
	 */
	private void initAuth() throws IOException {
		JWTAuthOptions jwtAuthOptions = new JWTAuthOptions()
				.addPubSecKey(new PubSecKeyOptions()
						.setAlgorithm("RS256")
						.setPublicKey(CryptoHelper.publicKey())
						.setSecretKey(CryptoHelper.privateKey()));

		jwtAuth = JWTAuth.create(vertx, jwtAuthOptions);
	}

	@Test
	@DisplayName("Single file transfer task smoke test")
	public void singleFileTransferSmokeTest(Vertx vertx, VertxTestContext ctx) {
		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.COMPLETED);
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());
		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());
		transferTask.setRootTaskId(parentTask.getUuid());
		transferTask.setParentTaskId(parentTask.getUuid());


		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskCreatedListener transferTaskCreatedListener = getMockTTCListenerInstance(vertx);
		TransferTaskAssignedListener transferTaskAssignedListener = getMockTTAListenerInstance(vertx);
		TransferTaskCancelListener transferTaskCancelListener = getMockTTCancelListenerInstance(vertx);
		TransferSftpVertical transferSftpVertical = getMockSFTPVerticalInstance(vertx);
		TransferHttpVertical transferHttpVertical = getMockHTTPVerticalInstance(vertx);
		TransferCompleteTaskListener transferCompleteTaskListener = getMockTCTListenerInstance(vertx);
		TransferErrorListener errorTaskListener = getMockErrListenerInstance(vertx);
		InteruptEventListener interuptEventListener = getMockInteruptListenerInstance(vertx);
		NotificationListener notificationListener = getMockNotificationListenerInstance(vertx);
		TransferTaskPausedListener transferTaskPausedListener = getMockTTPausedListenerInstance(vertx);
		TransferWatchListener transferWatchListener = getMockWatchListenerInstance(vertx);
		TransferHealthcheckListener transferHealthcheckListener = getMockHealthcheckListenerInstance(vertx);
		//FileTransferCreateServiceImpl fileTransferCreateService = getMockFTCSIVerticalInstance(vertx);
		TransferAPIVertical transferAPIVertical = getMockTransServUIVertInstance(vertx);
		TransferRetryListener transferRetryListener = getMockRetryListenerInstance(vertx);
		TransferFailureHandler transferFailureHandler = getMockFailListenerInstance(vertx);

		Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint createdDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint assignedDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint httpDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint requestCheckpoint = ctx.checkpoint();
		Checkpoint completedDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint watchCheckpoint = ctx.checkpoint();
		Checkpoint healthcheckCheckpoint = ctx.checkpoint();
		Checkpoint retryCheckpoint = ctx.checkpoint();
		Checkpoint errorCheckpoint = ctx.checkpoint();
		Checkpoint failureCheckpoint = ctx.checkpoint();
		Checkpoint httpVertCheckpoint = ctx.checkpoint();
		Checkpoint sftpVertCheckpoint = ctx.checkpoint();
		Checkpoint allVertCheckpoint = ctx.checkpoint();



		DeploymentOptions options = new DeploymentOptions().setConfig(config);

		vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
			dbDeploymentCheckpoint.flag();

			vertx.deployVerticle(transferAPIVertical, options, ctx.succeeding(apiId -> {
				apiDeploymentCheckpoint.flag();

				vertx.deployVerticle(transferTaskCreatedListener, options, ctx.succeeding(createdId -> {
					createdDeploymentCheckpoint.flag();

					vertx.deployVerticle(transferTaskAssignedListener, options, ctx.succeeding(assignedId -> {
						assignedDeploymentCheckpoint.flag();

						vertx.deployVerticle(transferHttpVertical, options, ctx.succeeding(httpId -> {
							httpDeploymentCheckpoint.flag();

							vertx.deployVerticle(transferCompleteTaskListener, options, ctx.succeeding(completedId -> {
								completedDeploymentCheckpoint.flag();

								RequestSpecification requestSpecification = new RequestSpecBuilder()
										//								.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
										.setBaseUri("http://localhost:" + port + "/")
										.build();

								vertx.deployVerticle(transferWatchListener, options, ctx.succeeding(watchCompletedId -> {
									watchCheckpoint.flag();

									vertx.deployVerticle(transferHealthcheckListener, options, ctx.succeeding(healthcheckCompletedId -> {
										healthcheckCheckpoint.flag();

										vertx.deployVerticle(transferRetryListener, options, ctx.succeeding(retryId -> {
											retryCheckpoint.flag();

											vertx.deployVerticle(transferSftpVertical, options, ctx.succeeding(sftpId -> {
												sftpVertCheckpoint.flag();

												ctx.verify(() -> {
													String response = given()
															.spec(requestSpecification)
															//										.header("X-JWT-ASSERTION-AGAVE_DEV", this.makeJwtToken(TEST_USERNAME))
															.contentType(ContentType.JSON)
															.body(parentTask.toJSON())
															.when()
															.post("api/transfers")
															.then()
															.assertThat()
															.statusCode(201)
															.extract()
															.asString();

													JsonObject createdTransferTask = new JsonObject(response);
													assertThat(createdTransferTask).isNotNull();
													requestCheckpoint.flag();

													verify(transferAPIVertical)._doPublishEvent("transfertask.created", createdTransferTask);
													verify(transferAPIVertical, never())._doPublishEvent("transfertask.error", createdTransferTask);

													verify(transferTaskCreatedListener)._doPublishEvent("transfertask.assigned", createdTransferTask);
													verify(transferTaskCreatedListener, never())._doPublishEvent("transfertask.error", createdTransferTask);

													verify(transferRetryListener)._doPublishEvent("transfertask.retry", createdTransferTask);
													verify(transferRetryListener, never())._doPublishEvent("transfertask.error", createdTransferTask);

													verify(transferTaskAssignedListener)._doPublishEvent("transfertask.http", createdTransferTask);
													verify(transferTaskAssignedListener, never())._doPublishEvent("transfertask.error", createdTransferTask);

													verify(transferSftpVertical)._doPublishEvent("transfertask.sftp", createdTransferTask);
													verify(transferSftpVertical, never())._doPublishEvent("transfertask.error", createdTransferTask);

													verify(transferHttpVertical)._doPublishEvent("transfer.completed", createdTransferTask);
													verify(transferHttpVertical, never())._doPublishEvent("transfertask.error", createdTransferTask);

													verify(transferCompleteTaskListener)._doPublishEvent("transfertask.completed", createdTransferTask);
													verify(transferCompleteTaskListener, never())._doPublishEvent("transfertask.error", createdTransferTask);

													verify(transferWatchListener)._doPublishEvent("transfertask.healthcheck", createdTransferTask);
													//verify(transferWatchListener, never())._doPublishEvent("transfertask.error", createdTransferTask);

													verify(transferHealthcheckListener)._doPublishEvent("transfertask.completed", createdTransferTask);
													verify(transferHealthcheckListener, never())._doPublishEvent("transfertask.error", createdTransferTask);

													ctx.completeNow();
												});
											}));
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
