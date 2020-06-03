package org.agaveplatform.service.transfers.listener;


import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
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
import org.agaveplatform.service.transfers.exception.TransferException;
import org.agaveplatform.service.transfers.model.TransferTask;
//import org.agaveplatform.service.transfers.resources.FileTransferCreateServiceImpl;
import org.agaveplatform.service.transfers.protocol.TransferAllProtocolVertical;
import org.agaveplatform.service.transfers.resources.TransferAPIVertical;
//import org.agaveplatform.service.transfers.resources.TransferTaskUnaryImpl;
//import org.agaveplatform.service.transfers.streaming.StreamingFileTaskImpl;
import org.agaveplatform.service.transfers.util.CryptoHelper;
import org.iplantc.service.common.Settings;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


@ExtendWith(VertxExtension.class)
@DisplayName("Transfers tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled
public class TransferTaskSmokeTest extends BaseTestCase {
	private static final Logger log = LoggerFactory.getLogger(TransferTaskSmokeTest.class);

	Vertx vertx = null;
	private JWTAuth jwtAuth;
	private static RequestSpecification requestSpecification;
	private TransferTaskDatabaseService dbService;
	List<String> messages = new ArrayList<String>();

	TransferErrorListener getMockErrListenerInstance(Vertx vertx) {
		TransferErrorListener listener = spy(new TransferErrorListener(vertx));
		when(listener.getEventChannel()).thenCallRealMethod();
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}

	TransferFailureHandler getMockFailListenerInstance(Vertx vertx) {
		TransferFailureHandler listener = spy(new TransferFailureHandler(vertx));
		when(listener.getEventChannel()).thenCallRealMethod();
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}

	InteruptEventListener getMockInteruptListenerInstance(Vertx vertx) {
		InteruptEventListener listener = spy(new InteruptEventListener(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		return listener;
	}
	NotificationListener getMockNotificationListenerInstance(Vertx vertx) {
//		NotificationListener listener = spy(new NotificationListener(vertx));
		NotificationListener listener = Mockito.spy(new NotificationListener(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		return listener;
	}
	TransferCompleteTaskListener getMockTCTListenerInstance(Vertx vertx) {
		TransferCompleteTaskListener listener = spy(new TransferCompleteTaskListener(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		return listener;
	}
	TransferTaskAssignedListener getMockTTAListenerInstance(Vertx vertx) {
		TransferTaskAssignedListener listener = spy(new TransferTaskAssignedListener(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		return listener;
	}
	TransferTaskCancelListener getMockTTCancelListenerInstance(Vertx vertx) {
		TransferTaskCancelListener listener = spy(new TransferTaskCancelListener(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		return listener;
	}
	TransferTaskCreatedListener getMockTTCListenerInstance(Vertx vertx) {
		TransferTaskCreatedListener listener = spy(new TransferTaskCreatedListener(vertx));
		doReturn(config).when(listener).config();
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		return listener;
	}
	TransferTaskPausedListener getMockTTPausedListenerInstance(Vertx vertx) {
		TransferTaskPausedListener listener = spy(new TransferTaskPausedListener(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		return listener;
	}

	TransferAllProtocolVertical getMockAllProtocolVerticalInstance(Vertx vertx) {
		TransferAllProtocolVertical listener = spy(new TransferAllProtocolVertical(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		return listener;
	}

	TransferAPIVertical getMockTransferAPIVerticalInstance(Vertx vertx) {
		TransferAPIVertical listener = spy(new TransferAPIVertical(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
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
	 * Initializes the jwt auth options and sets the jwt signing cert to a set of test generated keys.
	 * @throws IOException when the key cannot be read
	 */
	private void initAuth(Vertx vertx) throws IOException {
		CryptoHelper helper = new CryptoHelper();

		JWTAuthOptions jwtAuthOptions = new JWTAuthOptions()
				.addPubSecKey(new PubSecKeyOptions()
						.setAlgorithm("RS256")
						.setPublicKey(helper.getPublicKey())
						.setSecretKey(helper.getPrivateKey()));

		jwtAuth = JWTAuth.create(vertx, jwtAuthOptions);
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

				DeploymentOptions options = new DeploymentOptions().setConfig(config);
				vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, dbId -> {
					log.debug("Completed deploying transfer task db verticles");
					vertx.deployVerticle(TransferAPIVertical.class.getName(), options, apiId -> {
						log.debug("Completed deploying transfer api verticles");
						vertx.deployVerticle(TransferTaskCreatedListener.class.getName(), options, createdId -> {
							log.debug("Completed deploying transfer task createdverticles");
							vertx.deployVerticle(TransferTaskAssignedListener.class.getName(), options, assignedId -> {
								log.debug("Completed deploying transfer task assigned verticles");
								vertx.deployVerticle(TransferAllProtocolVertical.class.getName(), options, httpId -> {
									log.debug("Completed deploying transfer all verticles");
									vertx.deployVerticle(TransferCompleteTaskListener.class.getName(), options, completedId -> {
										log.debug("Completed deploying transfer complete verticles");

										ctx.verify(() -> {
											assertNotNull(jwtAuth);
											assertNotNull(config);

											ctx.completeNow();
										});
									});
								});
							});
						});
					});
				});
			} else {
				ctx.failNow(resp.cause());
			}
		});


	}

	@BeforeEach
	protected void beforeEach(Vertx vertx, VertxTestContext ctx) {

		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.QUEUED);
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());
		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		RequestSpecification requestSpecification = new RequestSpecBuilder()
				//								.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
				.setBaseUri("http://localhost:" + port + "/")
				.build();

		Checkpoint requestCheckpoint = ctx.checkpoint();
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
			ctx.completeNow();
		});
	}

	@Test
	@DisplayName("Single file transfer task smoke test")
	@Disabled
	public void singleFileTransferSmokeTest(Vertx vertx, VertxTestContext ctx) {
		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.QUEUED);
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());
		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.QUEUED);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());
		transferTask.setRootTaskId(parentTask.getUuid());
		transferTask.setParentTaskId(parentTask.getUuid());


		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskCreatedListener transferTaskCreatedListener = getMockTTCListenerInstance(vertx);
		TransferTaskAssignedListener transferTaskAssignedListener = getMockTTAListenerInstance(vertx);
		TransferTaskCancelListener transferTaskCancelListener = getMockTTCancelListenerInstance(vertx);
//		TransferSftpVertical transferSftpVertical = getMockSFTPVerticalInstance(vertx);
		TransferAllProtocolVertical transferAllProtocolVertical = getMockAllProtocolVerticalInstance(vertx);
		TransferCompleteTaskListener transferCompleteTaskListener = getMockTCTListenerInstance(vertx);
		TransferErrorListener errorTaskListener = getMockErrListenerInstance(vertx);
		InteruptEventListener interuptEventListener = getMockInteruptListenerInstance(vertx);
		NotificationListener notificationListener = getMockNotificationListenerInstance(vertx);
		TransferTaskPausedListener transferTaskPausedListener = getMockTTPausedListenerInstance(vertx);
		//FileTransferCreateServiceImpl fileTransferCreateService = getMockFTCSIVerticalInstance(vertx);
		TransferAPIVertical transferAPIVertical = getMockTransferAPIVerticalInstance(vertx);
//		TransferTaskUnaryImpl transferTaskUnary = getMockTransferTaskUnaryImpl(vertx);
//		StreamingFileTaskImpl streamingFileTask = getMockStreamingFileTaskImpl(vertx);

		Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint createdDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint assignedDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint httpDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint requestCheckpoint = ctx.checkpoint();
		Checkpoint completedDeploymentCheckpoint = ctx.checkpoint();

		Checkpoint apiListenerCheckpoint = ctx.checkpoint();
		Checkpoint createdListenerCheckpoint = ctx.checkpoint();
		Checkpoint assignedListenerCheckpoint = ctx.checkpoint();
		Checkpoint transferAllListenerCheckpoint = ctx.checkpoint();
		Checkpoint transferCompletedListenerCheckpoint = ctx.checkpoint();
		Checkpoint transferTaskCompletedListenerCheckpoint = ctx.checkpoint();

		DeploymentOptions options = new DeploymentOptions().setConfig(config);

		vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
			dbDeploymentCheckpoint.flag();

			vertx.deployVerticle(transferAPIVertical, options, ctx.succeeding(apiId -> {
				apiDeploymentCheckpoint.flag();

				vertx.deployVerticle(transferTaskCreatedListener, options, ctx.succeeding(createdId -> {
					createdDeploymentCheckpoint.flag();

					vertx.deployVerticle(transferTaskAssignedListener, options, ctx.succeeding(assignedId -> {
						assignedDeploymentCheckpoint.flag();

						vertx.deployVerticle(transferAllProtocolVertical, options, ctx.succeeding(httpId -> {
							httpDeploymentCheckpoint.flag();

							vertx.deployVerticle(transferCompleteTaskListener, options, ctx.succeeding(completedId -> {
								completedDeploymentCheckpoint.flag();

								RequestSpecification requestSpecification = new RequestSpecBuilder()
										//								.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
										.setBaseUri("http://localhost:" + port + "/")
										.build();

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
									ctx.verify(() -> {
										vertx.eventBus().addOutboundInterceptor(dc -> {
											String address = dc.message().address();
											if (address.equals(TRANSFERTASK_CREATED)) {
												createdListenerCheckpoint.flag();
											} else if (address.equals(TRANSFERTASK_ASSIGNED)) {
												assignedListenerCheckpoint.flag();
											} else if (address.equals(TRANSFER_ALL)) {
												transferAllListenerCheckpoint.flag();
											} else if (address.equals(TRANSFER_COMPLETED)) {
												transferCompletedListenerCheckpoint.flag();
											} else if (address.equals(TRANSFERTASK_COMPLETED)) {
												transferTaskCompletedListenerCheckpoint.flag();
												ctx.completeNow();
											} else if (address.equals(TRANSFERTASK_ERROR)) {
												ctx.failNow(new TransferException("Unexpected exception thrown during processing."));
											} else {
												ctx.failNow(new TransferException("Invalid event raised during transfer processing"));
											}
											dc.next();
										});
									});
								});
							}));
						}));
					}));
				}));
			}));
		}));



	}
}
