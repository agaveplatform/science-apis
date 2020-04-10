package org.agaveplatform.service.transfers.listener;


import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
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
import org.agaveplatform.service.transfers.resources.FileTransferCreateServiceImpl;
import org.agaveplatform.service.transfers.resources.TransferAPIVertical;
import org.agaveplatform.service.transfers.resources.TransferTaskUnaryImpl;
import org.agaveplatform.service.transfers.streaming.StreamingFileTaskImpl;
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
import static java.util.Arrays.asList;
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

	ErrorTaskListener getMockErrListenerInstance(Vertx vertx) {
		ErrorTaskListener listener = spy(new ErrorTaskListener(vertx));
		when(listener.getEventChannel()).thenCallRealMethod();
		when(listener.getVertx()).thenReturn(vertx);
		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}

	FailureHandler getMockFailListenerInstance(Vertx vertx) {
		FailureHandler listener = spy(new FailureHandler(vertx));
		when(listener.getEventChannel()).thenCallRealMethod();
		when(listener.getVertx()).thenReturn(vertx);
		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}

	InteruptEventListener getMockInteruptListenerInstance(Vertx vertx) {
		InteruptEventListener listener = spy(new InteruptEventListener(vertx));
		//when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}
	NotificationListener getMockNotificationListenerInstance(Vertx vertx) {
//		NotificationListener listener = spy(new NotificationListener(vertx));
		NotificationListener listener = Mockito.spy(new NotificationListener(vertx));

//		when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
//		doCallRealMethod().when(listener).start();




		return listener;
	}
	TransferCompleteTaskListenerImpl getMockTCTListenerInstance(Vertx vertx) {
		TransferCompleteTaskListenerImpl listener = spy(new TransferCompleteTaskListenerImpl(vertx));
		//when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}
	TransferTaskAssignedListener getMockTTAListenerInstance(Vertx vertx) {
		TransferTaskAssignedListener listener = spy(new TransferTaskAssignedListener(vertx));
		//when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}
	TransferTaskCancelListener getMockTTCancelListenerInstance(Vertx vertx) {
		TransferTaskCancelListener listener = spy(new TransferTaskCancelListener(vertx));
		//when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}
	TransferTaskCreatedListener getMockTTCListenerInstance(Vertx vertx) {
		TransferTaskCreatedListener listener = spy(new TransferTaskCreatedListener(vertx));
		//when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}
	TransferTaskPausedListener getMockTTPausedListenerInstance(Vertx vertx) {
		TransferTaskPausedListener listener = spy(new TransferTaskPausedListener(vertx));
		//when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}
	TransferSftpVertical getMockSFTPVerticalInstance(Vertx vertx) {
		TransferSftpVertical listener = spy(new TransferSftpVertical(vertx));
		//when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}

	TransferHttpVertical getMockHTTPVerticalInstance(Vertx vertx) {
		TransferHttpVertical listener = spy(new TransferHttpVertical(vertx));
		//when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}

	FileTransferCreateServiceImpl getMockFTCSIVerticalInstance(Vertx vertx) {
		FileTransferCreateServiceImpl listener = spy(new FileTransferCreateServiceImpl(vertx));
		//when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}

	TransferAPIVertical getMockTransServUIVertInstance(Vertx vertx) {
		TransferAPIVertical listener = spy(new TransferAPIVertical(vertx));
		//when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}

	TransferTaskUnaryImpl getMockTransferTaskUnaryImpl(Vertx vertx) {
		TransferTaskUnaryImpl listener = spy(new TransferTaskUnaryImpl(vertx));
		//when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		return listener;
	}

	StreamingFileTaskImpl getMockStreamingFileTaskImpl(Vertx vertx) {
		StreamingFileTaskImpl listener = spy(new StreamingFileTaskImpl(vertx));
		//when(listener.getEventChannel()).thenCallRealMethod();
//		when(listener.getVertx()).thenReturn(vertx);
//		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));

		return listener;
	}

//	TransferTaskAssignedListener getMockttaListenerInstance(Vertx vertx) {
//		TransferTaskAssignedListener tctli = spy(new TransferTaskAssignedListener(vertx));
//		when(tctli.getEventChannel()).thenReturn("transfertask.created");
//		when(tctli.getVertx()).thenReturn(vertx);
//		return tctli;
//	}
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
		parentTask.setEndTime(Instant.now());
		transferTask.setRootTaskId(parentTask.getParentTaskId());
		transferTask.setParentTaskId(parentTask.getParentTaskId());


		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskCreatedListener transferTaskCreatedListener = getMockTTCListenerInstance(vertx);
		TransferTaskAssignedListener transferTaskAssignedListener = getMockTTAListenerInstance(vertx);
		TransferTaskCancelListener transferTaskCancelListener = getMockTTCancelListenerInstance(vertx);
		TransferSftpVertical transferSftpVertical = getMockSFTPVerticalInstance(vertx);
		TransferHttpVertical transferHttpVertical = getMockHTTPVerticalInstance(vertx);
		TransferCompleteTaskListenerImpl transferCompleteTaskListener = getMockTCTListenerInstance(vertx);
		ErrorTaskListener errorTaskListener = getMockErrListenerInstance(vertx);
		InteruptEventListener interuptEventListener = getMockInteruptListenerInstance(vertx);
		NotificationListener notificationListener = getMockNotificationListenerInstance(vertx);
		TransferTaskPausedListener transferTaskPausedListener = getMockTTPausedListenerInstance(vertx);
		FileTransferCreateServiceImpl fileTransferCreateService = getMockFTCSIVerticalInstance(vertx);
		TransferAPIVertical transferAPIVertical = getMockTransServUIVertInstance(vertx);
		TransferTaskUnaryImpl transferTaskUnary = getMockTransferTaskUnaryImpl(vertx);
		StreamingFileTaskImpl streamingFileTask = getMockStreamingFileTaskImpl(vertx);

//		JsonObject createdTransferTask = null;
		Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint createdDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint assignedDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint httpDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint requestCheckpoint = ctx.checkpoint();
		Checkpoint completedDeploymentCheckpoint = ctx.checkpoint();

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

									verify(transferTaskAssignedListener)._doPublishEvent("transfertask.http", createdTransferTask);
									verify(transferTaskAssignedListener, never())._doPublishEvent("transfertask.error", createdTransferTask);

									verify(transferHttpVertical)._doPublishEvent("transfer.completed", createdTransferTask);
									verify(transferHttpVertical, never())._doPublishEvent("transfertask.error", createdTransferTask);

									verify(transferCompleteTaskListener)._doPublishEvent("transfertask.completed", createdTransferTask);
									verify(transferCompleteTaskListener, never())._doPublishEvent("transfertask.error", createdTransferTask);

									ctx.completeNow();
								});
							}));
						}));
					}));
				}));
			}));
		}));
	}

//
//		// we switch the call chain when we want to pass through a method invocation to a method returning void
////		doCallRealMethod().when(ttc).(any(), any(), any());
//
//		// mock out the db service so we can can isolate method logic rather than db
//		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);
//
//		// mock a successful outcome with updated json transfer task result from getById call to db
//		AsyncResult<JsonObject> getByAnyHandler = getMockAsyncResult(parentTask.toJson());
//
//		// mock the handler passed into getById
//		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
//			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(2, Handler.class))
//					.handle(getByAnyHandler);
//			return null;
//		}).when(dbService).getById(any(), any(), any());
//
//		// mock a successful response from allChildrenCancelledOrCompleted call to db
//		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);
//
//		// mock the handler passed into allChildrenCancelledOrCompletedHandler
//		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
//			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
//					.handle(allChildrenCancelledOrCompletedHandler);
//			return null;
//		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());
//
//		// mock the dbService getter in our mocked vertical so we don't need to use powermock
//		when(ttc.getDbService()).thenReturn(dbService);
//
//		Checkpoint processParentEventStartCheckpoint = ctx.checkpoint();
//		Checkpoint processParentEventEndCheckpoint = ctx.checkpoint();
//	}

//	@BeforeEach
//	public void prepare( Vertx vertx, VertxTestContext ctx)  {
//
//		// call the _createTestTransferTask in the BaseTestCase to build the TransferTask with the varaibles for the test.
//		TransferTask tt = _createTestTransferTask();
//
//		DeploymentOptions options = new DeploymentOptions().setConfig(config);
//		Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
//		Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
//		Checkpoint requestCheckpoint = ctx.checkpoint();
//
//		vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
//			dbDeploymentCheckpoint.flag();
//
//			TransferAPIVertical apiVert = new TransferAPIVertical(vertx);
//			vertx.deployVerticle(apiVert, options, ctx.succeeding(apiId -> {
//				apiDeploymentCheckpoint.flag();
//
////				RequestSpecification requestSpecification = new RequestSpecBuilder()
////						.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
////						.setBaseUri("http://localhost:" + port + "/")
////						.build();
//				ctx.verify(() -> {
////					String response = given()
////							.spec(requestSpecification)
////							.header("X-JWT-ASSERTION-AGAVE_DEV", this.makeJwtToken(TEST_USERNAME))
////							.contentType(ContentType.JSON)
////							.body(tt.toJSON())
////							.when()
////							.post("api/transfers")
////							.then()
////							.assertThat()
////							.statusCode(201)
////							.extract()
////							.asString();
////					TransferTask respTask = Json.decodeValue(response, TransferTask.class);
////					assertThat(respTask).isNotNull();
////					assertThat(respTask.getSource()).isEqualTo(tt.getSource());
////					assertThat(respTask.getDest()).isEqualTo(tt.getDest());
//					requestCheckpoint.flag();
//				});
//			}));
//		}));
//
////		vertx.deployVerticle(getMockTTAListenerInstance(vertx));
////		vertx.deployVerticle(getMockTTCListenerInstance(vertx));
////		vertx.deployVerticle(getMockTCTListenerInstance(vertx));
////
////		vertx.deployVerticle(new TransferTaskAssignedListener(vertx));
////		vertx.deployVerticle(new TransferTaskCancelListener(vertx));
////		vertx.deployVerticle(new TransferSftpVertical(vertx));
////		vertx.deployVerticle(new TransferCompleteTaskListenerImpl(vertx));
////		vertx.deployVerticle(new ErrorTaskListener());
////		vertx.deployVerticle(new InteruptEventListener(vertx));
////		vertx.deployVerticle(new NotificationListener());
////		vertx.deployVerticle(new TransferTaskPausedListener(vertx));
////		vertx.deployVerticle(new FileTransferCreateServiceImpl(vertx));
////		vertx.deployVerticle(new TransferServiceUIVertical());
////		vertx.deployVerticle(new TransferTaskUnaryImpl(vertx));
////		vertx.deployVerticle(new StreamingFileTaskImpl(vertx));
////		vertx.deployVerticle(new StreamingFileTaskImpl(vertx));
//	}
//
//	@Test
//	@Disabled
//	public void start(Vertx vertx, VertxTestContext ctx) {
//
//
//		// get the JsonObject to pass back and forth between verticles
//		TransferTask transferTask = _createTestTransferTask();
//
//
//		JsonObject json = transferTask.toJson();
//
////		// mock out the verticle we're testing so we can observe that its methods were called as expected
////		TransferTaskCreatedListener ttc = getMockListenerInstance(vertx);
////		String assignmentChannel = "transfertask.assigned." +
////				TENANT_ID +
////				"." + PROTOCOL +
////				"." + HOST +
////				"." + TEST_USERNAME;
////
////		String result = ttc.assignTransferTask(json);
////		assertEquals("http", result, "result should have been http");
////		verify(ttc)._doPublishEvent(assignmentChannel, json);
////		ctx.completeNow();
//
////		Path configPath = Paths.get(TransferServiceVerticalTest.class.getClassLoader().getResource("config.json").getPath());
////		String json = new String(Files.readAllBytes(configPath));
////		JsonObject conf = new JsonObject(json);
////
////		ctx.completeNow();
//////		vertx.deployVerticle(new TransferTaskDatabaseVerticle(),
//////				new DeploymentOptions().setConfig(conf).setWorker(true).setMaxWorkerExecuteTime(3600),
//////				ctx.succeeding(id -> {
//////					service = TransferTaskDatabaseService.createProxy(vertx, conf.getString(CONFIG_TRANSFERTASK_DB_QUEUE));
//////					ctx.completeNow();
//////				}));
//////
//////		vertx.deployVerticle(new TransferTaskCreatedListener(vertx,"transfertask.created"));
//////		vertx.deployVerticle(new TransferTaskAssignedListener(vertx));
//////		vertx.deployVerticle(new TransferTaskCancelListener(vertx));
//////		vertx.deployVerticle(new TransferSftpVertical(vertx));
//////		vertx.deployVerticle(new TransferCompleteTaskListenerImpl(vertx));
//////		vertx.deployVerticle(new ErrorTaskListener());
//////		vertx.deployVerticle(new InteruptEventListener(vertx));
//////		vertx.deployVerticle(new NotificationListener());
//////		vertx.deployVerticle(new TransferTaskPausedListener(vertx));
//////		vertx.deployVerticle(new FileTransferCreateServiceImpl(vertx));
//////		vertx.deployVerticle(new TransferServiceUIVertical());
//////		vertx.deployVerticle(new TransferTaskUnaryImpl(vertx));
//////		vertx.deployVerticle(new StreamingFileTaskImpl(vertx));
////
//	}
}
