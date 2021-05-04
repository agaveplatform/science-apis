package org.agaveplatform.service.transfers;


import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.exception.TransferException;
import org.agaveplatform.service.transfers.listener.*;
import org.agaveplatform.service.transfers.model.TransferTask;
//import org.agaveplatform.service.transfers.resources.FileTransferCreateServiceImpl;
import org.agaveplatform.service.transfers.protocol.TransferAllProtocolVertical;
import org.agaveplatform.service.transfers.resources.TransferAPIVertical;
//import org.agaveplatform.service.transfers.resources.TransferTaskUnaryImpl;
//import org.agaveplatform.service.transfers.streaming.StreamingFileTaskImpl;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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

	TransferTaskErrorListener getMockErrListenerInstance(Vertx vertx) throws IOException, InterruptedException {
		TransferTaskErrorListener listener = spy(new TransferTaskErrorListener(vertx));
		when(listener.getEventChannel()).thenCallRealMethod();
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		doCallRealMethod().when(listener)._doPublishNatsJSEvent(anyString(),anyString(), any(JsonObject.class));
		return listener;
	}

	TransferTaskErrorFailureHandler getMockFailListenerInstance(Vertx vertx) throws IOException, InterruptedException {
		TransferTaskErrorFailureHandler listener = spy(new TransferTaskErrorFailureHandler(vertx));
		when(listener.getEventChannel()).thenCallRealMethod();
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
		doCallRealMethod().when(listener)._doPublishNatsJSEvent(anyString(),anyString(), any(JsonObject.class));
		return listener;
	}

	InteruptEventListener getMockInteruptListenerInstance(Vertx vertx) throws IOException, InterruptedException {
		InteruptEventListener listener = spy(new InteruptEventListener(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		doCallRealMethod().when(listener)._doPublishNatsJSEvent(anyString(),anyString(), any(JsonObject.class));
		return listener;
	}
	TransferTaskNotificationListener getMockNotificationListenerInstance(Vertx vertx) throws IOException, InterruptedException {
//		NotificationListener listener = spy(new NotificationListener(vertx));
		TransferTaskNotificationListener listener = Mockito.spy(new TransferTaskNotificationListener(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		return listener;
	}
	TransferTaskCompleteTaskListener getMockTCTListenerInstance(Vertx vertx) throws IOException, InterruptedException {
		TransferTaskCompleteTaskListener listener = spy(new TransferTaskCompleteTaskListener(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		doCallRealMethod().when(listener)._doPublishNatsJSEvent(anyString(),anyString(), any(JsonObject.class));
		return listener;
	}
	TransferTaskAssignedListener getMockTTAListenerInstance(Vertx vertx) throws IOException, InterruptedException {
		TransferTaskAssignedListener listener = spy(new TransferTaskAssignedListener(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		return listener;
	}
	TransferTaskCancelListener getMockTTCancelListenerInstance(Vertx vertx) throws IOException, InterruptedException {
		TransferTaskCancelListener listener = spy(new TransferTaskCancelListener(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		doCallRealMethod().when(listener)._doPublishNatsJSEvent(anyString(),anyString(), any(JsonObject.class));
		return listener;
	}
	TransferTaskCreatedListener getMockTTCListenerInstance(Vertx vertx) throws IOException, InterruptedException {
		TransferTaskCreatedListener listener = spy(new TransferTaskCreatedListener(vertx));
		doReturn(config).when(listener).config();
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		doCallRealMethod().when(listener)._doPublishNatsJSEvent(anyString(),anyString(), any(JsonObject.class));
		return listener;
	}
	TransferTaskPausedListener getMockTTPausedListenerInstance(Vertx vertx) throws IOException, InterruptedException {
		TransferTaskPausedListener listener = spy(new TransferTaskPausedListener(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		doCallRealMethod().when(listener)._doPublishNatsJSEvent(anyString(),anyString(), any(JsonObject.class));
		return listener;
	}

	TransferAllProtocolVertical getMockAllProtocolVerticalInstance(Vertx vertx) throws IOException, InterruptedException {
		TransferAllProtocolVertical listener = spy(new TransferAllProtocolVertical(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		doCallRealMethod().when(listener)._doPublishNatsJSEvent(anyString(),anyString(), any(JsonObject.class));
		return listener;
	}

	TransferAPIVertical getMockTransferAPIVerticalInstance(Vertx vertx) throws IOException, InterruptedException {
		TransferAPIVertical listener = spy(new TransferAPIVertical(vertx));
		when(listener.config()).thenReturn(config);
		doCallRealMethod().when(listener)._doPublishEvent(any(), any());
		doCallRealMethod().when(listener)._doPublishNatsJSEvent(anyString(),anyString(), any(JsonObject.class));
		return listener;
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
									vertx.deployVerticle(TransferTaskCompleteTaskListener.class.getName(), options, completedId -> {
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
				//								//.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
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
	public void singleFileTransferSmokeTest(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException {
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
		TransferTaskCompleteTaskListener transferTaskCompleteTaskListener = getMockTCTListenerInstance(vertx);
		TransferTaskErrorListener errorTaskListener = getMockErrListenerInstance(vertx);
		InteruptEventListener interuptEventListener = getMockInteruptListenerInstance(vertx);
		TransferTaskNotificationListener transferTaskNotificationListener = getMockNotificationListenerInstance(vertx);
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

							vertx.deployVerticle(transferTaskCompleteTaskListener, options, ctx.succeeding(completedId -> {
								completedDeploymentCheckpoint.flag();

								RequestSpecification requestSpecification = new RequestSpecBuilder()
										//								//.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
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
											} else if (address.equals(TRANSFERTASK_FINISHED)) {
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
