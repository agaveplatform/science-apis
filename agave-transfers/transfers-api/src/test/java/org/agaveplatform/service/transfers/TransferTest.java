package org.agaveplatform.service.transfers;


import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
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
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle;
import org.agaveplatform.service.transfers.listener.*;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.protocol.TransferSftpVertical;
import org.agaveplatform.service.transfers.resources.FileTransferCreateServiceImpl;
import org.agaveplatform.service.transfers.resources.TransferServiceUIVertical;
import org.agaveplatform.service.transfers.util.CryptoHelper;
import org.iplantc.service.common.Settings;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.restassured.RestAssured.given;
import static java.util.Arrays.asList;
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(VertxExtension.class)
@DisplayName("Transfers tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
public class TransferTest extends BaseTestCase {
	private static final Logger log = LoggerFactory.getLogger(TransferTest.class);

	private Vertx vertx;
	private JWTAuth jwtAuth;
	private static RequestSpecification requestSpecification;
	private TransferTaskDatabaseService dbService;
	public static final String HOST = "foo.bar";
	public static final String PROTOCOL = "http";

	ErrorTaskListener getMockErrListenerInstance(Vertx vertx) {
		ErrorTaskListener errl = Mockito.mock(ErrorTaskListener.class);
		when(errl.getEventChannel()).thenReturn("transfertask.error");
		when(errl.getVertx()).thenReturn(vertx);
		return errl;
	}

	FailureHandler getMockFailListenerInstance(Vertx vertx) {
		FailureHandler failL = Mockito.mock(FailureHandler.class);
		when(failL.getEventChannel()).thenReturn("transfertask.error");
		when(failL.getVertx()).thenReturn(vertx);
		return failL;
	}

	InteruptEventListener getMockInteruptListenerInstance(Vertx vertx) {
		InteruptEventListener interuptListener = Mockito.mock(InteruptEventListener.class);
		when(interuptListener.getEventChannel()).thenReturn("transfertask.interupt.*.*.*.*");
		when(interuptListener.getVertx()).thenReturn(vertx);
		return interuptListener;
	}
	NotificationListener getMockNotificationListenerInstance(Vertx vertx) {
		NotificationListener notifListener = Mockito.mock(NotificationListener.class);
		when(notifListener.getEventChannel()).thenReturn("notification.*");
		when(notifListener.getVertx()).thenReturn(vertx);
		return notifListener;
	}
	TransferCompleteTaskListenerImpl getMockTCTListenerInstance(Vertx vertx) {
		TransferCompleteTaskListenerImpl transferCompleteLstnImp = Mockito.mock(TransferCompleteTaskListenerImpl.class);
		when(transferCompleteLstnImp.getEventChannel()).thenReturn("transfertask.created");
		when(transferCompleteLstnImp.getVertx()).thenReturn(vertx);
		return transferCompleteLstnImp;
	}
	TransferTaskAssignedListener getMockTTAListenerInstance(Vertx vertx) {
		TransferTaskAssignedListener trasTaskAsgnLstn = Mockito.mock(TransferTaskAssignedListener.class);
		when(trasTaskAsgnLstn.getEventChannel()).thenReturn("transfertask.created");
		when(trasTaskAsgnLstn.getVertx()).thenReturn(vertx);
		return trasTaskAsgnLstn;
	}
	TransferTaskCancelListener getMockTTCancelListenerInstance(Vertx vertx) {
		TransferTaskCancelListener tranTaskCancLstn = Mockito.mock(TransferTaskCancelListener.class);
		when(tranTaskCancLstn.getEventChannel()).thenReturn("transfertask.cancelled");
		when(tranTaskCancLstn.getVertx()).thenReturn(vertx);
		return tranTaskCancLstn;
	}
	TransferTaskCreatedListener getMockTTCListenerInstance(Vertx vertx) {
		TransferTaskCreatedListener tranTaskCreatedLstn = Mockito.mock(TransferTaskCreatedListener.class);
		when(tranTaskCreatedLstn.getEventChannel()).thenReturn("transfertask.created");
		when(tranTaskCreatedLstn.getVertx()).thenReturn(vertx);
		return tranTaskCreatedLstn;
	}
	TransferTaskPausedListener getMockTTPausedListenerInstance(Vertx vertx) {
		TransferTaskPausedListener tranTaskPausedLstn = Mockito.mock(TransferTaskPausedListener.class);
		when(tranTaskPausedLstn.getEventChannel()).thenReturn("transfertask.paused");
		when(tranTaskPausedLstn.getVertx()).thenReturn(vertx);
		return tranTaskPausedLstn;
	}
	TransferSftpVertical getMockSFTPVerticalInstance(Vertx vertx) {
		TransferSftpVertical transSftpVert = Mockito.mock(TransferSftpVertical.class);
		when(transSftpVert.getEventChannel()).thenReturn("transfertask.sftp");
		when(transSftpVert.getVertx()).thenReturn(vertx);
		return transSftpVert;
	}
	FileTransferCreateServiceImpl getMockFTCSIVerticalInstance(Vertx vertx) {
		FileTransferCreateServiceImpl fileTranCreateServImp = Mockito.mock(FileTransferCreateServiceImpl.class);
		when(fileTranCreateServImp.getEventChannel()).thenReturn("filetransfer.sftp");
		when(fileTranCreateServImp.getVertx()).thenReturn(vertx);
		return fileTranCreateServImp;
	}





//	TransferTaskAssignedListener getMockttaListenerInstance(Vertx vertx) {
//		TransferTaskAssignedListener tctli = Mockito.mock(TransferTaskAssignedListener.class);
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

	@BeforeAll
	public void prepare( Vertx vertx, VertxTestContext ctx)  {
		initConfig();
		initConfig();


		TransferTask tt = _createTestTransferTask();
		DeploymentOptions options = new DeploymentOptions().setConfig(config);
		Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
		Checkpoint requestCheckpoint = ctx.checkpoint();

//		vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
//			dbDeploymentCheckpoint.flag();
//
//			vertx.deployVerticle(TransferServiceUIVertical.class.getName(), options, ctx.succeeding(apiId -> {
//				apiDeploymentCheckpoint.flag();
//
//				RequestSpecification requestSpecification = new RequestSpecBuilder()
//						.addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
//						.setBaseUri("http://localhost:" + port + "/")
//						.build();
//				ctx.verify(() -> {
//					String response = given()
//							.spec(requestSpecification)
//							.header("X-JWT-ASSERTION-AGAVE_DEV", this.makeJwtToken(TEST_USERNAME))
//							.contentType(ContentType.JSON)
//							.body(tt.toJSON())
//							.when()
//							.post("api/transfers")
//							.then()
//							.assertThat()
//							.statusCode(201)
//							.extract()
//							.asString();
//					TransferTask respTask = Json.decodeValue(response, TransferTask.class);
//					assertThat(respTask).isNotNull();
//					assertThat(respTask.getSource()).isEqualTo(tt.getSource());
//					assertThat(respTask.getDest()).isEqualTo(tt.getDest());
//					requestCheckpoint.flag();
//				});
//			}));
//		}));

//		vertx.deployVerticle(getMockTTAListenerInstance(vertx));
		vertx.deployVerticle(getMockTTCListenerInstance(vertx));
//		vertx.deployVerticle(getMockTCTListenerInstance(vertx));
//
//		vertx.deployVerticle(new TransferTaskAssignedListener(vertx));
//		vertx.deployVerticle(new TransferTaskCancelListener(vertx));
//		vertx.deployVerticle(new TransferSftpVertical(vertx));
//		vertx.deployVerticle(new TransferCompleteTaskListenerImpl(vertx));
//		vertx.deployVerticle(new ErrorTaskListener());
//		vertx.deployVerticle(new InteruptEventListener(vertx));
//		vertx.deployVerticle(new NotificationListener());
//		vertx.deployVerticle(new TransferTaskPausedListener(vertx));
//		vertx.deployVerticle(new FileTransferCreateServiceImpl(vertx));
//		vertx.deployVerticle(new TransferServiceUIVertical());
//		vertx.deployVerticle(new TransferTaskUnaryImpl(vertx));
//		vertx.deployVerticle(new StreamingFileTaskImpl(vertx));
	}


	@Test
	//@Disabled
	public void start(Vertx vertx, VertxTestContext ctx) {


		// get the JsonObject to pass back and forth between verticles
		TransferTask transferTask = _createTestTransferTask();

		JsonObject json = transferTask.toJson();

//		// mock out the verticle we're testing so we can observe that its methods were called as expected
//		TransferTaskCreatedListener ttc = getMockListenerInstance(vertx);
//		String assignmentChannel = "transfertask.assigned." +
//				TENANT_ID +
//				"." + PROTOCOL +
//				"." + HOST +
//				"." + TEST_USERNAME;
//
//		String result = ttc.assignTransferTask(json);
//		assertEquals("http", result, "result should have been http");
//		verify(ttc)._doPublish(assignmentChannel, json);
//		ctx.completeNow();

//		Path configPath = Paths.get(TransferServiceVerticalTest.class.getClassLoader().getResource("config.json").getPath());
//		String json = new String(Files.readAllBytes(configPath));
//		JsonObject conf = new JsonObject(json);
//
//		ctx.completeNow();
////		vertx.deployVerticle(new TransferTaskDatabaseVerticle(),
////				new DeploymentOptions().setConfig(conf).setWorker(true).setMaxWorkerExecuteTime(3600),
////				ctx.succeeding(id -> {
////					service = TransferTaskDatabaseService.createProxy(vertx, conf.getString(CONFIG_TRANSFERTASK_DB_QUEUE));
////					ctx.completeNow();
////				}));
////
////		vertx.deployVerticle(new TransferTaskCreatedListener(vertx,"transfertask.created"));
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
//
	}
}
