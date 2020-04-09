package org.agaveplatform.service.transfers.resources;

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
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.util.CryptoHelper;
import org.iplantc.service.common.Settings;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.restassured.RestAssured.given;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers API integration tests")
//@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
public class TransferServiceVerticalTest extends BaseTestCase {

    private static final Logger log = LoggerFactory.getLogger(TransferServiceVerticalTest.class);
    private Vertx vertx;
    private JWTAuth jwtAuth;

    private static RequestSpecification requestSpecification;

    private TransferTaskDatabaseService dbService;

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

    @BeforeAll
    public void setUpService() throws IOException {
        // read in config options
        initConfig();

        // init the jwt auth used in the api calls
        initAuth();
    }


    @Test
   // @Disabled
    @DisplayName("List web root says hello")
    void register(Vertx vertx, VertxTestContext ctx) {
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
//        Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();

        vertx.deployVerticle(TransferServiceUIVertical.class.getName(), options, ctx.succeeding(apiId -> {

            apiDeploymentCheckpoint.flag();

            RequestSpecification requestSpecification = new RequestSpecBuilder()
                    .addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
                    .setBaseUri("http://localhost:" + port + "/")
                    .build();
            ctx.verify(() -> {
                String response = given()
                        .spec(requestSpecification)
                        .header("X-JWT-ASSERTION-AGAVE_DEV", this.makeJwtToken(TEST_USERNAME))
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
//    @Disabled
    void create(Vertx vertx, VertxTestContext ctx) {
        TransferTask tt = _createTestTransferTask();
        DeploymentOptions options = new DeploymentOptions().setConfig(config);
        Checkpoint dbDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint apiDeploymentCheckpoint = ctx.checkpoint();
        Checkpoint requestCheckpoint = ctx.checkpoint();

        vertx.deployVerticle(TransferTaskDatabaseVerticle.class.getName(), options, ctx.succeeding(dbId -> {
            dbDeploymentCheckpoint.flag();

            vertx.deployVerticle(TransferServiceUIVertical.class.getName(), options, ctx.succeeding(apiId -> {
                apiDeploymentCheckpoint.flag();

                RequestSpecification requestSpecification = new RequestSpecBuilder()
                        .addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
                        .setBaseUri("http://localhost:" + port + "/")
                        .build();
                ctx.verify(() -> {
                    String response = given()
                            .spec(requestSpecification)
                            .header("X-JWT-ASSERTION-AGAVE_DEV", this.makeJwtToken(TEST_USERNAME))
                            .contentType(ContentType.JSON)
                            .body(tt.toJSON())
                            .when()
                            .post("api/transfers")
                            .then()
                            .assertThat()
                            .statusCode(201)
                            .extract()
                            .asString();
                    TransferTask respTask = Json.decodeValue(response, TransferTask.class);
                    assertThat(respTask).isNotNull();
                    assertThat(respTask.getSource()).isEqualTo(tt.getSource());
                    assertThat(respTask.getDest()).isEqualTo(tt.getDest());
                    requestCheckpoint.flag();
                });
            }));
        }));
    }


//    @Test
//    @DisplayName("Register a user")
//    public void testServerRoot() {
//
//        vertx.createHttpClient().getNow(port, "localhost", "/",
//                response ->
//                        response.handler(body -> {
//                            context.assertTrue(body.toString().contains("Hello"));
//                            async.complete();
//                        }));
//    }
//
//    @Test
//    public void testGetAll() {
//
//        vertx.createHttpClient().getNow(port, "localhost", "/api/transfers",
//                response ->
//                        response.handler(body -> {
//                            JsonObject json = new JsonObject(body.toString());
//                            context.assertTrue(json.getString("message").contains("Hello"));
//                            async.complete();
//                        }));
//    }
//
//    @Test
//    public void checkThatWeCanAdd() {
//        final TransferTask transferTask = new TransferTask("agave://sftp//etc/hosts", "agave://irods3//home/testuser/hosts");
//        final String json = transferTask.toJSON();
//
//        vertx.createHttpClient().post(port, "localhost", "/api/transfers")
//                .putHeader("Content-Type", "application/json")
//                .putHeader("Content-Length", Integer.toString(json.length()))
//                .handler(response -> {
//                    context.assertEquals(response.statusCode(), 201);
//                    context.assertTrue(response.headers().get("content-type").contains("application/json"));
//                    response.bodyHandler(body -> {
//                        TransferTask respTransferTask = Json.decodeValue(body.toString(), TransferTask.class);
//                        context.assertEquals(respTransferTask.getSource(), transferTask.getSource(), "Source uri in response should be the same as the one in the request.");
//                        context.assertEquals(respTransferTask.getDest(), transferTask.getDest(), "Dest uri in response should be the same as the one in the request.");
//                        context.assertEquals(respTransferTask.getUuid(), transferTask.getUuid(), "UUID in response should be the same as the one in the request.");
//                        context.assertNotNull(respTransferTask.getId());
//                        async.complete();
//                    });
//                })
//                .write(json)
//                .end();
//    }
}