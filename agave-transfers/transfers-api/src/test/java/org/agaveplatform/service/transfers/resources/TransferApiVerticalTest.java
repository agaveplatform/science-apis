package org.agaveplatform.service.transfers.resources;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
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
import static org.mockito.Matchers.any;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers API integration tests")
//@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
public class TransferApiVerticalTest extends BaseTestCase {

    private static final Logger log = LoggerFactory.getLogger(TransferApiVerticalTest.class);
    private TransferTaskDatabaseService dbService;
    private static RequestSpecification requestSpecification;

    @Test
    @Disabled
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
                    .addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
                    .setBaseUri("http://localhost:" + port + "/")
                    .build();

//            String token = this.makeJwtToken(TEST_USERNAME);
            ctx.verify(() -> {
                String response = given()
                        .spec(requestSpecification)
//                        .auth().oauth2(token)
//                        .header("X-JWT-ASSERTION-AGAVE_DEV", token)
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
                        .addFilters(asList(new ResponseLoggingFilter(), new RequestLoggingFilter()))
                        .setBaseUri("http://localhost:" + port + "/")
                        .build();

                String token = this.makeTestJwt(TEST_USERNAME);

                ctx.verify(() -> {
                    String response = given()
                            .spec(requestSpecification)
//                            .auth().oauth2(token)
                            .header("X-JWT-ASSERTION-AGAVE_DEV", token)
                            .contentType(ContentType.JSON)
                            .body(tt.toJSON())
                            .when()
                            .post("api/transfers")
                            .then()
                            .assertThat()
                            .statusCode(201)
                            .extract()
                            .asString();
                    TransferTask respTask = new TransferTask(new JsonObject(response));
                    assertThat(respTask).isNotNull();
                    assertThat(respTask.getSource()).isEqualTo(tt.getSource());
                    assertThat(respTask.getDest()).isEqualTo(tt.getDest());
                    requestCheckpoint.flag();
                });
            }));
        }));
    }


}