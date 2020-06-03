package org.agaveplatform.service.transfers;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.jwt.JWTOptions;
import io.vertx.ext.web.handler.impl.AgaveJWTAuthProviderImpl;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.util.CryptoHelper;
import org.agaveplatform.service.transfers.util.ServiceUtils;
import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.common.Settings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseTestCase {
    private static final Logger log = LoggerFactory.getLogger(BaseTestCase.class);

    public final String TEST_USERNAME = "testuser";
    public final String TEST_OTHER_USERNAME = "testotheruser";
    public final String TEST_SHARE_USERNAME = "testshareuser";
    public final String TEST_ADMIN_USERNAME = "testadminuser";
    public final String TENANT_ID = "agave.dev";
    public final String TRANSFER_SRC = "http://foo.bar/cat/in/the/hat";
    public final String TRANSFER_DEST = "agave://sftp.example.com//dev/null";
    public final String TEST_USER = "testuser";

    protected JsonObject config;
    protected int port = 32331;
    protected AgaveJWTAuthProviderImpl jwtAuth;

    protected TransferTask _createTestTransferTask() {
        return new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USER, TENANT_ID, null, null);
    }

    /**
     * Creates a legacy {@link org.iplantc.service.transfer.model.TransferTask} for use when mocking URLCopy operations
     * @return
     */
    protected org.iplantc.service.transfer.model.TransferTask _createTestTransferTaskIPC() {
        return new org.iplantc.service.transfer.model.TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USER, null, null);
    }

    /**
     * Reads config file synchronously to ensure completion prior to setup.
     */
    protected void initConfig(Vertx vertx, Handler<AsyncResult<JsonObject>> handler) {
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", "config.json"));

        ConfigStoreOptions envPropsStore = new ConfigStoreOptions().setType("env");

        ConfigRetrieverOptions options = new ConfigRetrieverOptions()
                .addStore(fileStore)
                .addStore(envPropsStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(json -> {
            if (json.succeeded()) {
                JsonObject config = json.result();

                try {
                    // generate keys to use in the test. We persist them to temp files so we can pass them to the api via the
                    // config settings.
                    CryptoHelper cryptoHelper = new CryptoHelper();
                    Path privateKey = Files.write(Files.createTempFile("private", "pem"), cryptoHelper.getPrivateKey().getBytes());
                    Path publicKey = Files.write(Files.createTempFile("public", "pem"), cryptoHelper.getPublicKey().getBytes());

                    config.put("transfertask.http.port", getPort())
                            .put("transfertask.jwt.auth", true)
                            .put("transfertask.jwt.public_key", publicKey.toAbsolutePath().toString())
                            .put("transfertask.jwt.private_key", privateKey.toAbsolutePath().toString());

                    handler.handle(Future.succeededFuture(config));
                } catch (IOException e) {
                    log.error("Unable to read config options file", e);
                } catch (DecodeException e) {
                    log.error("Error parsing config options file", e);
                }
            } else {
                handler.handle(Future.failedFuture(json.cause()));
            }
        });
    }

    /**
     * Initializes the jwt auth options and sets the jwt signing cert to a set of test generated keys.
     * @throws IOException when the key cannot be read
     */
    protected void initAuth(Vertx vertx, Handler<AsyncResult<AgaveJWTAuthProviderImpl>> handler) throws IOException {
        initConfig(vertx, resp -> {
            if (resp.succeeded()) {
                try {
                    JWTAuthOptions jwtAuthOptions = new JWTAuthOptions()
                            .setJWTOptions(new JWTOptions()
                                    .setLeeway(30)
                                    .setAlgorithm("RS256"))
                            .setPermissionsClaimKey("http://wso2.org/claims/role")
                            .addPubSecKey(new PubSecKeyOptions()
                                    .setAlgorithm("RS256")
                                    .setPublicKey(CryptoHelper.publicKey(resp.result().getString("transfertask.jwt.public_key")))
                                    .setSecretKey(CryptoHelper.privateKey(resp.result().getString("transfertask.jwt.private_key"))));

                    config = resp.result();

                    AgaveJWTAuthProviderImpl jwtAuth = new AgaveJWTAuthProviderImpl(vertx, jwtAuthOptions);

                    handler.handle(Future.succeededFuture(jwtAuth));
                } catch (IOException e) {
                    handler.handle(Future.failedFuture(e));
                }
            } else {
                handler.handle(Future.failedFuture(resp.cause()));
            }
        });
    }

    /**
     * Generates a JWT token to authenticate to the service. Token is signed using the
     * test private key in the service config file.
     *
     * @param username Name of the test user
     * @param roles Comma separated list of roles to append to the jwt
     * @return signed jwt token
     */
    protected String makeTestJwt(String username, String roles) {
        // Add wso2 claims set
        String givenName = username.replace("user", "");
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
                .put("http://wso2.org/claims/emailaddress", username + "@example.com")
                .put("http://wso2.org/claims/fullname", StringUtils.capitalize(givenName) + " User")
                .put("http://wso2.org/claims/givenname", givenName)
                .put("http://wso2.org/claims/lastname", "User")
                .put("http://wso2.org/claims/primaryChallengeQuestion", "N/A")
                .put("http://wso2.org/claims/role", ServiceUtils.explode(",", List.of("Internal/everyone,Internal/subscriber", roles)))
                .put("http://wso2.org/claims/title", "N/A");

        JWTOptions jwtOptions = new JWTOptions()
                .setAlgorithm("RS256")
                .setExpiresInMinutes(10_080) // 7 days
                .setIssuer("transfers-api-integration-tests")
                .setSubject(username);

        return jwtAuth.generateToken(claims, jwtOptions);
    }

    /**
     * Generates a JWT token to authenticate to the service. Token is signed using the
     * test private key in the service config file. Only the default user roles are assigned
     * to this jwt.
     *
     * @param username Name of the test user
     * @return signed jwt token
     * @see #makeTestJwt(String, String)
     */
    protected String makeTestJwt(String username) {
        return makeTestJwt(username, "");
    }

    @BeforeAll
    public void setUpService(Vertx vertx, VertxTestContext ctx) throws IOException {
        Checkpoint authCheckpoint = ctx.checkpoint();

        // init the jwt auth used in the api calls
        initAuth(vertx, resp -> {
            authCheckpoint.flag();
            if (resp.succeeded()) {
                jwtAuth = resp.result();
                ctx.verify(() -> {
                    assertNotNull(jwtAuth);
                    assertNotNull(config);

                    ctx.completeNow();
                });

            } else {
                ctx.failNow(resp.cause());
            }
        });
    }

    @AfterAll
    public void tearDown(Vertx vertx, VertxTestContext ctx) {
        vertx.close(ctx.completing());
    }

    /**
     * Creates a mock {@link AsyncResult<Boolean>} that can be used as a handler controlling
     * the success outcomes
     * @param result the expected value of the call to {@link AsyncResult#result()}
     * @return a valid mock for testing boolean result behavior
     */
    protected AsyncResult<Boolean> getMockAsyncResult(Boolean result) {
        AsyncResult<Boolean> asyncResult = mock(AsyncResult.class);
        when(asyncResult.result()).thenReturn(result);
        when(asyncResult.succeeded()).thenReturn(true);
        when(asyncResult.failed()).thenReturn(false);
        when(asyncResult.cause()).thenReturn(null);

        return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<Boolean>} that can be used as a handler controlling
     * failure outcomes and response
     * @param cause the expected exception to be bubbled up
     * @return a valid mock for testing boolean result behavior
     */
    protected AsyncResult<Boolean> getBooleanFailureAsyncResult(Throwable cause) {
        AsyncResult<Boolean> asyncResult = mock(AsyncResult.class);
        when(asyncResult.result()).thenReturn(null);
        when(asyncResult.succeeded()).thenReturn(false);
        when(asyncResult.failed()).thenReturn(true);
        when(asyncResult.cause()).thenReturn(cause);

        return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<JsonObject>} that can be used as a handler controlling
     * the success outcomes
     * @param result the expected {@link JsonObject} returned from {@link AsyncResult#result()}
     * @return a valid mock for testing {@link JsonObject} result behavior
     */
    protected AsyncResult<JsonObject> getMockAsyncResult(JsonObject result) {
        AsyncResult<JsonObject> asyncResult = mock(AsyncResult.class);
        when(asyncResult.result()).thenReturn(result);
        when(asyncResult.succeeded()).thenReturn(true);
        when(asyncResult.failed()).thenReturn(false);
        when(asyncResult.cause()).thenReturn(null);

        return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<JsonArray>} that can be used as a handler controlling
     * the success outcomes
     * @param result the expected {@link JsonArray} returned from {@link AsyncResult#result()}
     * @return a valid mock for testing {@link JsonArray} result behavior
     */
    protected AsyncResult<JsonArray> getMockAsyncResult(JsonArray result) {
        AsyncResult<JsonArray> asyncResult = mock(AsyncResult.class);
        when(asyncResult.result()).thenReturn(result);
        when(asyncResult.succeeded()).thenReturn(true);
        when(asyncResult.failed()).thenReturn(false);
        when(asyncResult.cause()).thenReturn(null);

        return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<JsonObject>} that can be used as a handler controlling
     * failure outcomes and response
     * @param cause the expected exception to be bubbled up
     * @return a valid mock for testing {@link JsonObject} result behavior
     */
    protected AsyncResult<JsonObject> getJsonObjectFailureAsyncResult(Throwable cause) {
        AsyncResult<JsonObject> asyncResult = mock(AsyncResult.class);
        when(asyncResult.result()).thenReturn(null);
        when(asyncResult.succeeded()).thenReturn(false);
        when(asyncResult.failed()).thenReturn(true);
        when(asyncResult.cause()).thenReturn(cause);

        return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<TransferTask>} that can be used as a handler controlling
     * the success outcomes
     * @param result the expected {@link TransferTask} returned from {@link AsyncResult#result()}
     * @return a valid mock for testing {@link TransferTask} result behavior
     */
    protected AsyncResult<TransferTask> getMockAsyncResult(TransferTask result) {
        AsyncResult<TransferTask> asyncResult = mock(AsyncResult.class);
        when(asyncResult.result()).thenReturn(result);
        when(asyncResult.succeeded()).thenReturn(true);
        when(asyncResult.failed()).thenReturn(false);
        when(asyncResult.cause()).thenReturn(null);

        return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<TransferTask>} that can be used as a handler controlling
     * failure outcomes and response
     * @param cause the expected exception to be bubbled up
     * @return a valid mock for testing {@link TransferTask} result behavior
     */
    protected AsyncResult<TransferTask> getTransferTaskFailureAsyncResult(Throwable cause) {
        AsyncResult<TransferTask> asyncResult = mock(AsyncResult.class);
        when(asyncResult.result()).thenReturn(null);
        when(asyncResult.succeeded()).thenReturn(false);
        when(asyncResult.failed()).thenReturn(true);
        when(asyncResult.cause()).thenReturn(cause);

        return asyncResult;
    }


    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
