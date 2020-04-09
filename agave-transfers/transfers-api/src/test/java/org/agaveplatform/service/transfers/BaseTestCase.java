package org.agaveplatform.service.transfers;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.resources.TransferServiceVerticalTest;
import org.agaveplatform.service.transfers.util.CryptoHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseTestCase {
    private static final Logger log = LoggerFactory.getLogger(BaseTestCase.class);

    public final String TEST_USERNAME = "testuser";
    public final String TENANT_ID = "agave.dev";
    public final String TRANSFER_SRC = "http://foo.bar/cat/in/the/hat";
    public final String TRANSFER_DEST = "agave://sftp.example.com//dev/null";
    public final String TEST_USER = "testuser";

    protected JsonObject config;
    protected int port = 32331;

    protected TransferTask _createTestTransferTask() {
        return new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USER, TENANT_ID, null, null);
    }

    /**
     * Reads config file synchronously to ensure completion prior to setup.
     */
    protected void initConfig() {
        Path configPath = Paths.get(getClass().getClassLoader().getResource("config.json").getPath());
        try {
            String json = new String(Files.readAllBytes(configPath));
            config = new JsonObject(json)
                    .put( "transfertask.http.port", port);
        } catch (IOException e) {
            log.error("Unable to read config options file", e);
        } catch (DecodeException e) {
            log.error("Error parsing config options file", e);
        }
    }

    @BeforeAll
    public void setUpService() throws IOException {
        // read in config options
        initConfig();
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


}
