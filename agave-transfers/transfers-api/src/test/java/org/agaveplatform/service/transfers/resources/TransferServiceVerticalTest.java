package org.agaveplatform.service.transfers.resources;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
//import org.testng.annotations.AfterMethod;
//import org.testng.annotations.BeforeMethod;
//import org.testng.annotations.Test;
import java.io.IOException;
import java.net.ServerSocket;

//@Test(groups={"unit"})
@RunWith(VertxUnitRunner.class)
public class TransferServiceVerticalTest {

    private Vertx vertx;
    private int port = 8081;

    @Before
    public void setUp(TestContext context) throws IOException {
        vertx = Vertx.vertx();

        // Pick an available and random
        ServerSocket socket = new ServerSocket(0);
        port = socket.getLocalPort();
        socket.close();

        DeploymentOptions options = new DeploymentOptions()
                .setConfig(new JsonObject()
                        .put("HTTP_PORT", port)
                        .put("url", "jdbc:hsqldb:mem:test?shutdown=true")
                        .put("driver_class", "org.hsqldb.jdbcDriver")
                );
        vertx.deployVerticle(TransferServiceVertical.class.getName(), options, context.asyncAssertSuccess());
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testServerRoot(TestContext context) {
        final Async async = context.async();

        vertx.createHttpClient().getNow(port, "localhost", "/",
                response ->
                        response.handler(body -> {
                            context.assertTrue(body.toString().contains("Hello"));
                            async.complete();
                        }));
    }

    @Test
    public void testGetAll(TestContext context) {
        final Async async = context.async();

        vertx.createHttpClient().getNow(port, "localhost", "/api/transfers",
                response ->
                        response.handler(body -> {
                            JsonObject json = new JsonObject(body.toString());
                            context.assertTrue(json.getString("message").contains("Hello"));
                            async.complete();
                        }));
    }

    @Test
    public void checkThatWeCanAdd(TestContext context) {
        Async async = context.async();
        final TransferTask transferTask = new TransferTask("agave://sftp//etc/hosts", "agave://irods3//home/testuser/hosts");
        final String json = transferTask.toJSON();

        vertx.createHttpClient().post(port, "localhost", "/api/transfers")
                .putHeader("Content-Type", "application/json")
                .putHeader("Content-Length", Integer.toString(json.length()))
                .handler(response -> {
                    context.assertEquals(response.statusCode(), 201);
                    context.assertTrue(response.headers().get("content-type").contains("application/json"));
                    response.bodyHandler(body -> {
                        TransferTask respTransferTask = Json.decodeValue(body.toString(), TransferTask.class);
                        context.assertEquals(respTransferTask.getSource(), transferTask.getSource(), "Source uri in response should be the same as the one in the request.");
                        context.assertEquals(respTransferTask.getDest(), transferTask.getDest(), "Dest uri in response should be the same as the one in the request.");
                        context.assertEquals(respTransferTask.getUuid(), transferTask.getUuid(), "UUID in response should be the same as the one in the request.");
                        context.assertNotNull(respTransferTask.getId());
                        async.complete();
                    });
                })
                .write(json)
                .end();
    }
}