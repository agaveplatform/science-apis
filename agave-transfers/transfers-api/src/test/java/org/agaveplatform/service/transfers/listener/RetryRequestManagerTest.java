package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.MockTransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.UUIDType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ERROR;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("RetryRequestManager test")
public class RetryRequestManagerTest extends BaseTestCase {

    @AfterAll
    public void finish(Vertx vertx, VertxTestContext ctx) {
        vertx.close(ctx.completing());
    }

    /**
     * Generates a mock of the class under test with the inherited methods stubbed out.
     *
     * @return a mocked of {@link RetryRequestManager}
     */
    protected RetryRequestManager getMockRetryRequestManager() throws Exception {
        RetryRequestManager retryRequestManager = mock(RetryRequestManager.class);
        doCallRealMethod().when(retryRequestManager).request(anyString(), any(JsonObject.class), anyInt());
        doCallRealMethod().when(retryRequestManager).request(anyString(), any(JsonObject.class));
        return retryRequestManager;
    }

    NatsJetstreamMessageClient getMockNats() throws Exception {
        NatsJetstreamMessageClient natsClient = mock(NatsJetstreamMessageClient.class);
        doNothing().when(natsClient).push(any(), any());
        doNothing().when(natsClient).push(any(), any(), anyInt());
        return natsClient;
    }

    /**
     * Generates a mock of the {@link TransferTaskDatabaseService} with the {@link TransferTaskDatabaseService#getByUuid(String, String, Handler)}
     * method mocked out to return the given {@code transferTask};
     *
     * @param transferTaskToReturn {@link JsonObject} to return from the {@link TransferTaskDatabaseService#getByUuid(String, String, Handler)} handler
     * @return a mock of the db service with the getById mocked out to return the {@code transferTaskToReturn} as an async result.
     */
    private TransferTaskDatabaseService getMockTranserTaskDatabaseService(JsonObject transferTaskToReturn) {
        // mock out the db service so we can can isolate method logic rather than db
        return new MockTransferTaskDatabaseService.Builder()
                .getByUuid(transferTaskToReturn, false)
                .build();
    }

    @Test
    @DisplayName("RetryRequestManager calls nats client with given address and body")
    public void testRequest() throws Exception {
        TransferTask transferTask = _createTestTransferTask();
        JsonObject json = transferTask.toJson();

        String eventName = TRANSFERTASK_ERROR;
        String subject = UUIDType.TRANSFER.name().toLowerCase() + "." + transferTask.getTenantId() + "." +
                transferTask.getOwner() + "." + URI.create(TRANSFER_DEST).getHost() + "." + eventName;

        RetryRequestManager retryRequestManager = getMockRetryRequestManager();

        NatsJetstreamMessageClient natsClient = getMockNats();
        when(retryRequestManager.getMessageClient()).thenReturn(natsClient);
        when(retryRequestManager.createPushMessageSubject(eq(transferTask.getTenantId()), eq(transferTask.getOwner()),
                    eq(URI.create(TRANSFER_DEST).getHost()), eq(eventName)))
                .thenCallRealMethod();

        retryRequestManager.request(eventName, json, 5);

        verify(retryRequestManager).request(eventName, json);
        verify(natsClient).push(eq(subject), eq(json.toString()));
    }
}
