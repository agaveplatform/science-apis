package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import io.vertx.core.eventbus.*;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("RetryRequestManager test")
@Disabled
public class RetryRequestManagerTest extends BaseTestCase {

    @AfterAll
    public void finish(Vertx vertx, VertxTestContext ctx) {
        vertx.close(ctx.completing());
    }

    /**
     * Generates a mock of the vertical under test with the inherited methods stubbed out.
     *
     * @param vertx the test vertx instance
     * @return a mocked of {@link TransferTaskAssignedListener}
     */
    protected TransferTaskAssignedListener getMockTransferAssignedListenerInstance(Vertx vertx) throws IOException, InterruptedException {
        TransferTaskAssignedListener listener = mock(TransferTaskAssignedListener.class);
        when(listener.getEventChannel()).thenReturn(TRANSFERTASK_ASSIGNED);
        when(listener.getVertx()).thenReturn(vertx);
        when(listener.taskIsNotInterrupted(any())).thenReturn(true);
        when(listener.uriSchemeIsNotSupported(any())).thenReturn(false);
        doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
        doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());
//		when(listener.getRetryRequestManager()).thenCallRealMethod();
        doNothing().when(listener)._doPublishEvent(any(), any());
        doCallRealMethod().when(listener).processTransferTask(any(JsonObject.class), any());

        RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);
//		when(mockRetryRequestManager.getVertx()).thenReturn(vertx);
        doNothing().when(mockRetryRequestManager).request(anyString(),any(JsonObject.class),anyInt());

        when(listener.getRetryRequestManager()).thenReturn(mockRetryRequestManager);
        return listener;
    }

    /**
     * Generates a mock of the vertical under test with the inherited methods stubbed out.
     *
     * @param vertx the test vertx instance
     * @return a mocked of {@link TransferTaskAssignedListener}
     */
    protected TransferTaskErrorListener getMockTransferErrorListenerInstance(Vertx vertx) {
        TransferTaskErrorListener listener = mock(TransferTaskErrorListener.class);
        when(listener.getEventChannel()).thenReturn(TRANSFERTASK_ERROR);
        when(listener.getVertx()).thenReturn(vertx);
        when(listener.taskIsNotInterrupted(any())).thenReturn(true);

        return listener;
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
        TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

        // mock a successful outcome with updated json transfer task result from getById call to db
        AsyncResult<JsonObject> getByAnyHandler = getMockAsyncResult(transferTaskToReturn);

        // mock the handler passed into getById
        doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
            @SuppressWarnings("unchecked")
            Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
            handler.handle(getByAnyHandler);
            return null;
        }).when(dbService).getByUuid(any(), any(), any());

        return dbService;
    }


    @Test
    @DisplayName("RetryRequestManager - request and process TransferTaskAssignedListener")
    @Disabled
    public void requestTransferTaskAssignedEvent(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException {
        // generate a fake transfer task
        TransferTask rootTransferTask = _createTestTransferTask();
        rootTransferTask.setId(1L);
        JsonObject rootTransferTaskJson = rootTransferTask.toJson();
        rootTransferTaskJson.put("status", TRANSFERTASK_ASSIGNED);

        //mock TransferTaskAssignedListener
        TransferTaskAssignedListener mockAssignedListener = mock(TransferTaskAssignedListener.class);
        try {
            doCallRealMethod().when(mockAssignedListener).start();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        when(mockAssignedListener.config()).thenReturn(config);
        doReturn(getMockTranserTaskDatabaseService(rootTransferTaskJson)).when(TransferTaskDatabaseService.createProxy(any(Vertx.class), anyString()));
        doNothing().when(vertx.eventBus()).registerDefaultCodec(any(), any());

        //mock RetryRequestManager
        RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);
        when(mockRetryRequestManager.getVertx()).thenReturn(vertx);
        doCallRealMethod().when(mockRetryRequestManager).request(anyString(), any(JsonObject.class), anyInt());

        //Mock request sent onto the event bus
        Handler<DeliveryContext<Object>> inboundInterceptor = dc -> {

            try {
                mockAssignedListener.start();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        };

        vertx.eventBus().addInboundInterceptor(inboundInterceptor);

        mockRetryRequestManager.request(TRANSFERTASK_ASSIGNED, rootTransferTaskJson, 3);

        ctx.verify(() -> {
            assertTrue(ctx.completed(), "Request should be completed.");
        });
    }
}
