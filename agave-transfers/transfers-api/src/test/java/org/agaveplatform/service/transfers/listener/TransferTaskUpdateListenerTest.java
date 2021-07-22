package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.MockTransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ERROR;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_UPDATED;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("👋 TransferTaskUpdateListenerTest test")
//@Disabled
class TransferTaskUpdateListenerTest extends BaseTestCase {

    @AfterAll
    public void tearDown(Vertx vertx, VertxTestContext ctx) {
        vertx.close(ctx.completing());
    }

    /**
     * Generates a mock of the vertical under test with the inherited methods stubbed out.
     *
     * @param vertx the test vertx instance
     * @return a mocked of {@link TransferTaskUpdateListener}
     */
    protected TransferTaskUpdateListener getMockTransferUpdateListenerInstance(Vertx vertx) throws IOException, InterruptedException {
        TransferTaskUpdateListener listener = mock(TransferTaskUpdateListener.class);
        when(listener.getEventChannel()).thenReturn(TRANSFERTASK_UPDATED);
        when(listener.getVertx()).thenReturn(vertx);
        when(listener.createPushMessageSubject(any(), any(), any(), any())).thenCallRealMethod();
        when(listener.taskIsNotInterrupted(any())).thenReturn(true);
        when(listener.uriSchemeIsNotSupported(any())).thenReturn(false);
        doCallRealMethod().when(listener).doHandleError(any(), any(), any(), any());
        doCallRealMethod().when(listener).doHandleFailure(any(), any(), any(), any());
        doCallRealMethod().when(listener).handleMessage(any());
        doNothing().when(listener)._doPublishEvent(any(), any(), any());
        doCallRealMethod().when(listener).processEvent(any(JsonObject.class), any());

        RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);
        doNothing().when(mockRetryRequestManager).request(anyString(), any(JsonObject.class), anyInt());
        when(listener.getRetryRequestManager()).thenReturn(mockRetryRequestManager);
        return listener;
    }

    NatsJetstreamMessageClient getMockNats() throws Exception {
        NatsJetstreamMessageClient natsClient = mock(NatsJetstreamMessageClient.class);
        doNothing().when(natsClient).push(any(), any(), anyInt());
        return natsClient;
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

    /**
     * Creates a new mock remote data client that will return a valid mock {@link RemoteFileInfo} with
     * {@link RemoteFileInfo#isDirectory()}, {@link RemoteFileInfo#isFile()}, {@link RemoteFileInfo#getName()} mocked
     * out. The return value is based on the value of {@code isFile} passed in.
     *
     * @param remotePath   the path of the remote file item
     * @param isDir        true if the path should represent a directory, false otherwise
     * @param withChildren true if the listing should return more than just the root task. multiple remote file info will be added to directory if true
     * @return a remote data client that will mock out valie remote responses..
     */
    private RemoteDataClient getMockRemoteDataClient(String remotePath, boolean isDir, boolean withChildren) {
        RemoteDataClient remoteDataClient = mock(RemoteDataClient.class);
        try {
            // accept the mkdir by default. we don't need to test that here
            when(remoteDataClient.mkdirs(any())).thenReturn(true);
            // mock out the stat on the remote path. this should return a {@link RemoteFileInfo} instance for the path alone
            RemoteFileInfo remotePathFileInfo = generateRemoteFileInfo(remotePath, isDir);
            when(remoteDataClient.getFileInfo(remotePath)).thenReturn(remotePathFileInfo);

            // generate content for directory listing responses
            List<RemoteFileInfo> listing = null;
            if (isDir && withChildren) {
                // if the path should represent a directory, generate the items in the listing response
                listing = List.of(
                        generateRemoteFileInfo(remotePath + "/.", true),
                        generateRemoteFileInfo(remotePath + "/" + UUID.randomUUID(), true),
                        generateRemoteFileInfo(remotePath + "/" + UUID.randomUUID(), false),
                        generateRemoteFileInfo(remotePath + "/" + UUID.randomUUID(), false)
                );
            } else {
                // if the path should represent a file, a listing will only return the file item itself
                listing = List.of(remotePathFileInfo);
            }

            // mock out directory listing response
            when(remoteDataClient.ls(any())).thenReturn(listing);

        } catch (RemoteDataException | IOException ignored) {
        }

        return remoteDataClient;
    }

    /**
     * Generates a new {@link RemoteFileInfo} for the given {@code remotePath}. Directories will have
     * size 4096. Files will have a random size between 0 and {@link Integer#MAX_VALUE}. The current
     * date will be set as the last motified time. Name will be the given path.
     *
     * @param remotePath the path of the remote file item
     * @param isDir      false if the instance should have type {@link RemoteFileInfo#FILE_TYPE}, true if {@link RemoteFileInfo#DIRECTORY_TYPE}
     * @return a valid, populated instance, sans permissions.
     */
    private RemoteFileInfo generateRemoteFileInfo(String remotePath, boolean isDir) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        remoteFileInfo.setName(remotePath);
        remoteFileInfo.setLastModified(new Date());
        remoteFileInfo.setFileType(isDir ? RemoteFileInfo.DIRECTORY_TYPE : RemoteFileInfo.FILE_TYPE);
        remoteFileInfo.setOwner(TEST_USER);
        remoteFileInfo.setSize(isDir ? new Random().nextInt(Integer.MAX_VALUE) : 4096);

        return remoteFileInfo;
    }

    /**
     * Tests transfer task update behavior. In reality, this is not implemented in the service because we need to
     * ensure that we are not going to cause a race condition in the update, which is possible when the events
     * come in asynchronously.
     * @param vertx test vertx instance
     * @param ctx test context
     * @throws Exception the exception to thrown
     */
    @Test
    @DisplayName("TransferTaskUpdateListener - processTransferTask updates single file transfer task")
//    @Disabled
    public void processTransferTaskUpdatesSingleFileTransferTask(Vertx vertx, VertxTestContext ctx) throws Exception {
        // mock out the test class
        TransferTaskUpdateListener listener = getMockTransferUpdateListenerInstance(vertx);
        NatsJetstreamMessageClient natsClient = mock(NatsJetstreamMessageClient.class);

        // generate a fake transfer task
        TransferTask rootTransferTask = _createTestTransferTask();
        rootTransferTask.setId(1L);
        rootTransferTask.setStatus(TransferStatusType.STREAM_COPY_STARTED);

        JsonObject rootTransferTaskJson = rootTransferTask.toJson();
        // generate the expected updated JsonObject
//        JsonObject updatedTransferTaskJson = rootTransferTaskJson.copy().put("status", TransferStatusType.STREAM_COPY_STARTED.name());

        // now mock out our db interactions. Here we
        TransferTaskDatabaseService dbService = new MockTransferTaskDatabaseService.Builder()
//                .updateStatus(updatedTransferTaskJson, false)
                .build();

        // assign the mock db service to the test listener
        when(listener.getDbService()).thenReturn(dbService);

        listener.processEvent(rootTransferTaskJson, result -> {
            ctx.verify(() -> {
                assertTrue(result.succeeded(), "Task status update should return true on successful processing.");
                assertTrue(result.result(), "Callback result should be true after successful assignment.");

                //db status should be updated
//                verify(dbService, times(1)).updateStatus(
//                        eq(rootTransferTask.getTenantId()),
//                        eq(rootTransferTask.getUuid()),
//                        eq(rootTransferTask.getStatus().name()),
//                        any(Handler.class));
                verify(dbService, never()).updateStatus(any(), any(), any(), any());

                // no error event should have been raised
                verify(natsClient, never()).push(any(), any(), anyInt());
                ctx.completeNow();

            });
        });
    }

    @Test
    @DisplayName("TransferTaskUpdateListener - taskIsNotInterrupted")
    void taskIsNotInterruptedTest(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException {
        TransferTask tt = _createTestTransferTask();
        tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

        TransferTaskUpdateListener ta = new TransferTaskUpdateListener(vertx);

        ctx.verify(() -> {
            ta.addCancelledTask(tt.getUuid());
            assertFalse(ta.taskIsNotInterrupted(tt), "UUID of tt present in cancelledTasks list should indicate task is interrupted");
            ta.removeCancelledTask(tt.getUuid());

            ta.addPausedTask(tt.getUuid());
            assertFalse(ta.taskIsNotInterrupted(tt), "UUID of tt present in pausedTasks list should indicate task is interrupted");
            ta.removePausedTask(tt.getUuid());

            ta.addCancelledTask(tt.getParentTaskId());
            assertFalse(ta.taskIsNotInterrupted(tt), "UUID of tt parent present in cancelledTasks list should indicate task is interrupted");
            ta.removeCancelledTask(tt.getParentTaskId());

            ta.addPausedTask(tt.getParentTaskId());
            assertFalse(ta.taskIsNotInterrupted(tt), "UUID of tt parent present in pausedTasks list should indicate task is interrupted");
            ta.removePausedTask(tt.getParentTaskId());

            ta.addCancelledTask(tt.getRootTaskId());
            assertFalse(ta.taskIsNotInterrupted(tt), "UUID of tt root present in cancelledTasks list should indicate task is interrupted");
            ta.removeCancelledTask(tt.getRootTaskId());

            ta.addPausedTask(tt.getRootTaskId());
            assertFalse(ta.taskIsNotInterrupted(tt), "UUID of tt root present in pausedTasks list should indicate task is interrupted");
            ta.removePausedTask(tt.getRootTaskId());

            ctx.completeNow();
        });
    }
}

