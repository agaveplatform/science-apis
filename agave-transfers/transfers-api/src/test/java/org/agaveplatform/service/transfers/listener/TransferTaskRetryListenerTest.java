package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("👋 TransferRetryListenerTest test")
//@Disabled
class TransferTaskRetryListenerTest extends BaseTestCase {

    protected TransferTaskRetryListener getMockTransferRetryListenerInstance(Vertx vertx) throws Exception {
        TransferTaskRetryListener listener = mock(TransferTaskRetryListener.class);
        when(listener.getEventChannel()).thenReturn(TRANSFER_RETRY);
        when(listener.getVertx()).thenReturn(vertx);
        when(listener.createPushMessageSubject(any(), any(), any(), any())).thenCallRealMethod();
        when(listener.taskIsNotInterrupted(any())).thenReturn(true);
        when(listener.getRetryRequestManager()).thenCallRealMethod();
        when(listener.uriSchemeIsNotSupported(any())).thenReturn(false);
        doCallRealMethod().when(listener)._doPublishEvent(any(), any(), any());
        doCallRealMethod().when(listener).processRetryTransferTask(any(), any());
        doCallRealMethod().when(listener).doHandleError(any(), any(), any(), any());
        doCallRealMethod().when(listener).doHandleFailure(any(), any(), any(), any());
        doCallRealMethod().when(listener).handleMessage(any());

        Connection mockConnection = mock(Connection.class);
        when(listener.config()).thenReturn(config);
        when(listener.getRemoteDataClient(any(), any(), any())).thenReturn(mock(RemoteDataClient.class));

        NatsJetstreamMessageClient natsClient = mock(NatsJetstreamMessageClient.class);
        doNothing().when(natsClient).push(any(), any());
        when(listener.getMessageClient()).thenReturn(natsClient);


        return listener;
    }

    @AfterAll
    public void finish(Vertx vertx, VertxTestContext ctx) {
        vertx.close(ctx.completing());
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

        doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
            @SuppressWarnings("unchecked")
            Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
            handler.handle(getByAnyHandler);
            return null;
        }).when(dbService).update(any(), any(), any(), any());

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


    @Test
    @DisplayName("Process TransferTaskPublishesProtocolEvent")
    //@Disabled
    public void processTransferTaskPublishesProtocolEvent(Vertx vertx, VertxTestContext ctx) throws Exception {
        //TODO: test should verify that the correct protocol is used, protocol references an older mock and should be updated
        //JsonObject body = new JsonObject();
        TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

        JsonObject body = tt.toJson();

        TransferTaskRetryListener ta = getMockTransferRetryListenerInstance(vertx);

        //ta.processRetryTransferTask(body);
        ta.processRetryTransferTask(body, resp -> {
            if (resp.succeeded()) {
                System.out.println("Succeeded with the processTransferTask in retrying of the event ");
            } else {
                System.out.println("Error with return from retrying the event ");
            }
        });

        String protocolSelected = "http";

        assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
        ctx.completeNow();
    }

    @Test
    @DisplayName("handleMessage Test")
    public void processHandleMessageTest(Vertx vertx, VertxTestContext ctx) throws Exception{
        // mock out the test class
        TransferTaskRetryListener ta = getMockTransferRetryListenerInstance(vertx);
        // generate a fake transfer task
        TransferTask transferTask = _createTestTransferTask();
        JsonObject transferTaskJson = transferTask.toJson();

        Message msg = new Message(1, transferTask.toString());
        ta.handleMessage(msg);
        Thread.sleep(3);
        ctx.verify(() -> {
            verify(ta, atLeastOnce()).processRetryTransferTask(eq(transferTaskJson), any());

            ctx.completeNow();
        });
    }


    @Test
    @DisplayName("Process processRetryPublishesChildTasksForDirectory")
    //@Disabled
    public void processRetryPublishesChildTasksForDirectory(Vertx vertx, VertxTestContext ctx) throws Exception {

        TransferTask tt = _createTestTransferTask();
        tt.setSource(LOCAL_DIR);
        tt.setRootTaskId(UUID.randomUUID().toString());
        tt.setParentTaskId(UUID.randomUUID().toString());

        TransferTaskRetryListener ta = getMockTransferRetryListenerInstance(vertx);
        doCallRealMethod().when(ta).processRetry(any(TransferTask.class), any());

        RemoteDataClient srcClient = getMockRemoteDataClient(tt.getSource(), true, true);
        RemoteDataClient destClient = getMockRemoteDataClient(tt.getDest(), true, true);

        when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource())))).thenReturn(srcClient);
        when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest())))).thenReturn(destClient);

        ta.processRetry(tt, resp -> ctx.verify(() -> {
            assertTrue(resp.succeeded(), "processRetry should pass when retrying directory transfers");
            verify(ta, atLeastOnce())._doPublishEvent(eq(TRANSFERTASK_CREATED), any(JsonObject.class), any());
            verify(ta, never()).doHandleFailure(any(), anyString(), any(JsonObject.class), any());
            verify(ta, never()).doHandleError(any(), anyString(), any(JsonObject.class), any());
            ctx.completeNow();
        }));
    }

    @Test
    @DisplayName("Process processRetryPublishesErrorOnSystemUnavailble")
    public void processRetryPublishesErrorOnSystemUnavailble(Vertx vertx, VertxTestContext ctx) throws Exception {
        TransferTask tt = _createTestTransferTask();
        tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

        // mock out the db service so we can can isolate method logic rather than db
        TransferTaskDatabaseService dbService = getMockTranserTaskDatabaseService(tt.toJson());

        TransferTaskRetryListener ta = getMockTransferRetryListenerInstance(vertx);
        doCallRealMethod().when(ta).processRetry(any(), any());

        // allow the first one to succeed
        when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource()))))
                .thenReturn(mock(RemoteDataClient.class));
        // force the second one to fail due to being unavailable
        when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest()))))
                .thenThrow(new RemoteDataException("This should be thrown during the test and propagated back as the handler.cause() method."));

        ta.processRetry(tt, resp -> ctx.verify(() -> {
            assertFalse(resp.succeeded(), "processRetry should fail when remote data client is unavailable");
            assertEquals(resp.cause().getClass(), RemoteDataException.class, "processRetry should propagate RemoteDataException back to handler when thrown.");
            verify(ta, atLeastOnce()).getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource())));
            verify(ta, atLeastOnce()).getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest())));
            verify(ta, times(1)).doHandleError(any(), anyString(), any(JsonObject.class), any());
            ctx.completeNow();
        }));
    }

    @Test
    @DisplayName("TransferTaskRetryListenerTest - error event thrown on unknown dest system")
    //@Disabled
    public void processRetryPublishesErrorOnDestSystemUnknown(Vertx vertx, VertxTestContext ctx) throws Exception {
//        NatsJetstreamMessageClient nats = getMockNats();
        TransferTask tt = _createTestTransferTask();
        tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

        TransferTaskRetryListener ta = getMockTransferRetryListenerInstance(vertx);

        doCallRealMethod().when(ta).processRetry(any(), any());
        try {
            // allow the first one to succeed since it's not an agave URI
            when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource()))))
                    .thenReturn(mock(RemoteDataClient.class));
            // force the second one to fail since it is an agave URI and can result in a bad syste lookup.
            when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest()))))
                    .thenThrow(new SystemUnknownException("THis should be thrown during the test and propagated back as the handler.cause() method."));
        } catch (Exception e) {
            ctx.failNow(e);
        }

        ta.processRetry(tt, processRetryResult -> ctx.verify(() -> {
            assertFalse(processRetryResult.succeeded(), "processRetry should fail when system is unknown");
            assertEquals(processRetryResult.cause().getClass(), SystemUnknownException.class, "processRetry should propagate SystemUnknownException back to handler when thrown.");
            verify(ta, atLeastOnce()).getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource())));
            verify(ta, atLeastOnce()).getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest())));
            verify(ta, atLeastOnce())._doPublishEvent( eq(TRANSFERTASK_ERROR), any(JsonObject.class), any() );
            ctx.completeNow();
        }));
    }

    @Test
    @DisplayName("TransferRetryListenerTest - error event thrown on unknown source system")
    //@Disabled
    public void processTransferTaskPublishesErrorOnSrcSystemUnknown(Vertx vertx, VertxTestContext ctx) throws Exception {
//        NatsJetstreamMessageClient nats = getMockNats();
        TransferTask tt = _createTestTransferTask();
        tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        RemoteDataClient destClient = mock(RemoteDataClient.class);

        TransferTaskRetryListener ta = getMockTransferRetryListenerInstance(vertx);

        doCallRealMethod().when(ta).processRetry(any(), any());

        try {
            // force the source one to fail since it is an agave URI and can result in a bad syste lookup.
            when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource()))))
                    .thenThrow(new SystemUnknownException("This should be thrown during the test and propagated back as the handler.cause() method."));
            // allow the dest one to succeed since it's not an agave URI
            when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest()))))
                    .thenReturn(destClient);
        } catch (Exception e) {
            ctx.failNow(e);
        }

        ta.processRetry(tt, processRetryResult -> {
            ctx.verify(() -> {
                assertFalse(processRetryResult.succeeded(), "processRetry should fail when system is unknown");
                assertEquals(processRetryResult.cause().getClass(), SystemUnknownException.class, "processRetry should propagate SystemUnknownException back to handler when thrown.");
                verify(ta, times(1)).getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource())));
                verify(ta, never()).getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest())));
                verify(ta, times(1)).doHandleError(any(), anyString(), any(JsonObject.class), any());
                ctx.completeNow();
            });
        });
    }

    @Test
    @DisplayName("TransferRetryListener - isTaskInterruptedTest")
   // @Disabled
    void isTaskInterrupted(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException {
        TransferTask tt = _createTestTransferTask();
        tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

        TransferTaskRetryListener ta = new TransferTaskRetryListener(vertx);

        // mock out the db service so we can can isolate method logic rather than db
        TransferTaskDatabaseService dbService = getMockTranserTaskDatabaseService(tt.toJson());


        //doNothing().when(ta).getRetryRequestManager().request(any(), any(), any());

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

    @Test
    @DisplayName("TransferRetryListener - processRetryTransferTask retries active or errored transfers")
    public void processRetryTransferTaskRetriesActiveTransfersTest(Vertx vertx, VertxTestContext ctx) {
        ArrayList<TransferTask> tasks = new ArrayList<>();
//        NatsJetstreamMessageClient nats = getMockNats();

        for (TransferStatusType status : TransferStatusType.values()) {
            TransferTask tt = _createTestTransferTask();
            tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
            tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
            tt.setStatus(status);
            tasks.add(tt);
        }

        ctx.verify(()->{
            for (TransferTask tt : tasks) {
                TransferTaskRetryListener ta = getMockTransferRetryListenerInstance(vertx);

                // mock out the db service so we can can isolate method logic rather than db
                TransferTaskDatabaseService dbService = getMockTranserTaskDatabaseService(tt.toJson());

                when(ta.getDbService()).thenReturn(dbService);
                AsyncResult<Boolean> handlerResult = getMockAsyncResult(true);
                doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
                    @SuppressWarnings("unchecked")
                    Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(1, Handler.class);
                    handler.handle(handlerResult);
                    return null;
                }).when(ta).processRetry(any(), any());

                JsonObject jsonTransferTask = tt.toJson();

                ta.processRetryTransferTask(jsonTransferTask, isRetried -> {
                    if (isRetried.succeeded()) {
                        if (tt.getStatus().isActive() || tt.getStatus().equals(TransferStatusType.ERROR)) {
                                //Active transfer tasks or recoverable errors should be retried
                                verify(ta, times(1)).processRetry(eq(tt), any());
                            } else {
                                //Inactive transfer tasks should not be retried"
                                verify(ta, never()).processRetry(any(), any());
                            }
                    } else {
                        ctx.failNow(isRetried.cause());
                    }
                });
            }
            ctx.completeNow();
        });
    }

    @Test
    @DisplayName("TransferRetryListener - processRetryTransferTask fails whem max attempts reached")
    public void processRetryTransferTaskFailskWhenMaxAttemptsReachedTest(Vertx vertx, VertxTestContext ctx) throws Exception {
//        NatsJetstreamMessageClient nats = getMockNats();

        TransferTask tt = _createTestTransferTask();
        tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        tt.setStatus(TransferStatusType.ASSIGNED);
        tt.setAttempts(config.getInteger("TRANSFERTASK_MAX_ATTEMPTS") + 1);

        TransferTaskRetryListener ta = getMockTransferRetryListenerInstance(vertx);

        // mock out the db service so we can can isolate method logic rather than db
        TransferTaskDatabaseService dbService = getMockTranserTaskDatabaseService(tt.toJson());

        when(ta.getDbService()).thenReturn(dbService);

        JsonObject jsonTransferTask = tt.toJson();

        ta.processRetryTransferTask(jsonTransferTask, isRetried -> {
            ctx.verify(() -> {
                verify(ta, times(1))._doPublishEvent(eq(TRANSFER_FAILED), any(JsonObject.class), any());
                assertFalse(isRetried.result(), "TRANSFER_FAILED event should be sent when max attempts is reached.");
                ctx.completeNow();
            });
        });
    }


    @Test
    @DisplayName("TransferRetryListener - Task failed whem UriSchemeIsNotSupported ")
    public void failUriSchemeIsNotSupportedTest(Vertx vertx, VertxTestContext ctx) throws Exception {

        TransferTask tt = _createTestTransferTask();
        tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        RemoteDataClient destClient = mock(RemoteDataClient.class);
        TransferTaskRetryListener ta = getMockTransferRetryListenerInstance(vertx);

        try {
            // force the source one to fail since it is an agave URI and can result in a bad system lookup.
            when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource()))))
                    .thenThrow(new SystemUnknownException("This should be thrown during the test and propagated back as the handler.cause() method."));
            // allow the dest one to succeed since it's not an agave URI
            when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest()))))
                    .thenReturn(destClient);
        } catch (Exception e) {
            ctx.failNow(e);
        }

        // mock out the db service so we can can isolate method logic rather than db
        TransferTaskDatabaseService dbService = getMockTranserTaskDatabaseService(tt.toJson());

        when(ta.getDbService()).thenReturn(dbService);

        when(ta.uriSchemeIsNotSupported(any())).thenReturn(true);

        doCallRealMethod().when(ta).processRetry(any(), any());
        ta.processRetry(tt, resp -> {
            ctx.verify(() -> {
                assertTrue(resp.failed());
                ctx.completeNow();
            });
        });
    }

}