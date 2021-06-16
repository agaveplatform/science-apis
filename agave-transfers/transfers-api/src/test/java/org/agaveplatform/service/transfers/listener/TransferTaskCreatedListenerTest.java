package org.agaveplatform.service.transfers.listener;

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
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.systems.exceptions.SystemRoleException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers assignTransferTask tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TransferTaskCreatedListenerTest extends BaseTestCase {

	TransferTaskCreatedListener getMockListenerInstance(Vertx vertx) throws IOException, InterruptedException, TimeoutException, MessagingException {
		TransferTaskCreatedListener listener = Mockito.mock(TransferTaskCreatedListener.class);
		try {
			when(listener.getEventChannel()).thenReturn(TRANSFERTASK_CREATED);
			when(listener.getVertx()).thenReturn(vertx);
			when(listener.getRemoteSystemAO()).thenCallRealMethod();
			when(listener.taskIsNotInterrupted(any())).thenReturn(true);
			when(listener.uriSchemeIsNotSupported(any())).thenReturn(false);
			when(listener.getRetryRequestManager()).thenCallRealMethod();
			doCallRealMethod().when(listener)._doPublishEvent(any(), any(), any());

			NatsJetstreamMessageClient natsCleint = mock(NatsJetstreamMessageClient.class);
			doNothing().when(natsCleint).push(any(), any());
			when(listener.getMessageClient()).thenReturn(natsCleint);

			doCallRealMethod().when(listener).processEvent(any(), any());
			doCallRealMethod().when(listener).doHandleError(any(), any(), any(), any());
			doCallRealMethod().when(listener).doHandleFailure(any(), any(), any(), any());
			doCallRealMethod().when(listener).addCancelledTask(anyString());
			doCallRealMethod().when(listener).addPausedTask(anyString());
		} catch (Exception ignored){}

		return listener;
	}

	@Test
	@DisplayName("Transfer Task Created Listener - assignment succeeds for valid transfer task")
	public void assignTransferTask(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, TimeoutException, MessagingException {

		// get the JsonObject to pass back and forth between verticles
		TransferTask transferTask = _createTestTransferTask();
		JsonObject json = transferTask.toJson();

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskCreatedListener ttc = getMockListenerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(transferTask.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(transferTask.getTenantId()), eq(transferTask.getUuid()), any(), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(ttc.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

		try {
			// return true on permission checks for this test
			when(ttc.userHasMinimumRoleOnSystem(eq(transferTask.getTenantId()), eq(transferTask.getOwner()), anyString(), any(RoleType.class))).thenReturn(true);
		} catch (SystemUnknownException | SystemUnavailableException | SystemRoleException e) {
			ctx.failNow(e);
		}

		ttc.processEvent(json, ctx.succeeding(isAssigned -> ctx.verify(() -> {
			assertTrue(isAssigned);
			verify(ttc, times(1))._doPublishEvent(TRANSFERTASK_ASSIGNED, eq(json.put("status",TransferStatusType.ASSIGNED.name())), any());
			verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any(JsonObject.class), any());
			verify(dbService, times(1)).updateStatus(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), eq(TransferStatusType.ASSIGNED.name()), any());
			//verify(ttc,times(1)).userHasMinimumRoleOnSystem(transferTask.getTenantId(), transferTask.getOwner(), URI.create(transferTask.getSource()).getHost(), RoleType.GUEST);
			verify(ttc,times(1)).userHasMinimumRoleOnSystem(transferTask.getTenantId(), transferTask.getOwner(), URI.create(transferTask.getDest()).getHost(), RoleType.USER);
			ctx.completeNow();
		})));
	}

	@Test
	@DisplayName("Transfer Task Created Listener - assignment fails with invalid source")
	public void assignTransferTaskFailSrcTest(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, TimeoutException, MessagingException {

		// get the JsonObject to pass back and forth between verticles
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setSource("htt://");
		JsonObject json = transferTask.toJson();

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskCreatedListener ttc = getMockListenerInstance(vertx);
		when(ttc.uriSchemeIsNotSupported(any())).thenReturn(false);
		try {
			// return true on permission checks for this test
			when(ttc.userHasMinimumRoleOnSystem(eq(transferTask.getTenantId()), eq(transferTask.getOwner()), anyString(), any(RoleType.class))).thenReturn(true);
		} catch (SystemUnknownException | SystemUnavailableException | SystemRoleException e) {
			ctx.failNow(e);
		}

		ttc.processEvent(json, ctx.failing(cause -> ctx.verify(() -> {
			assertEquals(cause.getClass(), RemoteDataSyntaxException.class, "Result should have been RemoteDataSyntaxException");
			verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_ASSIGNED), eq(json), any());
			verify(ttc, never()).userHasMinimumRoleOnSystem(any(),any(),any(),any());

			JsonObject errorBody = new JsonObject()
					.put("cause", cause.getClass().getName())
					.put("message", cause.getMessage())
					.mergeIn(json);
			verify(ttc, times(1))._doPublishEvent(eq(TRANSFERTASK_ERROR), eq(errorBody), any());
			ctx.completeNow();
		})));
	}

	@Test
	@DisplayName("Transfer Task Created Listener - assignment fails with invalid dest")
	public void assignTransferTaskFailDestTest(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, TimeoutException, MessagingException {

		// get the JsonObject to pass back and forth between verticles
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setDest("htt://");
		JsonObject json = transferTask.toJson();

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskCreatedListener ttc = getMockListenerInstance(vertx);
		when(ttc.uriSchemeIsNotSupported(any())).thenReturn(true, false);
		try {
			// return true on permission checks for this test
			when(ttc.userHasMinimumRoleOnSystem(eq(transferTask.getTenantId()), eq(transferTask.getOwner()), anyString(), any(RoleType.class))).thenReturn(true);
		} catch (SystemUnknownException | SystemUnavailableException | SystemRoleException e) {
			ctx.failNow(e);
		}

		ttc.processEvent(json, ctx.failing(cause -> ctx.verify(() -> {
			assertEquals(cause.getClass(), RemoteDataSyntaxException.class, "Result should have been RemoteDataSyntaxException");
			verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_ASSIGNED), eq(json), any());
			verify(ttc, never()).userHasMinimumRoleOnSystem(any(),any(),any(),any());

			JsonObject errorBody = new JsonObject()
					.put("cause", cause.getClass().getName())
					.put("message", cause.getMessage())
					.mergeIn(json);
			//verify(ttc, times(1))._doPublishEvent(TRANSFERTASK_ERROR, errorBody);
//			verify(nats, times(1)).push(any(), any(), errorBody.toString());
			ctx.completeNow();
		})));
	}

	@Test
	@DisplayName("TransferTaskCreatedListener - taskIsNotInterrupted")
	void taskIsNotInterruptedTest(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException {
		TransferTask tt = _createTestTransferTask();
		tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

		TransferTaskCreatedListener ta = new TransferTaskCreatedListener(vertx);

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