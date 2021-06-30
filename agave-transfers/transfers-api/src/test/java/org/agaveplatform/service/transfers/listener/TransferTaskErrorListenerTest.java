package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ERROR;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("TransferErrorListener test")
//@Disabled
class TransferTaskErrorListenerTest extends BaseTestCase {
	private static final Logger log = LoggerFactory.getLogger(TransferTaskErrorListener.class);

	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	protected TransferTaskErrorListener getMockTransferErrorListenerInstance(Vertx vertx) {
		TransferTaskErrorListener listener = mock(TransferTaskErrorListener.class );
		try {
			when(listener.config()).thenReturn(config);
			when(listener.getEventChannel()).thenReturn(TRANSFERTASK_ERROR);
			when(listener.getVertx()).thenReturn(vertx);
			when(listener.config()).thenReturn(config);
			when(listener.getRecoverableExceptionsClassNames()).thenCallRealMethod();
			when(listener.taskIsNotInterrupted(any())).thenReturn(true);
			when(listener.getRecoverableExceptionsClassNames()).thenCallRealMethod();
			when(listener.getRetryRequestManager()).thenCallRealMethod();

			NatsJetstreamMessageClient natsCleint = mock(NatsJetstreamMessageClient.class);
			doNothing().when(natsCleint).push(any(), any());
			when(listener.getMessageClient()).thenReturn(natsCleint);

			doCallRealMethod().when(listener)._doPublishEvent(any(), any(), any());
			doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
			doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());
			doCallRealMethod().when(listener).processError(any(), any());
			doCallRealMethod().when(listener).processBody(any(), any());
			doCallRealMethod().when(listener).handleMessage(any());

		} catch (Exception ignored){}

		return listener;
	}

	@Test
	@DisplayName("handleMessage Test")
	public void processHandleMessageTest(Vertx vertx, VertxTestContext ctx) throws Exception{
		// mock out the test class
		TransferTaskErrorListener ta = getMockTransferErrorListenerInstance(vertx);
		// generate a fake transfer task
		TransferTask transferTask = _createTestTransferTask();
		JsonObject transferTaskJson = transferTask.toJson();

		Message msg = new Message(1, transferTask.toString());
		ta.handleMessage(msg);
		Thread.sleep(3);
		ctx.verify(() -> {
			verify(ta, atLeastOnce()).processError(eq(transferTaskJson), any());

			ctx.completeNow();
		});
	}

	@Test
	@DisplayName("TransferErrorListener.processError RemoteDataException and Status= QUEUED test")
	protected void processErrorRDE_test(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, MessagingException {
		TransferTask tt = _createTestTransferTask();
		tt.setId(4L);
		tt.setStatus(QUEUED);

		JsonObject body = tt.toJson();
		body.put("cause", RemoteDataException.class.getName());
		body.put("message", "Error Message");

		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);
//		NatsJetstreamMessageClient nats = getMockNats();

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByUuidResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByUuidResult);
			return null;
		}).when(dbService).getByUuid(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getId().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);

		txfrErrorListener.processError(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "processError should succeed when in an active state");
			assertTrue(resp.result(), "processError should return false when the body has a recoverable cause and has a status of QUEUED");
			// active statuses should be moved to error state
			verify(dbService).updateStatus(eq(tt.getTenantId()), eq(tt.getUuid()), eq(ERROR.name()), any());

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorListener.processError Child Task RemoteDataException and Status= QUEUED test")
	protected void processErrorChildRDE_test(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, MessagingException {
		String parentId = new AgaveUUID(UUIDType.TRANSFER).toString();
//		NatsJetstreamMessageClient nats = getMockNats();
		TransferTask tt = _createTestTransferTask();
		tt.setId(4L);
		tt.setStatus(QUEUED);
		tt.setParentTaskId(parentId);
		tt.setRootTaskId(parentId);

		JsonObject body = tt.toJson();
		body.put("cause", RemoteDataException.class.getName());
		body.put("message", "Error Message");

		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByUuidResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByUuidResult);
			return null;
		}).when(dbService).getByUuid(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getId().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);

		txfrErrorListener.processError(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "processError should succeed when in an active state");
			assertTrue(resp.result(), "processError should return false when the body has a recoverable cause and has a status of QUEUED");
			// active statuses should be moved to error state
			verify(dbService).updateStatus(eq(tt.getTenantId()), eq(tt.getUuid()), eq(ERROR.name()), any());

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorListener.processError Parent/Root Task IOException and Status= QUEUED test")
//	@Disabled
	protected void processErrorIOE_test(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, MessagingException {
		TransferTask tt = _createTestTransferTask();
		tt.setId(3L);
		tt.setStatus(QUEUED);

		JsonObject body = tt.toJson();
		body.put("cause", IOException.class.getName());
		body.put("message", "Error Message");

		log.info("Cause: = {}", body.getString("cause"));
		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);
//		NatsJetstreamMessageClient nats = getMockNats();

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByUuidResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByUuidResult);
			return null;
		}).when(dbService).getByUuid(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getId().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


		txfrErrorListener.processError(body, resp -> ctx.verify(() -> {

			verify(txfrErrorListener, times(1)).taskIsNotInterrupted(eq(tt));
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertTrue(resp.result(), "processError should succeed when the " +
					"TransferErrorListener.processError is called for an IOException. It is already in the QUEUE");
			// active statuses should be moved to error state
			verify(dbService).updateStatus(eq(tt.getTenantId()), eq(tt.getUuid()), eq(ERROR.name()), any());
			//verify(txfrErrorListener)._doPublishEvent(eq(TRANSFER_RETRY), eq(tt.toJson().put("status", ERROR.name())));

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorListener.processError Child Task IOException and Status= QUEUED test")
	protected void processErrorChildIOE_test(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, MessagingException {
		String parentId = new AgaveUUID(UUIDType.TRANSFER).toString();

		TransferTask tt = _createTestTransferTask();
		tt.setId(3L);
		tt.setStatus(QUEUED);
		tt.setParentTaskId(parentId);
		tt.setRootTaskId(parentId);

		JsonObject body = tt.toJson();
		body.put("cause", IOException.class.getName());
		body.put("message", "Error Message");

		log.info("Cause: = {}", body.getString("cause"));
		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByUuidResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByUuidResult);
			return null;
		}).when(dbService).getByUuid(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getId().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


		txfrErrorListener.processError(body, resp -> ctx.verify(() -> {
			verify(txfrErrorListener, times(1)).taskIsNotInterrupted(eq(tt));
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertTrue(resp.result(), "processError should succeed when the " +
					"TransferErrorListener.processError is called for an IOException. It is already in the QUEUE");
			// active statuses should be moved to error state
			verify(dbService).updateStatus(eq(tt.getTenantId()), eq(tt.getUuid()), eq(ERROR.name()), any());

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorListener.processError IOException and Status= COMPLETED test")
//	@Disabled
	protected void processErrorCOMPLETED_test(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, MessagingException {

		TransferTask tt = _createTestTransferTask();
		tt.setId(1L);
		tt.setStatus(COMPLETED);

		JsonObject body = tt.toJson();
		body.put("cause", IOException.class.getName());
		body.put("message", "Error Message");

		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByUuidResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByUuidResult);
			return null;
		}).when(dbService).getByUuid(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getId().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


		txfrErrorListener.processError(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertFalse(resp.result(), "processError should return FALSE when the TransferErrorListener.processError is called for an IOException and Status = COMPLETED");
			// no status update for tasks in done state
			verify(dbService, never()).updateStatus(eq(tt.getTenantId()), eq(tt.getUuid()), any(), any());

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorListener.processError Child Task IOException and Status= COMPLETED test")
//	@Disabled
	protected void processErrorChildCOMPLETED_test(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, MessagingException {
		String parentId = new AgaveUUID(UUIDType.TRANSFER).toString();

		TransferTask tt = _createTestTransferTask();
		tt.setId(1L);
		tt.setStatus(COMPLETED);
		tt.setParentTaskId(parentId);
		tt.setRootTaskId(parentId);

		JsonObject body = tt.toJson();
		body.put("cause", IOException.class.getName());
		body.put("message", "Error Message");

		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByUuidResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByUuidResult);
			return null;
		}).when(dbService).getByUuid(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getId().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

		txfrErrorListener.processError(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertFalse(resp.result(), "processError should return FALSE when the TransferErrorListener.processError is called for an IOException and Status = COMPLETED");
			// no status update for tasks in done state
			verify(dbService, never()).updateStatus(eq(tt.getTenantId()), eq(tt.getUuid()), any(), any());

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorListener.processError Parent/Root task InterruptedException and Status= FAILED test")
//	@Disabled
	protected void processErrorInterruptedException_FAILED_test(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, MessagingException {
		TransferTask tt = _createTestTransferTask();
		tt.setId(2L);
		tt.setStatus(FAILED);

		JsonObject body = tt.toJson();
		body.put("cause", "java.lang.InterruptedException");
		body.put("message", "Error Message");

		log.info("Cause: = {}", body.getString("cause"));
		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByUuidResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByUuidResult);
			return null;
		}).when(dbService).getByUuid(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getId().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


		txfrErrorListener.processError(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertFalse(resp.result(), "processError should return FALSE when the TransferErrorListener.processError is called for an IOException and Status = FAILED");
			// no status update for tasks in done state
			verify(dbService, never()).updateStatus(eq(tt.getTenantId()), eq(tt.getUuid()), any(), any());

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorListener.processError Child Task InterruptedException and Status= FAILED test")
	protected void processErrorChildInterruptedException_FAILED_test(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, MessagingException {
		String parentId = new AgaveUUID(UUIDType.TRANSFER).toString();

		TransferTask tt = _createTestTransferTask();
		tt.setId(2L);
		tt.setStatus(FAILED);
		tt.setParentTaskId(parentId);
		tt.setRootTaskId(parentId);

		JsonObject body = tt.toJson();
		body.put("cause", "java.lang.InterruptedException");
		body.put("message", "Error Message");

		log.info("Cause: = {}", body.getString("cause"));
		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByUuidResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByUuidResult);
			return null;
		}).when(dbService).getByUuid(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getId().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


		txfrErrorListener.processError(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertFalse(resp.result(), "processError should return FALSE when the TransferErrorListener.processError is called for an IOException and Status = FAILED");
			// no status update for tasks in done state
			verify(dbService, never()).updateStatus(eq(tt.getTenantId()), eq(tt.getUuid()), any(), any());

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorListener.processBody partial Transfer Task should return Transfer Task")
	protected void processBodyWithPartialTransferTaskTest(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, MessagingException {
		String parentId = new AgaveUUID(UUIDType.TRANSFER).toString();

		TransferTask tt = _createTestTransferTask();
		tt.setId(2L);
		tt.setStatus(FAILED);
		tt.setParentTaskId(parentId);
		tt.setRootTaskId(parentId);

		JsonObject body = new JsonObject()
				.put("id", tt.getId().toString())
				.put("cause", "java.lang.InterruptedException")
				.put("message", "Error Message");

		log.info("Cause: = {}", body.getString("cause"));
		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getId().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

		txfrErrorListener.processBody(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertNotNull(resp.result(), "processBody should return JsonObject for valid partial Transfer Task");
			assertEquals(tt.toJson(), resp.result(), "processBody should return the Transfer Task as JsonObject");

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorListener.processBody Transfer Task should return Transfer Task")
	protected void processBodyWithTransferTaskTest(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, MessagingException {
		String parentId = new AgaveUUID(UUIDType.TRANSFER).toString();

		TransferTask tt = _createTestTransferTask();
		tt.setId(2L);
		tt.setStatus(FAILED);
		tt.setParentTaskId(parentId);
		tt.setRootTaskId(parentId);

		JsonObject body = tt.toJson();
		body.put("cause", "java.lang.InterruptedException");
		body.put("message", "Error Message");

		log.info("Cause: = {}", body.getString("cause"));
		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);
//		NatsJetstreamMessageClient nats = getMockNats();

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getId().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

		txfrErrorListener.processBody(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertNotNull(resp.result(), "processBody should return JsonObject for valid partial Transfer Task");
			assertEquals(body, resp.result(), "processBody should return the Transfer Task as JsonObject");
			ctx.completeNow();
		}));
	}
}