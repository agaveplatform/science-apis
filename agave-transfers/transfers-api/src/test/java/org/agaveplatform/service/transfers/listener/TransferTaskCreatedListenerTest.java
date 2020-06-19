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
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.systems.exceptions.SystemRoleException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.net.URI;
import java.time.Instant;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers assignTransferTask tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TransferTaskCreatedListenerTest extends BaseTestCase {

//	private static final Logger log = LoggerFactory.getLogger(TransferTaskCreatedListenerTest.class);

	TransferTaskCreatedListener getMockListenerInstance(Vertx vertx) {
		TransferTaskCreatedListener ttc = Mockito.mock(TransferTaskCreatedListener.class);
		when(ttc.getEventChannel()).thenReturn(TRANSFERTASK_CREATED);
		when(ttc.getVertx()).thenReturn(vertx);
		when(ttc.getRemoteSystemAO()).thenCallRealMethod();
		when(ttc.taskIsNotInterrupted(any())).thenReturn(true);
		doCallRealMethod().when(ttc).assignTransferTask(any(), any());

		return ttc;
	}

	@Test
	@DisplayName("Transfer Task Created Listener - assignment succeeds for valid transfer task")
	public void assignTransferTask(Vertx vertx, VertxTestContext ctx) {

		// get the JsonObject to pass back and forth between verticles
		TransferTask transferTask = _createTestTransferTask();
		JsonObject json = transferTask.toJson();

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskCreatedListener ttc = getMockListenerInstance(vertx);
		//when(RemoteDataClientFactory.isSchemeSupported(any())).thenCallRealMethod();
		// the interrupt behavior should be tested independently of the happy pth test happening here.
		when(ttc.taskIsNotInterrupted(eq(transferTask))).thenReturn(true);

		doNothing().when(ttc)._doPublishEvent(any(), any());


		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = transferTask.toJson()
				.put("status", TransferStatusType.FAILED.name())
				.put("endTime", Instant.now())
				.put("lastUpdated", Instant.now());

		AsyncResult<JsonObject> expectedUpdateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(expectedUpdateStatusHandler);
			return null;
		}).when(dbService).updateStatus( eq(transferTask.getTenantId()), eq(transferTask.getUuid()), eq(transferTask.getStatus().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(ttc.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

		try {
			// return true on permission checks for this test
			when(ttc.userHasMinimumRoleOnSystem(eq(transferTask.getTenantId()), eq(transferTask.getOwner()), anyString(), any(RoleType.class))).thenReturn(true);
		} catch (SystemUnknownException | SystemUnavailableException | SystemRoleException e) {
			ctx.failNow(e);
		}

		ttc.assignTransferTask(json, ctx.succeeding(isAssigned -> ctx.verify(() -> {
			assertTrue(isAssigned);
			verify(ttc, times(1))._doPublishEvent(TRANSFERTASK_ASSIGNED, json);
			verify(ttc, never())._doPublishEvent(TRANSFERTASK_ERROR, new JsonObject());
			//verify(ttc,times(1)).userHasMinimumRoleOnSystem(transferTask.getTenantId(), transferTask.getOwner(), URI.create(transferTask.getSource()).getHost(), RoleType.GUEST);
			verify(ttc,times(1)).userHasMinimumRoleOnSystem(transferTask.getTenantId(), transferTask.getOwner(), URI.create(transferTask.getDest()).getHost(), RoleType.USER);
			ctx.completeNow();
		})));
	}

	@Test
	@DisplayName("Transfer Task Created Listener - assignment fails with invalid source")
	//@Disabled
	public void assignTransferTaskFailSrcTest(Vertx vertx, VertxTestContext ctx) {

		// get the JsonObject to pass back and forth between verticles
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setSource("htt://");
		JsonObject json = transferTask.toJson();

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskCreatedListener ttc = getMockListenerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = transferTask.toJson()
				.put("status", TransferStatusType.FAILED.name())
				.put("endTime", Instant.now())
				.put("lastUpdated", Instant.now());

		AsyncResult<JsonObject> expectedUpdateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(expectedUpdateStatusHandler);
			return null;
		}).when(dbService).updateStatus( eq(transferTask.getTenantId()), eq(transferTask.getUuid()), eq(transferTask.getStatus().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(ttc.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


		try {
			// return true on permission checks for this test
			when(ttc.userHasMinimumRoleOnSystem(eq(transferTask.getTenantId()), eq(transferTask.getOwner()), anyString(), any(RoleType.class))).thenReturn(true);
		} catch (SystemUnknownException | SystemUnavailableException | SystemRoleException e) {
			ctx.failNow(e);
		}

		ttc.assignTransferTask(json, ctx.failing(cause -> ctx.verify(() -> {
			assertEquals(cause.getClass(), RemoteDataSyntaxException.class, "Result should have been RemoteDataSyntaxException");
			verify(ttc, never())._doPublishEvent(TRANSFERTASK_ASSIGNED, json);
			verify(ttc,never()).userHasMinimumRoleOnSystem(any(),any(),any(),any());

			JsonObject errorBody = new JsonObject()
					.put("cause", cause.getClass().getName())
					.put("message", cause.getMessage())
					.mergeIn(json);
			verify(ttc, times(1))._doPublishEvent(TRANSFERTASK_ERROR, errorBody);
			ctx.completeNow();
		})));
	}



}