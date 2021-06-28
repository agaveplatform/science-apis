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
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.exception.TransferException;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers processPausedRequest tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
class TransferTaskPausedListenerTest extends BaseTestCase {

	private static final Logger log = LoggerFactory.getLogger(TransferTaskPausedListenerTest.class);

	TransferTaskPausedListener getMockListenerInstance(Vertx vertx) throws Exception {
		TransferTaskPausedListener listener = Mockito.mock(TransferTaskPausedListener.class);
		when(listener.getEventChannel()).thenReturn(MessageType.TRANSFERTASK_PAUSED);
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.getRetryRequestManager()).thenCallRealMethod();
		doCallRealMethod().when(listener)._doPublishEvent(any(), any(), any());
		//doNothing().when(listener)._doPublishEvent(any(), any(), any());
		doCallRealMethod().when(listener).processPauseRequest(any(), any());
		doCallRealMethod().when(listener).processParentEvent(any(), any(), any());
		doCallRealMethod().when(listener).doHandleError(any(), any(), any(), any());
		doCallRealMethod().when(listener).doHandleError(any(), any(), any());
		doCallRealMethod().when(listener).doHandleFailure(any(), any(), any(), any());
		doCallRealMethod().when(listener).doHandleFailure(any(), any(), any());

		NatsJetstreamMessageClient natsClient = Mockito.mock(NatsJetstreamMessageClient.class);
		doNothing().when(natsClient).push(any(), any(), anyInt());
		when(listener.getMessageClient()).thenReturn(natsClient);
		return listener;
	}

	protected TransferTaskPausedListener getMockTransferPausedListenerInstance(Vertx vertx) throws Exception {
		TransferTaskPausedListener listener = mock(TransferTaskPausedListener.class);
		when(listener.getEventChannel()).thenReturn(TRANSFERTASK_ASSIGNED);
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.taskIsNotInterrupted(any())).thenReturn(true);
		when(listener.uriSchemeIsNotSupported(any())).thenReturn(false);
		doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
		doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());
		when(listener.getRetryRequestManager()).thenCallRealMethod();
		doCallRealMethod().when(listener)._doPublishEvent(any(), any(), any());
		doCallRealMethod().when(listener).processPauseRequest(any(JsonObject.class), any());
		doCallRealMethod().when(listener).start();
		NatsJetstreamMessageClient natsClient = Mockito.mock(NatsJetstreamMessageClient.class);
		doNothing().when(natsClient).push(any(), any(), anyInt());
		when(listener.getMessageClient()).thenReturn(natsClient);
		doCallRealMethod().when(listener).start();
		doCallRealMethod().when(listener).handleMessage(any());
		doCallRealMethod().when(listener).handlePausedCompletedMessage(any());
		doCallRealMethod().when(listener).handlePausedSyncMessage(any());
		doCallRealMethod().when(listener).processPauseAckRequest(any(),any());
		return listener;
	}

	/**
	 * Generates a mock of the {@link TransferTaskDatabaseService} with the {@link TransferTaskDatabaseService#getByUuid(String, String, Handler)}
	 * method mocked out to return the given {@code jsonTransferTask} as the handler result;
	 *
	 * @param jsonTransferTask {@link JsonObject} to return from the {@link TransferTaskDatabaseService#getByUuid(String, String, Handler)} handler
	 * @return a mock of the db service with the {@link TransferTaskDatabaseService#getByUuid(String, String, Handler)) mocked out to return the {@code transferTaskToReturn} as an async result.
	 */
	private TransferTaskDatabaseService getMockTranserTaskDatabaseService(JsonObject jsonTransferTask) {
		// mock out the db service so we can can isolate method logic rather than db
		return new MockTransferTaskDatabaseService.Builder()
				.getByUuid(jsonTransferTask, false)
				.build();
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
	 * @param isDir     false if the instance should have type {@link RemoteFileInfo#FILE_TYPE}, true if {@link RemoteFileInfo#DIRECTORY_TYPE}
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
	@DisplayName("handleMessage Test")
	public void processHandleMessageTest(Vertx vertx, VertxTestContext ctx) throws Exception{
		// mock out the test class
		TransferTaskPausedListener listener = getMockTransferPausedListenerInstance(vertx);
		doCallRealMethod().when(listener).handleMessage(any());
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getMockAsyncResult(true));
			return null;
		}).when(listener).processPauseRequest(any(), any());

		// generate a fake transfer task
		TransferTask transferTask = _createTestTransferTask();
		JsonObject transferTaskJson = transferTask.toJson();

		Message msg = new Message(1, transferTaskJson.toString());
		listener.handleMessage(msg);
		Thread.sleep(3);
		ctx.verify(() -> {
			verify(listener, atLeastOnce()).processPauseRequest(eq(transferTaskJson), any());
			ctx.completeNow();
		});
	}


	@Test
//	@DisplayName("handleMessage Test")
	public void processHandleAckMessageTest(Vertx vertx, VertxTestContext ctx) throws Exception{
		// mock out the test class
		TransferTaskPausedListener listener = getMockTransferPausedListenerInstance(vertx);
		doCallRealMethod().when(listener).handlePausedAckMessage(any());
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getMockAsyncResult(true));
			return null;
		}).when(listener).processPauseAckRequest(any(), any());

		// generate a fake transfer task
		TransferTask transferTask = _createTestTransferTask();
		JsonObject transferTaskJson = transferTask.toJson();

		Message msg = new Message(1, transferTask.toJson().toString());
		listener.handlePausedAckMessage(msg);
		Thread.sleep(3);
		ctx.verify(() -> {

			verify(listener).processPauseAckRequest(eq(transferTaskJson), any());

			ctx.completeNow();
		});
	}

	/**
	 * Scaffolds the mock listener with basic db and method mocks. All calls to {@link TransferTaskPausedListener#processParentEvent(String, String, Handler)}
	 * on the mock will call the actual method.
	 *
	 * @param vertx the test vertx instance
	 * @param transferTask the test trasnsfer task used in mock creation
	 * @return a properly mocked listener to test processPauseRequest behavior
	 * @throws Exception
	 */
	private TransferTaskPausedListener initProcessPauseRequestTest(Vertx vertx, TransferTask transferTask) throws Exception {

		TransferTaskPausedListener listener = getMockListenerInstance(vertx);
		JsonObject jsonTransferTask = transferTask.toJson();

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = new MockTransferTaskDatabaseService.Builder()
				.getByUuid(jsonTransferTask, false)
				.updateStatus(jsonTransferTask, false)
				.allChildrenCancelledOrCompleted(true, false)
				.build();

		when(listener.getDbService()).thenReturn(dbService);

		// mock parent processing method as we test that independently
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getMockAsyncResult(true));
			return null;
		}).when(listener).processParentEvent(any(), any(), any());

		return listener;
	}

	/** Scaffolds the mock listener with basic db and method mocks. All calls to {@link TransferTaskPausedListener#processParentEvent(String, String, Handler)}
	 * on the mock will call the actual method.
	 *
	 * @param vertx the test vertx instance
	 * @param transferTask the test trasnsfer task used in mock creation
	 * @param isAllChildrenCancelledOrCompleted expected response from calls to {@link TransferTaskDatabaseService#allChildrenCancelledOrCompleted(String, String, Handler)}
	 * @return a properly mocked listener to test processPauseRequest behavior
	 * @throws Exception if mock fails for whatever reason.
	 */
	private TransferTaskPausedListener initProcessParentEventTest(Vertx vertx, TransferTask transferTask, boolean isAllChildrenCancelledOrCompleted) throws Exception {

		TransferTaskPausedListener listener = getMockListenerInstance(vertx);
		JsonObject jsonTransferTask = transferTask.toJson();

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = new MockTransferTaskDatabaseService.Builder()
				.getByUuid(jsonTransferTask, false)
				.updateStatus(jsonTransferTask, false)
				.allChildrenCancelledOrCompleted(isAllChildrenCancelledOrCompleted, false)
				.build();

		when(listener.getDbService()).thenReturn(dbService);

		// call the real method under testing
		doCallRealMethod().when(listener).processParentEvent(any(), any(), any());

		return listener;
	}

	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	@Test
//	@DisplayName("Processing pause event for non-root transfer task fails adn raises transfer exception")
	public void processPauseRequestNotRootTaskFailsWithErrorRaised(Vertx vertx, VertxTestContext ctx) throws Exception {

		// Set up our transfertask for testing
		String rootUuid = new AgaveUUID(UUIDType.TRANSFER).toString();
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setRootTaskId(rootUuid);

		TransferTaskPausedListener listener = getMockListenerInstance(vertx);

		TransferTaskDatabaseService dbService = new MockTransferTaskDatabaseService.Builder()
				.getByUuid(transferTask.toJson(), false)
				.build();

		when(listener.getDbService()).thenReturn(dbService);

		// now we run the actual test using our test transfer task data
		listener.processPauseRequest(transferTask.toJson(), ctx.failing(ex ->
			ctx.verify(() -> {
				assertTrue(ex instanceof TransferException, "processPauseRequest should fail with a TransferTaskException cause if it is not a root task.");

				// no event should be raised if we cannot pause the task
				verify(listener)._doPublishEvent(eq(TRANSFERTASK_ERROR),any(), any());

				// task should never be updated
				verify(dbService, never()).updateStatus(any(), any(), any(), any());

				ctx.completeNow();
			})));
	}

	@Test
//	@DisplayName("Processing pause event for non-parent transfer task fails and raises transfer exception")
	//@Disabled
	public void processPauseRequestChildTaskFailsWithTransferException(Vertx vertx, VertxTestContext ctx) throws Exception {

		// Set up our transfertask for testing
		String parentUuid = new AgaveUUID(UUIDType.TRANSFER).toString();
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setParentTaskId(parentUuid);

		JsonObject expectedUpdatedJsonObject = transferTask.toJson()
				.put("status", TransferStatusType.PAUSED.name())
				.put("endTime", Instant.now());

		TransferTaskPausedListener listener = getMockListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = new MockTransferTaskDatabaseService.Builder()
				.getByUuid(transferTask.toJson(), false)
				.updateStatus(expectedUpdatedJsonObject, false)
//				.allChildrenCancelledOrCompleted(true, false)
				.build();

		when(listener.getDbService()).thenReturn(dbService);

		// now we run the actual test using our test transfer task data
		listener.processPauseRequest(transferTask.toJson(), ctx.failing(pauseException ->
			ctx.verify(() -> {
				assertNotNull(pauseException, "processPauseRequest should fail with a transfer exception when parent task is not null");
				assertTrue(pauseException instanceof TransferException, "processPauseRequest should fail with a transfer exception if it is not a parent task.");

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, never()).processParentEvent(any(), any(), any());
				verify(dbService, never()).update(any(), any(), any(), any());
				verify(dbService, never()).updateStatus(any(), any(), any(), any());
				verify(dbService).getByUuid(any(), any(), any());

				// no event should be raised if we cannot pause the task
				verify(listener)._doPublishEvent(eq(TRANSFERTASK_ERROR),any(), any());

				ctx.completeNow();
			})));
	}

	@Test
//	@DisplayName("Processing pause event for inactive transfer task succeeds without doing anything and resolves true")
	//@Disabled
	public void processPauseRequestInactiveTaskFailsWithTransferException(Vertx vertx, VertxTestContext ctx) throws Exception {

		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.COMPLETED);

		TransferTaskPausedListener listener = initProcessPauseRequestTest(vertx, transferTask);

		// now we run the actual test using our test transfer task data
		listener.processPauseRequest(transferTask.toJson(), ctx.succeeding(resp ->
			ctx.verify(() -> {
				assertTrue(resp, "processPauseRequest should succed and return true when the task is not active.");
				// no event should be raised if we cannot pause the task
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR),any(), any());

				ctx.completeNow();
			})));
	}

	@Test
//	@DisplayName("Processing pause event for inactive transfer task succeeds and resolves true")
	//@Disabled
	public void processPauseRequestActiveTaskFailsWithExceptionWhenUUIDLookupFails(Vertx vertx, VertxTestContext ctx) throws Exception {

		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();

		TransferTaskPausedListener listener = getMockListenerInstance(vertx);
		JsonObject jsonTransferTask = transferTask.toJson();

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = new MockTransferTaskDatabaseService.Builder()
				.getByUuid(null, true)
				.build();

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(listener.getDbService()).thenReturn(dbService);

		// now we run the actual test using our test transfer task data
		listener.processPauseRequest(transferTask.toJson(), ctx.failing(pauseException ->
			ctx.verify(() -> {
				assertNotNull(pauseException, "Unknown uuid should return non-null exception");
				assertTrue(pauseException instanceof SQLException,
						"Task should fail and resolve SQLException when uuid is not found.");

				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR),any(), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PAUSED_ACK),any(), any());

				verify(dbService, never()).updateStatus(any() ,any(), any(), any());
				verify(listener, never()).processParentEvent(any(), any(), any());

				ctx.completeNow();
			})));
	}

	@Test
	@DisplayName("Transfer Task Paused Listener with no parent that is active - processPauseRequest")
	//@Disabled
	public void processPauseRequestActiveRootTaskSucceedsWithNoActiveChildren(Vertx vertx, VertxTestContext ctx) throws Exception {

		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		TransferTaskPausedListener listener = getMockListenerInstance(vertx);
		JsonObject jsonTransferTask = transferTask.toJson();
		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUpdatedJsonObject = transferTask.toJson()
				.put("status", TransferStatusType.PAUSED.name())
				.put("endTime", Instant.now());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = new MockTransferTaskDatabaseService.Builder()
				.getByUuid(jsonTransferTask, false)
				.updateStatus(expectedUpdatedJsonObject, false)
				.allChildrenCancelledOrCompleted(true, false)
				.build();

		when(listener.getDbService()).thenReturn(dbService);

		// mock parent processing method as we test that independently
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getMockAsyncResult(false));
			return null;
		}).when(listener).processParentEvent(any(), any(), any());


		// now we run the actual test using our test transfer task data
		listener.processPauseRequest(transferTask.toJson(), ctx.succeeding( resp ->
			ctx.verify(() -> {
				assertTrue(resp, "Response from processPauseRequest should be true when task is not active and there are no child tasks");

				// verify the db service was called to update the task status
				verify(dbService).updateStatus(eq(transferTask.getTenantId()),
						eq(transferTask.getUuid()), eq(TransferStatusType.PAUSE_WAITING.name()), any());

				// verify that the completed event was created. this should always be throws
				// if the updateStatus result succeeds.
				verify(listener)._doPublishEvent(eq(TRANSFERTASK_PAUSED_SYNC),any(), any());

				// make sure the parent was processed at least one time
				// TODO: why is this at least once? Do we know how many times it should be called?
				verify(listener, atLeastOnce()).processParentEvent(any(), any(), any());

				// make sure no error event is ever thrown
				//verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR),any(), any());
				//verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				ctx.completeNow();
			})));
	}

	@Test
	@DisplayName("TransferTask Paused Listener fails when transfer task has its own uuid as its parent or root")
	//@Disabled
	public void processPauseRequestFailsWhenUUIDMatchesOwnParentOrRoot(Vertx vertx, VertxTestContext ctx) throws Exception {

		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setRootTaskId(transferTask.getUuid());
		transferTask.setParentTaskId(transferTask.getUuid());

		TransferTaskPausedListener listener = getMockListenerInstance(vertx);
		JsonObject jsonTransferTask = transferTask.toJson();

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUpdatedJsonObject = transferTask.toJson().put("endTime", Instant.now());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = new MockTransferTaskDatabaseService.Builder()
				.getByUuid(jsonTransferTask, false)
//				.updateStatus(jsonTransferTask, false)
				.build();

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(listener.getDbService()).thenReturn(dbService);


//		// mock the handler passed into updateStatus
//		AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUpdatedJsonObject);
//		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
//			@SuppressWarnings("unchecked")
//			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
//			handler.handle(updateStatusHandler);
//			return null;
//		}).when(dbService).updateStatus(any(), any(), any(), any());
//
//		// mock a successful outcome with a puased result from processPauseRequest indicating the child has no active parents
//		AsyncResult<Boolean> processPauseRequest = getMockAsyncResult(Boolean.FALSE);
//
//		// mock the handler passed into processParentEvent
//		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
//			@SuppressWarnings("unchecked")
//			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
//			handler.handle(processPauseRequest);
//			return null;
//		}).when(listener).processParentEvent(any(), any(), any());

		// now we run the actual test using our test transfer task data
		listener.processPauseRequest(transferTask.toJson(), ctx.failing(pauseException -> ctx.verify(() -> {
				assertTrue(pauseException instanceof TransferException,
						"Task should fail and resolve TransferException when parent or root is same as own uuid");

				// verify the db service was called to update the task status
				verify(dbService, never()).updateStatus(any(), any(), any(), any());

				// verify that the completed event was created. this should always be throws
				// if the updateStatus result succeeds.
				//verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PAUSED), eq(parentTask.toJson()));
				verify(listener)._doPublishEvent(eq(TRANSFERTASK_ERROR), any(), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PAUSED_ACK), any(), any());
//				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any(), any());

			// make sure the parent was processed at least one time
				verify(listener, never()).processParentEvent(any(), any(), any());


				ctx.completeNow();
			})));
	}

	@Test
	@DisplayName("TransferTask Paused Listener with child and parent task that is active - processParentEvent")
	//@Disabled
	public void processPauseRequestProcessNotParentPaused(Vertx vertx, VertxTestContext ctx) throws Exception {

		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		TransferTask childTask = _createTestTransferTask();

		childTask.setStatus(TransferStatusType.TRANSFERRING);
		childTask.setStartTime(Instant.now());
		childTask.setEndTime(Instant.now());
		childTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));
		childTask.setParentTaskId(parentTask.getParentTaskId());
		childTask.setRootTaskId(parentTask.getRootTaskId());

		TransferTaskPausedListener listener = getMockListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(listener.getDbService()).thenReturn(dbService);

		JsonObject getByUuidJson = childTask.toJson();
		AsyncResult<JsonObject> getByUuidAsyncResult = getMockAsyncResult(getByUuidJson);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByUuidAsyncResult);
			return null;
		}).when(dbService).getByUuid(eq(childTask.getTenantId()), eq(childTask.getUuid()), anyObject() );

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUpdatedJsonObject = childTask.toJson()
				//.put("status", TransferStatusType.PAUSED.name())
				.put("endTime", Instant.now());

		AsyncResult<Boolean> updateStatusHandler = getMockAsyncResult(Boolean.TRUE);

		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(updateStatusHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

		doCallRealMethod().when(listener).processParentEvent(any(), any(), any());

		// now we run the actual test using our test transfer task data
		listener.processParentEvent(childTask.getTenantId(), childTask.getUuid(), result -> {
			ctx.verify(() -> {
				assertTrue(result.succeeded(), "Call to processParentEvent should succeed for active task");
				assertTrue(result.result(), "Response from processPauseRequest should be false when task is a child aka leaf task");

				// make sure the parent was processed at least one time
				verify(listener, atLeastOnce()).processParentEvent(any(), any(), any());


				ctx.completeNow();
			});
		});
	}

	@Override
	protected TransferTask _createTestTransferTask() {
		TransferTask transferTask = super._createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());
		transferTask.setSource(TRANSFER_SRC);

		return transferTask;
	}


	@Test
//	@DisplayName("Processing Inactive parent task raises TRANSFERTASK_PAUSED_ACK and resolves false")
	//@Disabled
	public void processParentEventInactiveTaskRaisesAckAndResolvesFalse(Vertx vertx, VertxTestContext ctx) throws Exception {

		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.COMPLETED);

		TransferTaskPausedListener listener = initProcessParentEventTest(vertx, parentTask, true);

		// now we run the actual test using our test transfer task data
		listener.processParentEvent(parentTask.getTenantId(), parentTask.getUuid(), ctx.succeeding(result ->
				ctx.verify(() -> {
//				assertTrue(ex instanceof TransferException, "processParentEvent should resolve failure with " +
//						"a transfer exception when the transfertask uuid is the same as the root or parent uuid.");

					assertFalse(result, "Inactive transfer task should immediately ack and resolve false");
					// make sure no error event is ever thrown
					verify(listener)._doPublishEvent(eq(TRANSFERTASK_PAUSED_ACK), any(), any());

					ctx.completeNow();
				})));
	}

	@Test
//	@DisplayName("Processing active parent task with active children resolves false and raises no event")
	public void processParentEventActiveTaskActiveChildrenResolvesFalse(Vertx vertx, VertxTestContext ctx) throws Exception {

		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();

		// Scaffold test and have the child search return false, indicating some children are still active.
		TransferTaskPausedListener listener = initProcessParentEventTest(vertx, transferTask, false);

		// now we run the actual test using our test transfer task data
		listener.processParentEvent(transferTask.getTenantId(), transferTask.getUuid(), ctx.succeeding(result ->
			ctx.verify(() -> {
				assertFalse(result, "Response from processParentEvent should be false when parent has active children");

				verify(listener, never())._doPublishEvent(any(), any(), any());

				ctx.completeNow();
			})));
	}

	@Test
//	@DisplayName("Processing active parent task with no active children resolves true and raises a TRANSFERTASK_PAUSED_ACK event")
	public void processParentEventActiveTaskNoActiveChildrenRaisesEventResolvesTrue(Vertx vertx, VertxTestContext ctx) throws Exception {

		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();

		// Scaffold test and have the child search return false, indicating some children are still active.
		TransferTaskPausedListener listener = initProcessParentEventTest(vertx, transferTask, true);

		// now we run the actual test using our test transfer task data
		listener.processParentEvent(transferTask.getTenantId(), transferTask.getUuid(), ctx.succeeding(result ->
				ctx.verify(() -> {
					assertTrue(result, "Response from processParentEvent should be true when parent has no active children");

					verify(listener)._doPublishEvent(eq(TRANSFERTASK_PAUSED_ACK), any(), any());

					ctx.completeNow();
				})));
	}

//	@Test
//	@DisplayName("TransferTask Paused Listener ")
//	//@Disabled
//	public void processParentEvent2ParentPaused(Vertx vertx, VertxTestContext ctx) throws Exception {
//
//		// Set up our transfertask for testing
//		TransferTask parentTask = _createTestTransferTask();
//		parentTask.setStatus(TransferStatusType.TRANSFERRING);
//		parentTask.setStartTime(Instant.now());
//		parentTask.setEndTime(Instant.now());
//		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));
//
//		TransferTask childTask = _createTestTransferTask();
//		childTask.setStatus(TransferStatusType.TRANSFERRING);
//		childTask.setStartTime(Instant.now());
//		childTask.setEndTime(Instant.now());
//		childTask.setSource(TRANSFER_SRC);
//
//		JsonObject rootTransferTaskJson = childTask.toJson();
//		JsonObject updatedTransferTaskJson = rootTransferTaskJson.copy().put("status", TransferStatusType.ASSIGNED.name());
//
//		TransferTaskPausedListener listener = getMockListenerInstance(vertx);
//
//		// mock out the db service so we can can isolate method logic rather than db
//		TransferTaskDatabaseService dbService = new MockTransferTaskDatabaseService.Builder()
//				.getByUuid(updatedTransferTaskJson, false)
////				.updateStatus(jsonTransferTask, false)
//				.build();
//
//		// now we are mocking the updateStatus
//		AsyncResult<JsonObject> updatedStatusAsyncResult = getMockAsyncResult(updatedTransferTaskJson);
//		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
//			@SuppressWarnings("unchecked")
//			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
//			handler.handle(updatedStatusAsyncResult);
//			return null;
//		}).when(dbService).updateStatus(any(), any(), any(), any(Handler.class));
//
//
//		JsonObject expectedParentTaskJson = parentTask.toJson();
//		AsyncResult<JsonObject> parentGetById = getMockAsyncResult(expectedParentTaskJson);
//		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
//			@SuppressWarnings("unchecked")
//			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
//			handler.handle(parentGetById);
//			return null;
//		}).when(dbService).getByUuid(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), anyObject() );
//
//		// mock the handler passed into allChildrenCancelledOrCompleted
//		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);
//		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
//			@SuppressWarnings("unchecked")
//			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
//			handler.handle(allChildrenCancelledOrCompletedHandler);
//			return null;
//		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());
//
//		// mock the dbService getter in our mocked vertical so we don't need to use powermock
//		when(listener.getDbService()).thenReturn(dbService);
//
//		doCallRealMethod().when(listener).processParentEvent(any(), any(), any());
//
//		// now we run the actual test using our test transfer task data
//		listener.processParentEvent(parentTask.getTenantId(), parentTask.getUuid(), result -> {
//			ctx.verify(() -> {
//				assertTrue(result.succeeded(), "Call to processParentEvent should succeed for active parent task");
//				assertTrue(result.result(), "Response from processParentEvent should be true when task is a parent with all children inactive");
//
//				// make sure the parent was processed at least one time
//				verify(listener, never()).processPauseRequest(any(), any());
//
//				// make sure no error event is ever thrown
//				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any(), any());
//
//				// parent task should have paused event thrown on it
////				verify(listener)._doPublishEvent(eq(TRANSFERTASK_PAUSED), argThat(new IsSameJsonTransferTask(parentTask.toJson())), any());
//
//				ctx.completeNow();
//			});
//		});
//	}


//	@Test
//	@DisplayName("Transfer Paused Listener smoke test with Assigned Vertical and checking Paused Vertical")
//	@Disabled
//	// This should be an integration tests
//	public void processTransferTaskAbortsChildProcessingOnInterrupt(Vertx vertx, VertxTestContext ctx) throws Exception {
//		// mock out the test class
//		TransferTaskPausedListener ta = getMockTransferPausedListenerInstance(vertx);
//
//		TransferTaskPausedListener tp = getMockListenerInstance(vertx);
//
//		// generate a fake transfer task
//		TransferTask rootTransferTask = _createTestTransferTask();
//		rootTransferTask.setId(1L);
//		JsonObject rootTransferTaskJson = rootTransferTask.toJson();
//		// generate the expected updated JsonObject
//		JsonObject updatedTransferTaskJson = rootTransferTaskJson.copy().put("status", TransferStatusType.ASSIGNED.name());
//
//
//		// mock out the db service so we can can isolate method logic rather than db
//		TransferTaskDatabaseService dbService = new MockTransferTaskDatabaseService.Builder()
//				.createOrUpdateChildTransferTask(rootTransferTaskJson, false)
//				.updateStatus(updatedTransferTaskJson, false)
//				.getByUuid(jsonTransferTask, false)
//				.build();
//
//
//		// assign the mock db service to the test listener
//		when(ta.getDbService()).thenReturn(dbService);
//
//		try {
//			// get mock remote data clients to mock the remote src system interactions
//			URI srcUri = URI.create(rootTransferTask.getSource());
//			RemoteDataClient srcRemoteDataClient = getMockRemoteDataClient(srcUri.getPath(), true, true);
//
//			// get mock remote data clients to mock the remote dest system interactions
//			URI destUri = URI.create(rootTransferTask.getDest());
//			RemoteDataClient destRemoteDataClient = getMockRemoteDataClient(destUri.getPath(), true, true);
//
//			when(ta.getRemoteDataClient(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getOwner()), eq(srcUri))).thenReturn(srcRemoteDataClient);
//			when(ta.getRemoteDataClient(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getOwner()), eq(destUri))).thenReturn(destRemoteDataClient);
//		} catch (Exception e) {
//			// bubble the failure up to the VertxTestContext so exceptions are reported with messages
//			try {
//				fail("Failed to initialize the remote data clients during test setup.", e);
//			} catch (Throwable t) {
//				ctx.failNow(t);
//			}
//		}
//
//		// mock the handler passed into processPauseRequest
//		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
//			@SuppressWarnings("unchecked")
//			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(1, Handler.class);
//			handler.handle(getMockAsyncResult(Boolean.FALSE));
//			return null;
//		}).when(tp).processPauseRequest(any(), any());
//
//		// mock the handler passed into processParentEvent
//		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
//			@SuppressWarnings("unchecked")
//			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
//			handler.handle(getMockAsyncResult(Boolean.FALSE));
//			return null;
//		}).when(tp).processParentEvent(any(), any(), any());
//
//
//		// we already mock this once in the mock listener setup. This will override the mock returning true the first
//		// invocation of the method and false thereafter.
//		when(ta.taskIsNotInterrupted(any())).thenReturn(true,false);
//
//		try {
//			ta.start();
//		} catch (IOException | TimeoutException | InterruptedException e) {
//			e.printStackTrace();
//
//		}
//
//		// Nothing is being tested here.
//		ctx.verify(() ->{
//			doCallRealMethod().when(ta).addCancelledTask(anyString());
//			ctx.completeNow();
//		});
//	}

}