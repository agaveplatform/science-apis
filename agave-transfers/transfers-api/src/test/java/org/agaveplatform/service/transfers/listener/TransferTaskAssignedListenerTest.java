package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

//import org.agaveplatform.service.transfers.exception.InterruptableTransferTaskException;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("👋 TransferTaskAssignedListener test")
class TransferTaskAssignedListenerTest extends BaseTestCase {
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
	protected TransferTaskAssignedListener getMockTransferAssignedListenerInstance(Vertx vertx) {
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
	 * Generates a mock of the {@link TransferTaskDatabaseService} with the {@link TransferTaskDatabaseService#getById(String, String, Handler)}
	 * method mocked out to return the given {@code transferTask};
	 *
	 * @param transferTaskToReturn {@link JsonObject} to return from the {@link TransferTaskDatabaseService#getById(String, String, Handler)} handler
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
		}).when(dbService).getById(any(), any(), any());

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
						generateRemoteFileInfo(remotePath + "/" + UUID.randomUUID().toString(), true),
						generateRemoteFileInfo(remotePath + "/" + UUID.randomUUID().toString(), false),
						generateRemoteFileInfo(remotePath + "/" + UUID.randomUUID().toString(), false)
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
	@DisplayName("TransferTaskAssignedListener - processTransferTask assigns single file transfer task")
	//@Disabled
	public void processTransferTaskAssignsSingleFileTransferTask(Vertx vertx, VertxTestContext ctx) {
		// mock out the test class
		TransferTaskAssignedListener ta = getMockTransferAssignedListenerInstance(vertx);

		// generate a fake transfer task
		TransferTask rootTransferTask = _createTestTransferTask();
		rootTransferTask.setId(1L);
		JsonObject rootTransferTaskJson = rootTransferTask.toJson();
		// generate the expected updated JsonObject
		JsonObject updatedTransferTaskJson = rootTransferTaskJson.copy().put("status", TransferStatusType.ASSIGNED.name());

		// now mock out our db interactions. Here we
		TransferTaskDatabaseService dbService = getMockTranserTaskDatabaseService(rootTransferTaskJson);
		AsyncResult<JsonObject> updatedStatusAsyncResult = getMockAsyncResult(updatedTransferTaskJson);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			// returning the given transfer task, adding an id if it doesn't have one.
//			handler.handle(Future.succeededFuture(arguments.getArgumentAt(1, TransferTask.class).toJson()));
			handler.handle(updatedStatusAsyncResult);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any(Handler.class));

		// assign the mock db service to the test listener
		when(ta.getDbService()).thenReturn(dbService);

		// get mock remote data clients to mock the remote src system interactions
		URI srcUri = URI.create(rootTransferTask.getSource());
		RemoteDataClient srcRemoteDataClient = getMockRemoteDataClient(srcUri.getPath(), false, false);

		// get mock remote data clients to mock the remote dest system interactions
		URI destUri = URI.create(rootTransferTask.getDest());
		RemoteDataClient destRemoteDataClient = getMockRemoteDataClient(destUri.getPath(), false, false);

		try {
			when(ta.getRemoteDataClient(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getOwner()), eq(srcUri))).thenReturn(srcRemoteDataClient);
			when(ta.getRemoteDataClient(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getOwner()), eq(destUri))).thenReturn(destRemoteDataClient);
		} catch (Exception e) {
			// bubble the failure up to the VertxTestContext so exceptions are reported with messages
			try {
				fail("Failed to initialize the remote data clients during test setup.", e);
			} catch (Throwable t) {
				ctx.failNow(t);
			}
		}

		ta.processTransferTask(rootTransferTaskJson, result -> {
			ctx.verify(() -> {
				assertTrue(result.succeeded(), "Task assignment should return true on successful processing.");
				assertTrue(result.result(), "Callback result should be true after successful assignment.");

				// remote file info should be obtained once.
				verify(srcRemoteDataClient, times(1)).getFileInfo(eq(srcUri.getPath()));

				// mkdir should never be called on the src client.
				verify(srcRemoteDataClient, never()).mkdirs(eq(destUri.getPath()));

//				if (isFile) {
				// task status should be updated to assigned before creating the event
//				verify(dbService).updateStatus(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getUuid()), eq(TransferStatusType.ASSIGNED.name()), any());

				// mkdir should only be called on directory items. this is a file.
				verify(destRemoteDataClient, never()).mkdirs(eq(destUri.getPath()));
				// listing should never be called when source is file
				verify(srcRemoteDataClient, never()).ls(any());

				// TRANSFER_ALL event should have been raised
				verify(ta, times(1))._doPublishEvent(eq(TRANSFER_ALL), eq(updatedTransferTaskJson));
				// no error event should have been raised
				verify(ta, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());

				ctx.completeNow();

			});
		});
	}

	@Test
	@DisplayName("TransferTaskAssignedListener - processTransferTask assigns empty directory transfer task")
	//@Disabled
	public void processTransferTaskAssignsEmptyDirectoryTransferTask(Vertx vertx, VertxTestContext ctx) {
		// mock out the test class
		TransferTaskAssignedListener ta = getMockTransferAssignedListenerInstance(vertx);

		// generate a fake transfer task
		TransferTask rootTransferTask = _createTestTransferTask();
		rootTransferTask.setId(1L);
		JsonObject rootTransferTaskJson = rootTransferTask.toJson();
		// generate the expected updated JsonObject
		JsonObject updatedTransferTaskJson = rootTransferTaskJson.copy().put("status", TransferStatusType.ASSIGNED.name());

		// now mock out our db interactions. Here we
		TransferTaskDatabaseService dbService = getMockTranserTaskDatabaseService(rootTransferTaskJson);
		AsyncResult<JsonObject> updatedTransferTaskAsyncResult = getMockAsyncResult(updatedTransferTaskJson);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			// returning the given transfer task, adding an id if it doesn't have one.
//			handler.handle(Future.succeededFuture(arguments.getArgumentAt(1, TransferTask.class).toJson()));
			handler.handle(updatedTransferTaskAsyncResult);
			return null;
		}).when(dbService).createOrUpdateChildTransferTask(any(), any(TransferTask.class), any(Handler.class));

		AsyncResult<JsonObject> updatedStatusAsyncResult = getMockAsyncResult(updatedTransferTaskJson);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			// returning the given transfer task, adding an id if it doesn't have one.
//			handler.handle(Future.succeededFuture(arguments.getArgumentAt(1, TransferTask.class).toJson()));
			handler.handle(updatedStatusAsyncResult);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any(Handler.class));

		// assign the mock db service to the test listener
		when(ta.getDbService()).thenReturn(dbService);

		// get mock remote data clients to mock the remote src system interactions
		URI srcUri = URI.create(rootTransferTask.getSource());
		RemoteDataClient srcRemoteDataClient = getMockRemoteDataClient(srcUri.getPath(), true, false);

		// get mock remote data clients to mock the remote dest system interactions
		URI destUri = URI.create(rootTransferTask.getDest());
		RemoteDataClient destRemoteDataClient = getMockRemoteDataClient(destUri.getPath(), true, false);

		try {
			when(ta.getRemoteDataClient(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getOwner()), eq(srcUri))).thenReturn(srcRemoteDataClient);
			when(ta.getRemoteDataClient(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getOwner()), eq(destUri))).thenReturn(destRemoteDataClient);
		} catch (Exception e) {
			// bubble the failure up to the VertxTestContext so exceptions are reported with messages
			try {
				fail("Failed to initialize the remote data clients during test setup.", e);
			} catch (Throwable t) {
				ctx.failNow(t);
			}
		}

		ta.processTransferTask(rootTransferTaskJson, result -> {
			ctx.verify(() -> {
				assertTrue(result.succeeded(), "Task assignment should return true on successful processing.");
				assertTrue(result.result(), "Callback result should be true after successful assignment.");

				verify(ta).taskIsNotInterrupted(eq(rootTransferTask));
//				verify(ta).taskIsNotInterrupted(eq(new TransferTask(updatedTransferTaskJson)));

				// remote file info should be obtained once.
				verify(srcRemoteDataClient, times(1)).getFileInfo(eq(srcUri.getPath()));

				// mkdir should never be called on the src client.
				verify(srcRemoteDataClient, never()).mkdirs(eq(destUri.getPath()));

				// mkdir should only be called on non-empty directory items.
				verify(destRemoteDataClient, never()).mkdirs(any());

				// listing should be called with srch path
				verify(srcRemoteDataClient, times(1)).ls(eq(srcUri.getPath()));

				verify(dbService).updateStatus(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getUuid()), eq(TransferStatusType.ASSIGNED.name()), any());

//				verify(dbService, times(1)).createOrUpdateChildTransferTask(eq(rootTransferTask.getTenantId()), eq(rootTransferTask), any());

				// TRANSFER_ALL event should have been raised
				verify(ta, times(1))._doPublishEvent(eq(TRANSFER_COMPLETED), eq(updatedTransferTaskJson));
				// no error event should have been raised
				verify(ta, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TransferTaskAssignedListener - processTransferTask assigns populated directory transfer task")
//	@Disabled
	public void processTransferTaskAssignsPopulatedDirectoryTransferTask(Vertx vertx, VertxTestContext ctx) {
		// mock out the test class
		TransferTaskAssignedListener ta = getMockTransferAssignedListenerInstance(vertx);

		// generate a fake transfer task
		TransferTask rootTransferTask = _createTestTransferTask();
		rootTransferTask.setId(1L);
		JsonObject rootTransferTaskJson = rootTransferTask.toJson();
		// generate the expected updated JsonObject
		JsonObject updatedTransferTaskJson = rootTransferTaskJson.copy().put("status", TransferStatusType.ASSIGNED.name());

		// now mock out our db interactions. Here we
		TransferTaskDatabaseService dbService = getMockTranserTaskDatabaseService(rootTransferTaskJson);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			// returning the given transfer task, adding an id if it doesn't have one.
			JsonObject childTask = arguments.getArgumentAt(1, TransferTask.class).toJson().put("id", Instant.now().toEpochMilli());
			handler.handle(Future.succeededFuture(childTask));
//			handler.handle(updatedTransferTaskAsyncResult);
			return null;
		}).when(dbService).createOrUpdateChildTransferTask(any(), any(), any(Handler.class));

		AsyncResult<JsonObject> updatedStatusAsyncResult = getMockAsyncResult(updatedTransferTaskJson);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updatedStatusAsyncResult);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any(Handler.class));

		// assign the mock db service to the test listener
		when(ta.getDbService()).thenReturn(dbService);

		// get mock remote data clients to mock the remote src system interactions
		URI srcUri = URI.create(rootTransferTask.getSource());
		RemoteDataClient srcRemoteDataClient = getMockRemoteDataClient(srcUri.getPath(), true, true);

		// get mock remote data clients to mock the remote dest system interactions
		URI destUri = URI.create(rootTransferTask.getDest());
		RemoteDataClient destRemoteDataClient = getMockRemoteDataClient(destUri.getPath(), true, true);

		try {
			when(ta.getRemoteDataClient(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getOwner()), eq(srcUri))).thenReturn(srcRemoteDataClient);
			when(ta.getRemoteDataClient(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getOwner()), eq(destUri))).thenReturn(destRemoteDataClient);
		} catch (Exception e) {
			// bubble the failure up to the VertxTestContext so exceptions are reported with messages
			try {
				fail("Failed to initialize the remote data clients during test setup.", e);
			} catch (Throwable t) {
				ctx.failNow(t);
			}
		}

		ta.processTransferTask(rootTransferTaskJson, result -> {
			ctx.verify(() -> {
				assertTrue(result.succeeded(), "Task assignment should return true on successful processing.");
				assertTrue(result.result(), "Callback result should be true after successful assignment.");

				// remote file info should be obtained once.
				verify(srcRemoteDataClient, times(1)).getFileInfo(eq(srcUri.getPath()));

				// mkdir should never be called on the src client.
				verify(srcRemoteDataClient, never()).mkdirs(eq(destUri.getPath()));

				// mkdir should only be called on non-empty directory items.
				verify(destRemoteDataClient, times(1)).mkdirs(any());

				// listing should be called with srch path
				verify(srcRemoteDataClient, times(1)).ls(eq(srcUri.getPath()));

				// should be called once after the children are processed
				verify(dbService).updateStatus(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getUuid()), eq(TransferStatusType.ASSIGNED.name()), any());

				// get the test list of remote child file items
				List<RemoteFileInfo> remoteFileInfoList = srcRemoteDataClient.ls(srcUri.getPath());

				// interruption check should happen on every iteration and once before the stream begins
				verify(ta, times(remoteFileInfoList.size())).taskIsNotInterrupted(eq(rootTransferTask));

				// we capture the constructed child transfer task passed to the db call to verify that it has
				// the expected values.
				ArgumentCaptor<TransferTask> childTransferTaskArgument = ArgumentCaptor.forClass(TransferTask.class);
				// the method is called mlutiple times, so we capture them all here.
				verify(dbService, times(remoteFileInfoList.size()-1)).createOrUpdateChildTransferTask(
						eq(rootTransferTask.getTenantId()), childTransferTaskArgument.capture(), any());

				// we can then extract the values after verifying that it was called the expected number of times.
				List<TransferTask> childTransferTasks = childTransferTaskArgument.getAllValues();

				// directory children should raise TRANSFERTASK_CREATED events
				verify(ta, times((int)remoteFileInfoList.stream().filter(RemoteFileInfo::isDirectory).count()-1))._doPublishEvent(eq(TRANSFERTASK_CREATED), any(JsonObject.class));
				// file item children should raise TRANSFER_ALL events
				verify(ta, times((int)remoteFileInfoList.stream().filter(RemoteFileInfo::isFile).count()))._doPublishEvent(eq(TRANSFER_ALL), any(JsonObject.class));

				// the method is called mlutiple times, so we capture the expected events using an or check.
//				verify(ta, times(remoteFileInfoList.size()))._doPublishEvent(or(eq(TRANSFERTASK_CREATED), eq(TRANSFERTASK_CREATED)), childJsonObjectArgument.capture());

//				// we can then extract the values after verifying that it was called the expected number of times.
//				List<JsonObject> childJsonObjects = childJsonObjectArgument.getAllValues();


				// we then iterate over the actual child transfer task values created in the test and validate each
				// looks as we expect
				for (int i=0; i<childTransferTasks.size(); i++) {
					RemoteFileInfo fileInfo = remoteFileInfoList.get(i);
					TransferTask tt = childTransferTasks.get(i);

					Path ttPath = Paths.get(URI.create(tt.getSource()).getPath()).normalize();
					Path parentPath = Paths.get(URI.create(rootTransferTask.getSource()).getPath()).normalize();

					assertEquals(rootTransferTask.getUuid(), tt.getParentTaskId(),
							"Actual parent id of task does not match expected parent task id");

					assertEquals((rootTransferTask.getRootTaskId() == null ? rootTransferTask.getUuid() : rootTransferTask.getRootTaskId()),
							tt.getRootTaskId(), "Actual root id of task does not match expected root task id");

					assertTrue(ttPath.startsWith(parentPath),
							String.format("Actual task source does not begin with parent source: %s !startsWith %s\n",
									ttPath.toString(), rootTransferTask.getSource()));

					assertEquals(rootTransferTask.getSource() + "/" + ttPath.getFileName().toString(),
							tt.getSource(), "Actual task source does not match expected value based on parent source");

					assertEquals(rootTransferTask.getDest() + "/" + ttPath.getFileName().toString(),
							tt.getDest(), "Actual dest does not match expected dest based on parent dest");
				}

				// no error event should have been raised
				verify(ta, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TransferTaskAssignedListener - processTransferTask aborts processing childen when interrupt is received")
	public void processTransferTaskAbortsChildProcessingOnInterrupt(Vertx vertx, VertxTestContext ctx) {
		// mock out the test class
		TransferTaskAssignedListener ta = getMockTransferAssignedListenerInstance(vertx);

		TransferTaskErrorListener te = getMockTransferErrorListenerInstance(vertx);
		doNothing().when(te).start();

		// generate a fake transfer task
		TransferTask rootTransferTask = _createTestTransferTask();
		rootTransferTask.setId(1L);
		JsonObject rootTransferTaskJson = rootTransferTask.toJson();
		// generate the expected updated JsonObject
		JsonObject updatedTransferTaskJson = rootTransferTaskJson.copy().put("status", TransferStatusType.ASSIGNED.name());

		// now mock out our db interactions. Here we
		TransferTaskDatabaseService dbService = getMockTranserTaskDatabaseService(rootTransferTaskJson);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			// returning the given transfer task, adding an id if it doesn't have one.
			JsonObject childTask = arguments.getArgumentAt(1, TransferTask.class).toJson().put("id", Instant.now().toEpochMilli());
			handler.handle(Future.succeededFuture(childTask));
//			handler.handle(updatedTransferTaskAsyncResult);
			return null;
		}).when(dbService).createOrUpdateChildTransferTask(any(), any(), any(Handler.class));

		AsyncResult<JsonObject> updatedStatusAsyncResult = getMockAsyncResult(updatedTransferTaskJson);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updatedStatusAsyncResult);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any(Handler.class));

		// assign the mock db service to the test listener
		when(ta.getDbService()).thenReturn(dbService);

		// get mock remote data clients to mock the remote src system interactions
		URI srcUri = URI.create(rootTransferTask.getSource());
		RemoteDataClient srcRemoteDataClient = getMockRemoteDataClient(srcUri.getPath(), true, true);

		// get mock remote data clients to mock the remote dest system interactions
		URI destUri = URI.create(rootTransferTask.getDest());
		RemoteDataClient destRemoteDataClient = getMockRemoteDataClient(destUri.getPath(), true, true);

		try {
			when(ta.getRemoteDataClient(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getOwner()), eq(srcUri))).thenReturn(srcRemoteDataClient);
			when(ta.getRemoteDataClient(eq(rootTransferTask.getTenantId()), eq(rootTransferTask.getOwner()), eq(destUri))).thenReturn(destRemoteDataClient);
		} catch (Exception e) {
			// bubble the failure up to the VertxTestContext so exceptions are reported with messages
			try {
				fail("Failed to initialize the remote data clients during test setup.", e);
			} catch (Throwable t) {
				ctx.failNow(t);
			}
		}

		// we already mock this once in the mock listener setup. This will override the mock returning true the first
		// invocation of the method and false thereafter.
		when(ta.taskIsNotInterrupted(any())).thenReturn(true,false);

		ta.processTransferTask(rootTransferTaskJson, result -> {
			ctx.verify(() -> {
				assertTrue(result.succeeded(), "Task assignment handler should have succeeded on interrupt during processing.");

				assertFalse(result.result(), "Callback handler result should be true after successful assignment.");

				// remote file info should be obtained once.
				verify(srcRemoteDataClient, times(1)).getFileInfo(eq(srcUri.getPath()));

				// mkdir should never be called on the src client.
				verify(srcRemoteDataClient, never()).mkdirs(eq(srcUri.getPath()));

				// mkdir should only be called on non-empty directory items.
				verify(destRemoteDataClient, times(1)).mkdirs(any());

				// listing should be called with srch path
				verify(srcRemoteDataClient, times(1)).ls(eq(srcUri.getPath()));

				// should be called once after the children are processed
				verify(dbService, never()).updateStatus(any(), any(), any(), any());

				// get the test list of remote child file items
				List<RemoteFileInfo> remoteFileInfoList = srcRemoteDataClient.ls(srcUri.getPath());

				// according to our above mock, this interruption check should be called twice. once upon entry
				// where it returns true. Another time when processing the first child when it shold return false and
				// trigger an abortion of the stream processing.
				verify(ta, times(2)).taskIsNotInterrupted(eq(rootTransferTask));

				// the method should never be reached. the interrupt should abort all further processing of the children
				verify(dbService, never()).createOrUpdateChildTransferTask(any(), any(), any());

				// directory children should raise TRANSFERTASK_CREATED events
				verify(ta, times(1))._doPublishEvent(eq(TRANSFERTASK_CANCELED_ACK), eq(rootTransferTaskJson));
				// file item children should never be reached
				verify(ta, never())._doPublishEvent(eq(TRANSFER_ALL), any(JsonObject.class));
				verify(ta, never())._doPublishEvent(eq(TRANSFERTASK_CREATED), any(JsonObject.class));

				// no error event should have been raised
				verify(ta, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TransferTaskAssignedListener - taskIsNotInterrupted")
	void taskIsNotInterruptedTest(Vertx vertx, VertxTestContext ctx) {
		TransferTask tt = _createTestTransferTask();
		tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(vertx);

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