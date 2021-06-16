package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang.NotImplementedException;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.local.Local;
import org.iplantc.service.transfer.model.TransferTaskImpl;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.UUID;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_ALL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfer All tests")
@Disabled
class TransferAllProtocolVerticalTest  extends BaseTestCase {
	private static final Logger log = LoggerFactory.getLogger(TransferAllProtocolVerticalTest.class);

	TransferAllProtocolVertical getMockAllProtocolVerticalInstance(Vertx vertx) {
		TransferAllProtocolVertical listener = Mockito.mock(TransferAllProtocolVertical.class);
		when(listener.getEventChannel()).thenReturn(TRANSFER_ALL);
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.getExecutor()).thenCallRealMethod();
		return listener;
	}

	URLCopy getMockUrlCopyInstance(RemoteDataClient sourceClient, RemoteDataClient destClient) throws TransferException, RemoteDataSyntaxException, RemoteDataException, IOException, InterruptedException {
		URLCopy urlCopy = Mockito.mock(URLCopy.class);
		doCallRealMethod().when(urlCopy).copy(any());

		return urlCopy;
	}

	RemoteDataClient getRemoteDataClientInstance() {
		return Mockito.mock(RemoteDataClient.class);
	}


	@Test
	@DisplayName("Test the getRemoteDataClient handles local systems")
	public void testGetRemoteDataClient() throws NotImplementedException, SystemUnknownException, AgaveNamespaceException, RemoteCredentialException, PermissionException, IOException, RemoteDataException, InterruptedException {
		TransferAllProtocolVertical transferAllProtocolVertical = new TransferAllProtocolVertical();
		try {
			// generate a path for a local file. It does not have to exist for this to work.
			URI testPath = Paths.get("/scratch/").resolve(UUID.randomUUID().toString()).toUri();
			// passing in the file uri should return a Local RDC
			RemoteDataClient remoteDataClient = transferAllProtocolVertical.getRemoteDataClient(TENANT_ID, TEST_USERNAME, testPath);
			assertNotNull(remoteDataClient, "File URI should not return null system");
			assertEquals(Local.class, remoteDataClient.getClass(), "File URI should return a Local RemoteDataClient., ");
		} catch (IllegalArgumentException e) {
			fail("Valid URI for local file should not thorw exception", e);
		}
	}

	@Test
	@DisplayName("Test the processCopyRequest")
	public void testProcessCopyRequest (Vertx vertx, VertxTestContext ctx) throws SystemUnknownException, AgaveNamespaceException, RemoteCredentialException,
			PermissionException, IOException, RemoteDataException, TransferException, RemoteDataSyntaxException, InterruptedException {

		// set up the parameters
		TransferTaskImpl legacyTransferTask = _createTestTransferTaskIPC();
		legacyTransferTask.setUuid(new AgaveUUID(UUIDType.TRANSFER).toString());
		// set the totalSize value so the check for a positive file size will succeed
		legacyTransferTask.setTotalSize(1024);
		legacyTransferTask.setBytesTransferred(1024);
		legacyTransferTask.setAttempts(1);
		legacyTransferTask.setStatus(TransferStatusType.COMPLETED);

		TransferTask transferTask = _createTestTransferTask();

		URI srcUri;
		URI destUri;
		srcUri = URI.create(legacyTransferTask.getSource());
		destUri = URI.create(legacyTransferTask.getDest());

		//mock out both RemoteDataClient. Having unique RDC is important because we want to ensure that we invoke
		// URLCopy#copy correctly and not copy dest to src instead. That is a valid test of our code's logic. By
		// mocking out each client individually, we ensure that we make this check.
		RemoteDataClient srcRemoteDataClientMock = getRemoteDataClientInstance();
		RemoteDataClient destRemoteDataClientMock = getRemoteDataClientInstance();

		// Mock the URLCopy class that will be returned from the getter in our tested class. That will allow us
		// to check the parameter order in the method signature.
		URLCopy urlCopyMock = mock(URLCopy.class);
		when(urlCopyMock.copy(any(TransferTask.class))).thenReturn(transferTask);
		//doAnswer().when(.copy());

		// pull in the mock for TransferAllProtocolVertical
		TransferAllProtocolVertical txfrAllVert = getMockAllProtocolVerticalInstance(vertx);
		// mock out everything call, but not being tested in the method
		when(txfrAllVert.getRemoteDataClient(eq(TENANT_ID), eq(TEST_USERNAME), eq(srcUri)) ).thenReturn(srcRemoteDataClientMock);
		when(txfrAllVert.getRemoteDataClient(eq(TENANT_ID), eq(TEST_USERNAME), eq(destUri)) ).thenReturn(destRemoteDataClientMock);
		when(txfrAllVert.getUrlCopy(srcRemoteDataClientMock, destRemoteDataClientMock)).thenReturn(urlCopyMock);

		// make the actual call to our method under test
		doCallRealMethod().when(txfrAllVert).processCopyRequest(any(), any(), any(), any());


		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		org.agaveplatform.service.transfers.model.TransferTask tt =  new org.agaveplatform.service.transfers.model.TransferTask( legacyTransferTask.getSource(),
				legacyTransferTask.getDest(),
				legacyTransferTask.getOwner(),
				legacyTransferTask.getTenantId(),
				null,
				null
		);

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject =  tt.toJson()
				.put("status", org.agaveplatform.service.transfers.enumerations.TransferStatusType.FAILED.name())
				.put("endTime", Instant.now());

		AsyncResult<JsonObject> expectedUpdateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(expectedUpdateStatusHandler);
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), eq(tt.getStatus().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrAllVert.getDbService()).thenReturn(dbService);

		// now actually call the mehtod under test

		txfrAllVert.processCopyRequest(srcRemoteDataClientMock, destRemoteDataClientMock, transferTask, resp -> {
			ctx.verify(() -> {
				assertTrue(resp.succeeded(), "Copy should complete and resolve true");
				// this shouldn't be called because we're passing in the src rdc
				verify(txfrAllVert, never()).getRemoteDataClient(TENANT_ID, TEST_USERNAME, srcUri);
				// this shouldn't be called because we're passing in the dest rdc
				verify(txfrAllVert, never()).getRemoteDataClient(TENANT_ID, TEST_USERNAME, destUri);
				// this should be called as the method get
				//verify(txfrAllVert, times(1)).getUrlCopy(srcRemoteDataClientMock, destRemoteDataClientMock);
				verify(txfrAllVert, times(1)).processCopyRequest(eq(srcRemoteDataClientMock), eq(destRemoteDataClientMock), eq(transferTask), anyObject());
				// verify the URLCopy#copy method was called
				//verify(urlCopyMock, times(1)).copy(transferTask, null);

				//assertTrue(result, "processCopyRequest should return true when the transfertask returned form URLCopy has status COMPLETED");
				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("Test the processCopyRequest exception handling")
	public void testProcessCopyRequestThrowsRemoteDataException(Vertx vertx, VertxTestContext ctx) throws SystemUnknownException, AgaveNamespaceException,
			RemoteCredentialException, PermissionException, IOException, RemoteDataException, TransferException, RemoteDataSyntaxException, InterruptedException {
		// set up the parameters
		TransferTaskImpl legacyTransferTask = _createTestTransferTaskIPC();
		TransferTask transferTask = _createTestTransferTask();
		legacyTransferTask.setUuid(new AgaveUUID(UUIDType.TRANSFER).toString());
		// set the totalSize value so the check for a positive file size will succeed
		legacyTransferTask.setTotalSize(1024);
		legacyTransferTask.setBytesTransferred(1024);
		legacyTransferTask.setAttempts(1);
		legacyTransferTask.setStatus(TransferStatusType.COMPLETED);

		URI srcUri;
		URI destUri;
		srcUri = URI.create(legacyTransferTask.getSource());
		destUri = URI.create(legacyTransferTask.getDest());

		//mock out both RemoteDataClient. Having unique RDC is important because we want to ensure that we invoke
		// URLCopy#copy correctly and not copy dest to src instead. That is a valid test of our code's logic. By
		// mocking out each client individually, we ensure that we make this check.
		RemoteDataClient srcRemoteDataClientMock = getRemoteDataClientInstance();
		RemoteDataClient destRemoteDataClientMock = getRemoteDataClientInstance();

		// Mock the URLCopy class that will be returned from the getter in our tested class. That will allow us
		// to check the parameter order in the method signature.
		URLCopy urlCopyMock = mock(URLCopy.class);
		// mock out an exception thrown from the copy call to test our exception handling
		when(urlCopyMock.copy(any(TransferTask.class))).thenThrow(new RemoteDataException("Permission Denied"));

		// pull in the mock for TransferAllProtocolVertical
		TransferAllProtocolVertical txfrAllVert = getMockAllProtocolVerticalInstance(vertx);
		// mock out everything call, but not being tested in the method
		when(txfrAllVert.getRemoteDataClient(eq(TENANT_ID), eq(TEST_USERNAME), eq(srcUri)) ).thenReturn(srcRemoteDataClientMock);
		when(txfrAllVert.getRemoteDataClient(eq(TENANT_ID), eq(TEST_USERNAME), eq(destUri)) ).thenReturn(destRemoteDataClientMock);
		when(txfrAllVert.getUrlCopy(srcRemoteDataClientMock, destRemoteDataClientMock)).thenReturn(urlCopyMock);
		doNothing().when(txfrAllVert)._doPublishEvent(anyString(), any(JsonObject.class), any());

		// make the actual call to our method under test
		doCallRealMethod().when(txfrAllVert).processCopyRequest(any(), any(), any(), any());

		// now actually call the mehtod under test
		txfrAllVert.processCopyRequest(srcRemoteDataClientMock, destRemoteDataClientMock, transferTask, resp -> {
			ctx.verify(() -> {
				assertFalse(resp.succeeded(), "copy request processing should fail");

				assertNotNull(resp.cause(), "copy request processing should return the exception when it fails.");

				// this shouldn't be called because we're passing in the src rdc
				verify(txfrAllVert, never()).getRemoteDataClient(TENANT_ID, TEST_USERNAME, srcUri);
				// this shouldn't be called because we're passing in the dest rdc
				verify(txfrAllVert, never()).getRemoteDataClient(TENANT_ID, TEST_USERNAME, destUri);
				// this should be called as the method get
				verify(txfrAllVert).getUrlCopy(srcRemoteDataClientMock, destRemoteDataClientMock);
				verify(txfrAllVert, times(1))._doPublishEvent(eq(MessageType.TRANSFERTASK_ERROR), anyObject(), any());
				// verify the URLCopy#copy method was called
				verify(urlCopyMock).copy( transferTask);

				ctx.completeNow();
			});
		});


	}

	@Test
	@DisplayName("TransferTaskAllVerticle - taskIsNotInterrupted")
		//@Disabled
	void taskIsNotInterruptedTest(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException {
		TransferTask tt = _createTestTransferTask();
		tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

		TransferAllProtocolVertical ta = new TransferAllProtocolVertical(vertx);

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