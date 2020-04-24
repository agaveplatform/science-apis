package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle;
import org.agaveplatform.service.transfers.listener.*;
import org.agaveplatform.service.transfers.listener.TransferTaskCancelListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.URLCopy;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfer All tests")
class TransferAllProtocolVerticalTest  extends BaseTestCase {

	TransferAllProtocolVertical getMockAllProtocolVerticalInstance(Vertx vertx) throws SystemUnknownException, AgaveNamespaceException, RemoteCredentialException, PermissionException, IOException, RemoteDataException, TransferException, RemoteDataSyntaxException {
		TransferAllProtocolVertical txfrAllVert = Mockito.mock(TransferAllProtocolVertical.class);
		when(txfrAllVert.getEventChannel()).thenReturn(TRANSFER_ALL);
		when(txfrAllVert.getVertx()).thenReturn(vertx);
		return txfrAllVert;
	}

	URLCopy getMockUrlCopyInstance(RemoteDataClient sourceClient, RemoteDataClient destClient) throws TransferException, RemoteDataSyntaxException, RemoteDataException, IOException {
		URLCopy urlCopy = Mockito.mock(URLCopy.class);
		doCallRealMethod().when(urlCopy).copy(anyString(), anyString(), any());

		return urlCopy;
	}
	RemoteDataClient getRemoteDataClientInstance() {
		RemoteDataClient rdc = Mockito.mock(RemoteDataClient.class);

		return rdc;
	}


	@Test
	@DisplayName("Transfer All Vertical - processEvent")
	@Disabled
	public void processEvent(Vertx vertx, VertxTestContext ctx) throws SystemUnknownException, AgaveNamespaceException, RemoteCredentialException, PermissionException, IOException, RemoteDataException, TransferException, RemoteDataSyntaxException {
		TransferTask transferTask = _createTestTransferTask();

		TransferAllProtocolVertical txfrAllVert = getMockAllProtocolVerticalInstance(vertx);
		String result = txfrAllVert.processEvent(transferTask.toJson());

		assertEquals("Complte", result);
		ctx.completeNow();
	}

	@Test
	@DisplayName("Test the processCopyRequest")
	public void testProcessCopyRequest () throws SystemUnknownException, AgaveNamespaceException, RemoteCredentialException, PermissionException, IOException, RemoteDataException, TransferException, RemoteDataSyntaxException {
		// set up the parameters
		org.iplantc.service.transfer.model.TransferTask legacyTransferTask = _createTestTransferTaskIPC();
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
		when(urlCopyMock.copy(eq(srcUri.getPath()), eq(destUri.getPath()), any(org.iplantc.service.transfer.model.TransferTask.class))).thenReturn(legacyTransferTask);

		// pull in the mock for TransferAllProtocolVertical
		TransferAllProtocolVertical txfrAllVert = getMockAllProtocolVerticalInstance(null);
		// mock out everything call, but not being tested in the method
		when(txfrAllVert.getRemoteDataClient(eq(TEST_USERNAME), eq(srcUri)) ).thenReturn(srcRemoteDataClientMock);
		when(txfrAllVert.getRemoteDataClient(eq(TEST_USERNAME), eq(destUri)) ).thenReturn(destRemoteDataClientMock);
		when(txfrAllVert.getUrlCopy(srcRemoteDataClientMock, destRemoteDataClientMock)).thenReturn(urlCopyMock);
		// make the actual call to our method under test
		when(txfrAllVert.processCopyRequest(any(), any(), any(), any(), any())).thenCallRealMethod();

		// now actually call the mehtod under test
		Boolean result = txfrAllVert.processCopyRequest(srcUri.getPath(), srcRemoteDataClientMock, destUri.getPath(), destRemoteDataClientMock, legacyTransferTask);
		// this shouldn't be called because we're passing in the src rdc
		verify(txfrAllVert, never()).getRemoteDataClient(TEST_USERNAME, srcUri);
		// this shouldn't be called because we're passing in the dest rdc
		verify(txfrAllVert, never()).getRemoteDataClient(TEST_USERNAME, destUri);
		// this should be called as the method get
		verify(txfrAllVert).getUrlCopy(srcRemoteDataClientMock, destRemoteDataClientMock);
		// verify the URLCopy#copy method was called
		verify(urlCopyMock).copy(srcUri.getPath(), destUri.getPath(), legacyTransferTask);

		assertTrue(result, "processCopyRequest should return true when the transfertask returned form URLCopy has status COMPLETED");

	}


	@Test
	@DisplayName("Test the processCopyRequest exception handling")
	public void testProcessCopyRequestThrowsRemoteDataException() throws SystemUnknownException, AgaveNamespaceException, RemoteCredentialException, PermissionException, IOException, RemoteDataException, TransferException, RemoteDataSyntaxException {
		// set up the parameters
		org.iplantc.service.transfer.model.TransferTask legacyTransferTask = _createTestTransferTaskIPC();
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
		when(urlCopyMock.copy(eq(srcUri.getPath()), eq(destUri.getPath()), any(org.iplantc.service.transfer.model.TransferTask.class))).thenThrow(new RemoteDataException("Permission Denied"));

		// pull in the mock for TransferAllProtocolVertical
		TransferAllProtocolVertical txfrAllVert = getMockAllProtocolVerticalInstance(null);
		// mock out everything call, but not being tested in the method
		when(txfrAllVert.getRemoteDataClient(eq(TEST_USERNAME), eq(srcUri)) ).thenReturn(srcRemoteDataClientMock);
		when(txfrAllVert.getRemoteDataClient(eq(TEST_USERNAME), eq(destUri)) ).thenReturn(destRemoteDataClientMock);
		when(txfrAllVert.getUrlCopy(srcRemoteDataClientMock, destRemoteDataClientMock)).thenReturn(urlCopyMock);
		// make the actual call to our method under test
		when(txfrAllVert.processCopyRequest(any(), any(), any(), any(), any())).thenCallRealMethod();

		// now actually call the mehtod under test
		try {
			// an exception should be thrown here
			Boolean result = txfrAllVert.processCopyRequest(srcUri.getPath(), srcRemoteDataClientMock, destUri.getPath(), destRemoteDataClientMock, legacyTransferTask);
			fail("processCopyRequest should rethrow RemoteDataException thrown by URLCopy#copy");
		} catch (RemoteDataException e) {
			// we wanted the exception and got it. Now every the rest of the behavior worked as expected
			// prior to the exception

			// this shouldn't be called because we're passing in the src rdc
			verify(txfrAllVert, never()).getRemoteDataClient(TEST_USERNAME, srcUri);
			// this shouldn't be called because we're passing in the dest rdc
			verify(txfrAllVert, never()).getRemoteDataClient(TEST_USERNAME, destUri);
			// this should be called as the method get
			verify(txfrAllVert).getUrlCopy(srcRemoteDataClientMock, destRemoteDataClientMock);
			// verify the URLCopy#copy method was called
			verify(urlCopyMock).copy(srcUri.getPath(), destUri.getPath(), legacyTransferTask);
		}

	}

}