package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.enumerations.TransferTaskEventType;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.transfer.*;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("URLCopy Tests")
public class URLCopyIT extends BaseTestCase {

    protected boolean allowRelayTransfers;
    protected Long FILE_SIZE = Long.valueOf(32768);

    @AfterAll
    protected void afterClass() {
        Settings.ALLOW_RELAY_TRANSFERS = allowRelayTransfers;
    }

    protected URLCopy getMockURLCopyInstance(Vertx vertx, TransferTask tt) throws TransferException, RemoteDataSyntaxException, RemoteDataException, IOException {
        URLCopy listener = mock(URLCopy.class);
        doCallRealMethod().when(listener).copy(any(TransferTask.class));
        doCallRealMethod().when(listener).copy(any(TransferTask.class), anyList());
        doCallRealMethod().when(listener).streamingTransfer(anyString(), anyString(), any(RemoteTransferListenerImpl.class));
        doCallRealMethod().when(listener).relayTransfer(anyString(), anyString(), any(TransferTask.class));
        doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
        when(listener.getVertx()).thenReturn(vertx);

        return listener;
    }

    RemoteDataClient getMockRemoteDataClientInstance(String path) throws IOException, RemoteDataException {
        RemoteDataClient mockRemoteDataClient = Mockito.mock(RemoteDataClient.class);
        when(mockRemoteDataClient.isThirdPartyTransferSupported()).thenReturn(false);
        when(mockRemoteDataClient.length(path)).thenReturn(new File(path).length());
        when(mockRemoteDataClient.resolvePath(path)).thenReturn(URI.create(path).getPath());
        doNothing().when(mockRemoteDataClient).get(anyString(), anyString(), any(RemoteTransferListener.class));

        doAnswer(invocation ->{
            RemoteTransferListener arg3 = invocation.getArgumentAt(2, RemoteTransferListener.class);
            arg3.started(FILE_SIZE, TRANSFER_SRC);
            return null;
        }).when(mockRemoteDataClient).get(anyString(), anyString(), any(RemoteTransferListener.class));

        doAnswer(invocation ->{
            RemoteTransferListener arg3 = invocation.getArgumentAt(2, RemoteTransferListener.class);
            arg3.progressed(FILE_SIZE);
            arg3.completed();
            return null;
        }).when(mockRemoteDataClient).put(anyString(), anyString(), any(RemoteTransferListener.class));


        return mockRemoteDataClient;
    }

    public RemoteTransferListenerImpl getMockRemoteTransferListener(TransferTask transferTask, RetryRequestManager retryRequestManager) {
        RemoteTransferListenerImpl mockRemoteTransferListenerImpl = mock(RemoteTransferListenerImpl.class);
        when(mockRemoteTransferListenerImpl.getRetryRequestManager()).thenReturn(retryRequestManager);
        when(mockRemoteTransferListenerImpl.isCancelled()).thenReturn(false);
        when(mockRemoteTransferListenerImpl.getTransferTask()).thenReturn(transferTask);
        doCallRealMethod().when(mockRemoteTransferListenerImpl).started(anyLong(), anyString());
        doCallRealMethod().when(mockRemoteTransferListenerImpl).progressed(anyLong());
        doCallRealMethod().when(mockRemoteTransferListenerImpl).completed();

        return mockRemoteTransferListenerImpl;
    }

    @Test
    @DisplayName("Test URLCopy relayTransfer")
    public void testUnaryCopy(Vertx vertx, VertxTestContext ctx) {
        try {
            allowRelayTransfers = Settings.ALLOW_RELAY_TRANSFERS;
            Settings.ALLOW_RELAY_TRANSFERS = true;

            Vertx mockVertx = mock(Vertx.class);
            RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);
            doNothing().when(mockRetryRequestManager).request(anyString(), any(JsonObject.class), anyInt());

            RemoteDataClient mockSrcRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_SRC);
            RemoteDataClient mockDestRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_DEST);

            TransferTask tt = _createTestTransferTask();
            tt.setId(1L);
            tt.setSource(TRANSFER_SRC);
            tt.setDest(TRANSFER_DEST);
            int attempts = tt.getAttempts();
            Instant lastUpdated = tt.getLastUpdated();

            URLCopy mockCopy = getMockURLCopyInstance(vertx, tt);
            when(mockCopy.getRetryRequestManager()).thenReturn(mockRetryRequestManager);
            when(mockCopy.getSourceClient()).thenReturn(mockSrcRemoteDataClient);
            when(mockCopy.getDestClient()).thenReturn(mockDestRemoteDataClient);

            //remote transfer listener
            RemoteTransferListenerImpl mockRemoteTransferListenerImpl = getMockRemoteTransferListener(tt, mockRetryRequestManager);
            doReturn(mockRemoteTransferListenerImpl).when(mockCopy).getRemoteTransferListenerForTransferTask(tt);

            //first leg child transfer task
            TransferTask srcChildTransferTask = new TransferTask(
                    tt.getSource(),
                    "https://workers.prod.agaveplatform.org/" + new File(TRANSFER_SRC).getPath(),
                    tt.getOwner(),
                    tt.getUuid(),
                    tt.getRootTaskId());
            srcChildTransferTask.setTenantId(tt.getTenantId());
            srcChildTransferTask.setStatus(TransferStatusType.READ_STARTED);

            //mock child remote transfer listener
            RemoteTransferListenerImpl mockChildRemoteTransferListenerImpl = getMockRemoteTransferListener(srcChildTransferTask, mockRetryRequestManager);
//            when(mockCopy.getRemoteTransferListenerForTransferTask(srcChildTransferTask)).thenReturn(mockChildRemoteTransferListenerImpl);

            //second leg child transfer task
            TransferTask destChildTransferTask = new TransferTask(
                    "https://workers.prod.agaveplatform.org/" + new File(LOCAL_TXT_FILE).getPath(),
                    tt.getDest(),
                    tt.getOwner(),
                    tt.getParentTaskId(),
                    tt.getRootTaskId()
            );
            destChildTransferTask.setTenantId(tt.getTenantId());
            destChildTransferTask.setStatus(TransferStatusType.WRITE_STARTED);

            RemoteTransferListenerImpl mockDestChildRemoteTransferListenerImpl = getMockRemoteTransferListener(destChildTransferTask, mockRetryRequestManager);

            when(mockCopy.getRemoteTransferListenerForTransferTask(any(TransferTask.class))).thenReturn(mockChildRemoteTransferListenerImpl, mockDestChildRemoteTransferListenerImpl);


            //Injecting the mocked arguments to the mocked URLCopy
            //Using InjectMocks annotation will not create a mock of URLCopy, which we need to pass in the mocked RemoteTransferListenerImpl
//            PowerMockito.whenNew(URLCopy.class).withArguments(any(RemoteDataClient.class), any(RemoteDataClient.class), any(Vertx.class), any(RetryRequestManager.class)).thenReturn(mockCopy);
//            TransferTask copiedTransfer = new URLCopy(mockSrcRemoteDataClient, mockDestRemoteDataClient, mockVertx, mockRetryRequestManager).copy(tt);

            TransferTask copiedTransfer = mockCopy.copy(tt);


            ctx.verify(() -> {
                //check that events are published correctly using the RetryRequestManager
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.RELAY_READ_STARTED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.RELAY_READ_COMPLETED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.UPDATED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.RELAY_WRITE_STARTED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.RELAY_WRITE_COMPLETED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.COMPLETED.name()),
                        any(JsonObject.class), eq(2));

                assertEquals(attempts + 1, copiedTransfer.getAttempts(), "TransferTask attempts should be incremented upon copy.");
                assertEquals(copiedTransfer.getStatus(), TransferStatusType.COMPLETED, "Expected successful copy to return TransferTask with COMPLETED status.");
                assertEquals(1, copiedTransfer.getTotalFiles(), "TransferTask total files be 1 upon successful transfer of a single file.");
                assertEquals(32768, copiedTransfer.getBytesTransferred(), "TransferTask total bytes transferred should be 32768, the size of the httpbin download name.");
                assertTrue(copiedTransfer.getLastUpdated().isAfter(lastUpdated), "TransferTask last updated should be updated after URLCopy completes.");
                assertNotNull(copiedTransfer.getStartTime(), "TransferTask start time should be set by URLCopy.");
                assertNotNull(copiedTransfer.getEndTime(), "TransferTask end time should be set by URLCopy.");
                assertTrue(copiedTransfer.getStartTime().isAfter(tt.getCreated()), "TransferTask start time should be after created time.");
                assertTrue(copiedTransfer.getEndTime().isAfter(tt.getStartTime()), "TransferTask end time should be after start time.");
                assertEquals(copiedTransfer.getTotalSize(), copiedTransfer.getBytesTransferred(), "Total size should be same as bytes transferred upon successful completion of single file transfer.");
                assertEquals(0, copiedTransfer.getTotalSkippedFiles(), "TransferTask skipped files should be zero upon successful completion of a single file.");

                ctx.completeNow();
            });

        } catch (Exception e) {
            Assertions.fail("Unary copy from " + TRANSFER_SRC + " to " + TRANSFER_DEST + " should not throw exception, " + e);
        } finally {
            Settings.ALLOW_RELAY_TRANSFERS = allowRelayTransfers;
        }
    }

    @Test
    @DisplayName("Test URLCopy streamingTransfer")
    public void testStreamingCopy(Vertx vertx, VertxTestContext ctx) {
        try {
            allowRelayTransfers = Settings.ALLOW_RELAY_TRANSFERS;
            Settings.ALLOW_RELAY_TRANSFERS = false;

            Vertx mockVertx = mock(Vertx.class);
            RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);

            RemoteDataClient mockSrcRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_SRC);
            RemoteDataClient mockDestRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_DEST);

            RemoteInputStream mockRemoteInputStream = mock(RemoteInputStream.class);
            when(mockRemoteInputStream.isBuffered()).thenReturn(true);
            when(mockRemoteInputStream.read(any(), eq(0), anyInt())).thenReturn(32768, -1);

            when(mockSrcRemoteDataClient.getInputStream(anyString(), eq(true))).thenReturn(mockRemoteInputStream);
            URI path = new URI(TRANSFER_SRC);
            doReturn(FILE_SIZE).when(mockSrcRemoteDataClient).length(path.getPath());
            RemoteOutputStream mockRemoteOutputStream = mock(RemoteOutputStream.class);
            when(mockRemoteOutputStream.isBuffered()).thenReturn(true);
            doCallRealMethod().when(mockRemoteOutputStream).write(any(), eq(0), anyInt());
            doNothing().when(mockRemoteOutputStream).write(any(), eq(0), anyInt());
            when(mockDestRemoteDataClient.getOutputStream(anyString(), eq(true), eq(false))).thenReturn(mockRemoteOutputStream);

            TransferTask tt = _createTestTransferTask();
            tt.setId(1L);
            tt.setSource(TRANSFER_SRC);
            tt.setDest(TRANSFER_DEST);
            int attempts = tt.getAttempts();
            Instant lastUpdated = tt.getLastUpdated();

            RemoteTransferListenerImpl mockRemoteTransferListenerImpl = getMockRemoteTransferListener(tt, mockRetryRequestManager);

            //Injecting the mocked arguments to the mocked URLCopy
            //Using InjectMocks annotation will not create a mock of URLCopy, which we need to pass in the mocked RemoteTransferListenerImpl
            URLCopy mockCopy = getMockURLCopyInstance(vertx, tt);
            when(mockCopy.getRetryRequestManager()).thenReturn(mockRetryRequestManager);
            when(mockCopy.getRemoteTransferListenerForTransferTask(any(TransferTask.class))).thenReturn(mockRemoteTransferListenerImpl);
            when(mockCopy.getSourceClient()).thenReturn(mockSrcRemoteDataClient);
            when(mockCopy.getDestClient()).thenReturn(mockDestRemoteDataClient);

            TransferTask copiedTransfer = mockCopy.copy(tt);

            ctx.verify(() -> {

                //check that events are published correctly using the RetryRequestManager
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.STREAM_COPY_STARTED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.STREAM_COPY_COMPLETED.name()),
                        any(JsonObject.class), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(TransferTaskEventType.COMPLETED.name()),
                        any(JsonObject.class), eq(2));

                assertEquals(attempts + 1, copiedTransfer.getAttempts(), "TransferTask attempts should be incremented upon copy.");
                assertEquals(copiedTransfer.getStatus(), TransferStatusType.COMPLETED, "Expected successful copy to return TransferTask with COMPLETED status.");
                assertEquals(1, copiedTransfer.getTotalFiles(), "TransferTask total files be 1 upon successful transfer of a single file.");
                assertEquals(32768, copiedTransfer.getBytesTransferred(), "TransferTask total bytes transferred should be 32768, the size of the httpbin download name.");
                assertTrue(copiedTransfer.getLastUpdated().isAfter(lastUpdated), "TransferTask last updated should be updated after URLCopy completes.");
                assertNotNull(copiedTransfer.getStartTime(), "TransferTask start time should be set by URLCopy.");
                assertNotNull(copiedTransfer.getEndTime(), "TransferTask end time should be set by URLCopy.");
                assertTrue(copiedTransfer.getStartTime().isAfter(tt.getCreated()), "TransferTask start time should be after created time.");
                assertTrue(copiedTransfer.getEndTime().isAfter(tt.getStartTime()), "TransferTask end time should be after start time.");
                assertEquals(copiedTransfer.getTotalSize(), tt.getBytesTransferred(), "Total size should be same as bytes transferred upon successful completion of single file transfer.");
                assertEquals(0, copiedTransfer.getTotalSkippedFiles(), "TransferTask skipped files should be zero upon successful completion of a single file.");

                ctx.completeNow();
            });

        } catch (Exception e) {
            Assertions.fail("Streaming copy from " + TRANSFER_SRC + " to " + TRANSFER_DEST + " should not throw exception, " + e);
        } finally {
            Settings.ALLOW_RELAY_TRANSFERS = allowRelayTransfers;
        }
    }

}
