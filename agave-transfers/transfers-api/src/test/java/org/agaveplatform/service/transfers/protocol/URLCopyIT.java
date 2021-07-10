package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.matchers.IsSameJsonTransferTask;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang.RandomStringUtils;
import org.iplantc.service.transfer.*;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.local.Local;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("URLCopy Tests")
public class URLCopyIT extends BaseTestCase {

    protected boolean allowRelayTransfers;
    protected Long FILE_SIZE = Long.valueOf(32768);
    protected double TRANSFER_RATE = 1.00;


    class IsSameTransferTask extends ArgumentMatcher<TransferTask> {
        TransferTask expectedTransferTask;

        public IsSameTransferTask(TransferTask expectedTransferTask) {
            this.expectedTransferTask = expectedTransferTask;
        }

        /**
         * Returns whether this matcher accepts the given argument.
         * <p>
         * The method should <b>never</b> assert if the argument doesn't match. It
         * should only return false.
         *
         * @param actualJsonTransferTask the argument
         * @return whether this matcher accepts the given argument.
         */
        @Override
        public boolean matches(Object actualJsonTransferTask) {
            if (!(actualJsonTransferTask instanceof TransferTask)) return false;

            return Objects.equals(expectedTransferTask.getSource(), ((TransferTask)actualJsonTransferTask).getSource()) &&
                    Objects.equals(expectedTransferTask.getDest(), ((TransferTask)actualJsonTransferTask).getDest()) &&
                    Objects.equals(expectedTransferTask.getOwner(), ((TransferTask)actualJsonTransferTask).getOwner()) &&
                    Objects.equals(expectedTransferTask.getParentTaskId(), ((TransferTask)actualJsonTransferTask).getParentTaskId()) &&
                    Objects.equals(expectedTransferTask.getRootTaskId(), ((TransferTask)actualJsonTransferTask).getRootTaskId()) &&
                    expectedTransferTask.getStatus() == ((TransferTask)actualJsonTransferTask).getStatus() &&
                    Objects.equals(expectedTransferTask.getTenantId(), ((TransferTask)actualJsonTransferTask).getTenantId());
        }
    }

    @AfterAll
    protected void afterClass() {
        Settings.ALLOW_RELAY_TRANSFERS = allowRelayTransfers;
    }

    protected URLCopy getMockURLCopyInstance(Vertx vertx, TransferTask tt) throws TransferException, RemoteDataSyntaxException, RemoteDataException, IOException, InterruptedException {
        URLCopy listener = mock(URLCopy.class);
        doCallRealMethod().when(listener).copy(any(TransferTask.class));
        doCallRealMethod().when(listener).copy(any(TransferTask.class), anyList());
        doCallRealMethod().when(listener).streamingTransfer(anyString(), anyString(), any(RemoteStreamingTransferListenerImpl.class));
        doCallRealMethod().when(listener).relayTransfer(anyString(), anyString(), any(TransferTask.class));
        doCallRealMethod().when(listener)._doPublishEvent(anyString(), any(JsonObject.class));
        when(listener.getVertx()).thenReturn(vertx);
        doCallRealMethod().when(listener).updateAggregateTaskFromChildTask(any(TransferTask.class), any(TransferTask.class));

        return listener;
    }

    RemoteDataClient getMockRemoteDataClientInstance(String path) throws IOException, RemoteDataException {
        RemoteDataClient mockRemoteDataClient = Mockito.mock(RemoteDataClient.class);
        when(mockRemoteDataClient.isThirdPartyTransferSupported()).thenReturn(false);
        when(mockRemoteDataClient.length(path)).thenReturn(new File(path).length());
        when(mockRemoteDataClient.resolvePath(path)).thenReturn(URI.create(path).getPath());

        doAnswer(invocation -> {
            RemoteTransferListenerImpl arg3 = invocation.getArgumentAt(2, RemoteTransferListenerImpl.class);
            arg3.started(FILE_SIZE, TRANSFER_SRC);
            arg3.progressed(FILE_SIZE);
            arg3.completed();
            return null;
        }).when(mockRemoteDataClient).get(anyString(), anyString(), any(RemoteTransferListener.class));

        doAnswer(invocation -> {
            RemoteTransferListenerImpl arg3 = invocation.getArgumentAt(2, RemoteTransferListenerImpl.class);
            arg3.started(FILE_SIZE, TRANSFER_DEST);
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

    public RemoteStreamingTransferListenerImpl getMockRemoteStreamingTransferListener(TransferTask transferTask, RetryRequestManager retryRequestManager) throws IOException, InterruptedException {
        RemoteStreamingTransferListenerImpl mockRemoteTransferListenerImpl = mock(RemoteStreamingTransferListenerImpl.class);
        when(mockRemoteTransferListenerImpl.getRetryRequestManager()).thenReturn(retryRequestManager);
        when(mockRemoteTransferListenerImpl.isCancelled()).thenReturn(false);
        when(mockRemoteTransferListenerImpl.getTransferTask()).thenReturn(transferTask);
        when(mockRemoteTransferListenerImpl.getFirstRemoteFilepath()).thenCallRealMethod();
        when(mockRemoteTransferListenerImpl.getLastUpdated()).thenCallRealMethod();
        when(mockRemoteTransferListenerImpl.getBytesLastCheck()).thenCallRealMethod();
        doCallRealMethod().when(mockRemoteTransferListenerImpl)._doPublishEvent(anyString(), any(JsonObject.class));
        doCallRealMethod().when(mockRemoteTransferListenerImpl).setTransferTask(any(TransferTask.class));
        doCallRealMethod().when(mockRemoteTransferListenerImpl).setFirstRemoteFilepath(anyString());
        doCallRealMethod().when(mockRemoteTransferListenerImpl).setLastUpdated(anyLong());
        doCallRealMethod().when(mockRemoteTransferListenerImpl).setBytesLastCheck(anyLong());
        doCallRealMethod().when(mockRemoteTransferListenerImpl).started(anyLong(), anyString());
        doCallRealMethod().when(mockRemoteTransferListenerImpl).progressed(anyLong());
        doCallRealMethod().when(mockRemoteTransferListenerImpl).completed();

        return mockRemoteTransferListenerImpl;
    }

    public RemoteUnaryTransferListenerImpl getMockRemoteUnaryTransferListener(TransferTask transferTask, RetryRequestManager retryRequestManager) throws IOException, InterruptedException {
        RemoteUnaryTransferListenerImpl mockRemoteTransferListenerImpl = mock(RemoteUnaryTransferListenerImpl.class);
        when(mockRemoteTransferListenerImpl.getRetryRequestManager()).thenReturn(retryRequestManager);
        when(mockRemoteTransferListenerImpl.isCancelled()).thenReturn(false);
        when(mockRemoteTransferListenerImpl.getTransferTask()).thenReturn(transferTask);
        when(mockRemoteTransferListenerImpl.getFirstRemoteFilepath()).thenCallRealMethod();
        when(mockRemoteTransferListenerImpl.getLastUpdated()).thenCallRealMethod();
        when(mockRemoteTransferListenerImpl.getBytesLastCheck()).thenCallRealMethod();
        doCallRealMethod().when(mockRemoteTransferListenerImpl)._doPublishEvent(anyString(), any(JsonObject.class));
        doCallRealMethod().when(mockRemoteTransferListenerImpl).setTransferTask(any(TransferTask.class));
        doCallRealMethod().when(mockRemoteTransferListenerImpl).setFirstRemoteFilepath(anyString());
        doCallRealMethod().when(mockRemoteTransferListenerImpl).setLastUpdated(anyLong());
        doCallRealMethod().when(mockRemoteTransferListenerImpl).setBytesLastCheck(anyLong());
        doCallRealMethod().when(mockRemoteTransferListenerImpl).started(anyLong(), anyString());
        doCallRealMethod().when(mockRemoteTransferListenerImpl).progressed(anyLong());
        doCallRealMethod().when(mockRemoteTransferListenerImpl).completed();
        doCallRealMethod().when(mockRemoteTransferListenerImpl).cancel();

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
            RemoteTransferListenerImpl mockRemoteTransferListenerImpl = getMockRemoteUnaryTransferListener(tt, mockRetryRequestManager);
            doReturn(mockRemoteTransferListenerImpl).when(mockCopy).getRemoteUnaryTransferListenerForTransferTask(tt);

            //first leg child transfer task
            TransferTask srcChildTransferTask = new TransferTask(
                    tt.getSource(),
                    new File(TRANSFER_SRC).toURI().toString(),
                    tt.getOwner(),
                    tt.getUuid(),
                    tt.getRootTaskId());
            srcChildTransferTask.setTenantId(tt.getTenantId());
            srcChildTransferTask.setStatus(TransferStatusType.READ_STARTED);
            JsonObject rootSrcChildJson = srcChildTransferTask.toJson();

            //mock child remote transfer listener
            RemoteUnaryTransferListenerImpl mockChildRemoteTransferListenerImpl = getMockRemoteUnaryTransferListener(srcChildTransferTask, mockRetryRequestManager);

            //second leg child transfer task
            TransferTask destChildTransferTask = new TransferTask(
                    new File(LOCAL_TXT_FILE).toURI().toString(),
                    tt.getDest(),
                    tt.getOwner(),
                    tt.getParentTaskId(),
                    tt.getRootTaskId()
            );
            destChildTransferTask.setTenantId(tt.getTenantId());
            destChildTransferTask.setStatus(TransferStatusType.WRITE_STARTED);
            JsonObject rootDestChildJson = destChildTransferTask.toJson();

            RemoteUnaryTransferListenerImpl mockDestChildRemoteTransferListenerImpl = getMockRemoteUnaryTransferListener(destChildTransferTask, mockRetryRequestManager);

            when(mockCopy.getRemoteUnaryTransferListenerForTransferTask(any(TransferTask.class))).thenReturn(mockChildRemoteTransferListenerImpl, mockDestChildRemoteTransferListenerImpl);

            TransferTask copiedTransfer = mockCopy.copy(tt);

            JsonObject readStartedJson = rootSrcChildJson.copy().put("startTime", srcChildTransferTask.getStartTime());
            readStartedJson.put("attempts", srcChildTransferTask.getAttempts());
            readStartedJson.put("totalFiles", 1);
            readStartedJson.put("totalSize", FILE_SIZE);

            JsonObject readInProgressJson = readStartedJson.copy().put("status", TransferStatusType.READ_IN_PROGRESS);
            readInProgressJson.put("transferRate", TRANSFER_RATE);
            readInProgressJson.put("bytesTransferred", srcChildTransferTask.getBytesTransferred());
            readInProgressJson.put("lastUpdated", srcChildTransferTask.getLastUpdated());

            JsonObject readCompletedJson = readInProgressJson.copy().put("status", TransferStatusType.READ_COMPLETED);
            readCompletedJson.put("transferRate", srcChildTransferTask.getTransferRate());
            readCompletedJson.put("endTime", srcChildTransferTask.getEndTime());

            JsonObject writeStartedJson = rootDestChildJson.copy().put("startTime", destChildTransferTask.getStartTime());
            writeStartedJson.put("attempts", destChildTransferTask.getAttempts());
            writeStartedJson.put("totalFiles", 1);
            writeStartedJson.put("totalSize", FILE_SIZE);

            JsonObject writeInProgressJson = writeStartedJson.copy().put("status", TransferStatusType.WRITE_IN_PROGRESS);
            writeInProgressJson.put("transferRate", TRANSFER_RATE);
            writeInProgressJson.put("bytesTransferred", destChildTransferTask.getBytesTransferred());
            writeInProgressJson.put("lastUpdated", destChildTransferTask.getLastUpdated());

            JsonObject writeCompletedJson = writeInProgressJson.copy().put("status", TransferStatusType.WRITE_COMPLETED);
            writeCompletedJson.put("transferRate", destChildTransferTask.getTransferRate());
            writeCompletedJson.put("endTime", destChildTransferTask.getEndTime());

            ctx.verify(() -> {
                //check that events are published correctly using the RetryRequestManager
                verify(mockRetryRequestManager, times(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                        eq(readStartedJson), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                        argThat(new IsSameJsonTransferTask(readInProgressJson)), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                        argThat(new IsSameJsonTransferTask(readCompletedJson)), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                        argThat(new IsSameJsonTransferTask(writeStartedJson)), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                        argThat(new IsSameJsonTransferTask(writeInProgressJson)), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                        argThat(new IsSameJsonTransferTask(writeCompletedJson)), eq(2));

                assertEquals(attempts + 1, copiedTransfer.getAttempts(), "TransferTask attempts should be incremented upon copy.");
                assertEquals(copiedTransfer.getStatus(), TransferStatusType.WRITE_COMPLETED, "Expected successful copy to return TransferTask with COMPLETED status.");
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
    @DisplayName("Test URLCopy local relayTransfer")
    public void testUnaryLocalCopy(Vertx vertx, VertxTestContext ctx) {
        File testFile = null;
        int testDataLength = FILE_SIZE.intValue();
        try {
            allowRelayTransfers = Settings.ALLOW_RELAY_TRANSFERS;
            Settings.ALLOW_RELAY_TRANSFERS = true;

            testFile = File.createTempFile("test", ".tmp");
            byte[] testData = RandomStringUtils.random(512, true, true).getBytes(StandardCharsets.UTF_8);
            Files.write(testFile.toPath(), testData, StandardOpenOption.APPEND);

            Vertx mockVertx = mock(Vertx.class);
            RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);
            doNothing().when(mockRetryRequestManager).request(anyString(), any(JsonObject.class), anyInt());

            RemoteDataClient mockSrcRemoteDataClient = new Local(null, "/","/");
            RemoteDataClient mockDestRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_DEST);

            TransferTask tt = _createTestTransferTask();
            tt.setId(1L);
            tt.setSource(testFile.toURI().toString());
            tt.setDest(TRANSFER_DEST);
            int attempts = tt.getAttempts();
            Instant lastUpdated = tt.getLastUpdated();
            JsonObject rootJson = tt.toJson();


            URLCopy mockCopy = getMockURLCopyInstance(vertx, tt);
            when(mockCopy.getRetryRequestManager()).thenReturn(mockRetryRequestManager);
            when(mockCopy.getSourceClient()).thenReturn(mockSrcRemoteDataClient);
            when(mockCopy.getDestClient()).thenReturn(mockDestRemoteDataClient);

            //remote transfer listener
            RemoteTransferListenerImpl mockRemoteTransferListenerImpl = getMockRemoteUnaryTransferListener(tt, mockRetryRequestManager);
            doReturn(mockRemoteTransferListenerImpl).when(mockCopy).getRemoteUnaryTransferListenerForTransferTask(tt);

            //second leg child transfer task
            TransferTask destChildTransferTask = new TransferTask(
                    testFile.toURI().toString(),
                    tt.getDest(),
                    tt.getOwner(),
                    tt.getParentTaskId(),
                    tt.getRootTaskId()
            );
            destChildTransferTask.setTenantId(tt.getTenantId());
            destChildTransferTask.setStatus(TransferStatusType.WRITE_STARTED);
            JsonObject rootDestChildJson = destChildTransferTask.toJson();

            RemoteUnaryTransferListenerImpl mockDestChildRemoteTransferListenerImpl = getMockRemoteUnaryTransferListener(destChildTransferTask, mockRetryRequestManager);

            when(mockCopy.getRemoteUnaryTransferListenerForTransferTask(any(TransferTask.class))).thenReturn(mockDestChildRemoteTransferListenerImpl);

            TransferTask copiedTransfer = mockCopy.copy(tt);
            JsonObject writeStartedJson = rootDestChildJson.copy().put("startTime", destChildTransferTask.getStartTime());
            writeStartedJson.put("attempts", destChildTransferTask.getAttempts());
            writeStartedJson.put("totalFiles", 1);
            writeStartedJson.put("totalSize", testDataLength);

            JsonObject writeInProgressJson = writeStartedJson.copy().put("status", TransferStatusType.WRITE_IN_PROGRESS);
            writeInProgressJson.put("transferRate", TRANSFER_RATE);
            writeInProgressJson.put("bytesTransferred", destChildTransferTask.getBytesTransferred());
            writeInProgressJson.put("lastUpdated", destChildTransferTask.getLastUpdated());

            JsonObject writeCompletedJson = writeInProgressJson.copy().put("status", TransferStatusType.WRITE_COMPLETED);
            writeCompletedJson.put("transferRate", destChildTransferTask.getTransferRate());
            writeCompletedJson.put("endTime", destChildTransferTask.getEndTime());

            ctx.verify(() -> {
                // only destination transfer task should have a listener. src is skipped because it is local
                verify(mockCopy, times(1)).getRemoteUnaryTransferListenerForTransferTask(any());

                //check that events are published correctly using the RetryRequestManager
                verify(mockRetryRequestManager, times(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                        argThat(new IsSameJsonTransferTask(writeStartedJson)), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                        argThat(new IsSameJsonTransferTask(writeInProgressJson)), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                        argThat(new IsSameJsonTransferTask(writeCompletedJson)), eq(2));

                // original tt should only be updated once because there should only be one listener created
                verify(mockCopy, times(1)).updateAggregateTaskFromChildTask(eq(tt), any());

                assertEquals(1, copiedTransfer.getAttempts(), "TransferTask attempts should be incremented upon copy.");
                assertEquals(copiedTransfer.getStatus(), TransferStatusType.WRITE_COMPLETED, "Expected successful copy to return TransferTask with COMPLETED status.");
                assertEquals(1, copiedTransfer.getTotalFiles(), "TransferTask total files be 1 upon successful transfer of a single file.");
                assertEquals(testDataLength, copiedTransfer.getBytesTransferred(), "TransferTask total bytes transferred should be 32768, the size of the httpbin download name.");
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
    @DisplayName("Test URLCopy cancel during read in Unary/RelayTransfer")
    public void testCancelReadUnaryCopy(Vertx vertx, VertxTestContext ctx) {

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

            URLCopy mockCopy = getMockURLCopyInstance(vertx, tt);
            when(mockCopy.getRetryRequestManager()).thenReturn(mockRetryRequestManager);
            when(mockCopy.getSourceClient()).thenReturn(mockSrcRemoteDataClient);
            when(mockCopy.getDestClient()).thenReturn(mockDestRemoteDataClient);

            //remote transfer listener
            RemoteTransferListenerImpl mockRemoteTransferListenerImpl = getMockRemoteUnaryTransferListener(tt, mockRetryRequestManager);
            doReturn(mockRemoteTransferListenerImpl).when(mockCopy).getRemoteUnaryTransferListenerForTransferTask(tt);
            when(mockRemoteTransferListenerImpl.isCancelled()).thenReturn(true);

            //first leg child transfer task
            TransferTask srcChildTransferTask = new TransferTask(
                    tt.getSource(),
                    new File(TRANSFER_SRC).toURI().toString(),
                    tt.getOwner(),
                    tt.getUuid(),
                    tt.getRootTaskId());
            srcChildTransferTask.setTenantId(tt.getTenantId());
            srcChildTransferTask.setStatus(TransferStatusType.READ_STARTED);

            //mock child remote transfer listener
            RemoteUnaryTransferListenerImpl mockChildRemoteTransferListenerImpl = getMockRemoteUnaryTransferListener(srcChildTransferTask, mockRetryRequestManager);
            doNothing().when(mockChildRemoteTransferListenerImpl).killCopyTask();
            doReturn(mockChildRemoteTransferListenerImpl).when(mockCopy).getRemoteUnaryTransferListenerForTransferTask(tt);
            when(mockChildRemoteTransferListenerImpl.isCancelled()).thenReturn(true);

            //second leg child transfer task
            TransferTask destChildTransferTask = new TransferTask(
                    new File(LOCAL_TXT_FILE).toURI().toString(),
                    tt.getDest(),
                    tt.getOwner(),
                    tt.getParentTaskId(),
                    tt.getRootTaskId()
            );
            destChildTransferTask.setTenantId(tt.getTenantId());
            destChildTransferTask.setStatus(TransferStatusType.WRITE_STARTED);

            RemoteUnaryTransferListenerImpl mockDestChildRemoteTransferListenerImpl = getMockRemoteUnaryTransferListener(destChildTransferTask, mockRetryRequestManager);
            doNothing().when(mockDestChildRemoteTransferListenerImpl).killCopyTask();
            when(mockCopy.getRemoteUnaryTransferListenerForTransferTask(any(TransferTask.class))).thenReturn(mockChildRemoteTransferListenerImpl, mockDestChildRemoteTransferListenerImpl);
            doNothing().when(mockCopy).killCopyTask();

            try {
                doCallRealMethod().when(mockCopy).checkCancelled(any(RemoteTransferListener.class));
                TransferTask copiedTransfer = mockCopy.copy(tt);
            } catch (ClosedByInterruptException e) {
                JsonObject readCancelledJson = srcChildTransferTask.toJson();
                readCancelledJson.put("status", TransferStatusType.CANCELLED);
                Thread.sleep(3);
                ctx.verify(() -> {
                    assertEquals(TransferStatusType.CANCELLED, tt.getStatus(), "Expected transfer task status to be CANCELLED when copy is cancelled or killed.");
//                    assertEquals(TransferStatusType.CANCELLED, srcChildTransferTask.getStatus(), "Expected child transfer task status to be CANCELLED when copy is cancelled or killed.");
//                    verify(mockRetryRequestManager, atLeast(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
//                            argThat(new IsSameJsonTransferTask(readCancelledJson)), eq(2));

                    ctx.completeNow();
                });
            }
        } catch (Exception e) {
            Assertions.fail("Expected cancel copy to throw ClosedByInterruptException but threw " + e);
        } finally {
            Settings.ALLOW_RELAY_TRANSFERS = allowRelayTransfers;
        }

    }

    @Test
    @DisplayName("Test URLCopy cancel during write in Unary/RelayTransfer")
    public void testCancelWriteUnaryCopy(Vertx vertx, VertxTestContext ctx) {
        try {
            allowRelayTransfers = Settings.ALLOW_RELAY_TRANSFERS;
            Settings.ALLOW_RELAY_TRANSFERS = true;

            RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);
            doNothing().when(mockRetryRequestManager).request(anyString(), any(JsonObject.class), anyInt());

            RemoteDataClient mockSrcRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_SRC);
            RemoteDataClient mockDestRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_DEST);

            TransferTask tt = _createTestTransferTask();
            tt.setId(1L);
            tt.setSource(TRANSFER_SRC);
            tt.setDest(TRANSFER_DEST);

            URLCopy mockCopy = getMockURLCopyInstance(vertx, tt);
            when(mockCopy.getRetryRequestManager()).thenReturn(mockRetryRequestManager);
            when(mockCopy.getSourceClient()).thenReturn(mockSrcRemoteDataClient);
            when(mockCopy.getDestClient()).thenReturn(mockDestRemoteDataClient);

            //remote transfer listener
            RemoteTransferListenerImpl mockRemoteTransferListenerImpl = getMockRemoteUnaryTransferListener(tt, mockRetryRequestManager);
            doReturn(mockRemoteTransferListenerImpl).when(mockCopy).getRemoteUnaryTransferListenerForTransferTask(tt);
            when(mockRemoteTransferListenerImpl.isCancelled()).thenReturn(true);

            //first leg child transfer task
            TransferTask srcChildTransferTask = new TransferTask(
                    tt.getSource(),
                    new File(TRANSFER_SRC).toURI().toString(),
                    tt.getOwner(),
                    tt.getUuid(),
                    tt.getRootTaskId());
            srcChildTransferTask.setTenantId(tt.getTenantId());
            srcChildTransferTask.setStatus(TransferStatusType.READ_STARTED);

            //mock child remote transfer listener
            RemoteUnaryTransferListenerImpl mockChildRemoteTransferListenerImpl = getMockRemoteUnaryTransferListener(srcChildTransferTask, mockRetryRequestManager);
            doReturn(mockChildRemoteTransferListenerImpl).when(mockCopy).getRemoteUnaryTransferListenerForTransferTask(tt);
            when(mockChildRemoteTransferListenerImpl.isCancelled()).thenReturn(false);

            //second leg child transfer task
            TransferTask destChildTransferTask = new TransferTask(
                    new File(LOCAL_TXT_FILE).toURI().toString(),
                    tt.getDest(),
                    tt.getOwner(),
                    tt.getParentTaskId(),
                    tt.getRootTaskId()
            );
            destChildTransferTask.setTenantId(tt.getTenantId());
            destChildTransferTask.setStatus(TransferStatusType.WRITE_STARTED);

            RemoteUnaryTransferListenerImpl mockDestChildRemoteTransferListenerImpl = getMockRemoteUnaryTransferListener(destChildTransferTask, mockRetryRequestManager);
            when(mockDestChildRemoteTransferListenerImpl.isCancelled()).thenReturn(true);
            when(mockCopy.getRemoteUnaryTransferListenerForTransferTask(any(TransferTask.class))).thenReturn(mockChildRemoteTransferListenerImpl, mockDestChildRemoteTransferListenerImpl);

            try {
                doCallRealMethod().when(mockCopy).checkCancelled(any(RemoteUnaryTransferListenerImpl.class));
                TransferTask copiedTransfer = mockCopy.copy(tt);

            } catch (ClosedByInterruptException e) {
                JsonObject writeCancelledJson = destChildTransferTask.toJson();
                writeCancelledJson.put("status", TransferStatusType.CANCELLED);

                ctx.verify(() -> {
                    assertEquals(TransferStatusType.CANCELLED, tt.getStatus(), "Expected transfer task status to be CANCELLED when copy is cancelled or killed.");
                    assertEquals(TransferStatusType.CANCELLED, destChildTransferTask.getStatus(), "Expected child transfer task status to be CANCELLED when copy is cancelled or killed.");
                    verify(mockRetryRequestManager, atLeast(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                            argThat(new IsSameJsonTransferTask(writeCancelledJson)), eq(2));

                    ctx.completeNow();
                });
            }
        } catch (Exception e) {
            Assertions.fail("Expected cancel copy to throw ClosedByInterruptionException but threw " + e);
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
            when(mockRemoteInputStream.read(any(), eq(0), anyInt())).thenReturn(FILE_SIZE.intValue(), -1);

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
            JsonObject rootJson = tt.toJson();

            RemoteStreamingTransferListenerImpl mockRemoteStreamingTransferListenerImpl = getMockRemoteStreamingTransferListener(tt, mockRetryRequestManager);

            //Injecting the mocked arguments to the mocked URLCopy
            //Using InjectMocks annotation will not create a mock of URLCopy, which we need to pass in the mocked RemoteTransferListenerImpl
            URLCopy mockCopy = getMockURLCopyInstance(vertx, tt);
            when(mockCopy.getRetryRequestManager()).thenReturn(mockRetryRequestManager);
            when(mockCopy.getRemoteStreamingTransferListenerForTransferTask(any(TransferTask.class))).thenReturn(mockRemoteStreamingTransferListenerImpl);
            when(mockCopy.getSourceClient()).thenReturn(mockSrcRemoteDataClient);
            when(mockCopy.getDestClient()).thenReturn(mockDestRemoteDataClient);

            TransferTask copiedTransfer = mockCopy.copy(tt);

            //mock JsonObjects used in the update event request
            JsonObject streamCopyStartedJson = rootJson.copy().put("status", TransferStatusType.STREAM_COPY_STARTED);
            streamCopyStartedJson.put("attempts", tt.getAttempts());
            streamCopyStartedJson.put("startTime", tt.getStartTime());
            streamCopyStartedJson.put("totalFiles", 1);
            streamCopyStartedJson.put("totalSize", FILE_SIZE);

            JsonObject streamInProgressJson = streamCopyStartedJson.copy().put("status", TransferStatusType.STREAM_COPY_IN_PROGRESS);
            streamInProgressJson.put("transferRate", TRANSFER_RATE);
            streamInProgressJson.put("bytesTransferred", tt.getBytesTransferred());
            streamInProgressJson.put("lastUpdated", tt.getLastUpdated());

            JsonObject streamCompletedJson = streamInProgressJson.copy().put("status", TransferStatusType.STREAM_COPY_COMPLETED);
            streamCompletedJson.put("transferRate", tt.getTransferRate());
            streamCompletedJson.put("endTime", tt.getEndTime());

            ctx.verify(() -> {

                //check that events are published correctly using the RetryRequestManager
                verify(mockRetryRequestManager, times(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                        eq(streamCopyStartedJson), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                        argThat(new IsSameJsonTransferTask(streamInProgressJson)), eq(2));
                verify(mockRetryRequestManager, times(1)).request(eq(MessageType.TRANSFERTASK_UPDATED),
                        argThat(new IsSameJsonTransferTask(streamCompletedJson)), eq(2));

                assertEquals(attempts + 1, copiedTransfer.getAttempts(), "TransferTask attempts should be incremented upon copy.");
                assertEquals(copiedTransfer.getStatus(), TransferStatusType.STREAM_COPY_COMPLETED, "Expected successful copy to return TransferTask with COMPLETED status.");
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

    @Test
    @DisplayName("Test URLCopy cancel streamingTransfer")
    public void testCancelStreamingCopy (Vertx vertx, VertxTestContext ctx) {
        try {
            allowRelayTransfers = Settings.ALLOW_RELAY_TRANSFERS;
            Settings.ALLOW_RELAY_TRANSFERS = false;

            RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);

            RemoteDataClient mockSrcRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_SRC);
            RemoteDataClient mockDestRemoteDataClient = getMockRemoteDataClientInstance(TRANSFER_DEST);

            RemoteInputStream mockRemoteInputStream = mock(RemoteInputStream.class);
            when(mockRemoteInputStream.isBuffered()).thenReturn(true);
            when(mockRemoteInputStream.read(any(), eq(0), anyInt())).thenReturn(FILE_SIZE.intValue(), -1);

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
            JsonObject rootJson = tt.toJson();

            RemoteStreamingTransferListenerImpl mockRemoteStreamingTransferListenerImpl = getMockRemoteStreamingTransferListener(tt, mockRetryRequestManager);
            when(mockRemoteStreamingTransferListenerImpl.isCancelled()).thenReturn(true, false);
            doCallRealMethod().when(mockRemoteStreamingTransferListenerImpl).cancel();

            //Injecting the mocked arguments to the mocked URLCopy
            //Using InjectMocks annotation will not create a mock of URLCopy, which we need to pass in the mocked RemoteTransferListenerImpl
            URLCopy mockCopy = getMockURLCopyInstance(vertx, tt);
            when(mockCopy.getRetryRequestManager()).thenReturn(mockRetryRequestManager);
            when(mockCopy.getRemoteStreamingTransferListenerForTransferTask(any(TransferTask.class))).thenReturn(mockRemoteStreamingTransferListenerImpl);
            when(mockCopy.getSourceClient()).thenReturn(mockSrcRemoteDataClient);
            when(mockCopy.getDestClient()).thenReturn(mockDestRemoteDataClient);

            try {
                doCallRealMethod().when(mockCopy).checkCancelled(any(RemoteUnaryTransferListenerImpl.class));
                mockCopy.copy(tt);

            } catch (ClosedByInterruptException e) {
                JsonObject writeCancelledJson = rootJson;
                writeCancelledJson.put("status", TransferStatusType.CANCELLED);
                Thread.sleep(3);

                ctx.verify(() -> {
                    assertEquals(TransferStatusType.CANCELLED, tt.getStatus(), "Expected transfer task status to be CANCELLED when copy is cancelled or killed.");
                    verify(mockRemoteStreamingTransferListenerImpl, times(1)).cancel();
                    ctx.completeNow();
                });
            }

        } catch (Exception e) {
            Assertions.fail("Expected cancel copy to throw ClosedByInterruptException but threw " + e);
        } finally {
            Settings.ALLOW_RELAY_TRANSFERS = allowRelayTransfers;
        }
    }


}
