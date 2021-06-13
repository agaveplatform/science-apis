package org.agaveplatform.service.transfers.listener;

import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@DisplayName("AbstractTransferTaskListener tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AbstractTransferTaskListenerIT {

    final String TENANT_ID = "agave.dev";
    final String TRANSFER_SRC = "http://httpbin:8000/stream-bytes/32768";
    final String TRANSFER_DEST = "agave://sftp/";
    final String TEST_USER = "testuser";

    protected TransferTask _createTestTransferTask() {
        return new TransferTask(TENANT_ID, TRANSFER_SRC, TRANSFER_DEST, TEST_USER, null, null);
    }
    @Test
    @DisplayName("TransferTaskCreatedListener - taskIsNotInterrupted")
    void taskIsNotInterruptedTest() {
        TransferTask tt = _createTestTransferTask();
        tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

        AbstractTransferTaskListener tc = mock(AbstractTransferTaskListener.class);
        when(tc.getCancelledTasks()).thenCallRealMethod();
        doCallRealMethod().when(tc).setCancelledTasks(any());
        when(tc.getPausedTasks()).thenCallRealMethod();
        doCallRealMethod().when(tc).setPausedTasks(any());

        // have to set them here because they won't be initialized when the mock is created.
        tc.setCancelledTasks(new ConcurrentHashSet<String>());
        tc.setPausedTasks(new ConcurrentHashSet<String>());

        // pass through mock for the rest of the methods we are testing
        doCallRealMethod().when(tc).addPausedTask(any());
        doCallRealMethod().when(tc).removePausedTask(any());
        doCallRealMethod().when(tc).addCancelledTask(any());
        doCallRealMethod().when(tc).removeCancelledTask(any());
        when(tc.taskIsNotInterrupted(any())).thenCallRealMethod();
        doNothing().when(tc)._doPublishEvent(any(), any());

        // this object is expected to be sent when an interrupte is detected.
        JsonObject cancelledMessage = new JsonObject().put("message", "Transfer was Canceled or Paused");

        tc.addCancelledTask(tt.getUuid());
        assertFalse(tc.taskIsNotInterrupted(tt), "UUID of transfer task present in cancelledTasks list should indicate task is interrupted");
        tc.removeCancelledTask(tt.getUuid());

        tc.addPausedTask(tt.getUuid());
        assertFalse(tc.taskIsNotInterrupted(tt), "UUID of transfer task present in pausedTasks list should indicate task is interrupted");
        tc.removePausedTask(tt.getUuid());

        tc.addCancelledTask(tt.getParentTaskId());
        assertFalse(tc.taskIsNotInterrupted(tt), "UUID of transfer task parent present in cancelledTasks list should indicate task is interrupted");
        tc.removeCancelledTask(tt.getParentTaskId());

        tc.addPausedTask(tt.getParentTaskId());
        assertFalse(tc.taskIsNotInterrupted(tt), "UUID of transfer task parent present in pausedTasks list should indicate task is interrupted");
        tc.removePausedTask(tt.getParentTaskId());

        tc.addCancelledTask(tt.getRootTaskId());
        assertFalse(tc.taskIsNotInterrupted(tt), "UUID of transfer task root present in cancelledTasks list should indicate task is interrupted");
        tc.removeCancelledTask(tt.getRootTaskId());

        tc.addPausedTask(tt.getRootTaskId());
        assertFalse(tc.taskIsNotInterrupted(tt), "UUID of transfer task root present in pausedTasks list should indicate task is interrupted");
        tc.removePausedTask(tt.getRootTaskId());

        // a message should have been sent for every cancelled or paused discovery
//        verify(tc, times(6))._doPublishEvent(eq(MessageType.TRANSFERTASK_CANCELED), eq(cancelledMessage));
    }

    @Test
    @DisplayName("TransferTaskCreatedListener - uriSchemeIsNotSupportedTest passes valid schema ")
    void uriSchemeIsNotSupportedTestReturnsFalseForValidSchema() throws IOException {
        AbstractTransferTaskListener tc = mock(AbstractTransferTaskListener.class);
        when(tc.uriSchemeIsNotSupported(any(URI.class))).thenCallRealMethod();

        List<URI> testURIs = List.of(
                URI.create("file:///dev/null"),
                URI.create("file:/dev/null"),
                URI.create("http://example.com/download"),
                URI.create("ftp://ftp.example.com/download")
        );

        for(URI testUri: testURIs) {
            assertFalse(tc.uriSchemeIsNotSupported(testUri),
                    "uriSchemeIsNotSupported should return false for valid test URI," + testUri.toString());
        }
    }

    @Test
    @DisplayName("TransferTaskCreatedListener - uriSchemeIsNotSupportedTest fails unsupported schema")
    void uriSchemeIsNotSupportedTestReturnsTrieForUnsupportedSchema() throws IOException {
        AbstractTransferTaskListener tc = mock(AbstractTransferTaskListener.class);
        when(tc.uriSchemeIsNotSupported(any(URI.class))).thenCallRealMethod();

        List<URI> testURIs = List.of(
                URI.create("mysql://mysql/foobar"),
                URI.create("/dev/null"),
                URI.create("bacde://example.com/download"));

        for(URI testUri: testURIs) {
            assertTrue(tc.uriSchemeIsNotSupported(testUri),
                    "uriSchemeIsNotSupported should return true for valid test URI," + testUri.toString());
        }
    }

    @Test
    @DisplayName("TransferTaskCreatedListener - uriSchemeIsNotSupportedTest fails null URI")
    void uriSchemeIsNotSupportedTestReturnsTrieForNull() {
        AbstractTransferTaskListener tc = mock(AbstractTransferTaskListener.class);
        when(tc.uriSchemeIsNotSupported(any(URI.class))).thenCallRealMethod();

        assertTrue(tc.uriSchemeIsNotSupported(null),
                "uriSchemeIsNotSupported should return true for null URI");

    }
}