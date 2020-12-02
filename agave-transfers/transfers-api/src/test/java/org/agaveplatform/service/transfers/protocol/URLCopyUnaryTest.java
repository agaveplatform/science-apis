package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.aspectj.lang.annotation.Before;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.Settings;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.FieldSetter;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("URLCopy Unary Tests")
public class URLCopyUnaryTest extends BaseTestCase {


    @Test
    public void copyTest() throws TransferException, RemoteDataSyntaxException, RemoteDataException, IOException {
        TransferTask transfer = _createTestTransferTask();
        transfer.setId(1L);
        transfer.setSource(TRANSFER_SRC);
        transfer.setDest(TRANSFER_DEST);

        RemoteDataClient mockSourceClient = mock(RemoteDataClient.class);
        RemoteDataClient mockDestClient = mock(RemoteDataClient.class);
        Vertx mockVertx = mock(Vertx.class);
        RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);
        when(mockSourceClient.isThirdPartyTransferSupported()).thenReturn(false);

        URLCopy mockURLCopy = spy(new URLCopy(mockSourceClient, mockDestClient, mockVertx, mockRetryRequestManager));
        doReturn(false).when(mockURLCopy).isKilled();
//        doReturn(transfer).when(mockURLCopy).relayTransfer(anyString(), anyString(), any(TransferTask.class));

        Settings.ALLOW_RELAY_TRANSFERS = true;
        try {
            TransferTask copiedTask = mockURLCopy.copy(transfer);
            verify(mockURLCopy, times(1)).relayTransfer(anyString(), anyString(), any(TransferTask.class));
            assertEquals(copiedTask, transfer, "TransferTask should match after copy is complete.");
        } catch (Exception e){
            fail("Valid Source and Destination should not throw exception.");
        }
    }

    @Test
    public void copyMissingDirectoryTest() throws TransferException, RemoteDataSyntaxException, RemoteDataException, IOException {
        TransferTask transfer = _createTestTransferTask();
        transfer.setId(1L);
        transfer.setSource(MISSING_DIRECTORY);
        transfer.setDest(TRANSFER_DEST);

        RemoteDataClient mockSourceClient = mock(RemoteDataClient.class);
        RemoteDataClient mockDestClient = mock(RemoteDataClient.class);
        Vertx mockVertx = mock(Vertx.class);
        RetryRequestManager mockRetryRequestManager = mock(RetryRequestManager.class);
        when(mockSourceClient.isThirdPartyTransferSupported()).thenReturn(false);
//        when(mockSourceClient.resolvePath(MISSING_DIRECTORY)).thenThrow(new FileNotFoundException());

        doThrow(FileNotFoundException.class).when(mockSourceClient).resolvePath(anyString());
        URLCopy mockURLCopy = spy(new URLCopy(mockSourceClient, mockDestClient, mockVertx, mockRetryRequestManager));
        doReturn(false).when(mockURLCopy).isKilled();

        Settings.ALLOW_RELAY_TRANSFERS = true;
        TransferTask copiedTask = new TransferTask();
        try {
           copiedTask = mockURLCopy.copy(transfer);
           fail("Missing Directory should throw exception.");
        } catch (Exception e){
            assertEquals(copiedTask.getStatus(), TransferStatusType.FAILED, "TransferStatusType should be 'FAILED' when exception is thrown");
        }
    }

}
