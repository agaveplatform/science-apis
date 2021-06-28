package org.agaveplatform.service.transfers.model;

import org.junit.jupiter.api.*;

import static org.agaveplatform.service.transfers.BaseTestCase.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("TransferTaskMessage Tests")
class TransferTaskMessageTest {

    protected TransferTask _createTestTransferTask() {
        return new TransferTask(TENANT_ID, TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, null, null);
    }

    @Test
    void toJson() {
        TransferTask transferTask = _createTestTransferTask();
        TransferTaskMessage transferTaskMessage = new TransferTaskMessage(transferTask);

        assertEquals(transferTask.toJson(), transferTaskMessage.toJson(), "Json representation should be the same as original transfer task");
    }

    @Test
    void toJsonFromJson() {
        TransferTask transferTask = _createTestTransferTask();
        TransferTaskMessage transferTaskMessage = new TransferTaskMessage(transferTask.toJson());

        assertEquals(transferTask.toJson(), transferTaskMessage.toJson(), "Json representation should be the same as original transfer task json object");
    }
}