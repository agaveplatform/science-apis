package org.agaveplatform.service.transfers.model;

import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.junit.jupiter.api.*;

import static org.agaveplatform.service.transfers.BaseTestCase.*;
import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("TransferTaskNotificationMessage Tests")
class TransferTaskNotificationMessageTest {

    protected TransferTask _createTestTransferTask() {
        return new TransferTask(TENANT_ID, TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, null, null);
    }

    @Test
    void toJson() {

        TransferTask transferTask = _createTestTransferTask();
        TransferTaskNotificationMessage transferTaskMessage = new TransferTaskNotificationMessage(transferTask, null);
        JsonObject testMessageJson = transferTaskMessage.toJson();

        assertTrue(testMessageJson.containsKey(TransferTaskNotificationMessage.SOURCE_MESSAGE_TYPE_FIELD), "original address field should always be present");

        assertNull(testMessageJson.getString(TransferTaskNotificationMessage.SOURCE_MESSAGE_TYPE_FIELD),"original address should be null when no throwable provided");
    }

    @Test
    void toJsonUsesMessageTypeConstant() {

        TransferTask transferTask = _createTestTransferTask();
        TransferTaskNotificationMessage transferTaskMessage = new TransferTaskNotificationMessage(transferTask, MessageType.TRANSFERTASK_ASSIGNED);
        JsonObject testMessageJson = transferTaskMessage.toJson();

        assertTrue(testMessageJson.containsKey(TransferTaskNotificationMessage.SOURCE_MESSAGE_TYPE_FIELD), "original address field should always be present");
        
        assertEquals(MessageType.TRANSFERTASK_ASSIGNED, testMessageJson.getString(TransferTaskNotificationMessage.SOURCE_MESSAGE_TYPE_FIELD), "original address should match simple name of throwable in constructor");
    }

    @Test
    void toJsonFromJson() {
        TransferTask transferTask = _createTestTransferTask();
        TransferTaskNotificationMessage sourceTransferTaskMessage = new TransferTaskNotificationMessage(transferTask, MessageType.TRANSFERTASK_ASSIGNED);
        TransferTaskNotificationMessage transferTaskMessage = new TransferTaskNotificationMessage(sourceTransferTaskMessage.toJson());
        JsonObject testMessageJson = transferTaskMessage.toJson();

        assertTrue(testMessageJson.containsKey(TransferTaskNotificationMessage.SOURCE_MESSAGE_TYPE_FIELD), "original address field should always be present");

        assertEquals(sourceTransferTaskMessage.toJson(), testMessageJson, "Json representation should be the same as original transfer task notification message json object");
    }
}