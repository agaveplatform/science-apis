package org.agaveplatform.service.transfers.model;

import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.exception.TransferException;
import org.junit.jupiter.api.*;

import static org.agaveplatform.service.transfers.BaseTestCase.*;
import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("TransferTaskErrorMessage tests")
class TransferTaskErrorMessageTest {

    private final String TEST_ERROR_MESSAGE = "This is the expected error message for the test object";

    protected TransferTask _createTestTransferTask() {
        return new TransferTask(TENANT_ID, TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, null, null);
    }

    @Test
    void toJson() {

        TransferTask transferTask = _createTestTransferTask();
        TransferTaskErrorMessage transferTaskMessage = new TransferTaskErrorMessage(transferTask, null);
        JsonObject testMessageJson = transferTaskMessage.toJson();

        assertTrue(testMessageJson.containsKey(TransferTaskErrorMessage.ERROR_CLASS_NAME_FIELD), "cause field should always be present");
        assertTrue(testMessageJson.containsKey(TransferTaskErrorMessage.ERROR_MESSAGE_FIELD), "errorMessage field should always be present");

        assertNull(testMessageJson.getString(TransferTaskErrorMessage.ERROR_CLASS_NAME_FIELD),"cause should be null when no throwable provided");
        assertNull(testMessageJson.getString(TransferTaskErrorMessage.ERROR_MESSAGE_FIELD), "errorMessage should be null when no throwable or message provided");
    }

    @Test
    void toJsonUsesThrowableMessage() {

        TransferTask transferTask = _createTestTransferTask();
        TransferTaskErrorMessage transferTaskMessage = new TransferTaskErrorMessage(transferTask, new TransferException(TEST_ERROR_MESSAGE));
        JsonObject testMessageJson = transferTaskMessage.toJson();

        assertTrue(testMessageJson.containsKey(TransferTaskErrorMessage.ERROR_CLASS_NAME_FIELD), "cause field should always be present");
        assertTrue(testMessageJson.containsKey(TransferTaskErrorMessage.ERROR_MESSAGE_FIELD), "errorMessage field should always be present");

        assertEquals(testMessageJson.getString(TransferTaskErrorMessage.ERROR_CLASS_NAME_FIELD), TransferException.class.getSimpleName(), "cause should match simple name of throwable in constructor");
        assertEquals(testMessageJson.getString(TransferTaskErrorMessage.ERROR_MESSAGE_FIELD), TEST_ERROR_MESSAGE, "errorMessage should match message from throwable in constructor when no message provided");
    }

    @Test
    void toJsonUsesErrorMessage() {

        TransferTask transferTask = _createTestTransferTask();
        TransferTaskErrorMessage transferTaskMessage = new TransferTaskErrorMessage(transferTask, new TransferException(), TEST_ERROR_MESSAGE);
        JsonObject testMessageJson = transferTaskMessage.toJson();

        assertTrue(testMessageJson.containsKey(TransferTaskErrorMessage.ERROR_CLASS_NAME_FIELD), "cause field should always be present");
        assertTrue(testMessageJson.containsKey(TransferTaskErrorMessage.ERROR_MESSAGE_FIELD), "errorMessage field should always be present");

        assertEquals(testMessageJson.getString(TransferTaskErrorMessage.ERROR_CLASS_NAME_FIELD), TransferException.class.getSimpleName(), "cause should match simple name of throwable in constructor");
        assertEquals(testMessageJson.getString(TransferTaskErrorMessage.ERROR_MESSAGE_FIELD), TEST_ERROR_MESSAGE, "errorMessage should match message from throwable in constructor when no message provided");
    }

    @Test
    void toJsonFromJson() {
        TransferTask transferTask = _createTestTransferTask();
        TransferTaskErrorMessage sourceTransferTaskMessage = new TransferTaskErrorMessage(transferTask, new TransferException(), TEST_ERROR_MESSAGE);
        TransferTaskErrorMessage transferTaskMessage = new TransferTaskErrorMessage(sourceTransferTaskMessage.toJson());
        JsonObject testMessageJson = transferTaskMessage.toJson();

        assertTrue(testMessageJson.containsKey(TransferTaskErrorMessage.ERROR_CLASS_NAME_FIELD), "cause field should always be present");
        assertTrue(testMessageJson.containsKey(TransferTaskErrorMessage.ERROR_MESSAGE_FIELD), "errorMessage field should always be present");

        assertEquals(sourceTransferTaskMessage.toJson(), testMessageJson, "Json representation should be the same as original transfer task error message json object");
    }
}