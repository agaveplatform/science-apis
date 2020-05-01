package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.mockito.Mockito.*;
//import static org.mockito.Matchers.anyString;
//import static org.mockito.ArgumentMatchers.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfers Task Canceled test")
//@Disabled
class TransferTaskCancelListenerTest extends BaseTestCase {


	@Test
	@DisplayName("Transfer Task Cancel Listener - processCancelAck")
	public void processCancelAck(Vertx vertx, VertxTestContext ctx){
// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.COMPLETED);
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());
		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

//		TransferTask transferTask = _createTestTransferTask();
//		transferTask.setStatus(TransferStatusType.TRANSFERRING);
//		transferTask.setStartTime(Instant.now());
//		transferTask.setEndTime(Instant.now());
//		transferTask.setRootTaskId(parentTask.getUuid());
//		transferTask.setParentTaskId(parentTask.getUuid());


		TransferTaskCancelListener ttc = Mockito.mock(TransferTaskCancelListener.class);
		Mockito.when(ttc.getEventChannel()).thenReturn(TRANSFERTASK_CANCELLED);
		Mockito.when(ttc.getVertx()).thenReturn(vertx);
		Mockito.when(ttc.processCancelAck(Mockito.any())).thenCallRealMethod();
		Mockito.when(ttc.getTransferTask(parentTask.getParentTaskId())).thenReturn(parentTask);
//		Mockito.when(ttc.getTransferTask(transferTask.getParentTaskId())).thenReturn(transferTask);
		Mockito.when(ttc.allChildrenCancelledOrCompleted(parentTask.getUuid())).thenReturn(true);
		doCallRealMethod().when(ttc).setTransferTaskCancelledIfNotCompleted(parentTask.getUuid());

		String result = ttc.processCancelAck(parentTask.toJson());

		verify(ttc)._doPublishEvent(TRANSFERTASK_CANCELED_COMPLETED, parentTask.toJson());
		verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_CANCELED_ACK), any());

//		assertEquals(uuid, result, "result should have been uuid: " + uuid);
		assertEquals(parentTask.getUuid(), result, "Transfer task was not acknowledged in repsonse uuid");

		ctx.completeNow();


	}
}