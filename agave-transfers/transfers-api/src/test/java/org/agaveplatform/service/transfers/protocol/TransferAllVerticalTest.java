package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.listener.*;
import org.agaveplatform.service.transfers.listener.TransferTaskCancelListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfer All test")
@Disabled
class TransferAllVerticalTest  extends BaseTestCase {

	TransferAllVertical getMockListenerInstance(Vertx vertx) {
		TransferAllVertical txfrAllVert = Mockito.mock(TransferAllVertical.class);
		when(txfrAllVert.getEventChannel()).thenReturn(TRANSFER_COMPLETED);
		when(txfrAllVert.getVertx()).thenReturn(vertx);

		return txfrAllVert;
	}

	@Test
	@DisplayName("Transfer All Vertical - processEvent")
	@Disabled
	void processEvent(Vertx vertx, VertxTestContext ctx) {
		TransferTask parentTask = _createTestTransferTask();

		TransferAllVertical txfrAllVert = getMockListenerInstance(vertx);

		String result = txfrAllVert.processEvent(parentTask.toJson());


		assertEquals("Complte", result);
		ctx.completeNow();
	}
}