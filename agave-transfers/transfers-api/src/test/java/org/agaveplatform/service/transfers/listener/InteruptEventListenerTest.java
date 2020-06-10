package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.protocol.TransferAllProtocolVertical;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_COMPLETED;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_INTERUPTED;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Interrupt Event Listener All tests")
@Disabled
class InteruptEventListenerTest extends BaseTestCase {
	private static final Logger logger = LoggerFactory.getLogger(InteruptEventListenerTest.class);

	protected InteruptEventListener getMockInteruptEventListenerInstance(Vertx vertx) {
		InteruptEventListener ttc = mock(InteruptEventListener.class );
		when(ttc.getDefaultEventChannel()).thenReturn(TRANSFERTASK_INTERUPTED);
		when(ttc.getVertx()).thenReturn(vertx);

		return ttc;
	}

	@Test
	@DisplayName("This is a test of the start function")
	@Disabled
	protected void startTest(Vertx vertx, VertxTestContext ctx) {
		InteruptEventListener transferInteruptEventListener = getMockInteruptEventListenerInstance(vertx);

		//verify(transferInteruptEventListener.start())._doPublishEvent(TRANSFERTASK_INTERUPTED, json);

	}
}