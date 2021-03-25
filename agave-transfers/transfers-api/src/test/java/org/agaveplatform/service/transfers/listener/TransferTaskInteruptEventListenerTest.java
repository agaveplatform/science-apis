package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_INTERUPTED;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Interrupt Event Listener All tests")
@Disabled
class TransferTaskInteruptEventListenerTest extends BaseTestCase {
	private static final Logger logger = LoggerFactory.getLogger(TransferTaskInteruptEventListenerTest.class);

	protected InteruptEventListener getMockInteruptEventListenerInstance(Vertx vertx) throws IOException, InterruptedException {
		InteruptEventListener listener = mock(InteruptEventListener.class );
		when(listener.getDefaultEventChannel()).thenReturn(TRANSFERTASK_INTERUPTED);
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.getRetryRequestManager()).thenCallRealMethod();
		doNothing().when(listener)._doPublishEvent(any(), any());
		doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
		doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());

		return listener;
	}

	@Test
	@DisplayName("This is a test of the start function")
	@Disabled
	protected void startTest(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException {
		InteruptEventListener transferInteruptEventListener = getMockInteruptEventListenerInstance(vertx);

		//verify(transferInteruptEventListener.start())._doPublishEvent(TRANSFERTASK_INTERUPTED, json);

	}
}