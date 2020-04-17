package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.util.CryptoHelper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfers Watch Listener Test")
class TransferWatchListenerTest extends BaseTestCase {

	private TransferTaskDatabaseService dbService;
	private Vertx vertx;
	private JWTAuth jwtAuth;

	/**
	 * Initializes the jwt auth options and the
	 * @throws IOException when the key cannot be read
	 */
	private void initAuth() throws IOException {
		JWTAuthOptions jwtAuthOptions = new JWTAuthOptions()
				.addPubSecKey(new PubSecKeyOptions()
						.setAlgorithm("RS256")
						.setPublicKey(CryptoHelper.publicKey())
						.setSecretKey(CryptoHelper.privateKey()));

		jwtAuth = JWTAuth.create(vertx, jwtAuthOptions);
	}

	@BeforeAll
	public void setUpService() throws IOException {
		// read in config options
		initConfig();

		// init the jwt auth used in the api calls
		initAuth();
	}


	TransferWatchListener getMockListenerInstance(Vertx vertx) {
		TransferWatchListener twc = Mockito.mock(TransferWatchListener.class);
		when(twc.getEventChannel()).thenReturn(TRANSFERTASK_HEALTHCHECK);
		when(twc.getVertx()).thenReturn(vertx);
		when(twc.processEvent()).thenCallRealMethod();

		return twc;
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - processEvent")
	public void processEvent(Vertx vertx, VertxTestContext ctx) {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferWatchListener twc = getMockListenerInstance(vertx);

		TransferTask transferTask = _createTestTransferTask();
		JsonObject json = transferTask.toJson();

		Future<Boolean> result = twc.processEvent();
		verify(twc, never())._doPublishEvent(eq(TRANSFERTASK_HEALTHCHECK), any());

		ctx.completeNow();
	}
}
