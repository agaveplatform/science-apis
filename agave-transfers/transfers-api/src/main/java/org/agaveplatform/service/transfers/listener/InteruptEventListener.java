package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Subscription;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.FLUSH_DELAY_NATS;

@Deprecated
public class InteruptEventListener extends AbstractNatsListener {
	private static final Logger log = LoggerFactory.getLogger(InteruptEventListener.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_INTERUPTED;

	public InteruptEventListener() { super(); }
	public InteruptEventListener(Vertx vertx) {
		super(vertx);
	}
	public InteruptEventListener(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	@Override
	public void start() throws TimeoutException, InterruptedException, IOException {
		//EventBus bus = vertx.eventBus();
		Connection nc = null;
		nc = _connect();
		Dispatcher d = nc.createDispatcher((msg) -> {});
		//bus.<JsonObject>consumer(getEventChannel(), msg -> {
		Subscription s = d.subscribe(EVENT_CHANNEL, msg -> {
			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
			String response = new String(msg.getData(), StandardCharsets.UTF_8);
			JsonObject body = new JsonObject(response) ;
			String uuid = body.getString("uuid");
			String source = body.getString("source");
			String tenantId = body.getString("tenant_id");

			log.info("Transfer task paused {} created: {} -> {}",tenantId, uuid, source);
		});
		d.subscribe(EVENT_CHANNEL);
		nc.flush(Duration.ofMillis(config().getInteger(String.valueOf(FLUSH_DELAY_NATS))));
	}


	public boolean interruptEvent( String uuid, String source, String username, String tenantId ) throws IOException, InterruptedException, TimeoutException {
		EventBus bus = vertx.eventBus();

		Connection nc = null;
		nc = _connect();

		Dispatcher d = nc.createDispatcher((msg) -> {});
		AtomicBoolean interupt = new AtomicBoolean(false);
		Subscription s = d.subscribe(EVENT_CHANNEL, msg -> {
			if (bus.consumer("paused." + tenantId + "." + username + "." + uuid).isRegistered()) {
				log.info("Transfer task paused {} created: {} -> {}", tenantId, uuid, source);
				interupt.set(true);
			}

		});
		d.subscribe(EVENT_CHANNEL);
		nc.flush(Duration.ofMillis(config().getInteger(String.valueOf(FLUSH_DELAY_NATS))));
		return interupt.get();
	}

}
