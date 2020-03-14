package org.iplantc.io.nats.dispatcher;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;

public class Dispatcher {

	public static void main(String[] args) {

		try {
			Options options = new Options.Builder().
					server("nats://demo.nats.io:4222").
					pingInterval(Duration.ofSeconds(20)). // Set Ping Interval
					maxPingsOut(5). // Set max pings in flight
					build();

			// [begin subscribe_queue]
			Connection nc = Nats.connect(options);

			// Use a latch to wait for 10 messages to arrive
			CountDownLatch latch = new CountDownLatch(10);

			// Create a dispatcher and inline message handler
			Dispatcher d = nc.createDispatcher((msg) -> {
				String str = new String(msg.getData(), StandardCharsets.UTF_8);
				System.out.println("MsgData: "+str);
				//latch.countDown();
				System.out.println("ReplyTo: "+ msg.getReplyTo());
				nc.publish(msg.getReplyTo(), "A message".getBytes(StandardCharsets.UTF_8));
			});

			// Subscribe
			d.subscribe("irods4", "workers");

			// Wait for a message to come in
			latch.await();

			// Close the connection
			nc.close();
			// [end subscribe_queue]
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
