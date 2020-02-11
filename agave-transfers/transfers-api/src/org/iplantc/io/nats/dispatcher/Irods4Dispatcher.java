package org.iplantc.io.nats.dispatcher;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

public class Irods4Dispatcher {

	public static void main(String[] args) {

		try {
			// [begin subscribe_queue]
			Connection nc = Nats.connect("nats://demo.nats.io:4222");

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
