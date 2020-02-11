package org.iplantc.io.nats.publish;

import io.nats.client.*;

import org.apache.log4j.Logger;
		import java.io.IOException;
		import java.util.UUID;
		import java.time.Duration;
		import java.nio.charset.StandardCharsets;

public class Publish {

	public static void main(String[] args) {
		try {
			int count = 0;
			int x = 0;
			while (count < 3) {
				x = publish();
				if (x < 1 ) {
					System.out.println(count);
					try {
						Thread.sleep(1000);
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
					count++;
					continue;
				}  else {
					break;
				}
			}
		}
		catch (Exception ex){
			System.out.println("Error "+ ex.toString() );
		}
	}

	private static int publish() {
		try {
			// [begin publish_with_reply]
			Connection nc = Nats.connect("nats://demo.nats.io:4222");

			// Create a unique subject nam
			String uniqueReplyTo = NUID.nextGlobal();

			//for (int i = 0; i<3; i++) {
			// Listen for a single response
			Subscription sub = nc.subscribe(uniqueReplyTo);
			sub.unsubscribe(1);

			// Send the request
			nc.publish("sftp", uniqueReplyTo, "All is Well".getBytes(StandardCharsets.UTF_8));

			// Read the reply
			Message msg = sub.nextMessage(Duration.ofSeconds(1));

			try{
				// Use the response
				System.out.println(new String( msg.getData(), StandardCharsets.UTF_8) );
			}
			catch (NullPointerException ex){
				//resend the request after a 5 second pause
				//Thread.sleep(5000);
				System.out.println("Error "+ ex.toString() );
				return 0;
			}
			//}ne
			// Close the connection
			nc.close();

			return 1;
			// [end publish_with_reply]
		} catch (Exception e) {
			e.printStackTrace();
			return 0;
		}
	}

}