package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public interface TransferTaskListener {

	void _doPublishEvent(String event, Object body);

	/**
	 * Handles processing of the message received for the event channel
	 * @param body teh json body of the message
	 */
	Future<Boolean> processEvent(JsonObject body);

//	/**
//	 * Sets the vertx instance for this listener
//	 * @param vertx the current instance of vertx
//	 */
//	void setVertx(Vertx vertx);
//
//	/**
//	 * Sets the eventChannel on which to listen
//	 * @param eventChannel the channel on which the verticle will listen.
//	 */
//	public void setEventChannel(String eventChannel);
//
//	/**
//	 * @return the message type to listen to
//	 */
//	public String getEventChannel() ;
//
//	/**
//	 * The defauilt event channel on which the verticle will listen when none other is provided.
//	 * @return
//	 */
//	public String getDefaultEventChannel();
}
