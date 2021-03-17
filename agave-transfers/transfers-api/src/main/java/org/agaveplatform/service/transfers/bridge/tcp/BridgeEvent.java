package org.agaveplatform.service.transfers.bridge.tcp;

import io.vertx.codegen.annotations.CacheReturn;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;

/**
 * Represents an event that occurs on the event bus bridge.
 * <p>
 * Please consult the documentation for a full explanation.
 */
@VertxGen
public interface BridgeEvent extends io.vertx.ext.bridge.BaseBridgeEvent {

    /**
     * Get the raw JSON message for the event. This will be null for SOCKET_CREATED or SOCKET_CLOSED events as there is
     * no message involved.
     *
     * @param message the raw message
     * @return this reference, so it can be used fluently
     */
    @Fluent
    BridgeEvent setRawMessage(JsonObject message);

    /**
     * Get the SockJSSocket instance corresponding to the event
     *
     * @return  the SockJSSocket instance
     */
    @CacheReturn
    NetSocket socket();
}