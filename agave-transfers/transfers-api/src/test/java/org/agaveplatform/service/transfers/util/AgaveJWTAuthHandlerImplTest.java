package org.agaveplatform.service.transfers.util;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.auth.jwt.impl.JWTAuthProviderImpl;
import io.vertx.ext.jwt.JWTOptions;
import io.vertx.ext.web.handler.impl.AgaveJWTAuthHandlerImpl;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.iplantc.service.common.Settings;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;


//@ExtendWith(VertxExtension.class)
@DisplayName("JWTAuthOptions tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
class AgaveJWTAuthHandlerImplTest extends BaseTestCase {

	private Vertx vertx;

	@Test
	@Disabled
	void testsomethingparseCredentials(Vertx vertx, VertxTestContext ctx) {
		String token = this.makeTestJwt(TEST_USERNAME);
		AgaveJWTAuthHandlerImpl authHandler = new AgaveJWTAuthHandlerImpl(jwtAuth, "false");
	}

	@Test
	@Disabled
	void parseMultiTenantAuthorization() {
	}


}