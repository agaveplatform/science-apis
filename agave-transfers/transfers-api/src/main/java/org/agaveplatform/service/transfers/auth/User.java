package org.agaveplatform.service.transfers.auth;

import java.util.List;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AbstractUser;
import io.vertx.ext.auth.AuthProvider;

public class User extends AbstractUser {

    private final String name;
    private final List<String> permissions;

    public User(String name, List<String> permissions) {
        this.name = name;
        this.permissions = permissions;
    }

    @Override
    protected void doIsPermitted(String permission, Handler<AsyncResult<Boolean>> resultHandler) {
        resultHandler.handle(Future.succeededFuture(permissions.contains(permission)));
    }

    @Override
    public JsonObject principal() {
        return new JsonObject().put("name", name);
    }

    @Override
    public void setAuthProvider(AuthProvider authProvider) {
    }

}