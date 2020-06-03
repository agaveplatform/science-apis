package org.agaveplatform.service.transfers.resources;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.agaveplatform.service.transfers.Settings;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.BooleanUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builder to handle construction and formatting of response messages.
 */
public class AgaveResponseBuilder {
    private AgaveResponse.RequestStatus status = AgaveResponse.RequestStatus.success;
    private String message;
    private Object result;
    private boolean prettyPrint;
    private boolean naked;
    private List<String> fields;
    private String version = Settings.API_VERSION;

    public AgaveResponseBuilder() {}

    public static AgaveResponseBuilder getInstance(RoutingContext routingContext) {
        return new AgaveResponseBuilder().setRoutingContext(routingContext);
    }

    /**
     * Populates the formatting parameters (naked, prettyPrint, fields) from the url query parameters in the
     * {@code routingContext}.
     *
     * @param routingContext the routing context of the current request
     * @return the builder instance.
     */
    public AgaveResponseBuilder setRoutingContext(RoutingContext routingContext) {
        return setPrettyPrint(parseBooleanFromRoutingContext(routingContext, "prettyPrint"))
                .setNaked(parseBooleanFromRoutingContext(routingContext, "naked"))
                .setFields(parseFieldsFromRoutingContext(routingContext));

    }

    public AgaveResponseBuilder setStatus(AgaveResponse.RequestStatus status) {
        this.status = status;
        return this;
    }

    protected AgaveResponseBuilder setFields(List<String> fields) {
        this.fields = fields == null ? new ArrayList<String>() : fields;
        return this;
    }

    public AgaveResponseBuilder setMessage(String message) {
        this.message = message;
        return this;
    }

    public AgaveResponseBuilder setPrettyPrint(boolean prettyPrint) {
        this.prettyPrint = prettyPrint;
        return this;
    }

    public AgaveResponseBuilder setNaked(boolean naked) {
        this.naked = naked;
        return this;
    }

    public AgaveResponseBuilder setResult(JsonObject result) {
        this.result = result;
        return this;
    }

    public AgaveResponseBuilder setResult(JsonArray result) {
        this.result = result;
        return this;
    }

    protected AgaveResponseBuilder setVersion(String version) {
        this.version = version;
        return this;
    }

    public AgaveResponse build() {
        AgaveResponse ar = new AgaveResponse(status);
        ar.setVersion(version);
        ar.setMessage(message);
        ar.setResult(result);
        ar.setFields(fields);
        ar.setNaked(naked);
        ar.setPrettyPrint(prettyPrint);
        return ar;
    }

    /**
     * Parses out the fields to include in the response object from the {@code fields} query parameter. When empty
     * or not specified, this defaults to the full object response.
     *
     * @param routingContext the current request context
     * @return an array of fields in the response or an empty array if not given.
     */
    protected List<String> parseFieldsFromRoutingContext(RoutingContext routingContext) {
        List<String> params = routingContext.queryParam("fields");
        List<String> fields = new ArrayList<>();
        // if there was a query parameter named "fields"
        if (!params.isEmpty()) {

            // read the first value
            String sFields = params.get(0);

            // if the value is not blank, explode the value
            if (StringUtils.isNotBlank(sFields)) {
                // fields are comma separated. exclude empty values caused by bookend or double commas
                Splitter splitter = Splitter.on(CharMatcher.anyOf(",")).trimResults().omitEmptyStrings();
                Iterable<String> splitFields = splitter.split(sFields);
                // if any values are not blank, then use the whole list of values
                if (splitFields.iterator().hasNext()) {
                    fields = Arrays.asList(Iterables.toArray(splitFields, String.class));
                }
            }
        }
        // fields may be empty, but never null
        return fields;
    }

    /**
     * Parses out the {@code parameterName} field from the query parameter in the {@code routingContext}. The first
     * value will to selected and parsed as a truthy value.
     *
     * @param routingContext the current request context
     * @param parameterName the name of the url query parameter to parse as a boolean value
     * @return true if present and truthy. false otherwise
     */
    protected boolean parseBooleanFromRoutingContext(RoutingContext routingContext, String parameterName) {
        List<String> params = routingContext.queryParam(parameterName);
        boolean val = false;
        // if there was a query parameter matching
        if (!params.isEmpty()) {
            val = Boolean.parseBoolean(params.get(0));
        }
        // fields may be empty, but never null
        return val;
    }

    /**
     * Parses out the {@code parameterName} field from the query parameter in the {@code routingContext}. The first
     * value will to selected and parsed as a double, then case as an integer value.
     *
     * @param routingContext the current request context
     * @param parameterName the name of the url query parameter to parse as a boolean value
     * @return true if present and truthy. false otherwise
     */
    protected int parseIntegerFromRoutingContext(RoutingContext routingContext, String parameterName) {
        List<String> params = routingContext.queryParam(parameterName);
        int val = 0;
        // if there was a query parameter matching
        if (!params.isEmpty()) {
            val = (int) Float.parseFloat(params.get(0));
        }
        // fields may be empty, but never null
        return val;
    }

    /**
     * Parses out the {@code parameterName} field from the query parameter in the {@code routingContext}. The first
     * value will to selected and parsed as a string.
     *
     * @param routingContext the current request context
     * @param parameterName the name of the url query parameter to parse as a boolean value
     * @return true if present and truthy. false otherwise
     */
    protected String parseStringFromRoutingContext(RoutingContext routingContext, String parameterName) {
        List<String> params = routingContext.queryParam(parameterName);
        // if there was a query parameter matching
        return params.isEmpty() ? null : params.get(0);
    }


}