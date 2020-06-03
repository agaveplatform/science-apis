package org.agaveplatform.service.transfers.resources;


import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.common.Settings;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper class for all responses from the api
 * 
 * @author dooley
 * @see AgaveResponseBuilder
 */
public class AgaveResponse {

	protected enum RequestStatus {success, error};

	/**
	 * Indicates whether to pretty print the response
	 */
	private boolean prettyPrint = false;
	/**
	 * Indicates whether to skip the standard wrapper and just print the result as the output
	 */
	private boolean naked = false;
	/**
	 * A list of fields to include in the result object(s). If populated, this provides a white list of fields
	 * used to construct a custom response object.
	 */
	private List<String> fields = new ArrayList<>();

	/**
	 * The descriptive response message providing additional details about the output.
	 */
	private String message;
	/**
	 * The status of the request. Currently success or error
	 */
	private RequestStatus status;
	/**
	 * The raw result of the query to be wrapped and returned as the response field in the wrapped {@link JsonObject}
	 * or the response itself when a {@link #naked} response is requested.
	 */
	private Object result;
	/**
	 * The version of the service sending this response.
	 */
	private String version;

	/**
	 * Default constructor for an empty response
	 * @param status status type of the response
	 */
	protected AgaveResponse(RequestStatus status) {
		setStatus(status);
	}
	/**
	 * Constructor for services building their JSON responses
	 * as {@link String} values. 
	 * 
	 * @param status the {@link RequestStatus} of the response
	 * @param message the descriptive message to return in the response
	 * @param result the json object returned from the request
	 */
	protected AgaveResponse(RequestStatus status, String message, JsonObject result)
	{	
		setStatus(status);
		setMessage(message);
		setResult(result);
		setVersion(Settings.SERVICE_VERSION);
	}

	/**
	 * Default constructor for responses generating {@link JsonObject} naked
	 * responses. This saves a couple serialization and deserialization steps
	 * along the say, which reduces load on the server, memory utilization, 
	 * and processing time. 
	 *
	 * @param status the {@link RequestStatus} of the response
	 * @param message the descriptive message to return in the response
	 * @param result the json array returned from the request
	 */
	protected AgaveResponse(RequestStatus status, String message, JsonArray result)
	{
		setStatus(status);
		setMessage(message);
		setResult(result);
		setVersion(Settings.SERVICE_VERSION);
	}

	public List<String> getFields() {
		return fields;
	}

	protected void setFields(List<String> fields) {
		this.fields = fields;
	}

	public String getMessage() {
		return message;
	}

	protected void setMessage(String message) {
		this.message = message;
	}

	public RequestStatus getStatus() {
		return status;
	}

	protected void setStatus(RequestStatus status) {
		this.status = status;
	}

	public Object getResult() {
		return result;
	}

	protected void setResult(Object result) {
		this.result = result;
	}

	public String getVersion() {
		return version;
	}

	protected void setVersion(String version) {
		this.version = version;
	}

	/**
	 * Should the response be pretty printed
	 * @return the prettyPrint
	 */
	public boolean isPrettyPrint() {
		return prettyPrint;
	}

	/**
	 * @param prettyPrint the prettyPrint to set
	 */
	public void setPrettyPrint(boolean prettyPrint) {
		this.prettyPrint = prettyPrint;
	}

	/**
	 * Returns true if the response should be returned to the client without the traditional response wrapper.
	 *
	 * @return true if {@code naked} url query parameter was set to a truthy value
	 */
	public boolean isNaked() {
		return naked;
	}

	public void setNaked(boolean naked) {
		this.naked = naked;
	}


	/**
	 * Formats the response both successful and unsuccessful calls into a 
	 * JSON wrapper object with attributes status, message, and result 
	 * where result contains the JSON output of the call.
	 *
	 * @return a json representation
	 */
	public String toString() {
		if (isNaked()) {
			if (getResult() == null) {
			   return null;
			} else if (getResult() instanceof JsonObject) {
				JsonObject json = filterResult((JsonObject)getResult());
				return isPrettyPrint() ? json.encodePrettily() : json.encode();
			} else if (getResult() instanceof JsonArray) {
				JsonArray json = filterResult((JsonArray)getResult());
				return isPrettyPrint() ? json.encodePrettily() : json.encode();
			} else {
				return getResult().toString();
			}
		} else {
			JsonObject json = new JsonObject()
				.put("status", status)
				.put("message", message)
				.put("version", getVersion());

			if (getResult() == null) {
				return null;
			} else if (getResult() instanceof JsonObject) {
				json.put("result", filterResult((JsonObject)getResult()));
				return isPrettyPrint() ? json.encodePrettily() : json.encode();
			} else if (getResult() instanceof JsonArray) {
				json.put("result", filterResult((JsonArray)getResult()));

			} else {
				json.put("result", getResult().toString());
			}

			return isPrettyPrint() ? json.encodePrettily() : json.encode();
		}
	}


	/**
	 * Applies user-supplied path filters to the response json 
	 * 
	 * @param json the result returned in this response object
	 * @return the properly filtered response containing only the provided fields, or all fields if none are specified
	 */
	protected JsonObject filterResult(JsonObject json) {
		// if the result is empty, just return it as is
		if (json == null || json.isEmpty() || getFields().isEmpty()) return json;

		JsonObject filteredChild = new JsonObject();
		for (String field: getFields()) {
			String[] tokens = StringUtils.split(field, ".");
			if (tokens.length == 2) {
				if (json.containsKey(tokens[0])) {
					if (json.getJsonObject(tokens[0]).containsKey(tokens[1])) {
						if (filteredChild.containsKey(tokens[0])) {
							filteredChild.getJsonObject(tokens[0]).put(tokens[1], filteredChild.getJsonObject(tokens[0]).getValue(tokens[1]));
						}
						else {
							filteredChild.put(tokens[0], new JsonObject()
									.put(tokens[1], json.getJsonObject(tokens[0]).getValue(tokens[1])));
						}
					}
					else {
						if (filteredChild.containsKey(tokens[0])) {
							(filteredChild.getJsonObject(tokens[0])).putNull(tokens[1]);
						}
						else {
							filteredChild.put(tokens[0], new JsonObject().putNull(tokens[1]));
						}
					}
				}
			}
			else if (tokens.length == 1) {
				if (json.containsKey(field)) {
					filteredChild.put(field, json.getValue(field));
				}
			}
		}

		return filteredChild;
	}

	/**
	 * Applies user-supplied list of field names to filter the response {@link JsonObject} contained in the in the
	 * {@code originalJsonArray}. If no fields were provided to the class, then the original value will be provided.
	 *
	 * @param originalJsonArray the result returned in this response object
	 * @return the properly filtered response containing only the provided fields, or all fields if none are specified
	 */
	protected JsonArray filterResult(JsonArray originalJsonArray) {
		// if the result is empty, just return it as is
		if (originalJsonArray == null || originalJsonArray.isEmpty() || getFields().isEmpty()) return originalJsonArray;

		JsonArray filteredJson = new JsonArray();
		originalJsonArray.forEach(child -> {
			JsonObject filteredChild = filterResult((JsonObject) child);
			if ( ! filteredChild.isEmpty() ) {
				filteredJson.add(filteredChild);
			}
		});

		return filteredJson;
	}
}
