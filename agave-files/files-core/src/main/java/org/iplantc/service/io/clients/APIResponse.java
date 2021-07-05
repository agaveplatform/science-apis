/**
 * 
 */
package org.iplantc.service.io.clients;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.io.exceptions.TaskException;

/**
 * Wrapper to hold responses from the API. Responses come in the form
 * <code>
 * {
 * 		"status": "success",
 * 		"message": "",
 * 		"result": {}
 * }
 * </code>
 * @author dooley
 *
 */
public class APIResponse 
{
	private static final ObjectMapper mapper = new ObjectMapper();
	public enum APIResponseStatus { SUCCESS, ERROR }
	private APIResponseStatus status;
	private JsonNode result;
	private String message;
	
	public APIResponse(String text) throws TaskException
	{	
		if (APIServiceUtils.isEmpty(text)) 
		{
			throw new TaskException("Empty response received.");
		} 
		else
		{
			try {
				JsonNode response = mapper.readTree(text);

				if (response.has("status") && !response.get("status").isNull()) {
					if ("success".equals(response.get("status").asText())) {
						this.status = APIResponseStatus.SUCCESS;
					} else {
						this.status = APIResponseStatus.ERROR;
					}
					this.message = response.get("message").asText();
					this.result = response.has("result") ? response.get("result") : response;
				} else {
					throw new TaskException("Unrecognized response from the server.\n" + text);
				}
			}
			catch (JsonProcessingException e) {
				throw new TaskException("Unable to parse response from the server.\n" + text, e);
			}
			catch (Exception e) {
				throw new TaskException("Unable to parse response from the server.\n" + text, e);
			}
		}
	}

	/**
	 * @return the status
	 */
	public APIResponseStatus getStatus()
	{
		return status;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(APIResponseStatus status)
	{
		this.status = status;
	}

	/**
	 * @return the result
	 */
	public JsonNode getResult()
	{
		return result;
	}

	/**
	 * @param result the result to set
	 */
	public void setResult(JsonNode result)
	{
		this.result = result;
	}

	/**
	 * @return the message
	 */
	public String getMessage()
	{
		return message;
	}

	/**
	 * @param message the message to set
	 */
	public void setMessage(String message)
	{
		this.message = message;
	}

	public boolean isSuccess()
	{
		return (status != null && APIResponseStatus.SUCCESS.equals(status));
	}
}
