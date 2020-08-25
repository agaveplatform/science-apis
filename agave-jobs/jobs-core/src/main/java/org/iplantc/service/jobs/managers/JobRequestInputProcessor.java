/**
 *
 */
package org.iplantc.service.jobs.managers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareInput;
import org.iplantc.service.apps.util.ServiceUtils;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.io.permissions.PermissionManager;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.exceptions.JobProcessingException;
import org.iplantc.service.transfer.RemoteDataClientFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * @author dooley
 *
 */
public class JobRequestInputProcessor {

    protected ObjectNode jobInputs;
    private String internalUsername;
    private String username;
    private Software software;
    private ObjectMapper mapper = new ObjectMapper();

    public JobRequestInputProcessor(String username, String internalUsername, Software software) {
        setUsername(username);
        setInternalUsername(internalUsername);
        setSoftware(software);
        setMapper(new ObjectMapper());
    }

    /**
     * Validates the {@link SoftwareInput} values passed into a job request.
     *
     * @param jobRequestMap the map of software inputs given in the job request
     * @throws JobProcessingException if one or more inputs is invalid
     */
    public void process(Map<String, Object> jobRequestMap) throws JobProcessingException {
        setJobInputs(getMapper().createObjectNode());

        for (SoftwareInput softwareInput : getSoftware().getInputs()) {

            ArrayNode jobRequestInputValue = null;

            try {
                // add hidden inputs into the input array so we have a full
                // record of all inputs for this job in the history.
                if (!softwareInput.isVisible()) {

                    // hidden fields are required
                    if (jobRequestMap.containsKey(softwareInput.getKey())) {
                        throw new JobProcessingException(400,
                                "Invalid value for " + softwareInput.getKey()
                                        + ". " + softwareInput.getKey()
                                        + " is a fixed value that "
                                        + "cannot be set manually. ");
                    }
                    // validate the default value to ensure it's still valid
                    else {
                        jobRequestInputValue = doProcessSoftwareInputValue(softwareInput, softwareInput.getDefaultValueAsJsonArray());
                    }
                }
                // missing fields could be completely optional
                else if (!jobRequestMap.containsKey(softwareInput.getKey())) {
                    // if it's requried, that's a problem
                    if (softwareInput.isRequired()) {
                        throw new JobProcessingException(400,
                                "No input specified for " + softwareInput.getKey());
                    }
                    // if not, carry on
                    else {
                        continue;
                    }
                }
                // the field is present and availble to receive input. validate it
                else {
                    jobRequestInputValue = doProcessSoftwareInputValue(softwareInput, jobRequestMap.get(softwareInput.getKey()));
                }

                // empty result set should exclude the input from being included in the processed job requet
                if (jobRequestInputValue.size() > 0) {
                    getJobInputs().set(softwareInput.getKey(), jobRequestInputValue);
                }
            } catch (PermissionException e) {
                throw new JobProcessingException(403, e.getMessage(), e);
            } catch (JobProcessingException e) {
                throw e;
            } catch (Exception e) {
                throw new JobProcessingException(400,
                        "Failed to parse input for " + softwareInput.getKey(), e);
            }
        }
    }

    /**
     * Processes a single value provided for a {@link SoftwareInput} field in a
     * job request
     * @param softwareInput the software input for which we are parsing the value
     * @param value a single input string value
     * @return the sanitized json array of input values
     * @throws PermissionException if the user does not have permission to access the input
     */
    protected ArrayNode doProcessSoftwareInputValue(SoftwareInput softwareInput, Object value)
            throws PermissionException, JobProcessingException {
        if (value != null) {
            if (value instanceof String) {
                return doProcessSoftwareInputValue(softwareInput, (String) value);
//            } else if (value instanceof String[]) {
//                return doProcessSoftwareInputValue(softwareInput, (String[]) value);
//            } else if (value instanceof ArrayNode) {
//                return doProcessSoftwareInputValue(softwareInput, (ArrayNode) value);
            } else {
                throw new JobProcessingException(400,
                        "Unsupported type for input " + softwareInput.getKey());
            }
        } else {
            return doProcessSoftwareInputValue(softwareInput, getMapper().createArrayNode());
        }
    }

    /**
     * Processes a single value provided for a {@link SoftwareInput} field in a
     * job request.
     * @param softwareInput the software input for which we are parsing the value
     * @param value a single input string value
     * @return the sanitized json array of input values
     * @throws PermissionException if the user does not have permission to access the input
     */
    protected ArrayNode doProcessSoftwareInputValue(SoftwareInput softwareInput, String value)
            throws PermissionException, JobProcessingException {
        ArrayNode jobRequestInputValue = getMapper().createArrayNode();
        // blank values would be ignored when splitting, so we check for them first and preserve them
        // as a single empty array
        if (StringUtils.isNotBlank(value)) {
            String[] explodedInputs = null;

            if (value.contains(Settings.AGAVE_SERIALIZED_LIST_DELIMITER)) {
                // If the job is submitted via a form and the input is provided via a single, semicolon
                // separated list, then the actual parameter values cannot contain a semicolon, which is
                // undesirable given that the wrapper scripts are run as shell scripts and the parameter
                // value may legitimately need to include a semicolon.
                // We can't solve this in every situation, so we work around it when we can, which is when
                // the job request is made via JSON. In that case, each value of the parameter's JSON array
                // is parsed, replacing all instances of a semicolon with a random 128 character, base64
                // encoded delimiter generated when the server starts up. Here, we search for the delimiter
                // and, if found, split the concatenated input values on that value, thereby allowing for
                // the preservation of semicolons in the user's original parameter value.
                explodedInputs = StringUtils.splitByWholeSeparatorPreserveAllTokens(value, Settings.AGAVE_SERIALIZED_LIST_DELIMITER);

            } else {
                // Unfortunately, we cannot reliably do this for single value form requests, so in those
                // situations, we preserve the original value and fall back to the documented behavior of
                // splitting on semicolon.
                explodedInputs = StringUtils.splitByWholeSeparatorPreserveAllTokens(value, ";");
            }

            for (String singleInput : explodedInputs) {
                String singleStrippedInput = StringUtils.stripToNull(singleInput);
                if (singleStrippedInput != null) {
                    jobRequestInputValue.add(singleStrippedInput);
                }
            }
        }

        // now process the list of inputs for valid values
        return doProcessSoftwareInputValue(softwareInput, jobRequestInputValue);

    }

    /**
     * Processes a single array of uri strings provided for a {@link SoftwareInput} field in a
     * job request
     * @param softwareInput the software input for which we are parsing the value
     * @param explodedInputs an array of single input string values
     * @return the sanitized json array of input values
     * @throws PermissionException if the user does not have permission to access the input
     */
    protected ArrayNode doProcessSoftwareInputValue(SoftwareInput softwareInput, String[] explodedInputs)
            throws PermissionException, JobProcessingException {
        ArrayNode jobRequestInputValue = getMapper().createArrayNode();

        // skip empty inputs
        if (explodedInputs != null && explodedInputs.length > 0) {
            // interate over the array, sanitizing each input and pruning those that are invalid
            for (String singleInput : explodedInputs) {
                String singleStrippedInput = StringUtils.stripToNull(singleInput);
                if (singleStrippedInput != null) {
                    jobRequestInputValue.add(singleStrippedInput);
                }
            }
        }

        // now process the list of inputs for valid values
        return doProcessSoftwareInputValue(softwareInput, jobRequestInputValue);
    }

    /**
     * Processes a single json array provided for a {@link SoftwareInput} field in a
     * job request
     * @param softwareInput the software input for which we are parsing the value
     * @param valueArray an JSON {@link ArrayNode} of single input string values
     * @return the sanitized json array of input values
     * @throws PermissionException if the user does not have permission to access the input
     */
    protected ArrayNode doProcessSoftwareInputValue(SoftwareInput softwareInput, ArrayNode valueArray)
            throws PermissionException, JobProcessingException {
        ArrayNode jobRequestInputValue = getMapper().createArrayNode();

        ArrayNode filteredValueArray = getMapper().createArrayNode();
        if (valueArray == null) {
            filteredValueArray = getMapper().createArrayNode();
        } else {
            // strip out all null and empty values
            for (JsonNode inputJsonNode: valueArray) {
                if (!inputJsonNode.isNull() || StringUtils.isNotBlank(inputJsonNode.textValue())) {
                    filteredValueArray.add(inputJsonNode.textValue());
                }
            }
        }

        // check for too many values provided
        if (softwareInput.getMinCardinality() > filteredValueArray.size()) {
            throw new JobProcessingException(400,
                    softwareInput.getKey() + " requires at least "
                            + softwareInput.getMinCardinality()
                            + " values");
        }
        // check for too few values provided
        else if (softwareInput.getMaxCardinality() != -1
                && softwareInput.getMaxCardinality() < filteredValueArray.size()) {
            throw new JobProcessingException(400,
                    softwareInput.getKey() + " may have at most "
                            + softwareInput.getMaxCardinality()
                            + " values");
        }
        // process each value individually
        else {
            for (JsonNode json : filteredValueArray) {
                // only string values are supported
                if (!json.isValueNode()) {
                    throw new JobProcessingException(400,
                            "Invalid input value, "
                                    + json.toString()
                                    + ", for "
                                    + softwareInput.getKey()
                                    + ". Value must be a string value representing "
                                    + "a file or folder for which you have access.");
                }
                // ignore empty values. default values are not autofilled by agave.
                // users must supply them on their own. The long exception being
                // when a SoftwareInput is not visible. In that situation, the
                // default value is always injected into the job request and
                // already present here.
                else if (json.isNull()) {
                    continue;
                }

                String sValue = doProcessSingleValue(softwareInput, json.textValue());

                jobRequestInputValue.add(sValue);
            }

            return jobRequestInputValue;
        }
    }

    /**
     * Validate and sanitize a single string value provided for a {@link SoftwareInput} field in a
     * job request
     * @param softwareInput the software input for which we are parsing the value
     * @param sValue one value from the potenial array of values in the job request for a given {@code softwareInput}
     * @return the validated value
     * @throws PermissionException if the user does not have permission to access the input
     * @throws JobProcessingException if the input is invalid, either due to URL or system issues
     */
    protected String doProcessSingleValue(SoftwareInput softwareInput, String sValue)
            throws PermissionException, JobProcessingException {
        // if a SoftwareInput#validator is present, the input must
        // match the regex
        if (StringUtils.isNotEmpty(softwareInput.getValidator())
                && !ServiceUtils.doesValueMatchValidatorRegex(sValue, softwareInput.getValidator())) {
            throw new JobProcessingException(
                    400,
                    "Invalid input value, " + sValue + ", for " + softwareInput.getKey()
                            + ". Value must match the following expression: "
                            + softwareInput.getValidator());
        }
        // no validator, so verify the value is a suppported file or folder
        else {
            URI inputUri;
            try {
                // is the uri valid?
                inputUri = new URI(sValue);

                // is the schema supported?
                if (inputUri.getScheme() != null && !RemoteDataClientFactory.isSchemeSupported(inputUri)) {
                    throw new JobProcessingException(400,
                            "Invalid value for " + softwareInput.getKey()
                                    + ". URI with the " + inputUri.getScheme()
                                    + " scheme are not currently supported. "
                                    + "Please specify your input as a relative path, "
                                    + "an Agave resource URL, "
                                    + "or a URL with one of the following schemes: "
                                    + "http, https, sftp, or agave.");
                }
                // can the user read the file/folder?
                else if (!canUserReadUri(getUsername(), getInternalUsername(), inputUri)) {
                    throw new JobProcessingException(403,
                            "You do not have permission to access this the input file or directory "
                                    + "at " + sValue);
                }
            } catch (URISyntaxException e) {
                throw new JobProcessingException(403,
                        "Invalid value for " + softwareInput.getKey()
                                + "Please specify your input as a relative path, "
                                + "an Agave resource URL, "
                                + "or a URL with one of the following schemes: "
                                + "http, https, sftp, or agave.");
            }
        }

        return sValue;
    }

    /**
     * Checks the permission and existence of a remote URI. This wraps the
     * {@link PermissionManager#canUserReadUri(String, String, URI)} method
     * to allow mocking of this class. Note that read permissions are
     *
     * @param username the name of the job owner for whom permissions must be validated
     * @param internalUsername internal user for the reques
     * @param inputUri the URI for which permissions must be checked for {@code username}
     * @return true if the user can read the file item, false otherwise
     * @throws PermissionException if the user does not have permission to access the input
     */
    public boolean canUserReadUri(String username, String internalUsername, URI inputUri) throws PermissionException {
        return PermissionManager.canUserReadUri(getUsername(), getInternalUsername(), inputUri);
    }

    /**
     * @return the jobInputs
     */
    public ObjectNode getJobInputs() {
        return jobInputs;
    }

    /**
     * @param jobInputs the jobInputs to set
     */
    public void setJobInputs(ObjectNode jobInputs) {
        this.jobInputs = jobInputs;
    }

    /**
     * @return the internalUsername
     */
    public String getInternalUsername() {
        return internalUsername;
    }

    /**
     * @param internalUsername the internalUsername to set
     */
    public void setInternalUsername(String internalUsername) {
        this.internalUsername = internalUsername;
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return the software
     */
    public Software getSoftware() {
        return software;
    }

    /**
     * @param software the software to set
     */
    public void setSoftware(Software software) {
        this.software = software;
    }

    /**
     * @return the object mapper
     */
    protected ObjectMapper getMapper() {
        return this.mapper;
    }

    /**
     * @param mapper the mapper to set
     */
    public void setMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }
}
