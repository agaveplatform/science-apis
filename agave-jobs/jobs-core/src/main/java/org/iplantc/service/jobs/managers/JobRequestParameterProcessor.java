/**
 * 
 */
package org.iplantc.service.jobs.managers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.iplantc.service.apps.exceptions.SoftwareException;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareParameter;
import org.iplantc.service.apps.model.enumerations.SoftwareParameterType;
import org.iplantc.service.apps.util.ServiceUtils;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.exceptions.JobProcessingException;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @author dooley
 *
 */
public class JobRequestParameterProcessor {

	protected ObjectNode jobParameters;
	private ObjectMapper mapper;
	private Software software;
	
	public JobRequestParameterProcessor(Software software) {
		this.setSoftware(software);
		this.setMapper(new ObjectMapper());
	}
	/**
	 * Validates the {@link SoftwareParameter} values passed into a job request.
	 * @param jobRequestMap the map of parameter values submitted with the job request
	 * @throws JobProcessingException if an invalid value is provided
	 */
	public void process(Map<String, Object> jobRequestMap) 
	throws JobProcessingException {
		
		setJobParameters(getMapper().createObjectNode());

		for (SoftwareParameter softwareParameter : getSoftware().getParameters())
		{
			ArrayNode validatedJobParamValueArray = getMapper().createArrayNode();

			try
			{
				// add hidden parameters into the input array so we have a full record
				// of all parameters for this job in the history.
				if (!softwareParameter.isVisible())
				{
					if (jobRequestMap.containsKey(softwareParameter.getKey())) {
						throw new JobProcessingException(400,
								"Invalid parameter value for " + softwareParameter.getKey() +
								". " + softwareParameter.getKey() + " is a fixed value that "
								+ "cannot be set manually. ");
					} else if (softwareParameter.getType().equals(SoftwareParameterType.bool) ||
							softwareParameter.getType().equals(SoftwareParameterType.flag)) {
						if (softwareParameter.getDefaultValueAsJsonArray().size() > 0) {
							getJobParameters().set(softwareParameter.getKey(), softwareParameter.getDefaultValueAsJsonArray().get(0));
						} else {
							getJobParameters().put(softwareParameter.getKey(), false);
						}
					} else {
						getJobParameters().set(softwareParameter.getKey(), softwareParameter.getDefaultValueAsJsonArray());
					}
				}
				else if (!jobRequestMap.containsKey(softwareParameter.getKey()))
				{
					if (softwareParameter.isRequired())
					{
						throw new JobProcessingException(400,
								"No input parameter specified for " + softwareParameter.getKey());
					}
					else
					{
						continue;
					}
				}
				else
				{
					String[] explodedParameters = null;
				 	if (jobRequestMap.get(softwareParameter.getKey()) == null) {
				 		continue;
					} else if (jobRequestMap.get(softwareParameter.getKey()) instanceof String[]) {
						// arrays are kept in tact as is null values will be pruned later
				 		explodedParameters = (String[])jobRequestMap.get(softwareParameter.getKey());
					} else if (jobRequestMap.get(softwareParameter.getKey()) instanceof String) {
						String stringVal = (String)jobRequestMap.get(softwareParameter.getKey());
						if (StringUtils.isBlank(stringVal)) {
							// blank values would be ignored when splitting, so we check for them first and preserve them
							// as a single value array
							explodedParameters = new String[]{(String) jobRequestMap.get(softwareParameter.getKey())};
						} else if (stringVal.contains(Settings.AGAVE_SERIALIZED_LIST_DELIMITER)) {
							// If the job is submitted via a form and the parameter is provided via a single, semicolon
							// separated list, then the actual parameter values cannot contain a semicolon, which is
							// undesirable given that the wrapper scripts are run as shell scripts and the parameter
							// value may legitimately need to include a semicolon.
							// We can't solve this in every situation, so we work around it when we can, which is when
							// the job request is made via JSON. In that case, each value of the parameter's JSON array
							// is parsed, replacing all instances of a semicolon with a random 128 character, base64
							// encoded delimiter generated when the server starts up. Here, we search for the delimiter
							// and, if found, split the concatenated input values on that value, thereby allowing for
							// the preservation of semicolons in the user's original parameter value.
							explodedParameters = StringUtils.splitByWholeSeparatorPreserveAllTokens(stringVal, Settings.AGAVE_SERIALIZED_LIST_DELIMITER);
						} else {
							// Unfortunately, we cannot reliably do this for single value form requests, so in those
							// situations, we preserve the original value and fall back to the documented behavior of
							// splitting on semicolon.
						 	explodedParameters = StringUtils.splitByWholeSeparatorPreserveAllTokens(stringVal, ";");
						}
					} else {
						explodedParameters = new String[] { String.valueOf(jobRequestMap.get(softwareParameter.getKey())) };
					}

					if (softwareParameter.getMinCardinality() > explodedParameters.length)
					{
						throw new JobProcessingException(400,
								softwareParameter.getKey() + " requires at least " +
										softwareParameter.getMinCardinality() + " values");
					}
					else if (softwareParameter.getMaxCardinality() != -1 &&
						softwareParameter.getMaxCardinality() < explodedParameters.length)
					{
						throw new JobProcessingException(400,
								softwareParameter.getKey() + " may have at most " +
								softwareParameter.getMaxCardinality() + " values");
					}
					else if (softwareParameter.getType().equals(SoftwareParameterType.enumeration))
					{
						List<String> validParamValues = null;
						try {
							validParamValues = softwareParameter.getEnumeratedValuesAsList();
						} catch (SoftwareException e) {
							throw new JobProcessingException(400,
									"Unable to validate parameter value for " + softwareParameter.getKey() +
									" against the enumerated values defined for this parameter.", e);
						}

						if (validParamValues.isEmpty())
						{
							throw new JobProcessingException(400,
									"Invalid parameter value for " + softwareParameter.getKey() +
									". Value must be one of: " + ServiceUtils.explode(",  ", validParamValues));
						}
						else if (explodedParameters.length == 0) {
							continue;
						}
						else
						{
							for (String jobParam: explodedParameters)
							{
								// skip null values. Since enums are strings, they can legitimately be blank or
								// padded and still be valid values.
								if (jobParam == null) {
									continue;
								}

								if (validParamValues.contains(jobParam))
								{
									if (explodedParameters.length == 1) {
										getJobParameters().put(softwareParameter.getKey(), jobParam);
									} else {
										validatedJobParamValueArray.add(jobParam);
									}
								}
								else
								{
									throw new JobProcessingException(400,
											"Invalid parameter value, '" + jobParam + "', for " + softwareParameter.getKey() +
											". Value must be one of: " + ServiceUtils.explode(",  ", validParamValues));
								}
							}

							if (validatedJobParamValueArray.size() > 1) {
								getJobParameters().set(softwareParameter.getKey(), validatedJobParamValueArray);
							}
						}
					}
					else if (softwareParameter.getType().equals(SoftwareParameterType.bool) ||
							softwareParameter.getType().equals(SoftwareParameterType.flag))
					{
						if (explodedParameters.length > 1)
						{
							throw new JobProcessingException(400,
									"Invalid parameter value for " + softwareParameter.getKey() +
									". Boolean and flag parameters do not support multiple values.");
						}
						else if (explodedParameters.length == 0) {
							// ignore empty arrays. This represents no value passed in by the user, so we skip adding
							// this to the resulting job parameter object and carry on.
							continue;
						}
						else
						{
							// trim the param since whitespace will break the truthy check
							String paramValue = StringUtils.stripToEmpty(explodedParameters[0]);
							if (StringUtils.isBlank(paramValue)) {
								// ignore empty values for flag and boolean values because they only accept truthy
								// values. Empty strings and null values get filtered to blank in the previous stanza,
								// and represent no value passed in by the user, so we skip adding this to the resulting
								// job parameter object and carry on.
								continue;
							}
							else if (paramValue.toString().equalsIgnoreCase("true")
									|| paramValue.toString().equals("1")
									|| paramValue.toString().equalsIgnoreCase("on"))
							{
								// case insensitive values of true, on, and 1 are truthy values.
								getJobParameters().put(softwareParameter.getKey(), true);
							}
							else if (paramValue.toString().equalsIgnoreCase("false")
									|| paramValue.toString().equals("0")
									|| paramValue.toString().equalsIgnoreCase("off"))
							{
								// case insensitive values of false, off, and 0 are falsy values.
								getJobParameters().put(softwareParameter.getKey(), false);
							}
							else
							{
								// anything else gets rejected
								throw new JobProcessingException(400,
										"Invalid parameter value for " + softwareParameter.getKey() +
											". Value " + paramValue + " must be a case-insensitive truthy value. 1, on, or true " +
											"evaluate to true. 0, off, and false evaluate to false.");
							}
						}
					}
					else if (softwareParameter.getType().equals(SoftwareParameterType.number))
					{
						if (explodedParameters.length == 0) {
							continue;
						}
						else
						{
							for (String jobParam: explodedParameters)
							{
								// trim the param since whitespace will break the numeric check
								jobParam = StringUtils.stripToEmpty(jobParam);
								// skip null and blank values
								if (StringUtils.isEmpty(jobParam)) {
									continue;
								}

								try
								{
									if (NumberUtils.isDigits(jobParam))
									{
										if (ServiceUtils.doesValueMatchValidatorRegex(jobParam, softwareParameter.getValidator())) {
											if (explodedParameters.length == 1) {
												getJobParameters().put(softwareParameter.getKey(), Long.valueOf(jobParam));
											} else {
												validatedJobParamValueArray.add(Long.valueOf(jobParam));
											}
										} else {
											throw new JobProcessingException(400,
													"Invalid parameter value for " + softwareParameter.getKey() +
													". Value '" + jobParam + "' must match the regular expression " +
													softwareParameter.getValidator());
										}

									}
									else if (NumberUtils.isNumber(jobParam))
									{
										if (ServiceUtils.doesValueMatchValidatorRegex(jobParam, softwareParameter.getValidator())) {
											if (explodedParameters.length == 1) {
												getJobParameters().put(softwareParameter.getKey(), new BigDecimal(jobParam).toPlainString());
											} else {
												validatedJobParamValueArray.add(new BigDecimal(jobParam).toPlainString());
											}
										} else {
											throw new JobProcessingException(400,
													"Invalid parameter value for " + softwareParameter.getKey() +
													". Value '" + jobParam + "' must match the regular expression " +
													softwareParameter.getValidator());
										}
									}
									else {
										throw new JobProcessingException(400,
												"Invalid parameter value for " + softwareParameter.getKey() +
														". Value '" + jobParam + "' must be a number.");
									}
								} catch (NumberFormatException e) {
									throw new JobProcessingException(400,
											"Invalid parameter value for " + softwareParameter.getKey() +
											". Value '" + jobParam + "' must be a number.");
								}
							}

							if (validatedJobParamValueArray.size() > 1) {
								getJobParameters().set(softwareParameter.getKey(), validatedJobParamValueArray);
							}
						}
					}
					else // string parameter
					{
						if (explodedParameters.length == 0) {
							continue;
						}
						else
						{
							for (String jobParam: explodedParameters) {
								if (jobParam == null) {
									continue;
								} else {
									if (ServiceUtils.doesValueMatchValidatorRegex(jobParam, softwareParameter.getValidator())) {
										validatedJobParamValueArray.add(jobParam);
									} else {
										throw new JobProcessingException(400,
												"Invalid parameter value for " + softwareParameter.getKey() +
												". Value '" + jobParam + "' must match the regular expression " +
												softwareParameter.getValidator());
									}
								}
							}

							if (validatedJobParamValueArray.size() == 1) {
								getJobParameters().put(softwareParameter.getKey(), validatedJobParamValueArray.iterator().next().asText());
							} else {
								getJobParameters().set(softwareParameter.getKey(), validatedJobParamValueArray);
							}
						}
					}
				}
			}
			catch (JobProcessingException e) {
				throw e;
			}
			catch (Exception e) {
				throw new JobProcessingException(500,
						"Failed to parse parameter "+ softwareParameter.getKey(), e);
			}
		}
	}

	/**
	 * @return the jobParameters
	 */
	public ObjectNode getJobParameters() {
		return jobParameters;
	}

	/**
	 * @param jobParameters the jobParameters to set
	 */
	public void setJobParameters(ObjectNode jobParameters) {
		this.jobParameters = jobParameters;
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
	 * @return the mapper
	 */
	public ObjectMapper getMapper() {
		return mapper;
	}
	/**
	 * @param mapper the mapper to set
	 */
	public void setMapper(ObjectMapper mapper) {
		this.mapper = mapper;
	}
}