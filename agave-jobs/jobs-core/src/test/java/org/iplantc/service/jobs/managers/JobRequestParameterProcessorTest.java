package org.iplantc.service.jobs.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareParameter;
import org.iplantc.service.apps.model.SoftwareParameterEnumeratedValue;
import org.iplantc.service.apps.model.enumerations.SoftwareParameterType;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.dao.AbstractDaoTest;
import org.iplantc.service.jobs.exceptions.JobProcessingException;
import org.iplantc.service.jobs.model.JSONTestDataUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class JobRequestParameterProcessorTest {

    private boolean pass = false;
    private boolean fail = true;
    private ObjectMapper mapper = new ObjectMapper();

   /**
     * Read in the sample software json from disk.
     * @return json representation of software
     */
    protected JSONObject getDefaultSoftwareJson() {
        JSONObject jsonSoftware = null;
        try {
            jsonSoftware = JSONTestDataUtil.getInstance().getTestDataObject(AbstractDaoTest.FORK_SOFTWARE_TEMPLATE_FILE);
            jsonSoftware.put("name", UUID.randomUUID().toString());

            return jsonSoftware;
        } catch (Exception e) {
            fail("Failed to read default software template file: " + AbstractDaoTest.FORK_SOFTWARE_TEMPLATE_FILE, e);
        }

        return jsonSoftware;
    }

    /**
     * Creates a {@link SoftwareParameter} with teh given constraints, validator, etc.
     * @param key the key of the parameter
     * @param type the type of the parameter
     * @param defaultValue the default value of the parameter
     * @param validator the validator to use on job requests with this parameter
     * @param required whether the parameter is required
     * @param visible whether the parameter should be visible
     * @return a software parameter with the given constraints
     */
    private SoftwareParameter createParameter(String key, SoftwareParameterType type, Object defaultValue, String validator, boolean required, boolean visible) throws JsonProcessingException, IOException
    {
        SoftwareParameter param = new SoftwareParameter();
        param.setKey(key);
        param.setType(type);
        param.setDefaultValue(defaultValue == null ? null : defaultValue.toString());
        param.setRequired(required);
        param.setVisible(visible);
        if (SoftwareParameterType.enumeration == type) {
            List<SoftwareParameterEnumeratedValue> enums = new ArrayList<SoftwareParameterEnumeratedValue>();
            if (defaultValue != null) {
                String val = null;
                if (defaultValue instanceof ArrayNode) {
                    val = ((ArrayNode) defaultValue).get(0).textValue();
                } else {
                    val = String.valueOf(defaultValue);
                }
                enums.add(new SoftwareParameterEnumeratedValue(val, val, param));
            }

            enums.add(new SoftwareParameterEnumeratedValue("BETA", "BETA", param));
            enums.add(new SoftwareParameterEnumeratedValue("GAMMA", "GAMMA", param));
            enums.add(new SoftwareParameterEnumeratedValue("DELTA", "DELTA", param));
            param.setEnumValues(enums);
        } else {
            param.setValidator(validator);
        }
        return param;
    }


    /**
     * Generatest test cases for each {@link SoftwareParameterType} to test that when {@link SoftwareParameter#isVisible()}
     * is false, a job request to set that parameter will result in an exception
     * @return test cases
     */
    @DataProvider
    public Object[][] processThrowsExceptionForHiddenParameterInRequestProvider() {
        List<Object[]> testData = new ArrayList<Object[]>();
        testData.add(new Object[]{ SoftwareParameterType.flag, Boolean.TRUE });

        testData.add(new Object[]{ SoftwareParameterType.bool, Boolean.TRUE });

        testData.add(new Object[]{ SoftwareParameterType.enumeration, "ALPHA" });

        testData.add(new Object[]{ SoftwareParameterType.number, 512L });

        testData.add(new Object[]{ SoftwareParameterType.string, UUID.randomUUID().toString() });

        return testData.toArray(new Object[][] {});
    }

    /**
     * Tests required hidden {@link SoftwareParameter} throws an exception when included in the job request
     */
    @Test(enabled=true, dataProvider = "processThrowsExceptionForHiddenParameterInRequestProvider", expectedExceptions = JobProcessingException.class)
    public void processRequiredHiddenParametersThrowExceptionWhenProvided(SoftwareParameterType type, Object defaultValue) throws JobProcessingException
    {
        String message = "Hidden parameters provided in the job request should throw an exception saying they cannot be set.";
        try {
            SoftwareParameter param = createParameter("hiddenparam", type, defaultValue, 	null, 		true, 		false);
            _genericProcess(List.of(param), param.getKey(), mapper.createObjectNode().putNull(param.getKey()));

            fail(message);
        } catch (IOException e) {
            fail("Error creating the software parameter to test", e);
        }
    }

    /**
     * Tests required visible {@link SoftwareParameter} throws an exception when no value provided in the job request
     */
    @Test(enabled=true, dataProvider = "processThrowsExceptionForHiddenParameterInRequestProvider")
    public void processRequiredVisibleParametersAllowEmptyValueAndExcludeFromJobProcessedParameters(SoftwareParameterType type, Object defaultValue)
    {
        String message = "Null parameters value in job request should be allowed and parameter excluded form processed job parameters.";
        try {
            SoftwareParameter param = createParameter("hiddenparam", type, defaultValue, null, true, true);
            ObjectNode jobParameters = _genericProcess(List.of(param), param.getKey(), mapper.createObjectNode().putNull(param.getKey()));
            assertFalse(jobParameters.has(param.getKey()), message);
        } catch(JobProcessingException e) {
            fail(message, e);
        } catch (IOException e) {
            fail("Error creating the software parameter to test", e);
        }
    }

    /**
     * Tests required visible {@link SoftwareParameter} throws an exception when not included in the job request
     */
    @Test(enabled=true, dataProvider = "processThrowsExceptionForHiddenParameterInRequestProvider", expectedExceptions = JobProcessingException.class)
    public void processRequiredVisibleParametersThrowExceptionWhenNotPresentInJobRequest(SoftwareParameterType type, Object defaultValue) throws JobProcessingException
    {
        String message = "Required visible parameters missing from the job request should result in a JobProcessingException.";
        try {
            SoftwareParameter param = createParameter("hiddenparam", type, defaultValue, 	null, 		true, 		true);
            _genericProcess(List.of(param), param.getKey(), null);

            fail(message);
        } catch (IOException e) {
            fail("Error creating the software parameter to test", e);
        }
    }

    /**
     * Tests required hidden {@link SoftwareParameter} throws an exception when included in the job request
     */
    @Test(enabled=true, dataProvider = "processThrowsExceptionForHiddenParameterInRequestProvider", expectedExceptions = JobProcessingException.class)
    public void processOptionalHiddenParametersThrowExceptionWhenProvided(SoftwareParameterType type, Object defaultValue) throws JobProcessingException
    {
        String message = "Hidden parameters provided in the job request should throw an exception saying they cannot be set.";
        try {
            SoftwareParameter param = createParameter("hiddenparam", type, defaultValue, 	null, 		false, 		false);
            _genericProcess(List.of(param), param.getKey(), mapper.createObjectNode().putNull(param.getKey()));

            fail(message);
        } catch (IOException e) {
            fail("Error creating the software parameter to test", e);
        }
    }


    /**
     * Generates test cases for each {@link SoftwareParameterType} to test that the default value for the parameter
     * is set by {@link JobRequestParameterProcessor#process(Map).}. Hidden parameters cannot have
     * null default values, so we do not need to test situations where the default value is null.
     *
     * @return test cases
     */
    @DataProvider
    public Object[][] defaultValueParameterProvider() {
        List<Object[]> testData = new ArrayList<Object[]>();
        testData.add(new Object[]{ SoftwareParameterType.flag, Boolean.TRUE });
        testData.add(new Object[]{ SoftwareParameterType.flag, Boolean.FALSE });

        testData.add(new Object[]{ SoftwareParameterType.bool, Boolean.TRUE });
        testData.add(new Object[]{ SoftwareParameterType.bool, Boolean.FALSE });

        testData.add(new Object[]{ SoftwareParameterType.enumeration, "ALPHA" });
        testData.add(new Object[]{ SoftwareParameterType.enumeration, "BETA" });


        testData.add(new Object[]{ SoftwareParameterType.number, 512L });
        testData.add(new Object[]{ SoftwareParameterType.number, 0 });
        testData.add(new Object[]{ SoftwareParameterType.number, -1 });

        testData.add(new Object[]{ SoftwareParameterType.string, "" });
        testData.add(new Object[]{ SoftwareParameterType.string, UUID.randomUUID().toString() });
        testData.add(new Object[]{ SoftwareParameterType.string, "null" });
        testData.add(new Object[]{ SoftwareParameterType.string, "   " });

        return testData.toArray(new Object[][] {});
    }

    /**
     * Tests required hidden {@link SoftwareParameter} throws an exception when included in the job request
     */
    @Test(enabled=true, dataProvider = "defaultValueParameterProvider")
    public void processSetsDefaultValueForRequiredHiddenParameter(SoftwareParameterType type, Object defaultValue)
    {
        String message = String.format("The hidden %s software parameter's default value of %s should be set during parameter parsing",
                type.name(), String.valueOf(defaultValue));
        try {
            SoftwareParameter param = createParameter("hiddenparam", type, defaultValue, 	null, 		true, 		false);
            ObjectNode jobParameters = _genericProcess(List.of(param), param.getKey(), null);
            // boolean and flag parameter types cannot have multiple values. it makes no sense, so we don't
            // get an array back for their default value, we just get boolean value
            if (type == SoftwareParameterType.bool || type == SoftwareParameterType.flag) {
                assertEquals(jobParameters.get(param.getKey()).asBoolean(), defaultValue, message);
            } else {
                assertEquals(jobParameters.get(param.getKey()).toString(), new JSONArray().put(String.valueOf(defaultValue)).toString(), message);
            }
        }
        catch (JobProcessingException e) {
            fail(message, e);
        }
        catch (IOException e) {
            fail("Error creating the software parameter to test", e);
        }
    }

    /**
     * Tests optional hidden {@link SoftwareParameter} uses the default parameter value.
     */
    @Test(enabled=true, dataProvider = "defaultValueParameterProvider")
    public void processSetsDefaultValueForOptionalHiddenParameter(SoftwareParameterType type, Object defaultValue)
    {
        String message = String.format("The hidden %s software parameter's default value of %s should be set during parameter parsing",
                type.name(), String.valueOf(defaultValue));
        try {
            SoftwareParameter param = createParameter("hiddenparam", type, defaultValue, 	null, 		false, 		false);
            ObjectNode jobParameters = _genericProcess(List.of(param), param.getKey(), null);

            // boolean and flag parameter types cannot have multiple values. it makes no sense, so we don't
            // get an array back for their default value, we just get boolean value
            if (type == SoftwareParameterType.bool || type == SoftwareParameterType.flag) {
                assertEquals(jobParameters.get(param.getKey()).asBoolean(), defaultValue, message);
            } else {
                assertEquals(jobParameters.get(param.getKey()).toString(), new JSONArray().put(String.valueOf(defaultValue)).toString(), message);
            }
        }
        catch (JobProcessingException e) {
            fail(message, e);
        }
        catch (IOException e) {
            fail("Error creating the software parameter to test", e);
        }
    }

    /**
     * Tests optional visible {@link SoftwareParameter} does not have its default value set when the parameter is not
     * present in the job request.
     */
    @Test(enabled=true, dataProvider = "defaultValueParameterProvider")
    public void processIgnoresDefaultValueForOptionalVisibleParameter(SoftwareParameterType type, Object defaultValue)
    {
        String message = String.format("The optional visible %s software parameter's should not be set when absent from the job request parameters",
                type.name());
        try {
            SoftwareParameter param = createParameter("hiddenparam", type, defaultValue, 	null, 		false, 		true);
            ObjectNode jobParameters = _genericProcess(List.of(param), param.getKey(), null);
            assertFalse(jobParameters.has(param.getKey()), message);
        }
        catch (JobProcessingException e) {
            fail(message, e);
        }
        catch (IOException e) {
            fail("Error creating the software parameter to test", e);
        }
    }

    /**
     * Generates test cases for each {@link SoftwareParameterType} to test that a non-null custom value for the
     * parameter is set by {@link JobRequestParameterProcessor#process(Map).}.
     *
     * @return test cases
     */
    @DataProvider
    public Object[][] customValueParameterProvider() {
        List<Object[]> testData = new ArrayList<Object[]>();
        String parameterName = "testparam";
        testData.add(new Object[]{ SoftwareParameterType.flag, Boolean.TRUE, mapper.createObjectNode().put(parameterName, Boolean.FALSE) });
        testData.add(new Object[]{ SoftwareParameterType.flag, Boolean.FALSE, mapper.createObjectNode().put(parameterName, Boolean.TRUE) });

        testData.add(new Object[]{ SoftwareParameterType.bool, Boolean.TRUE, mapper.createObjectNode().put(parameterName, Boolean.FALSE) });
        testData.add(new Object[]{ SoftwareParameterType.bool, Boolean.FALSE, mapper.createObjectNode().put(parameterName, Boolean.TRUE) });

        testData.add(new Object[]{ SoftwareParameterType.enumeration, "ALPHA", mapper.createObjectNode().put(parameterName, "ALPHA") });
        testData.add(new Object[]{ SoftwareParameterType.enumeration, "ALPHA", mapper.createObjectNode().put(parameterName, "BETA") });
        testData.add(new Object[]{ SoftwareParameterType.enumeration, "ALPHA", mapper.createObjectNode().put(parameterName, "GAMMA") });
        testData.add(new Object[]{ SoftwareParameterType.enumeration, "ALPHA", mapper.createObjectNode().put(parameterName, "DELTA") });


        testData.add(new Object[]{ SoftwareParameterType.number, 512L, mapper.createObjectNode().put(parameterName, -1) });
        testData.add(new Object[]{ SoftwareParameterType.number, 512L, mapper.createObjectNode().put(parameterName, 225L) });
        testData.add(new Object[]{ SoftwareParameterType.number, -1, mapper.createObjectNode().put(parameterName, 0L) });

        testData.add(new Object[]{ SoftwareParameterType.string, UUID.randomUUID().toString(), mapper.createObjectNode().put(parameterName, UUID.randomUUID().toString()) });
        testData.add(new Object[]{ SoftwareParameterType.string, UUID.randomUUID().toString(), mapper.createObjectNode().put(parameterName, "null") });
        testData.add(new Object[]{ SoftwareParameterType.string, UUID.randomUUID().toString(), mapper.createObjectNode().put(parameterName, "   ") });
        testData.add(new Object[]{ SoftwareParameterType.string, UUID.randomUUID().toString(), mapper.createObjectNode().put(parameterName, "") });

        return testData.toArray(new Object[][] {});
    }

    /**
     * Tests valid optional visible {@link SoftwareParameter} supplied in job request is accepted and used.
     */
    @Test(enabled=true, dataProvider = "customValueParameterProvider")
    public void processUsesValueForOptionalVisibleParameter(SoftwareParameterType type, Object defaultValue, ObjectNode customValue)
    {
        String message = String.format("The optional visible %s software parameter's value from the job request parameters should be set when valid and present.",
                type.name());
        try {
            SoftwareParameter param = createParameter("testparam", type, defaultValue, 	null, 		false, 		true);
            ObjectNode jobParameters = _genericProcess(List.of(param), param.getKey(), customValue);
            assertTrue(jobParameters.has(param.getKey()), message);
            assertEquals(jobParameters.get(param.getKey()), customValue.get(param.getKey()), message);
        }
        catch (JobProcessingException e) {
            fail(String.format("The optional visible %s software parameter's should not throw an exception", type.name()), e);
        }
        catch (IOException e) {
            fail("Error creating the software parameter to test", e);
        }
    }

    /**
     * Tests valid required visible {@link SoftwareParameter} supplied in job request is accepted and used.
     */
    @Test(enabled=true, dataProvider = "customValueParameterProvider")
    public void processUsesValueForRequiredVisibleParameter(SoftwareParameterType type, Object defaultValue, ObjectNode customValue)
    {
        String message = String.format("The required visible %s software parameter's value from the job request parameters should be set when valid and present.",
                type.name());
        try {
            SoftwareParameter param = createParameter("testparam", type, defaultValue, 	null, 		true, 		true);
            ObjectNode jobParameters = _genericProcess(List.of(param), param.getKey(), customValue);
            assertTrue(jobParameters.has(param.getKey()), message);
            assertEquals(jobParameters.get(param.getKey()), customValue.get(param.getKey()), message);
        }
        catch (JobProcessingException e) {
            fail(String.format("The optional visible %s software parameter's should not throw an exception", type.name()), e);
        }
        catch (IOException e) {
            fail("Error creating the software parameter to test", e);
        }
    }




    @DataProvider
    private Object[][] processMultipleStringJobParametersProvider() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        JSONObject softwareJson = getDefaultSoftwareJson();
        JSONObject parameter = softwareJson.getJSONArray("parameters").getJSONObject(0);
        String parameterKey = parameter.getString("id");
        String parameterDefaultValue = parameter.getJSONObject("value").getString("default");
        String parameterDefaultValue2 = parameterDefaultValue + "2";
        String parameterDefaultValue3 = parameterDefaultValue + "3";
        String parameterDefaultValue4 = parameterDefaultValue + "4";

//        ObjectNode emptyInputObject = mapper.createObjectNode();

        ObjectNode singleValueObject = mapper.createObjectNode().put(parameterKey, parameterDefaultValue);

        ObjectNode mulipleRedundantDelimitedValuesObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", parameterDefaultValue));

        ObjectNode singleValueArrayObject = mapper.createObjectNode();
        singleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue);

        ObjectNode multipleRedundantValueArrayObject = mapper.createObjectNode();
        multipleRedundantValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue);

        ObjectNode multipleDelimitedValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", parameterDefaultValue, parameterDefaultValue2));

        ObjectNode multipleValueArrayObject = mapper.createObjectNode();
        multipleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2);

        ObjectNode tripleValueArrayObject = mapper.createObjectNode();
        tripleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3);

        ObjectNode tripleValueArrayWithRedundantValueObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2);

        ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3);

        ObjectNode quadValueArrayWithTwoRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithTwoRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue2);

        ObjectNode quadValueArrayWithSameRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithSameRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue);

        ObjectNode quadValueArrayObject = mapper.createObjectNode();
        quadValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3).add(parameterDefaultValue4);

        boolean pass = false;
        boolean fail = true;
        // need to verify cardinality
        return new Object[][] {
//                { SoftwareParameterType.string, 0, 1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.string, 0, 1, singleValueObject, pass, "Single truthy value should pass with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.string, 0, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.string, 0, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.string, 0, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input value with redundant values should pass with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.string, 0, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.string, 0, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.string, 0, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.string, 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 0 and maxCard = 1" },

//                { SoftwareParameterType.string, 0, -1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.string, 0, -1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.string, 0, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.string, 0, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.string, 0, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited input values with redundant values should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.string, 0, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.string, 0, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.string, 0, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array with redundant values should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.string, 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 0 and maxCard = -1" },

//                { SoftwareParameterType.string, 1, 1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.string, 1, 1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.string, 1, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.string, 1, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.string, 1, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.string, 1, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.string, 1, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.string, 1, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.string, 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 1" },

//                { SoftwareParameterType.string, 1, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.string, 1, -1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.string, 1, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.string, 1, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.string, 1, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited parameters with redundant values should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.string, 1, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.string, 1, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.string, 1, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.string, 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 1 and maxCard = -1" },

//                { SoftwareParameterType.string, 1, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.string, 1, 2, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.string, 1, 2, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.string, 1, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.string, 1, 2, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited parameters with redundant values should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.string, 1, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.string, 1, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.string, 1, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.string, 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 2" },

//                { SoftwareParameterType.string, 2, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.string, 2, 2, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.string, 2, 2, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.string, 2, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.string, 2, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.string, 2, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.string, 2, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.string, 2, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.string, 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 2 and maxCard = 2" },

//                { SoftwareParameterType.string, 2, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.string, 2, -1, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.string, 2, -1, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.string, 2, -1, multipleDelimitedValueObject, pass, "Multiple delimited input pass should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.string, 2, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.string, 2, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.string, 2, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.string, 2, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },

                { SoftwareParameterType.string, 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.string, 3, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 3 and maxCard = -1" },
                { SoftwareParameterType.string, 4, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 4 and maxCard = -1" },

                { SoftwareParameterType.string, 2, -1, quadValueArrayWithTwoRedundantValueObject, pass, "Quad input value array with two redundant values should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.string, 3, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 3 and maxCard = -1" },
                { SoftwareParameterType.string, 4, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 4 and maxCard = -1" },

                { SoftwareParameterType.string, 0, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.string, 1, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.string, 2, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.string, 3, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 3 and maxCard = -1" },
                { SoftwareParameterType.string, 4, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 4 and maxCard = -1" },

                { SoftwareParameterType.string, 0, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.string, 1, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.string, 2, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.string, 3, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 3 and maxCard = -1" },
                { SoftwareParameterType.string, 4, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 4 and maxCard = -1" },
        };
    }

    /**
     * Test data for long numeric job parameter values
     * @return long numeric test cases
     * @throws Exception
     */
    @DataProvider
    private Object[][] processMultipleLongJobParametersProvider() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        JSONObject softwareJson = getDefaultSoftwareJson();
        JSONObject parameter = softwareJson.getJSONArray("parameters").getJSONObject(0);
        String parameterKey = parameter.getString("id");
        Long parameterDefaultValue = 2L;
        Long parameterDefaultValue2 = 3L;
        Long parameterDefaultValue3 = 4L;
        Long parameterDefaultValue4 = 5L;

        ObjectNode emptyInputObject = mapper.createObjectNode();

        ObjectNode singleValueObject = mapper.createObjectNode().put(parameterKey, parameterDefaultValue);

        ObjectNode mulipleRedundantDelimitedValuesObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", parameterDefaultValue));

        ObjectNode singleValueArrayObject = mapper.createObjectNode();
        singleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue);

        ObjectNode singleLeftPaddedValueArrayObject = mapper.createObjectNode();
        singleLeftPaddedValueArrayObject.putArray(parameterKey).add(String.format(" %1$s", parameterDefaultValue));

        ObjectNode singleRightPaddedValueArrayObject = mapper.createObjectNode();
        singleRightPaddedValueArrayObject.putArray(parameterKey).add(String.format("%1$s ", parameterDefaultValue));

        ObjectNode singleLeftRightPaddedValueArrayObject = mapper.createObjectNode();
        singleLeftRightPaddedValueArrayObject.putArray(parameterKey).add(String.format(" %1$s ", parameterDefaultValue));

        ObjectNode multipleRedundantValueArrayObject = mapper.createObjectNode();
        multipleRedundantValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue);

        ObjectNode multipleDelimitedValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", parameterDefaultValue, parameterDefaultValue2));

        ObjectNode multipleValueArrayObject = mapper.createObjectNode();
        multipleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2);

        ObjectNode tripleValueArrayObject = mapper.createObjectNode();
        tripleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3);

        ObjectNode tripleValueArrayWithRedundantValueObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2);

        ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3);

        ObjectNode quadValueArrayWithTwoRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithTwoRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue2);

        ObjectNode quadValueArrayWithSameRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithSameRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue);

        ObjectNode quadValueArrayObject = mapper.createObjectNode();
        quadValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3).add(parameterDefaultValue4);

        boolean pass = false;
        boolean fail = true;

        // need to verify cardinality
        return new Object[][] {
                { SoftwareParameterType.number, 0, 1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input value with redundant values should pass with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 0 and maxCard = 1" },

                { SoftwareParameterType.number, 0, -1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited input values with redundant values should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array with redundant values should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 0 and maxCard = -1" },

                { SoftwareParameterType.number, 0, -1, singleLeftPaddedValueArrayObject, pass, "Single parameter array value with left padding should pass with cardinality of zero or more"},
                { SoftwareParameterType.number, 0, -1, singleRightPaddedValueArrayObject, pass, "Single parameter array value with right padding should pass with cardinality of zero or more"},
                { SoftwareParameterType.number, 0, -1, singleLeftRightPaddedValueArrayObject, pass, "Single parameter array value with left and right padding should pass with cardinality of zero or more"},

                { SoftwareParameterType.number, 1, 1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 1" },

                { SoftwareParameterType.number, 1, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited parameters with redundant values should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 1 and maxCard = -1" },

                { SoftwareParameterType.number, 1, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited parameters with redundant values should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 2" },

                { SoftwareParameterType.number, 2, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 2 and maxCard = 2" },

                { SoftwareParameterType.number, 2, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, multipleDelimitedValueObject, pass, "Multiple delimited input pass should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },

                { SoftwareParameterType.number, 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 3, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 3 and maxCard = -1" },
                { SoftwareParameterType.number, 4, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 4 and maxCard = -1" },

                { SoftwareParameterType.number, 2, -1, quadValueArrayWithTwoRedundantValueObject, pass, "Quad input value array with two redundant values should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 3, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 3 and maxCard = -1" },
                { SoftwareParameterType.number, 4, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 4 and maxCard = -1" },

                { SoftwareParameterType.number, 0, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 3, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 3 and maxCard = -1" },
                { SoftwareParameterType.number, 4, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 4 and maxCard = -1" },

                { SoftwareParameterType.number, 0, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 3, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 3 and maxCard = -1" },
                { SoftwareParameterType.number, 4, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 4 and maxCard = -1" },
        };
    }

    /**
     * Test data for double numeric values
     * @return
     * @throws Exception
     */
    @DataProvider
    private Object[][] processMultipleDoubleJobParametersProvider() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        JSONObject softwareJson = getDefaultSoftwareJson();
        JSONObject parameter = softwareJson.getJSONArray("parameters").getJSONObject(0);
        String parameterKey = parameter.getString("id");
        Double parameterDefaultValue = 1.1;
        Double parameterDefaultValue2 = 2.2;
        Double parameterDefaultValue3 = 3.3;
        Double parameterDefaultValue4 = 4.4;

        ObjectNode emptyInputObject = mapper.createObjectNode();

        ObjectNode singleValueObject = mapper.createObjectNode().put(parameterKey, parameterDefaultValue);

        ObjectNode mulipleRedundantDelimitedValuesObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", parameterDefaultValue));

        ObjectNode singleValueArrayObject = mapper.createObjectNode();
        singleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue);

        ObjectNode multipleRedundantValueArrayObject = mapper.createObjectNode();
        multipleRedundantValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue);

        ObjectNode multipleDelimitedValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", parameterDefaultValue, parameterDefaultValue2));

        ObjectNode multipleValueArrayObject = mapper.createObjectNode();
        multipleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2);

        ObjectNode tripleValueArrayObject = mapper.createObjectNode();
        tripleValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3);

        ObjectNode tripleValueArrayWithRedundantValueObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2);

        ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3);

        ObjectNode quadValueArrayWithTwoRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithTwoRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue2);

        ObjectNode quadValueArrayWithSameRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithSameRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue);

        ObjectNode quadValueArrayObject = mapper.createObjectNode();
        quadValueArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).add(parameterDefaultValue3).add(parameterDefaultValue4);

        boolean pass = false;
        boolean fail = true;
        // need to verify cardinality
        return new Object[][] {
                { SoftwareParameterType.number, 0, 1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input value with redundant values should pass with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
                { SoftwareParameterType.number, 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 0 and maxCard = 1" },

                { SoftwareParameterType.number, 0, -1, emptyInputObject, pass, "Empty input value should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, singleValueObject, pass, "Single input value should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited input values with redundant values should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array with redundant values should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 0 and maxCard = -1" },

                { SoftwareParameterType.number, 1, 1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1" },
                { SoftwareParameterType.number, 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 1" },

                { SoftwareParameterType.number, 1, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited parameters with redundant values should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 1 and maxCard = -1" },

                { SoftwareParameterType.number, 1, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, singleValueObject, pass, "Single input value should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, singleValueArrayObject, pass, "Single input array should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited parameters with redundant values should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
                { SoftwareParameterType.number, 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 2" },

                { SoftwareParameterType.number, 2, 2, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, multipleDelimitedValueObject, pass, "Multiple delimited input values should pass with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
                { SoftwareParameterType.number, 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 2 and maxCard = 2" },

                { SoftwareParameterType.number, 2, -1, emptyInputObject, fail, "Empty input value should fail with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, singleValueObject, fail, "Single input value should fail with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, singleValueArrayObject, fail, "Single input array should fail with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, multipleDelimitedValueObject, pass, "Multiple delimited input pass should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },

                { SoftwareParameterType.number, 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 3, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 3 and maxCard = -1" },
                { SoftwareParameterType.number, 4, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 4 and maxCard = -1" },

                { SoftwareParameterType.number, 2, -1, quadValueArrayWithTwoRedundantValueObject, pass, "Quad input value array with two redundant values should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 3, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 3 and maxCard = -1" },
                { SoftwareParameterType.number, 4, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 4 and maxCard = -1" },

                { SoftwareParameterType.number, 0, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 3, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 3 and maxCard = -1" },
                { SoftwareParameterType.number, 4, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 4 and maxCard = -1" },

                { SoftwareParameterType.number, 0, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 0 and maxCard = -1" },
                { SoftwareParameterType.number, 1, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 1 and maxCard = -1" },
                { SoftwareParameterType.number, 2, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 2 and maxCard = -1" },
                { SoftwareParameterType.number, 3, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 3 and maxCard = -1" },
                { SoftwareParameterType.number, 4, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 4 and maxCard = -1" },
        };
    }

    /**
     * Test data for boolean numeric values
     * @return
     * @throws Exception
     */
    @DataProvider
    private Object[][] processMultipleBooleanJobParametersProvider() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JSONObject softwareJson = getDefaultSoftwareJson();
        JSONObject parameter = softwareJson.getJSONArray("parameters").getJSONObject(0);
        String parameterKey = parameter.getString("id");

        ObjectNode emptyInputObject = mapper.createObjectNode();

        boolean pass = false;
        boolean fail = true;
        ArrayList<Object[]> testCases = new ArrayList<>();

        // add all combination truthy and falsy values to check
        for (Object truthy : List.of(Boolean.TRUE, 1L, "true", "TRUE", "tRuE", "1", "on", "ON", "oN")) {
            for (Object falsy : List.of(Boolean.FALSE, 0L, "false", "FALSE", "fAlSe", "0", "off", "OFF", "oFf")) {

                ObjectNode multipleDelimitedValueObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey, String.format("%1$s;%1$s", truthy, falsy)).toString());

                ObjectNode multipleValueArrayObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                        new JSONArray().put(truthy).put(falsy)).toString());

                ObjectNode tripleValueArrayWithRedundantValueObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                        new JSONArray().put(truthy).put(truthy).put(falsy)).toString());

                ObjectNode quadValueArrayWithRedundantValueObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                        new JSONArray().put(truthy).put(truthy).put(truthy).put(falsy)).toString());

                ObjectNode quadValueArrayWithTwoRedundantValueObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                        new JSONArray().put(truthy).put(truthy).put(falsy).put(falsy)).toString());


                testCases.addAll(Arrays.asList(
                        new Object[]{SoftwareParameterType.bool, 0, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 0 and maxCard = 1"},
                        new Object[]{SoftwareParameterType.bool, 0, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 0 and maxCard = 1"},
                        new Object[]{SoftwareParameterType.bool, 0, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array with redundant values should fail with minCard = 0 and maxCard = 1"},
                        new Object[]{SoftwareParameterType.bool, 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 0 and maxCard = 1"},

                        new Object[]{SoftwareParameterType.bool, 0, -1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 0 and maxCard = -1"},
                        new Object[]{SoftwareParameterType.bool, 0, -1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 0 and maxCard = -1"},
                        new Object[]{SoftwareParameterType.bool, 0, -1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array with redundant values should fail with minCard = 0 and maxCard = -1"},
                        new Object[]{SoftwareParameterType.bool, 0, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 0 and maxCard = -1"},

                        new Object[]{SoftwareParameterType.bool, 1, 1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = 1"},
                        new Object[]{SoftwareParameterType.bool, 1, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = 1"},
                        new Object[]{SoftwareParameterType.bool, 1, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1"},
                        new Object[]{SoftwareParameterType.bool, 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 1"},

                        new Object[]{SoftwareParameterType.bool, 1, -1, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = -1"},
                        new Object[]{SoftwareParameterType.bool, 1, -1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = -1"},
                        new Object[]{SoftwareParameterType.bool, 1, -1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = -1"},
                        new Object[]{SoftwareParameterType.bool, 1, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = -1"},

                        new Object[]{SoftwareParameterType.bool, 1, 2, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = 2"},
                        new Object[]{SoftwareParameterType.bool, 1, 2, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = 2"},
                        new Object[]{SoftwareParameterType.bool, 1, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2"},
                        new Object[]{SoftwareParameterType.bool, 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 2"},

                        new Object[]{SoftwareParameterType.bool, 2, 2, multipleDelimitedValueObject, fail, "Multiple delimited input values should fail with minCard = 2 and maxCard = 2"},
                        new Object[]{SoftwareParameterType.bool, 2, 2, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 2 and maxCard = 2"},
                        new Object[]{SoftwareParameterType.bool, 2, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2"},
                        new Object[]{SoftwareParameterType.bool, 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 2 and maxCard = 2"},

                        new Object[]{SoftwareParameterType.bool, 2, -1, multipleDelimitedValueObject, fail, "Multiple delimited input pass should fail with minCard = 2 and maxCard = -1"},
                        new Object[]{SoftwareParameterType.bool, 2, -1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 2 and maxCard = -1"},
                        new Object[]{SoftwareParameterType.bool, 2, -1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = -1"},

                        new Object[]{SoftwareParameterType.bool, 2, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 2 and maxCard = -1"},
                        new Object[]{SoftwareParameterType.bool, 3, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 3 and maxCard = -1"},
                        new Object[]{SoftwareParameterType.bool, 4, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 4 and maxCard = -1"},

                        new Object[]{SoftwareParameterType.bool, 2, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 2 and maxCard = -1"},
                        new Object[]{SoftwareParameterType.bool, 3, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 3 and maxCard = -1"},
                        new Object[]{SoftwareParameterType.bool, 4, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 4 and maxCard = -1"})
                    );
            }

        }

        // add all truthy only values
        for (Object truthy : List.of(Boolean.TRUE,  1L, "true", "TRUE", "tRuE", "1", "on", "ON", "oN")) {

            ObjectNode singleValueObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey, truthy).toString());

            ObjectNode mulipleRedundantDelimitedValuesObject = (ObjectNode) mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", truthy));

            ObjectNode singleValueArrayObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                    new JSONArray().put(truthy)).toString());

            ObjectNode multipleRedundantValueArrayObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                    new JSONArray().put(truthy).put(truthy)).toString());

            ObjectNode quadValueArrayWithSameRedundantTruthyValueObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                    new JSONArray().put(truthy).put(truthy).put(truthy).put(truthy)).toString());

            testCases.addAll(Arrays.asList(
                    new Object[]{SoftwareParameterType.bool, 0, 1, singleValueObject, pass, "Single truthy value should pass with minCard = 0 and maxCard = 1"},
                    new Object[]{SoftwareParameterType.bool, 0, 1, singleValueArrayObject, pass, "Single truthy array should pass with minCard = 0 and maxCard = 1"},
                    new Object[]{SoftwareParameterType.bool, 0, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input value with redundant values should fail with minCard = 0 and maxCard = 1"},

                    new Object[]{SoftwareParameterType.bool, 0, -1, singleValueObject, pass, "Single truthy value should pass with minCard = 0 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 0, -1, singleValueArrayObject, pass, "Single truthy array should pass with minCard = 0 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 0, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 0 and maxCard = -1"},

                    new Object[]{SoftwareParameterType.bool, 1, 1, singleValueObject, pass, "Single truthy value should pass with minCard = 1 and maxCard = 1"},
                    new Object[]{SoftwareParameterType.bool, 1, 1, singleValueArrayObject, pass, "Single truthy array should pass with minCard = 1 and maxCard = 1"},
                    new Object[]{SoftwareParameterType.bool, 1, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 1 and maxCard = 1"},

                    new Object[]{SoftwareParameterType.bool, 1, -1, singleValueObject, pass, "Single truthy value should pass with minCard = 1 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 1, -1, singleValueArrayObject, pass, "Single truthy array should pass with minCard = 1 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 1, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 1 and maxCard = -1"},

                    new Object[]{SoftwareParameterType.bool, 1, 2, singleValueObject, pass, "Single truthy value should pass with minCard = 1 and maxCard = 2"},
                    new Object[]{SoftwareParameterType.bool, 1, 2, singleValueArrayObject, pass, "Single truthy array should pass with minCard = 1 and maxCard = 2"},
                    new Object[]{SoftwareParameterType.bool, 1, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 1 and maxCard = 2"},

                    new Object[]{SoftwareParameterType.bool, 2, 2, singleValueObject, fail, "Single truthy value should fail with minCard = 2 and maxCard = 2"},
                    new Object[]{SoftwareParameterType.bool, 2, 2, singleValueArrayObject, fail, "Single truthy array should fail with minCard = 2 and maxCard = 2"},
                    new Object[]{SoftwareParameterType.bool, 2, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 2 and maxCard = 2"},

                    new Object[]{SoftwareParameterType.bool, 2, -1, singleValueObject, fail, "Single truthy value should fail with minCard = 2 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 2, -1, singleValueArrayObject, fail, "Single truthy array should fail with minCard = 2 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 2, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 2 and maxCard = -1"},

                    new Object[]{SoftwareParameterType.bool, 0, -1, quadValueArrayWithSameRedundantTruthyValueObject, fail, "Quad input value array with same redundant truthy value should fail with minCard = 0 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 1, -1, quadValueArrayWithSameRedundantTruthyValueObject, fail, "Quad input value array with same redundant truthy value should fail with minCard = 1 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 2, -1, quadValueArrayWithSameRedundantTruthyValueObject, fail, "Quad input value array with same redundant truthy value should fail with minCard = 2 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 3, -1, quadValueArrayWithSameRedundantTruthyValueObject, fail, "Quad input value array with same redundant truthy value should fail with minCard = 3 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 4, -1, quadValueArrayWithSameRedundantTruthyValueObject, fail, "Quad input value array with same redundant truthy value should fail with minCard = 4 and maxCard = -1"})
                );
        }

        // add all falsy only values
        for (Object falsy : List.of(Boolean.FALSE, 0L, "false", "FALSE", "fAlSe", "0", "off", "OFF", "oFf")) {

            ObjectNode singleValueObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey, falsy).toString());

            ObjectNode singleLeftPaddedValueObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                    new JSONArray().put(" " + falsy)).toString());

            ObjectNode singleRightPaddedValueObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                    new JSONArray().put(falsy + " ")).toString());

            ObjectNode singleLeftRightPaddedValueObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                    new JSONArray().put(" " + falsy + " ")).toString());

            ObjectNode mulipleRedundantDelimitedValuesObject = (ObjectNode) mapper.createObjectNode().put(parameterKey, String.format("%1$s;%1$s", falsy));

            ObjectNode singleValueArrayObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                    new JSONArray().put(falsy)).toString());

            ObjectNode singleLeftPaddedValueArrayObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                    new JSONArray().put(" " + falsy)).toString());

            ObjectNode singleRightPaddedValueArrayObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                    new JSONArray().put(falsy + " ")).toString());

            ObjectNode singleLeftRightPaddedValueArrayObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                    new JSONArray().put(" " + falsy + " ")).toString());

            ObjectNode multipleRedundantValueArrayObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                    new JSONArray().put(falsy).put(falsy)).toString());


            ObjectNode quadValueArrayWithSameRedundantFalsyValueObject = (ObjectNode) mapper.readTree(new JSONObject().put(parameterKey,
                    new JSONArray().put(falsy).put(falsy).put(falsy).put(falsy)).toString());

            testCases.addAll(Arrays.asList(
                    new Object[]{SoftwareParameterType.bool, 0, 1, singleValueObject, pass, "Single falsy value should pass with minCard = 0 and maxCard = 1"},
                    new Object[]{SoftwareParameterType.bool, 0, -1, singleLeftPaddedValueObject, pass, "Single left padded falsy value should pass with minCard = 0 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 0, -1, singleRightPaddedValueObject, pass, "Single right padded falsy value should pass with minCard = 0 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 0, -1, singleLeftRightPaddedValueObject, pass, "Single left right padded falsy value should pass with minCard = 0 and maxCard = -1"},

                    new Object[]{SoftwareParameterType.bool, 0, 1, singleValueArrayObject, pass, "Single falsy array should pass with minCard = 0 and maxCard = 1"},
                    new Object[]{SoftwareParameterType.bool, 0, -1, singleLeftPaddedValueArrayObject, pass, "Single left padded falsy array value should pass with minCard = 0 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 0, -1, singleRightPaddedValueArrayObject, pass, "Single right padded falsy array value should pass with minCard = 0 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 0, -1, singleLeftRightPaddedValueArrayObject, pass, "Single left right padded falsy array value should pass with minCard = 0 and maxCard = -1"},

                    new Object[]{SoftwareParameterType.bool, 0, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input value with redundant values should fail with minCard = 0 and maxCard = 1"},

                    new Object[]{SoftwareParameterType.bool, 0, -1, singleValueObject, pass, "Single falsy value should pass with minCard = 0 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 0, -1, singleValueArrayObject, pass, "Single falsy array should pass with minCard = 0 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 0, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 0 and maxCard = -1"},


                    new Object[]{SoftwareParameterType.bool, 1, 1, singleValueObject, pass, "Single falsy value should pass with minCard = 1 and maxCard = 1"},
                    new Object[]{SoftwareParameterType.bool, 1, 1, singleValueArrayObject, pass, "Single falsy array should pass with minCard = 1 and maxCard = 1"},
                    new Object[]{SoftwareParameterType.bool, 1, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 1 and maxCard = 1"},

                    new Object[]{SoftwareParameterType.bool, 1, -1, singleValueObject, pass, "Single falsy value should pass with minCard = 1 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 1, -1, singleValueArrayObject, pass, "Single falsy array should pass with minCard = 1 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 1, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 1 and maxCard = -1"},

                    new Object[]{SoftwareParameterType.bool, 1, 2, singleValueObject, pass, "Single falsy value should pass with minCard = 1 and maxCard = 2"},
                    new Object[]{SoftwareParameterType.bool, 1, 2, singleValueArrayObject, pass, "Single falsy array should pass with minCard = 1 and maxCard = 2"},
                    new Object[]{SoftwareParameterType.bool, 1, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 1 and maxCard = 2"},

                    new Object[]{SoftwareParameterType.bool, 2, 2, singleValueObject, fail, "Single falsy value should fail with minCard = 2 and maxCard = 2"},
                    new Object[]{SoftwareParameterType.bool, 2, 2, singleValueArrayObject, fail, "Single falsy array should fail with minCard = 2 and maxCard = 2"},
                    new Object[]{SoftwareParameterType.bool, 2, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 2 and maxCard = 2"},

                    new Object[]{SoftwareParameterType.bool, 2, -1, singleValueObject, fail, "Single falsy value should fail with minCard = 2 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 2, -1, singleValueArrayObject, fail, "Single falsy array should fail with minCard = 2 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 2, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited parameters with redundant values should fail with minCard = 2 and maxCard = -1"},

                    new Object[]{SoftwareParameterType.bool, 0, -1, quadValueArrayWithSameRedundantFalsyValueObject, fail, "Quad input value array with same redundant falsy value should fail with minCard = 0 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 1, -1, quadValueArrayWithSameRedundantFalsyValueObject, fail, "Quad input value array with same redundant falsy value should fail with minCard = 1 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 2, -1, quadValueArrayWithSameRedundantFalsyValueObject, fail, "Quad input value array with same redundant falsy value should fail with minCard = 2 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 3, -1, quadValueArrayWithSameRedundantFalsyValueObject, fail, "Quad input value array with same redundant falsy value should fail with minCard = 3 and maxCard = -1"},
                    new Object[]{SoftwareParameterType.bool, 4, -1, quadValueArrayWithSameRedundantFalsyValueObject, fail, "Quad input value array with same redundant falsy value should fail with minCard = 4 and maxCard = -1"})
                );
        }

        return testCases.toArray(new Object[][]{});
    }

    /**
     * Private utility class to process multiple job parameters of all types
     * @param type the parameter type under test
     * @param minCardinality the min parameter cardinality
     * @param maxCardinality the max parameter cardinality
     * @param jobParameters the parameters to submit to the job processor
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    private ObjectNode _processMultipleJobParameters(SoftwareParameterType type, int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode result = null;
        // mock out the software object referenced in the JobRequestParameterProcessor
        Software software = mock(Software.class);
        try {
            JSONArray jsonParameters = getDefaultSoftwareJson().getJSONArray("parameters");
            SoftwareParameter testParameter = null;
            ArrayList<SoftwareParameter> softwareParameters = new ArrayList<>();
            for (int i = 0; i < jsonParameters.length(); i++) {
                SoftwareParameter p = SoftwareParameter.fromJSON(jsonParameters.getJSONObject(i));
                if (i == 0) {
                    p.setKey(jobParameters.fieldNames().next());
                    p.setValidator(null);
                    p.setMaxCardinality(maxCardinality);
                    p.setMinCardinality(minCardinality);
                    p.setType(type);
                    testParameter = p;
                }
                softwareParameters.add(testParameter);
            }
            //
            //        // mock out the JobRequestParameterProcessor class
            //        JobRequestParameterProcessor jobRequestParameterProcessor = mock(JobRequestParameterProcessor.class);
            //        when(jobRequestParameterProcessor.getMapper()).thenReturn(mapper);
            //        // mock the job parameters being loaded with teh parsed data as it's mocked and won't be present
            //        jobRequestParameterProcessor.jobParameters = mapper.createObjectNode();
            //        doNothing().when(jobRequestParameterProcessor).setJobParameters(any());
            //        when(jobRequestParameterProcessor.getJobParameters()).thenCallRealMethod();
            //
            //        // return the software mock when requested
            //        when(jobRequestParameterProcessor.getSoftware()).thenReturn(software);
            //        when(software.getParameters()).thenReturn(softwareParameters);

            result = _genericProcessMultiple(softwareParameters, testParameter.getKey(), jobParameters, shouldThrowException, message);
        } catch (JSONException e) {
            fail("Failed to parse software json for test parameters", e);
        }

        return result;

    }

    /**
     * Tests string job app parameter cardinality on jobs submitted as json
     *
     * @param minCardinality the min parameter cardinality
     * @param maxCardinality the max parameter cardinality
     * @param jobParameters the parameters to submit to the job processor
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(enabled=false, dataProvider = "processMultipleStringJobParametersProvider")
    public void processMultipleStringJobParameters(SoftwareParameterType type, int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message)
    {
        _processMultipleJobParameters(SoftwareParameterType.string, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
    }

    /**
     * Tests integer job app parameter cardinality on jobs submitted as json
     *
     * @param minCardinality the min parameter cardinality
     * @param maxCardinality the max parameter cardinality
     * @param jobParameters the parameters to submit to the job processor
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(enabled=false, dataProvider = "processMultipleLongJobParametersProvider")
    public void processMultipleLongJobParameters(SoftwareParameterType type, int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message)
    {
        _processMultipleJobParameters(SoftwareParameterType.number, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
    }

    /**
     * Tests decimal job app parameter cardinality on jobs submitted as json
     *
     * @param minCardinality the min parameter cardinality
     * @param maxCardinality the max parameter cardinality
     * @param jobParameters the parameters to submit to the job processor
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(enabled=false, dataProvider = "processMultipleDoubleJobParametersProvider")
    public void processMultipleDoubleJobParameters(SoftwareParameterType type, int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message)
    {
        _processMultipleJobParameters(SoftwareParameterType.number, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
    }

    /**
     * Tests boolean job app parameter cardinality on jobs submitted as json
     *
     * @param minCardinality the min parameter cardinality
     * @param maxCardinality the max parameter cardinality
     * @param jobParameters the parameters to submit to the job processor
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(enabled=true, dataProvider = "processMultipleBooleanJobParametersProvider")
    public void processMultipleBooleanJobParameters(SoftwareParameterType type, int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message)
    {
        _processMultipleJobParameters(SoftwareParameterType.bool, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
    }

    /**
     * Tests flag job app parameter cardinality on jobs submitted as json
     *
     * @param minCardinality the min parameter cardinality
     * @param maxCardinality the max parameter cardinality
     * @param jobParameters the parameters to submit to the job processor
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(enabled=false, dataProvider = "processMultipleBooleanJobParametersProvider")
    public void processMultipleFlagJobParameters(SoftwareParameterType type, int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message)
    {
        _processMultipleJobParameters(SoftwareParameterType.flag, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
    }


    /**
     * Test data for null string numeric values
     * @return the test data
     * @throws Exception
     */
    @DataProvider
    private Object[][] processMultipleNullStringJobParametersProvider() throws Exception
    {
        JSONObject softwareJson = getDefaultSoftwareJson();
        JSONObject parameter = softwareJson.getJSONArray("parameters").getJSONObject(0);
        String parameterKey = parameter.getString("id");
        String parameterDefaultValue = parameter.getJSONObject("value").getString("default");
        String parameterDefaultValue2 = parameterDefaultValue + "2";

        ObjectNode singleNullValueObject = mapper.createObjectNode().putNull(parameterKey);

        ObjectNode singleNullValueArrayObject = mapper.createObjectNode();
        singleNullValueArrayObject.putArray(parameterKey).addNull();

        ObjectNode multipleDelimitedNullValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;", parameterDefaultValue, parameterDefaultValue2));

        ObjectNode multipleValueArrayWithNullObject = mapper.createObjectNode();
        multipleValueArrayWithNullObject.putArray(parameterKey).add(parameterDefaultValue).addNull();

        ObjectNode tripleValueNullArrayObject = mapper.createObjectNode();
        tripleValueNullArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).addNull();

        ObjectNode tripleValueArrayWithRedundantValueAndOneNullObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantValueAndOneNullObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).addNull();

        ObjectNode tripleValueArrayWithValueAndRedundantNullObject = mapper.createObjectNode();
        tripleValueArrayWithValueAndRedundantNullObject.putArray(parameterKey).add(parameterDefaultValue).addNull().addNull();

        ObjectNode tripleValueArrayWithRedundantNullObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantNullObject.putArray(parameterKey).addNull().addNull().addNull();

        ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).addNull();

        boolean pass = false;
        boolean fail = true;
        // need to verify cardinality
        return new Object[][] {
                { 0, 1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 0 and maxCard = 1" },

                { 0, -1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple parameter value array with redundant value and null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple parameter value array with one value and redundant null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithRedundantNullObject, pass, "Triple parameter value array with redundant null values should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 0 and maxCard = -1" },

                { 1, 1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 1" },

                { 1, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple parameter value array with redundant value and null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple parameter value array with one value and redundant null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 1 and maxCard = -1" },

                { 1, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 2" },

                { 2, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 2 and maxCard = 2" },

                { 2, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter pass should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 2 and maxCard = -1" },
        };
    }

    /**
     * Test data for null long numeric values
     * @return test cases
     * @throws Exception 
     */
    @DataProvider
    private Object[][] processMultipleNullLongJobParametersProvider() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        JSONObject softwareJson = getDefaultSoftwareJson();
        JSONObject parameter = softwareJson.getJSONArray("parameters").getJSONObject(0);
        String parameterKey = parameter.getString("id");
        Long parameterDefaultValue = 2L;
        Long parameterDefaultValue2 = 3L;

        ObjectNode singleNullValueObject = mapper.createObjectNode().putNull(parameterKey);

        ObjectNode singleNullValueArrayObject = mapper.createObjectNode();
        singleNullValueArrayObject.putArray(parameterKey).addNull();

        ObjectNode multipleDelimitedNullValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;", parameterDefaultValue, parameterDefaultValue2));

        ObjectNode multipleValueArrayWithNullObject = mapper.createObjectNode();
        multipleValueArrayWithNullObject.putArray(parameterKey).add(parameterDefaultValue).addNull();

        ObjectNode multipleLeftPaddedValueArrayObject = mapper.createObjectNode();
        multipleLeftPaddedValueArrayObject.putArray(parameterKey).add(String.format(" %1$s", parameterDefaultValue)).addNull();

        ObjectNode multipleRightPaddedValueArrayObject = mapper.createObjectNode();
        multipleRightPaddedValueArrayObject.putArray(parameterKey).add(String.format("%1$s ", parameterDefaultValue)).addNull();

        ObjectNode multipleLeftRightPaddedValueArrayObject = mapper.createObjectNode();
        multipleLeftRightPaddedValueArrayObject.putArray(parameterKey).add(String.format(" %1$s ", parameterDefaultValue)).addNull();

        ObjectNode tripleValueNullArrayObject = mapper.createObjectNode();
        tripleValueNullArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).addNull();

        ObjectNode tripleValueArrayWithRedundantValueAndOneNullObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantValueAndOneNullObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).addNull();

        ObjectNode tripleValueArrayWithValueAndRedundantNullObject = mapper.createObjectNode();
        tripleValueArrayWithValueAndRedundantNullObject.putArray(parameterKey).add(parameterDefaultValue).addNull().addNull();

        ObjectNode tripleValueArrayWithRedundantNullObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantNullObject.putArray(parameterKey).addNull().addNull().addNull();

        ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).addNull();

        boolean pass = false;
        boolean fail = true;
        // need to verify cardinality
        return new Object[][] {
                { 0, 1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 0 and maxCard = 1" },

                { 0, -1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple parameter value array with redundant value and null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple parameter value array with one value and redundant null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithRedundantNullObject, pass, "Triple parameter value array with redundant null values should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 0 and maxCard = -1" },

                { 0, -1, multipleLeftPaddedValueArrayObject, pass, "Multiple parameter array value with left padding and a null value should pass with cardinality of zero or more"},
                { 0, -1, multipleRightPaddedValueArrayObject, pass, "Single parameter array value with right padding and a null value  should pass with cardinality of zero or more"},
                { 0, -1, multipleLeftRightPaddedValueArrayObject, pass, "Single parameter array value with left and right padding and a null value  should pass with cardinality of zero or more"},

                { 1, 1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 1" },

                { 1, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple parameter value array with redundant value and null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple parameter value array with one value and redundant null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 1 and maxCard = -1" },

                { 1, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 2" },

                { 2, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 2 and maxCard = 2" },

                { 2, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter pass should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 2 and maxCard = -1" },
        };
    }

    /**
     * Test data for null double numeric values
     * @return test data
     * @throws Exception
     */
    @DataProvider
    private Object[][] processMultipleNullDoubleJobParametersProvider() throws JSONException {
        ObjectMapper mapper = new ObjectMapper();
        JSONObject softwareJson = getDefaultSoftwareJson();
        JSONObject parameter = softwareJson.getJSONArray("parameters").getJSONObject(0);
        String parameterKey = parameter.getString("id");
        Double parameterDefaultValue = 2.2;
        Double parameterDefaultValue2 = 3.3;

        ObjectNode singleNullValueObject = mapper.createObjectNode().putNull(parameterKey);

        ObjectNode singleNullValueArrayObject = mapper.createObjectNode();
        singleNullValueArrayObject.putArray(parameterKey).addNull();

        ObjectNode multipleDelimitedNullValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;", parameterDefaultValue, parameterDefaultValue2));

        ObjectNode multipleValueArrayWithNullObject = mapper.createObjectNode();
        multipleValueArrayWithNullObject.putArray(parameterKey).add(parameterDefaultValue).addNull();

        ObjectNode multipleLeftPaddedValueArrayObject = mapper.createObjectNode();
        multipleLeftPaddedValueArrayObject.putArray(parameterKey).add(String.format(" %1$s", parameterDefaultValue)).addNull();

        ObjectNode multipleRightPaddedValueArrayObject = mapper.createObjectNode();
        multipleRightPaddedValueArrayObject.putArray(parameterKey).add(String.format("%1$s ", parameterDefaultValue)).addNull();

        ObjectNode multipleLeftRightPaddedValueArrayObject = mapper.createObjectNode();
        multipleLeftRightPaddedValueArrayObject.putArray(parameterKey).add(String.format(" %1$s ", parameterDefaultValue)).addNull();

        ObjectNode tripleValueNullArrayObject = mapper.createObjectNode();
        tripleValueNullArrayObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue2).addNull();

        ObjectNode tripleValueArrayWithRedundantValueAndOneNullObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantValueAndOneNullObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).addNull();

        ObjectNode tripleValueArrayWithValueAndRedundantNullObject = mapper.createObjectNode();
        tripleValueArrayWithValueAndRedundantNullObject.putArray(parameterKey).add(parameterDefaultValue).addNull().addNull();

        ObjectNode tripleValueArrayWithRedundantNullObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantNullObject.putArray(parameterKey).addNull().addNull().addNull();

        ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(parameterDefaultValue).add(parameterDefaultValue).add(parameterDefaultValue2).addNull();

        boolean pass = false;
        boolean fail = true;
        // need to verify cardinality
        return new Object[][] {
                { 0, 1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 0 and maxCard = 1" },

                { 0, -1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple parameter value array with redundant value and null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple parameter value array with one value and redundant null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithRedundantNullObject, pass, "Triple parameter value array with redundant null values should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 0 and maxCard = -1" },

                { 0, -1, multipleLeftPaddedValueArrayObject, pass, "Multiple parameter array value with left padding and a null value should pass with cardinality of zero or more"},
                { 0, -1, multipleRightPaddedValueArrayObject, pass, "Single parameter array value with right padding and a null value  should pass with cardinality of zero or more"},
                { 0, -1, multipleLeftRightPaddedValueArrayObject, pass, "Single parameter array value with left and right padding and a null value  should pass with cardinality of zero or more"},

                { 1, 1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 1" },

                { 1, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple parameter value array with redundant value and null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple parameter value array with one value and redundant null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 1 and maxCard = -1" },

                { 1, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, multipleDelimitedNullValueObject, pass, "Multiple delimited parameter values with null should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, multipleValueArrayWithNullObject, pass, "Multiple parameter value array with null value should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 2" },

                { 2, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 2 and maxCard = 2" },

                { 2, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter pass should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueNullArrayObject, pass, "Triple parameter value array with one null should pass with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad parameter value array with redundant values and null should pass with minCard = 2 and maxCard = -1" },
        };
    }

    /**
     * Test data for null boolean numeric values
     * @return test cases
     * @throws Exception
     */
    @DataProvider
    private Object[][] processMultipleNullBooleanJobParametersProvider() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        JSONObject softwareJson = getDefaultSoftwareJson();
        JSONObject parameter = softwareJson.getJSONArray("parameters").getJSONObject(0);
        String parameterKey = parameter.getString("id");

        ObjectNode singleNullValueObject = mapper.createObjectNode().putNull(parameterKey);

        ObjectNode singleNullValueArrayObject = mapper.createObjectNode();
        singleNullValueArrayObject.putArray(parameterKey).addNull();

        ObjectNode multipleDelimitedNullValueObject = mapper.createObjectNode().put(parameterKey, String.format("%1$s;", Boolean.TRUE));

        ObjectNode multipleValueArrayWithNullObject = mapper.createObjectNode();
        multipleValueArrayWithNullObject.putArray(parameterKey).add(Boolean.TRUE).addNull();

        ObjectNode tripleValueNullArrayObject = mapper.createObjectNode();
        tripleValueNullArrayObject.putArray(parameterKey).add(Boolean.TRUE).add(Boolean.FALSE).addNull();

        ObjectNode tripleValueArrayWithRedundantValueAndOneNullObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantValueAndOneNullObject.putArray(parameterKey).add(Boolean.TRUE).add(Boolean.TRUE).addNull();

        ObjectNode tripleValueArrayWithValueAndRedundantNullObject = mapper.createObjectNode();
        tripleValueArrayWithValueAndRedundantNullObject.putArray(parameterKey).add(Boolean.TRUE).addNull().addNull();

        ObjectNode tripleValueArrayWithRedundantNullObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantNullObject.putArray(parameterKey).addNull().addNull().addNull();

        ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithRedundantValueObject.putArray(parameterKey).add(Boolean.TRUE).add(Boolean.TRUE).add(Boolean.FALSE).addNull();

        boolean pass = false;
        boolean fail = true;
        // need to verify cardinality
        return new Object[][] {
                { 0, 1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 0 and maxCard = 1" },

                { 0, -1, singleNullValueObject, pass, "Null parameter value should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, singleNullValueArrayObject, pass, "Single null parameter array should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 0 and maxCard = -1" },
                { 0, -1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 0 and maxCard = -1" },
                { 0, -1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 0 and maxCard = -1" },

                { 1, 1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 1" },

                { 1, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = -1" },
                { 1, -1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = -1" },

                { 1, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 1 and maxCard = 2" },
                { 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 1 and maxCard = 2" },

                { 2, 2, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter values with null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 2 and maxCard = 2" },

                { 2, -1, singleNullValueObject, fail, "Null parameter value should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, singleNullValueArrayObject, fail, "Single null parameter array should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, multipleDelimitedNullValueObject, fail, "Multiple delimited parameter pass should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, multipleValueArrayWithNullObject, fail, "Multiple parameter value array with null value should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueNullArrayObject, fail, "Triple parameter value array with one null should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple parameter value array with redundant value and null should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple parameter value array with one value and redundant null should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithRedundantNullObject, fail, "Triple parameter value array with redundant null values should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, quadValueArrayWithRedundantValueObject, fail, "Quad parameter value array with redundant values and null should fail with minCard = 2 and maxCard = -1" },
        };
    }

    /**
     * Tests null parameter values on jobs submitted as json
     *
     * @param minCardinality the min parameter cardinality
     * @param maxCardinality the max parameter cardinality
     * @param jobParameters the parameters to submit to the job processor
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(enabled=true, dataProvider = "processMultipleNullStringJobParametersProvider")
    public void processMultipleNullStringJobParameters(int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message)
    {
        _processMultipleJobParameters(SoftwareParameterType.string, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);

    }

    /**
     * Tests integer job app parameter cardinality on jobs submitted as json
     *
     * @param minCardinality the min parameter cardinality
     * @param maxCardinality the max parameter cardinality
     * @param jobParameters the parameters to submit to the job processor
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(enabled=true, dataProvider = "processMultipleNullLongJobParametersProvider")
    public void processMultipleNullLongJobParameters(int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message)
    {
        _processMultipleJobParameters(SoftwareParameterType.number, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
    }

    /**
     * Tests decimal job app parameter cardinality on jobs submitted as json
     *
     * @param minCardinality the min parameter cardinality
     * @param maxCardinality the max parameter cardinality
     * @param jobParameters the parameters to submit to the job processor
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(enabled=true, dataProvider = "processMultipleNullDoubleJobParametersProvider")
    public void processMultipleNullDoubleJobParameters(int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message)
    {
        _processMultipleJobParameters(SoftwareParameterType.number, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
    }

    /**
     * Tests boolean job app parameter cardinality on jobs submitted as json
     *
     * @param minCardinality the min parameter cardinality
     * @param maxCardinality the max parameter cardinality
     * @param jobParameters the parameters to submit to the job processor
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(enabled=true, dataProvider = "processMultipleNullBooleanJobParametersProvider")
    public void processMultipleNullBooleanJobParameters(int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message)
    {
        ObjectNode result = _processMultipleJobParameters(SoftwareParameterType.bool, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
        assertEquals(result.size(), 0, "Null value to boolean software parameter should result in the parameter being excluded from the processed job request parameters.");
    }

    /**
     * Tests flag job app parameter cardinality on jobs submitted as json
     *
     * @param minCardinality the min parameter cardinality
     * @param maxCardinality the max parameter cardinality
     * @param jobParameters the parameters to submit to the job processor
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(enabled=true, dataProvider = "processMultipleNullBooleanJobParametersProvider")
    public void processMultipleNullFlagJobParameters(int minCardinality, int maxCardinality, ObjectNode jobParameters, boolean shouldThrowException, String message)
    {
        ObjectNode result =_processMultipleJobParameters(SoftwareParameterType.flag, minCardinality, maxCardinality, jobParameters, shouldThrowException, message);
        assertEquals(result.size(), 0, "Null value to boolean software parameter should result in the parameter being excluded from the processed job request parameters.");
    }


    /**
     * Shorthand method to generate a test {@link SoftwareParameter} using the given fields. Defaults to min
     * cardinality 0, max cardinality 1, null default, not required, visible true.
     *
     * @param key parameter key
     * @param type parameter type
     * @return populated software parameter with the given values
     */
    private SoftwareParameter createTestSoftwareParameter(String key, SoftwareParameterType type) {
       return createTestSoftwareParameter(key, type, true, true, 0, -1, null);
    }

    /**
     * Creates a test {@link SoftwareParameter} using the given fields
     * @param key the key of the parameter under test
     * @param type the software parameter type
     * @param required
     * @param visible
     * @param minCardinality
     * @param maxCardinality
     * @param defaultValue
     * @return populated software parameter with the given values
     */
    private SoftwareParameter createTestSoftwareParameter(String key, SoftwareParameterType type, boolean required, boolean visible, int minCardinality, int maxCardinality, Object defaultValue) {
        SoftwareParameter param = new SoftwareParameter();
        param.setKey(key);
        param.setType(type);
        param.setRequired(required);
        param.setVisible(visible);
        param.setDefaultValue(String.valueOf(defaultValue));
        param.setMaxCardinality(maxCardinality);
        param.setMinCardinality(minCardinality);
        return param;
    }

    /**
     * Generic method to setup and run the {@link JobRequestParameterProcessor#process(Map)} and test for the expected
     * {@link JobProcessingException} to be thrown.
     * @param softwareParameters the software parameters to add to the mock {@link Software} object for use when processing the response
     * @param key the key of the parameter under test
     * @param json the json object representing the job request's "parameters" value. The object should contain the test data as key and value of the object.
     * @throws JobProcessingException the processing exception thrown during the test
     * @return the processed job parameters
     */
    private ObjectNode _genericProcess(List<SoftwareParameter> softwareParameters, String key, ObjectNode json) throws JobProcessingException
    {
        // mock out the class under test so we control the scope of things under test
        JobRequestParameterProcessor jobRequestParameterProcessor = mock(JobRequestParameterProcessor.class);

        // mock the software object and have it return our test parameters
        Software software = mock(Software.class);
        when(software.getParameters()).thenReturn(softwareParameters);

        // mock out the call to get the job's software. we return our mock with the parameters under test
        when(jobRequestParameterProcessor.getSoftware()).thenReturn(software);
        doCallRealMethod().when(jobRequestParameterProcessor).process(any());

        // mock out the mapper method so we have a valid instance in the mocked class
        ObjectMapper mapper = new ObjectMapper();
        when(jobRequestParameterProcessor.getMapper()).thenReturn(mapper);

        // mock out the job parameters so we can check them after the test
        jobRequestParameterProcessor.jobParameters = mapper.createObjectNode();
        doNothing().when(jobRequestParameterProcessor).setJobParameters(any());
        when(jobRequestParameterProcessor.getJobParameters()).thenCallRealMethod();

        // convert the json request object into a map
        HashMap<String, Object> parameterMap = jsonObjectToMap(key, json);

        // run the test
        jobRequestParameterProcessor.process(parameterMap);

        // return whatever came back
        return jobRequestParameterProcessor.getJobParameters();
    }

    /**
     * Generic method to run the JobManager.processJob(JsonNode, String, String) method.
     * @param json the json object representing the job request
     * @param shouldThrowException true if processing should throw an exception
     * @param message the assertion message to be returned if the test fails
     * @return the processed job parameters
     * @deprecated
     */
    private ObjectNode _genericProcessMultiple(List<SoftwareParameter> softwareParameters, String key, ObjectNode json, boolean shouldThrowException, String message)
    {
        JobRequestParameterProcessor jobRequestParameterProcessor = mock(JobRequestParameterProcessor.class);

        try
        {
            Software software = mock(Software.class);
            when(software.getParameters()).thenReturn(softwareParameters);

            when(jobRequestParameterProcessor.getSoftware()).thenReturn(software);
            doCallRealMethod().when(jobRequestParameterProcessor).process(any());

            ObjectMapper mapper = new ObjectMapper();
            when(jobRequestParameterProcessor.getMapper()).thenReturn(mapper);

            jobRequestParameterProcessor.jobParameters = mapper.createObjectNode();
            doNothing().when(jobRequestParameterProcessor).setJobParameters(any());
            when(jobRequestParameterProcessor.getJobParameters()).thenCallRealMethod();

            HashMap<String, Object> parameterMap = jsonObjectToMap(key, json);

            jobRequestParameterProcessor.process(parameterMap);

        }
        catch (JobProcessingException e) {
            if (!shouldThrowException) {
                fail(message, e);
            }
        }

        return jobRequestParameterProcessor.getJobParameters();
    }

    /**
     * Converts a json object into a {@link Map} that can be processed for job request parameters
     * @param key the key for which the job parameter should be stored
     * @param jobParameters a {@link ObjectNode} containing the value(s) of the job parameter under test
     * @return a map of the job parameters converted to a map
     */
    private HashMap<String, Object> jsonObjectToMap(String key, ObjectNode jobParameters) {
        HashMap<String, Object> parameterMap = new HashMap<>();

        // if the parameters were null, we do nothing. we will pass an empty job parameter map
        if (jobParameters != null) {
            if (jobParameters.isNull() || jobParameters.get(key).isNull()) {
                parameterMap.put(key, null);
            } else if (jobParameters.get(key).isArray()) {
                StringBuilder sb = new StringBuilder();
                ((ArrayNode) jobParameters.get(key)).forEach(item -> {
                    if (sb.length() > 0) {
                        sb.append(Settings.AGAVE_SERIALIZED_LIST_DELIMITER);
                    }

                    if (!item.isNull() && !item.isMissingNode()) {
                        sb.append(item.asText());
                    }
                });
                parameterMap.put(key, sb.toString());
            } else if (jobParameters.get(key).isDouble()) {
                parameterMap.put(key, jobParameters.get(key).decimalValue().toPlainString());
            } else if (jobParameters.get(key).isNumber()) {
                parameterMap.put(key, jobParameters.get(key).longValue());
            } else if (jobParameters.get(key).isTextual()) {
                parameterMap.put(key, jobParameters.get(key).textValue());
            } else if (jobParameters.get(key).isBoolean()) {
                parameterMap.put(key, jobParameters.get(key).asBoolean() ? "true" : "false");
            }
        }
        return parameterMap;
    }

//    @DataProvider
//    protected Object[][] testProcessProvider() {
//        ObjectMapper mapper = new ObjectMapper();
//
//        ArrayList<Object[]> testData = new ArrayList<>();
//        testData.add(new Object[] { SoftwareParameterType.number, 0, 0 });
//        testData.add(new Object[] { SoftwareParameterType.number, -1, -1 });
//        testData.add(new Object[] { SoftwareParameterType.number, 1.5, 1.5 });
//        testData.add(new Object[] { SoftwareParameterType.number, "0;1;2", mapper.createArrayNode().add(0).add(1).add(2) });
//        return testData.toArray(new Object[][]{});
//    }
//
//    @Test(enabled=false, dataProvider = "testProcessProvider")
//    public void testProcess(SoftwareParameterType parameterType, Object value, Object expectedValue) throws JobProcessingException {
//        String paramKey = "paramName";
//
//        SoftwareParameter testParam = createTestSoftwareParameter(paramKey, parameterType);
//        Software software = mock(Software.class);
//        when(software.getParameters()).thenReturn(List.of(testParam));
//
//        JobRequestParameterProcessor jobRequestParameterProcessor = mock(JobRequestParameterProcessor.class);
//        when(jobRequestParameterProcessor.getSoftware()).thenReturn(software);
//        doCallRealMethod().when(jobRequestParameterProcessor).process(any());
//
//        ObjectMapper mapper = new ObjectMapper();
//        when(jobRequestParameterProcessor.getMapper()).thenReturn(mapper);
//
//        jobRequestParameterProcessor.jobParameters = mapper.createObjectNode();
//        doNothing().when(jobRequestParameterProcessor).setJobParameters(any());
//        when(jobRequestParameterProcessor.getJobParameters()).thenCallRealMethod();
//
//        jobRequestParameterProcessor.process(new HashMap<String, Object>() {{
//            put(testParam.getKey(), value);
//        }});
//
//        ObjectNode result = jobRequestParameterProcessor.getJobParameters();
//
//        assertEquals(result.size(), 1, "Job request parameter for software parameter of type " +
//                parameterType.name() + " should be added to the job params.");
//        assertTrue(result.has(paramKey),
//                "Job request parameter key should be present in the job parameter object after processing.");
////        assertEquals(result.get(paramKey).getNodeType(), JsonNodeType.NUMBER,
////                "Job request parameter value should match type of software parameter type.");
//        assertEquals(result.get(paramKey).toString(), String.valueOf(expectedValue),
//                "Job request parameter value did not match expected value.");
//
//    }
//

//    @DataProvider
//    public Object[][] processJsonJobParametersProvider() throws JSONException, IOException
//    {
//        JSONObject softwareJson = getDefaultSoftwareJson();
//        JSONObject parameter = softwareJson.getJSONArray("parameters").getJSONObject(0);
//        String paramKey = parameter.getString("id");
//        String parameterDefaultValue = parameter.getJSONObject("value").getString("default");
//
//        // need to verify cardinality
//        Map<SoftwareParameterType, Object> defaultValues = new HashMap<SoftwareParameterType, Object>();
//        defaultValues.put(SoftwareParameterType.flag, Boolean.TRUE);
//        defaultValues.put(SoftwareParameterType.bool, Boolean.TRUE);
//        defaultValues.put(SoftwareParameterType.enumeration, new ObjectMapper().createArrayNode().add("ALPHA"));
//        defaultValues.put(SoftwareParameterType.number, 512L);
//        defaultValues.put(SoftwareParameterType.string, "somedefaultvalue");
//
//        Map<SoftwareParameterType, Object> validTestValues = new HashMap<SoftwareParameterType, Object>();
//        validTestValues.put(SoftwareParameterType.flag, Boolean.FALSE);
//        validTestValues.put(SoftwareParameterType.bool, Boolean.FALSE);
//        validTestValues.put(SoftwareParameterType.enumeration, new ObjectMapper().createArrayNode().add("BETA"));
//        validTestValues.put(SoftwareParameterType.number, 215L);
//        validTestValues.put(SoftwareParameterType.string, "anoteruservalue");
//
//        Map<SoftwareParameterType, List<Object>> invalidTestValues = new HashMap<SoftwareParameterType, List<Object>>();
//
//        for (SoftwareParameterType type: SoftwareParameterType.values()) {
//            List<Object> invalidValues = new ArrayList<Object>();
//            for (SoftwareParameterType validType: validTestValues.keySet()) {
//                if (type.equals(validType) ||
//                        (type.equals(SoftwareParameterType.bool) && validType.equals(SoftwareParameterType.flag)) ||
//                        (type.equals(SoftwareParameterType.flag) && validType.equals(SoftwareParameterType.bool)))
//                    continue;
//                else
//                    invalidValues.add(validTestValues.get(validType));
//            }
//            invalidTestValues.put(type, invalidValues);
//        }
//
//        List<Object[]> testData = new ArrayList<Object[]>();
//        for (SoftwareParameterType type: SoftwareParameterType.values())
//        {
//            if (type.equals(SoftwareParameterType.flag) || type.equals(SoftwareParameterType.bool))
//            {
//                //	(name    type		is_default					validator	        is_required	            is_visible),    test value                                                                  expected value                                                         expected result,     failed test message
//////                testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), 	null, 		false, 		true),  mapper.createObjectNode().put(paramKey, (Boolean)validTestValues.get(type)), mapper.createObjectNode().put(paramKey, (Boolean)validTestValues.get(type)),  pass, "User supplied value should be used for visible optional values of " + type.name() + " parameter" });
//////                testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), 	null, 		true, 		true),      mapper.createObjectNode().put(paramKey, (Boolean)validTestValues.get(type)), mapper.createObjectNode().put(paramKey, (Boolean)validTestValues.get(type)),  pass, "User supplied value should be used for visible required values of " + type.name() + " parameter" });
//////
//////                testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), 	null, 		false, 		true),  mapper.createObjectNode().put(paramKey, Boolean.TRUE),                       mapper.createObjectNode().put(paramKey, true),                             pass, "User supplied value TRUE should be used for visible optional values of " + type.name() + " parameter" });
//////                testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), 	null, 		true, 		true),      mapper.createObjectNode().put(paramKey, Boolean.TRUE),                       mapper.createObjectNode().put(paramKey, true),                             pass, "User supplied value TRUE should be used for visible required values of " + type.name() + " parameter" });
//////                testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), 	null, 		false, 		true),  mapper.createObjectNode().put(paramKey, Boolean.FALSE),                      mapper.createObjectNode().put(paramKey, false),                            pass, "User supplied value false should be used for visible optional values of " + type.name() + " parameter" });
//////                testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), 	null, 		true, 		true),      mapper.createObjectNode().put(paramKey, Boolean.FALSE),                      mapper.createObjectNode().put(paramKey, false),                            pass, "User supplied value false should be used for visible required values of " + type.name() + " parameter" });
////            }
////            else if (type.equals(SoftwareParameterType.enumeration))
////            {
////                ObjectNode expectedDefault = mapper.createObjectNode();
////                expectedDefault.putArray(paramKey).add("ALPHA");
////
////                ObjectNode expectedValid = mapper.createObjectNode();
////                expectedValid.putArray(paramKey).add("BETA");
////                //	name			type		default					validator	required	visible
//////                testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), 	null, 		false, 		true),  expectedValid,                                                              expectedValid,                                                                 pass, "User supplied value should be used for visible optional values of " + type.name() + " parameter" });
//////                testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), 	null, 		true, 		true),      expectedValid,                                                              expectedValid,                                                                 pass, "User supplied value should be used for visible required values of " + type.name() + " parameter" });
////
////                // validate all enumerated values will work
//////                SoftwareParameter p = createParameter(paramKey, type, defaultValues.get(type), null, 		true, 		true);
//////                for (String enumValue: p.getEnumeratedValuesAsList())
//////                {
//////                    expectedValid = mapper.createObjectNode();
//////                    expectedValid.putArray(paramKey).add(enumValue);
//////                    testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), null, 		true, 		true),      expectedValid,                                                              expectedValid,                                                                 pass, "Valid required enumerated value of " + enumValue + " is within the available values of " + p.getEnumeratedValues().toString() + " and should pass" });
//////                }
////            }
////            else
////            {
////                //	name			type		default					validator	required	visible
//////                testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), 	null, 		false, 		true),  mapper.createObjectNode().put(paramKey,validTestValues.get(type).toString()), mapper.createObjectNode().put(paramKey,validTestValues.get(type).toString()),pass, "User supplied value should be used for visible optional values of " + type.name() + " parameter" });
//////                testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), 	null, 		true, 		true),      mapper.createObjectNode().put(paramKey,validTestValues.get(type).toString()), mapper.createObjectNode().put(paramKey,validTestValues.get(type).toString()),pass, "User supplied value should be used for visible required values of " + type.name() + " parameter" });
////            }
////
//////            if (!type.equals(SoftwareParameterType.string))
//////            {
//////                for (Object invalidValue: invalidTestValues.get(type)) {
//////                    if (!type.equals(SoftwareParameterType.number)) {
//////                        testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), 	null, 		true, 	true),  mapper.createObjectNode().put(paramKey, invalidValue.toString()),           null,                                                                          fail, "Invalid required value " + invalidValue + " should fail for " + type.name() + " parameter" });
//////                        testData.add(new Object[] { createParameter(paramKey, type, defaultValues.get(type), 	null, 		false,	true),  mapper.createObjectNode().put(paramKey, invalidValue.toString()),           null,                                                                          fail, "Invalid optional value " + invalidValue + " should fail for " + type.name() + " parameter" });
//////                    }
//////                }
//            }
//        }
//        return testData.toArray(new Object[][] {});
//    }
//
//    /**
//     * Tests job app parameter validation on jobs submitted as json
//     *
//     * @param appParameter the app parameter to test
//     * @param jobParameters the job parameters under test
//     * @param expectedParameters the job parameters after processing
//     * @param shouldThrowException true if processing should throw an exception
//     * @param message the message to assert for a failed test
//     */
//    @Test(enabled=false, dataProvider = "processJsonJobParametersProvider")
//    public void processJsonJobParameters(SoftwareParameter appParameter, ObjectNode jobParameters, ObjectNode expectedParameters, boolean shouldThrowException, String message) throws JobProcessingException
//    {
//        ObjectMapper mapper = new ObjectMapper();
//
//        // mock out the software object referenced in the JobRequestParameterProcessor
//        Software software = mock(Software.class);
//        when(software.getParameters()).thenReturn(List.of(appParameter));
//
//        // mock out the JobRequestParameterProcessor class
//        JobRequestParameterProcessor jobRequestParameterProcessor = mock(JobRequestParameterProcessor.class);
//        when(jobRequestParameterProcessor.getMapper()).thenReturn(mapper);
//        // mock the job parameters being loaded with teh parsed data as it's mocked and won't be present
//        jobRequestParameterProcessor.jobParameters = mapper.createObjectNode();
//        doNothing().when(jobRequestParameterProcessor).setJobParameters(any());
//        when(jobRequestParameterProcessor.getJobParameters()).thenCallRealMethod();
//
//        // return the software mock when requested
//        when(jobRequestParameterProcessor.getSoftware()).thenReturn(software);
//        // call the actual method under test
//        doCallRealMethod().when(jobRequestParameterProcessor).process(any());
//
//        try {
//            // the map of job request parameters
//            HashMap<String, Object> parameterMap = jsonObjectToMap(appParameter.getKey(), jobParameters);
//
//            // now run the test
//            jobRequestParameterProcessor.process(parameterMap);
//
//            // get the job paramaeters parsed during processing
//            ObjectNode result = jobRequestParameterProcessor.getJobParameters();
//
//            if (expectedParameters != null) {
//                // validate our expectated resulting parameter size
//                assertEquals(result.size(), expectedParameters.size(),
//                        "Unexpected number of software parameters was found in the parsed parameters.");
//                // validate existence of test parameter
//                assertTrue(result.has(appParameter.getKey()), message);
//
//                JsonNode foundParameterJson = result.get(appParameter.getKey());
//                String foundParameter;
//                if (foundParameterJson.isArray()) {
//                    foundParameter = foundParameterJson.iterator().next().asText();
//                } else {
//                    foundParameter = foundParameterJson.asText();
//                }
//
//                String expectedParameter = expectedParameters.get(appParameter.getKey()).asText();
//                if (appParameter.getType().equals(SoftwareParameterType.number)) {
//                    foundParameter = String.valueOf(Double.parseDouble(foundParameter));
//                    expectedParameter = String.valueOf(Double.parseDouble(expectedParameter));
//                } else if (appParameter.getType().equals(SoftwareParameterType.enumeration)) {
//                    expectedParameter = ((ArrayNode) expectedParameters.get(appParameter.getKey())).get(0).asText();
//                } else if (appParameter.getType().equals(SoftwareParameterType.bool) ||
//                        appParameter.getType().equals(SoftwareParameterType.flag)) {
//                    // already serialized to boolean string
//                }
//
//                Assert.assertEquals(foundParameter, expectedParameter,
//                        "Unexpected value for field " + appParameter.getKey() + " found. Expected " +
//                                expectedParameter + " found " + foundParameter);
//
//            } else {
//                assertEquals(result.size(), 0, message);
//            }
//
//        } catch (JobProcessingException e) {
//            if (!shouldThrowException) {
//                fail(message, e);
//            }
//        } catch (Exception e) {
//            fail("Failed to process job", e);
//        }
//    }

}