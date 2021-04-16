package org.iplantc.service.jobs.managers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.apps.model.Software;
import org.iplantc.service.apps.model.SoftwareInput;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.io.permissions.PermissionManager;
import org.iplantc.service.jobs.Settings;
import org.iplantc.service.jobs.exceptions.JobProcessingException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@Test(groups={"integration"})
public class JobRequestInputProcessorIT {

    ObjectMapper mapper = new ObjectMapper();

    /**
     * Tests required hidden {@link SoftwareInput} throws an exception when included in the job request
     */
    @Test(expectedExceptions = JobProcessingException.class)
    public void processRequiredHiddenParametersThrowExceptionWhenProvided() throws JobProcessingException {
        String message = "Hidden parameters provided in the job request should throw an exception saying they cannot be set.";
        try {
            SoftwareInput input = createInput("hiddeninput", "http://example.com/", null, true, false);
            _genericProcess(List.of(input), mapper.createObjectNode().putNull(input.getKey()));

            fail(message);
        } catch (PermissionException e) {
            fail(message, e);
        }
    }

    /**
     * Tests optional hidden {@link SoftwareInput} throws an exception when included in the job request
     */
    @Test(expectedExceptions = JobProcessingException.class)
    public void processOptionalHiddenParametersThrowExceptionWhenProvided() throws JobProcessingException {
        String message = "Hidden parameters provided in the job request should throw an exception saying they cannot be set.";
        try {
            SoftwareInput input = createInput("hiddeninput", "http://example.com/", null, false, false);
            _genericProcess(List.of(input), mapper.createObjectNode().putNull(input.getKey()));

            fail(message);
        } catch (PermissionException e) {
            fail(message, e);
        }
    }

    @DataProvider
    protected Object[][] processRequiredVisibleParametersAllowEmptyValueAndExcludeFromJobProcessedParametersProvider() {
        String key = "testInput";
        ObjectNode nullInput = mapper.createObjectNode().putNull(key);
        ObjectNode emptyInput = mapper.createObjectNode().put(key, "");
        ObjectNode blankInput = mapper.createObjectNode().put(key, "  ");

        List<Object[]> testData = new ArrayList<>();
        for (ObjectNode inputValue: new ObjectNode[]{nullInput, emptyInput, blankInput}) {
            testData.add(new Object[]{  1, inputValue });
            testData.add(new Object[]{ -1, inputValue });
        }

        return testData.toArray(new Object[][]{});
    }

    /**
     * Tests required visible {@link SoftwareInput} allows null value when min cardinality is 0
     */
    @Test(dataProvider = "processRequiredVisibleParametersAllowEmptyValueAndExcludeFromJobProcessedParametersProvider")
    public void processRequiredVisibleParametersAllowEmptyValueAndExcludeFromJobProcessedParameters(int maxCardinality, ObjectNode testInput) {
        String message = "Null input value in job request should be allowed and input excluded from processed job inputs when minCard = 0 and maxCard = " + maxCardinality;
        try {
            SoftwareInput input = createInput("testInput", "http://example.com/", null, true, true);
            input.setMinCardinality(0);
            input.setMaxCardinality(maxCardinality);
            ObjectNode jobInputs = _genericProcess(List.of(input), testInput);
            assertFalse(jobInputs.has(input.getKey()), message);
        } catch (PermissionException|JobProcessingException e) {
            fail(message, e);
        }
    }

    @DataProvider
    protected Object[][] processRequiredVisibleParametersThrowsExceptionWhenBlankValueAndCardinalityNotZeroProvider() {

        String key = "testInput";
        ObjectNode nullInput = mapper.createObjectNode().putNull(key);
        ObjectNode emptyInput = mapper.createObjectNode().put(key, "");
        ObjectNode blankInput = mapper.createObjectNode().put(key, "  ");

        List<Object[]> testData = new ArrayList<>();
        for (ObjectNode inputValue: new ObjectNode[]{nullInput, emptyInput, blankInput}) {
            testData.add(new Object[]{ 1, 1, inputValue });
            testData.add(new Object[]{ 1, -1, inputValue });
            testData.add(new Object[]{ 2, 2, inputValue });
            testData.add(new Object[]{ 2, -1, inputValue });
        }

        return testData.toArray(new Object[][]{});
    }

    /**
     * Tests required visible {@link SoftwareInput} allows null value when min cardinality is 0
     */
    @Test(dataProvider = "processRequiredVisibleParametersThrowsExceptionWhenBlankValueAndCardinalityNotZeroProvider", expectedExceptions = JobProcessingException.class)
    public void processRequiredVisibleParametersThrowsExceptionWhenBlankValueAndCardinalityNotZero(int minCardinality, int maxCardinality, ObjectNode testInputs) throws JobProcessingException {
        String message = "Null input value in job request should throw an exception when minCard = " + minCardinality + " and maxCard = " + maxCardinality;
        try {
            SoftwareInput input = createInput("hiddeninput", "http://example.com/", null, true, true);
            input.setMinCardinality(minCardinality);
            input.setMaxCardinality(maxCardinality);
            ObjectNode jobInputs = _genericProcess(List.of(input), testInputs);
            assertFalse(jobInputs.has(input.getKey()), message);
        } catch (PermissionException e) {
            fail(message, e);
        }
    }

    @DataProvider
    protected Object[][] processRequiredVisibleParametersThrowsExceptionWhenMultipleBlankValueAndCardinalityNotZeroProvider() {
        String key = "testInput";
        String emptyValue = "";
        String blankValue = "  ";

        List<Object[]> testData = new ArrayList<>();
        for (String inputValue1: List.of(emptyValue, blankValue)) {
            for (String inputValue2: List.of(emptyValue, blankValue)) {
                ObjectNode testNode = mapper.createObjectNode();
                testNode.putArray(key).add(inputValue1).add(inputValue2);

                testData.add(new Object[]{1, 1, testNode});
                testData.add(new Object[]{1, -1, testNode});
                testData.add(new Object[]{2, 2, testNode});
                testData.add(new Object[]{2, -1, testNode});
            }
            ObjectNode testNode = mapper.createObjectNode();
            testNode.putArray(key).add(inputValue1).addNull();

            testData.add(new Object[]{1, 1, testNode});
            testData.add(new Object[]{1, -1, testNode});
            testData.add(new Object[]{2, 2, testNode});
            testData.add(new Object[]{2, -1, testNode});
        }

        return testData.toArray(new Object[][]{});
    }

    /**
     * Tests required visible {@link SoftwareInput} throws exception when multiple blank values are provided and
     * cardinality is greater than zero.
     */
    @Test(dataProvider = "processRequiredVisibleParametersThrowsExceptionWhenMultipleBlankValueAndCardinalityNotZeroProvider", expectedExceptions = JobProcessingException.class)
    public void processRequiredVisibleParametersThrowsExceptionWhenMultipleBlankValueAndCardinalityNotZero(int minCardinality, int maxCardinality, ObjectNode testInputs) throws JobProcessingException {
        String message = "Null input value in job request should throw an exception when minCard = " + minCardinality + " and maxCard = " + maxCardinality;
        try {
            SoftwareInput input = createInput("hiddeninput", "http://example.com/", null, true, true);
            input.setMinCardinality(minCardinality);
            input.setMaxCardinality(maxCardinality);
            _genericProcess(List.of(input), testInputs);
            fail(message);
        } catch (PermissionException e) {
            fail(message, e);
        }
    }

    /**
     * Tests required visible {@link SoftwareInput} throws an exception when not included in the job request
     */
    @Test(expectedExceptions = JobProcessingException.class)
    public void processRequiredVisibleParametersThrowExceptionWhenNotPresentInJobRequest() throws JobProcessingException {
        String message = "Required visible parameters missing from the job request should result in a JobProcessingException.";
        try {
            SoftwareInput input = createInput("hiddeninput", "http://example.com/", null, true, true);
            _genericProcess(List.of(input), mapper.createObjectNode());

            fail(message);
        } catch (PermissionException e) {
            fail(message);
        }
    }


    /**
     * Creates a {@link SoftwareInput} with teh given constraints, validator, etc.
     *
     * @param key          the key of the input
     * @param defaultValue the default value of the input
     * @param validator    the validator to use on job requests with this input
     * @param required     whether the input is required
     * @param visible      whether the input should be visible
     * @return a software input with the given constraints
     */
    private SoftwareInput createInput(String key, Object defaultValue, String validator, boolean required, boolean visible)
    {
        SoftwareInput input = new SoftwareInput();
        input.setKey(key);
        input.setDefaultValue(defaultValue == null ? null : defaultValue.toString());
        input.setRequired(required);
        input.setVisible(visible);
        input.setValidator(validator);

        return input;
    }


    @DataProvider
    private Object[][] processMultipleJobInputsProvider() {
        ObjectMapper mapper = new ObjectMapper();

        String inputKey = "testInput";
        String inputDefaultValue = "https://example.com/index.html";
        String inputDefaultValue2 = inputDefaultValue + "2";
        String inputDefaultValue3 = inputDefaultValue + "3";
        String inputDefaultValue4 = inputDefaultValue + "4";

        ObjectNode multipleRedundantValueArrayObject = mapper.createObjectNode();
        multipleRedundantValueArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue);

        ObjectNode multipleValueArrayObject = mapper.createObjectNode();
        multipleValueArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue2);

        ObjectNode tripleValueArrayObject = mapper.createObjectNode();
        tripleValueArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue2).add(inputDefaultValue3);

        ObjectNode tripleValueArrayWithRedundantValueObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue2);

        ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue2).add(inputDefaultValue3);

        ObjectNode quadValueArrayWithTwoRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithTwoRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue2).add(inputDefaultValue2);

        ObjectNode quadValueArrayWithSameRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithSameRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue);

        ObjectNode quadValueArrayObject = mapper.createObjectNode();
        quadValueArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue2).add(inputDefaultValue3).add(inputDefaultValue4);

        boolean pass = false;
        boolean fail = true;
        // need to verify cardinality
        return new Object[][]{
                {0, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 0 and maxCard = 1"},
                {0, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 0 and maxCard = 1"},

                {0, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 0 and maxCard = -1"},
                {0, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 0 and maxCard = -1"},

                {1, 1, multipleValueArrayObject, fail, "Multiple input value array should fail with minCard = 1 and maxCard = 1"},
                {1, 1, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1"},
                {1, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 1"},
                {1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 1"},

                {1, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = -1"},
                {1, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1"},

                {1, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 1 and maxCard = 2"},
                {1, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2"},

                {2, 2, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = 2"},
                {2, 2, tripleValueArrayObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2"},

                {2, -1, multipleValueArrayObject, pass, "Multiple input value array should pass with minCard = 2 and maxCard = -1"},
                {2, -1, tripleValueArrayObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1"},

                {0, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 0 and maxCard = -1"},
                {1, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 1 and maxCard = -1"},
                {2, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 2 and maxCard = -1"},
                {3, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 3 and maxCard = -1"},
                {4, -1, quadValueArrayObject, pass, "Quad input value array with unique values should pass with minCard = 4 and maxCard = -1"},
        };
    }

    @DataProvider
    private Object[][] processRedundantJobInputProvider()
    {
        ObjectMapper mapper = new ObjectMapper();

        String inputKey = "testInput";
        String inputDefaultValue = "https://example.com/index.html";
        String inputDefaultValue2 = inputDefaultValue + "2";
        String inputDefaultValue3 = inputDefaultValue + "3";
        String inputDefaultValue4 = inputDefaultValue + "4";

        ObjectNode mulipleRedundantDelimitedValuesObject = mapper.createObjectNode().put(inputKey, String.format("%1$s;%1$s", inputDefaultValue));

        ObjectNode singleValueArrayObject = mapper.createObjectNode();
        singleValueArrayObject.putArray(inputKey).add(inputDefaultValue);

        ObjectNode multipleRedundantValueArrayObject = mapper.createObjectNode();
        multipleRedundantValueArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue);

        ObjectNode multipleValueArrayObject = mapper.createObjectNode();
        multipleValueArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue2);

        ObjectNode tripleValueArrayObject = mapper.createObjectNode();
        tripleValueArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue2).add(inputDefaultValue3);

        ObjectNode tripleValueArrayWithRedundantValueObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue2);

        ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue2).add(inputDefaultValue3);

        ObjectNode quadValueArrayWithTwoRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithTwoRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue2).add(inputDefaultValue2);

        ObjectNode quadValueArrayWithSameRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithSameRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue);

        ObjectNode quadValueArrayObject = mapper.createObjectNode();
        quadValueArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue2).add(inputDefaultValue3).add(inputDefaultValue4);

        boolean pass = false;
        boolean fail = true;
        // need to verify cardinality
        return new Object[][] {
            { 0, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input value with redundant values should pass with minCard = 0 and maxCard = 1" },
            { 0, 1, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array with redundant values should fail with minCard = 0 and maxCard = 1" },
            { 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 0 and maxCard = 1" },

            { 0, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited input values with redundant values should pass with minCard = 0 and maxCard = -1" },
            { 0, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array with redundant values should pass with minCard = 0 and maxCard = -1" },
            { 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 0 and maxCard = -1" },

            { 1, 1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited input values with redundant values should fail with minCard = 1 and maxCard = 1"},

            { 1, -1, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited inputs with redundant values should pass with minCard = 1 and maxCard = -1" },
            { 1, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 1 and maxCard = -1" },
            { 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 1 and maxCard = -1" },

            { 1, 2, mulipleRedundantDelimitedValuesObject, pass, "Multiple delimited inputs with redundant values should pass with minCard = 1 and maxCard = 2" },
            { 1, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 1 and maxCard = 2" },
            { 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 1 and maxCard = 2" },

            { 2, -1, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 2 and maxCard = -1" },
            { 2, -1, tripleValueArrayWithRedundantValueObject, pass, "Triple input value array should pass with minCard = 2 and maxCard = -1" },

            { 2, 2, mulipleRedundantDelimitedValuesObject, fail, "Multiple delimited inputs with redundant values should fail with minCard = 2 and maxCard = 2" },
            { 2, 2, tripleValueArrayWithRedundantValueObject, fail, "Triple input value array should fail with minCard = 2 and maxCard = 2" },
            { 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 2 and maxCard = 2" },

            { 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 2 and maxCard = -1" },
            { 3, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values should pass with minCard = 3 and maxCard = -1" },
            { 4, -1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values should fail with minCard = 4 and maxCard = -1" },

            { 2, -1, quadValueArrayWithTwoRedundantValueObject, pass, "Quad input value array with two redundant values should pass with minCard = 2 and maxCard = -1" },
            { 3, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 3 and maxCard = -1" },
            { 4, -1, quadValueArrayWithTwoRedundantValueObject, fail, "Quad input value array with two redundant values should fail with minCard = 4 and maxCard = -1" },

            { 0, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should fail with minCard = 0 and maxCard = -1" },
            { 1, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should fail with minCard = 1 and maxCard = -1" },
            { 2, -1, quadValueArrayWithSameRedundantValueObject, pass, "Quad input value array with same redundant value should fail with minCard = 2 and maxCard = -1" },
            { 3, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 3 and maxCard = -1" },
            { 4, -1, quadValueArrayWithSameRedundantValueObject, fail, "Quad input value array with same redundant value should fail with minCard = 4 and maxCard = -1" },
        };
    }

	/**
     * Tests job app input cardinality on jobs submitted as json
     *
     * @param jobInputs test validation of multiple inputs in a job request
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(dataProvider = "processMultipleJobInputsProvider")
    public void processMultipleJobInputs(int minCardinality, int maxCardinality, ObjectNode jobInputs, boolean shouldThrowException, String message)
    {
        try {
            SoftwareInput softwareInput = createInput("testInput", null, null, false, true);
            softwareInput.setMaxCardinality(maxCardinality);
            softwareInput.setMinCardinality(minCardinality);

            ObjectNode processedInputs = _genericProcess(List.of(softwareInput), jobInputs);

            assertFalse(shouldThrowException, message);

            assertTrue(processedInputs.has(softwareInput.getKey()), message);

            assertEquals(processedInputs.get(softwareInput.getKey()).size(), jobInputs.get(softwareInput.getKey()).size(),
                    "Incorrect number of inputs returned after processing job inputs.");
            // we now need to compare the resulting inputs from the test inputs
            List<String> processedInputValues = new ArrayList<>();
            for (JsonNode jsonNode : processedInputs.get(softwareInput.getKey())) {
                processedInputValues.add(jsonNode.textValue());
            }

            for (JsonNode jsonNode : jobInputs.get(softwareInput.getKey())) {
                String jobInput = jsonNode.textValue();
                assertTrue(processedInputValues.contains(jobInput),
                        "Processed inputs should contain all valid test values. Missing " + jobInput);
            }
        } catch (JobProcessingException e) {
            if (!shouldThrowException) {
                fail(message, e);
            }
        } catch (PermissionException e) {
            fail("Failed to setup software input test", e);
        }
    }

    @DataProvider
    private Object[][] processMultipleDelimitedJobInputsProvider() {
        String inputDefaultValue = "https://example.com/index.html";
        String inputDefaultValue2 = inputDefaultValue + "2";

        String multipleDelimitedValue = String.format("%s;%s", inputDefaultValue, inputDefaultValue2);

        boolean pass = false;
        boolean fail = true;
        // need to verify cardinality
        return new Object[][]{
                {0, 1, multipleDelimitedValue, fail, "Multiple delimited input values should fail with minCard = 0 and maxCard = 1"},
                {0, -1, multipleDelimitedValue, pass, "Multiple delimited input values should pass with minCard = 0 and maxCard = -1"},
                {1, 1, multipleDelimitedValue, fail, "Multiple delimited input values should fail with minCard = 1 and maxCard = 1"},
                {1, -1, multipleDelimitedValue, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = -1"},
                {1, 2, multipleDelimitedValue, pass, "Multiple delimited input values should pass with minCard = 1 and maxCard = 2"},
                {2, 2, multipleDelimitedValue, pass, "Multiple delimited input values should pass with minCard = 2 and maxCard = 2"},
                {2, -1, multipleDelimitedValue, pass, "Multiple delimited input pass should fail with minCard = 2 and maxCard = -1"},
        };
    }

    /**
     * Tests delimited job app input cardinality on jobs submitted as json
     *
     * @param jobInputs test validation of multiple inputs in a job request
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(dataProvider = "processMultipleDelimitedJobInputsProvider")
    public void processMultipleDelimitedJobInputs(int minCardinality, int maxCardinality, String jobInputs, boolean shouldThrowException, String message)
    {
        try {
            SoftwareInput softwareInput = createInput("testInput", null, null, false, true);
            softwareInput.setMaxCardinality(maxCardinality);
            softwareInput.setMinCardinality(minCardinality);

            ObjectNode processedInputs = _genericProcess(List.of(softwareInput), mapper.createObjectNode().put(softwareInput.getKey(), jobInputs));

            assertFalse(shouldThrowException, message);

            assertTrue(processedInputs.has(softwareInput.getKey()), message);

            assertEquals(processedInputs.get(softwareInput.getKey()).size(), jobInputs.split(";").length,
                    "Incorrect number of inputs returned after processing job inputs.");
            // we now need to compare the resulting inputs from the test inputs
            List<String> processedInputValues = new ArrayList<>();
            for (JsonNode jsonNode : processedInputs.get(softwareInput.getKey())) {
                processedInputValues.add(jsonNode.textValue());
            }

            for (String jobInputValue : jobInputs.split(";")) {
                assertTrue(processedInputValues.contains(jobInputValue),
                        "Processed inputs should contain all valid test values. Missing " + jobInputValue);
            }
        } catch (JobProcessingException e) {
            if (!shouldThrowException) {
                fail(message, e);
            }
        } catch (PermissionException e) {
            fail("Failed to setup software input test", e);
        }
    }

    @DataProvider
    private Object[][] processMultipleJobNullInputsProvider()
    {
        ObjectMapper mapper = new ObjectMapper();

        String inputKey = "testInput";
        String inputDefaultValue = "https://example.com/index.html";
        String inputDefaultValue2 = inputDefaultValue + "2";

        ObjectNode multipleValueArrayWithNullObject = mapper.createObjectNode();
        multipleValueArrayWithNullObject.putArray(inputKey).add(inputDefaultValue).addNull();

        ObjectNode tripleValueNullArrayObject = mapper.createObjectNode();
        tripleValueNullArrayObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue2).addNull();

        ObjectNode tripleValueArrayWithRedundantValueAndOneNullObject = mapper.createObjectNode();
        tripleValueArrayWithRedundantValueAndOneNullObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).addNull();

        ObjectNode tripleValueArrayWithValueAndRedundantNullObject = mapper.createObjectNode();
        tripleValueArrayWithValueAndRedundantNullObject.putArray(inputKey).add(inputDefaultValue).addNull().addNull();

        ObjectNode quadValueArrayWithRedundantValueObject = mapper.createObjectNode();
        quadValueArrayWithRedundantValueObject.putArray(inputKey).add(inputDefaultValue).add(inputDefaultValue).add(inputDefaultValue2).addNull();

        boolean pass = false;
        boolean fail = true;
        // need to verify cardinality
        return new Object[][] {
                { 0, 1, multipleValueArrayWithNullObject, pass, "Multiple input value array with null value should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueNullArrayObject, fail, "Triple input value array with one null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple input value array with redundant value and null should fail with minCard = 0 and maxCard = 1" },
                { 0, 1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple input value array with one value and redundant null should pass with minCard = 0 and maxCard = 1" },
                { 0, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values and null should fail with minCard = 0 and maxCard = 1" },

                { 0, -1, multipleValueArrayWithNullObject, pass, "Multiple input value array with null value should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueNullArrayObject, pass, "Triple input value array with one null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple input value array with redundant value and null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple input value array with one value and redundant null should pass with minCard = 0 and maxCard = -1" },
                { 0, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values and null should pass with minCard = 0 and maxCard = -1" },

                { 1, 1, multipleValueArrayWithNullObject, pass, "Multiple input value array with null value should pass with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueNullArrayObject, fail, "Triple input value array with one null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithRedundantValueAndOneNullObject, fail, "Triple input value array with redundant value and null should fail with minCard = 1 and maxCard = 1" },
                { 1, 1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple input value array with one value and redundant null should pass with minCard = 1 and maxCard = 1" },
                { 1, 1, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values and null should fail with minCard = 1 and maxCard = 1" },

                { 1, -1, multipleValueArrayWithNullObject, pass, "Multiple input value array with null value should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueNullArrayObject, pass, "Triple input value array with one null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple input value array with redundant value and null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple input value array with one value and redundant null should pass with minCard = 1 and maxCard = -1" },
                { 1, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values and null should pass with minCard = 1 and maxCard = -1" },

                { 1, 2, multipleValueArrayWithNullObject, pass, "Multiple input value array with null value should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueNullArrayObject, pass, "Triple input value array with one null should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple input value array with redundant value and null should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, tripleValueArrayWithValueAndRedundantNullObject, pass, "Triple input value array with one value and redundant null should pass with minCard = 1 and maxCard = 2" },
                { 1, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values and null should fail with minCard = 1 and maxCard = 2" },

                { 2, 2, multipleValueArrayWithNullObject, fail, "Multiple input value array with null value should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueNullArrayObject, pass, "Triple input value array with one null should pass with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple input value array with redundant value and null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple input value array with one value and redundant null should fail with minCard = 2 and maxCard = 2" },
                { 2, 2, quadValueArrayWithRedundantValueObject, fail, "Quad input value array with redundant values and null should fail with minCard = 2 and maxCard = 2" },

                { 2, -1, multipleValueArrayWithNullObject, fail, "Multiple input value array with null value should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueNullArrayObject, pass, "Triple input value array with one null should pass with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithRedundantValueAndOneNullObject, pass, "Triple input value array with redundant value and null should pass with minCard = 2 and maxCard = -1" },
                { 2, -1, tripleValueArrayWithValueAndRedundantNullObject, fail, "Triple input value array with one value and redundant null should fail with minCard = 2 and maxCard = -1" },
                { 2, -1, quadValueArrayWithRedundantValueObject, pass, "Quad input value array with redundant values and null should pass with minCard = 2 and maxCard = -1" },
        };
    }

    /**
     * Tests job app input cardinality on jobs submitted as json
     *
     * @param minCardinality the min cardinality of the test input
     * @param maxCardinality the max cardinality of the test input
     * @param jobInputs test validation of multiple inputs in a job request
     * @param shouldThrowException true if processing should throw an exception
     * @param message the message to assert for a failed test
     */
    @Test(dataProvider = "processMultipleJobNullInputsProvider")
    public void processMultipleJobNullInputs(int minCardinality, int maxCardinality, ObjectNode jobInputs, boolean shouldThrowException, String message)
    {
        try {
            SoftwareInput softwareInput = createInput("testInput", null, null, false, true);
            softwareInput.setMaxCardinality(maxCardinality);
            softwareInput.setMinCardinality(minCardinality);

            ObjectNode processedInputs = _genericProcess(List.of(softwareInput), jobInputs);

            assertFalse(shouldThrowException, message);

            assertTrue(processedInputs.has(softwareInput.getKey()), message);

            // we now need to compare the resulting inputs from the test inputs
            List<String> processedInputValues = new ArrayList<>();
            for (JsonNode jsonNode : processedInputs.get(softwareInput.getKey())) {
                processedInputValues.add(jsonNode.textValue());
            }

            List<String> jobInputValues = new ArrayList<>();
            for (JsonNode jsonNode : jobInputs.get(softwareInput.getKey())) {
                if (!jsonNode.isNull()) {
                    jobInputValues.add(jsonNode.textValue());
                }
            }

            assertEquals(processedInputValues.size(), jobInputValues.size(),
                    "Incorrect number of inputs returned after processing job inputs.");
            for (String jobInputValue: jobInputValues) {
                assertTrue(processedInputValues.contains(jobInputValue),
                        "Processed inputs should contain all valid test values. Missing " + jobInputValue);
            }
        } catch (JobProcessingException e) {
            if (!shouldThrowException) {
                fail(message, e);
            }
        } catch (PermissionException e) {
            Assert.fail("Test setup should not fail", e);
        }
    }

    /**
     * Default software takes a {@link SoftwareInput}. Here we generate tests for different values of that
     * {@link SoftwareInput).
     * @return test cases
     * @throws IOException
     * @throws JSONException
     */
    @DataProvider
    protected Object[][] processJsonJobInputsProvider() {
        String key = "testInput";

        // need to verify cardinality
        return new Object[][] {
                { mapper.createObjectNode().put(key, "/path/to/folder"), "absolute path should pass" },
                { mapper.createObjectNode().put(key, "path/to/folder"), "relative path should pass" },
                { mapper.createObjectNode().put(key, "folder"), "relative file should pass" },
                { mapper.createObjectNode().put(key, "http://example.com"), "HTTP uri schema should pass" },
        };
    }

    /**
     * Tests job app input validation on jobs submitted as json
     *
     * @param jobInputs test validation of multiple inputs in a job request
     * @param message the message to assert for a failed test
     */
    @Test(dataProvider = "processJsonJobInputsProvider")
    public void processJsonJobInputs(ObjectNode jobInputs, String message)
    {
        try {
            SoftwareInput softwareInput = createInput("testInput", null, null, false, true);
            ObjectNode processedInputs = _genericProcess(List.of(softwareInput), jobInputs);

            assertTrue(processedInputs.has(softwareInput.getKey()), message);
            assertEquals(processedInputs.get(softwareInput.getKey()).get(0).asText(), jobInputs.get(softwareInput.getKey()).asText(), message);

        } catch (JobProcessingException e) {
            fail(message, e);
        } catch (PermissionException e) {
            fail("Failed to setup software input test", e);
        }
    }

    /**
     * Generates unsupported schema for upper, lower, and mixed case to test processing fails for unsupported schema.
     *
     * @return test cases
     */
    @DataProvider
    protected Object[][] processUnsupportedSchemaThrowsExceptionProvider() {
        List<Object[]> testData = new ArrayList<>();
        for (String schema: new String[]{"GsiFTP", "GridFTP", "Azure", "S3", "Irods", "Irods3", "Irods4", "MySQL"}) {
            testData.add(new Object[]{ schema });
            testData.add(new Object[]{ schema.toUpperCase() });
            testData.add(new Object[]{ schema.toLowerCase() });
        }

        return testData.toArray(new Object[][]{});
    }

    /**
     * Tests job app input validation throws exception for unsupported schema
     *
     * @param schema invalid schema to test
     */
    @Test(dataProvider = "processUnsupportedSchemaThrowsExceptionProvider", expectedExceptions = JobProcessingException.class)
    public void processUnsupportedSchemaThrowsException(String schema) throws JobProcessingException
    {
        try {
            SoftwareInput softwareInput = createInput("testInput", null, null, false, true);
            _genericProcess(List.of(softwareInput), mapper.createObjectNode().put(softwareInput.getKey(), schema + "://example.com/test.txt"));
        } catch (PermissionException e) {
            fail("Failed to setup software input test", e);
        }
    }

    /**
     * Tests job app input validation throws exception for invalid uri
     */
    @Test(expectedExceptions = JobProcessingException.class)
    public void processInvalidUriThrowsException() throws JobProcessingException
    {
        try {
            SoftwareInput softwareInput = createInput("testInput", null, null, false, true);
            _genericProcess(List.of(softwareInput), mapper.createObjectNode().put(softwareInput.getKey(), "j:/^^  //example.com/test.txt"));
        } catch (PermissionException e) {
            fail("Failed to setup software input test", e);
        }
    }

    /**
     * Tests job app input validation throws exception for permission denied
     */
    @Test(expectedExceptions = JobProcessingException.class)
    public void processPermissionDeniedThrowsJobProcessingException() throws JobProcessingException
    {
        String inputKey = "testInput";
        String testValue = "https://example.com/index.html";

        try {
            SoftwareInput softwareInput = createInput(inputKey, null, null, false, true);
            JobRequestInputProcessor jobRequestInputProcessor = createMockJobRequestInputProcessor(List.of(softwareInput));
            when(jobRequestInputProcessor.canUserReadUri(any(), any(), any())).thenReturn(false);

            HashMap<String, Object> inputMap = jsonObjectToMap(inputKey, mapper.createObjectNode().put(inputKey, testValue));

            jobRequestInputProcessor.process(inputMap);
        } catch (PermissionException e) {
            fail("False response from canUserReadUri should not result in PermissionException being throw", e);
        }
    }

    /**
     * Tests job app input validation throws exception for permission denied
     */
    @Test(expectedExceptions = JobProcessingException.class)
    public void processPermissionExceptionThrowsPermissionException() throws JobProcessingException
    {
        String inputKey = "testInput";
        String testValue = "https://example.com/index.html";

        try {
            SoftwareInput softwareInput = createInput(inputKey, null, null, false, true);
            JobRequestInputProcessor jobRequestInputProcessor = createMockJobRequestInputProcessor(List.of(softwareInput));
            when(jobRequestInputProcessor.canUserReadUri(any(), any(), any())).thenThrow(new PermissionException("Mocked permission exception should be wrapped as JobProcessingException thrown from process()"));

            HashMap<String, Object> inputMap = jsonObjectToMap(inputKey, mapper.createObjectNode().put(inputKey, testValue));

            jobRequestInputProcessor.process(inputMap);
        } catch (PermissionException e) {
            fail("PermissionException thrown from canUserReadUri should be wrapped and thrown as JobProcessingException from process method", e);
        }
    }


    /**
     * Generate different forms of single input value.
     * @return test cases
     */
    @DataProvider
    protected Object[][] processSingleInputsThrowExceptionWhenMinCardinalityGreaterThanOneProvider() {
        String key = "testInput";
        String testUrl = "https://example.com/index.html";
        ObjectNode singleValueObject = mapper.createObjectNode().put(key, testUrl);
        ObjectNode singleValueArrayObject = mapper.createObjectNode();
        singleValueArrayObject.putArray(key).add(testUrl);

        return new Object[][] {
                { singleValueArrayObject },
                { singleValueObject },
        };
    }

    /**
     * Tests job input processing respects min cardinality when a single string input is provided and throws a
     * {@link JobProcessingException}.
     */
    @Test(dataProvider = "processSingleInputsThrowExceptionWhenMinCardinalityGreaterThanOneProvider", expectedExceptions = JobProcessingException.class)
    public void processSingleInputsThrowExceptionWhenMinCardinalityGreaterThanOne(ObjectNode jobInputs) throws JobProcessingException
    {
        try {
            SoftwareInput softwareInput = createInput("testInput", null, null, false, true);
            softwareInput.setMinCardinality(2);
            softwareInput.setMaxCardinality(-1);
            _genericProcess(List.of(softwareInput), jobInputs);

            fail("Single input value for sofware input with min cardinality greater than 1 should throw JobProcessingException");
        } catch (PermissionException e) {
            fail("Failed to setup software input test", e);
        }
    }

    /**
     * Default software takes a {@link SoftwareInput}. Here we generate tests for different values of that
     * {@link SoftwareInput).
     * @return test cases
     * @throws IOException
     * @throws JSONException
     */
    @DataProvider
    protected Object[][] processTrimsJsonJobInputsProvider() {
        String testUrl = "https://example.com/index.html";
        String singleSpace = " ";
        String doubleSpace = "  ";

        List<Object[]> testData = new ArrayList<>();
        for (String inputValue1: List.of(singleSpace, doubleSpace)) {
            for (String inputValue2: List.of(singleSpace, doubleSpace)) {
                testData.add(new Object[]{ inputValue2 + testUrl});
                testData.add(new Object[]{ testUrl + inputValue2});
                testData.add(new Object[]{ inputValue1 + testUrl + inputValue2});
            }
        }

        return testData.toArray(new Object[][]{});
    }

    /**
     * Tests job app input validation trims input values
     *
     * @param testUrl padded url to test
     */
    @Test(dataProvider = "processTrimsJsonJobInputsProvider")
    public void processTrimsJsonJobInputs(String testUrl) throws Exception
    {
        String message = "All input values should be trimmed";

        SoftwareInput softwareInput = createInput("testInput", null, null, false, true);

        ObjectNode processedInputs = _genericProcess(List.of(softwareInput), mapper.createObjectNode().put(softwareInput.getKey(), testUrl));

        assertTrue(processedInputs.has(softwareInput.getKey()), message);
        assertEquals(processedInputs.get(softwareInput.getKey()).get(0).asText(), StringUtils.strip(testUrl), message);
    }

    /**
     * Generates test input objects that will fail the {@link SoftwareInput#getValidator()}.
     * @return test cases
     */
    @DataProvider
    protected Object[][] processThrowsExceptionForInputNotMatchingValidatorProvider() {
        String key = "testInput";
        String testUrl = "agave://example.com/index.html";
        String testUrl2 = "https://example.com/index.html";

        ObjectNode singleInputValue = mapper.createObjectNode().put(key, testUrl);

        ObjectNode singleInputArrayValue = mapper.createObjectNode();
        singleInputArrayValue.putArray(key).add(testUrl);

        ObjectNode multipleInputArrayValue = mapper.createObjectNode();
        multipleInputArrayValue.putArray(key).add(testUrl2).add(testUrl);

        return new Object[][]{
                { singleInputValue },
                { singleInputArrayValue },
                { multipleInputArrayValue },
        };
    }

    /**
     * Tests job app input validation trims input values
     */
    @Test(dataProvider = "processThrowsExceptionForInputNotMatchingValidatorProvider", expectedExceptions = JobProcessingException.class)
    public void processThrowsExceptionForInputNotMatchingValidator(ObjectNode testInputs) throws JobProcessingException
    {
        String message = "Inputs not matching validator should fail";

        SoftwareInput softwareInput = createInput("testInput", null, "^https://", false, true);
        softwareInput.setMaxCardinality(-1);
        try {
            _genericProcess(List.of(softwareInput), testInputs);
        } catch (PermissionException e) {
            fail("Permission exception should not be thrown when the validation check fails.", e);
        }
    }

    /**
     * Generates test input objects that will pass the {@link SoftwareInput#getValidator()}.
     * @return test cases
     */
    @DataProvider
    protected Object[][] processValidatesAcceptableInputProvider() {
        String key = "testInput";
        String testUrl = "https://example.com/index.html";
        String testUrl2 = "https://localhost/index.html";
        ObjectNode singleInputValue = mapper.createObjectNode().put(key, testUrl);
        ObjectNode singleInputArrayValue = mapper.createObjectNode();
        singleInputArrayValue.putArray(key).add(testUrl);
        ObjectNode multipleInputArrayValue = mapper.createObjectNode();
        singleInputArrayValue.putArray(key).add(testUrl2).add(testUrl);

        return new Object[][]{
                { singleInputValue },
                { singleInputArrayValue },
                { multipleInputArrayValue },
        };
    }

    /**
     * Tests job app input validation succeeds
     *
     * @param testInputs the input to test
     */
    @Test(dataProvider = "processValidatesAcceptableInputProvider")
    public void processValidatesAcceptableInput(ObjectNode testInputs) throws JobProcessingException
    {
        String message = "Inputs matching validator should succeed";
        String testUrl = "https://example.com/index.html";

        ObjectNode processedInputs;
        SoftwareInput softwareInput = createInput("testInput", null, "^https://", false, true);
        try {
            processedInputs = _genericProcess(List.of(softwareInput), mapper.createObjectNode().put(softwareInput.getKey(), testUrl));

            assertTrue(processedInputs.has(softwareInput.getKey()), message);
            assertEquals(processedInputs.get(softwareInput.getKey()).get(0).asText(), StringUtils.strip(testUrl), message);

        } catch (PermissionException e) {
            fail("Permission exception should not be thrown when the validation check fails.", e);
        }
    }

    /**
     * Default software takes a {@link SoftwareInput}. Here we generate tests for different values of that
     * {@link SoftwareInput).
     * @return test cases
     * @throws IOException
     * @throws JSONException
     */
    @DataProvider
    protected Object[][] processTrimsMultipleJsonJobInputsProvider() {
        String testUrl = "https://example.com/index.html";
        String testUrl2 = "http://example.com/index.html";
        String singleSpace = " ";
        String doubleSpace = "  ";

        List<Object[]> testData = new ArrayList<>();
        for (String inputValue1: List.of(singleSpace, doubleSpace)) {
            for (String inputValue2: List.of(singleSpace, doubleSpace)) {
                testData.add(new Object[]{ inputValue2 + testUrl, inputValue2 + testUrl2 });
                testData.add(new Object[]{ testUrl + inputValue2, testUrl2 + inputValue2 });
                testData.add(new Object[]{ inputValue1 + testUrl + inputValue2, inputValue1 + testUrl2 + inputValue2 });
            }
        }

        return testData.toArray(new Object[][]{});
    }

    /**
     * Tests job app input validation trims multiple input values
     *
     * @param testUrl padded url to test
     */
    @Test(dataProvider = "processTrimsMultipleJsonJobInputsProvider")
    public void processTrimsMultipleJsonJobInputs(String testUrl, String testUrl2) throws Exception
    {
        String message = "All input values should be trimmed";

        SoftwareInput softwareInput = createInput("testInput", null, null, false, true);
        softwareInput.setMinCardinality(2);
        softwareInput.setMaxCardinality(-1);

        ObjectNode testInputs = mapper.createObjectNode();
        testInputs.putArray(softwareInput.getKey()).add(testUrl).add(testUrl2);

        ObjectNode processedInputs = _genericProcess(List.of(softwareInput), testInputs);

        assertTrue(processedInputs.has(softwareInput.getKey()), message);
        assertEquals(processedInputs.get(softwareInput.getKey()).get(0).asText(), StringUtils.strip(testUrl), message);
        assertEquals(processedInputs.get(softwareInput.getKey()).get(1).asText(), StringUtils.strip(testUrl2), message);
    }

    /**
     * Tests job app input validation on jobs submitted as json
     */
	@Test
    public void processJsonJobInputDefaults()
    {
        try {
            SoftwareInput input2 = new SoftwareInput();
            input2.setDefaultValue("/usr/bin/date");
            input2.setKey("hiddenTime");
            input2.setVisible(false);
            input2.setRequired(true);

            SoftwareInput input3 = new SoftwareInput();
            input3.setDefaultValue("/usr/bin/mkdir");
            input3.setKey("optionalKey1");
            input3.setRequired(false);

            SoftwareInput input4 = new SoftwareInput();
            input4.setDefaultValue("/usr/bin/top");
            input4.setKey("requiredKey2");
            input4.setRequired(true);

            SoftwareInput input5 = new SoftwareInput();
            input5.setDefaultValue("/usr/bin/ls");
            input5.setKey("optionalKey2");
            input5.setRequired(false);

            ObjectNode jobInputs = mapper.createObjectNode()
                    .put(input5.getKey(), "wazzup")
                    .put(input4.getKey(), "top")
                    .put("dummyfield", "something");

            ObjectNode processedInputs = _genericProcess(List.of(input2, input3, input4, input5), jobInputs);

            Assert.assertTrue(processedInputs.has(input2.getKey()), "Hidden fields should always be added");
            assertEquals(processedInputs.get(input2.getKey()), input2.getDefaultValueAsJsonArray(), "Hidden fields should always be added");

            assertFalse(processedInputs.has(input3.getKey()), "Optional fields should not be added if user does not supply value");
            assertFalse(processedInputs.has("dummyfield"), "User supplied fields not part of the job should not be persisted.");

            Assert.assertTrue(processedInputs.has(input4.getKey()), "User supplied required fields should be persisted");
            assertEquals(processedInputs.get(input4.getKey()).get(0).textValue(), "top", "Required field that user supplies as input should be the value persisted with the job");

            Assert.assertTrue(processedInputs.has(input5.getKey()), "User supplied optional fields should be persisted");
            assertEquals(processedInputs.get(input5.getKey()).get(0).textValue(), "wazzup", "Option field that user supplies as input should be the value persisted with the job");

        } catch (Exception e) {
            Assert.fail("Failed to process job", e);
        }
    }

//    /**
//     * Generic method to run the JobManager.processJob(JsonNode, String, String) method.
//     * @param json the json object representing the job request
//     * @param shouldThrowException true if processing should throw an exception
//     * @param message the assertion message to be returned if the test fails
//     */
//    private ObjectNode _genericProcessMultiple(List<SoftwareInput> softwareInputs, ObjectNode json, boolean shouldThrowException, String message) {
//        JobRequestInputProcessor jobRequestInputProcessor = null;
//        try {
//            jobRequestInputProcessor = createMockJobRequestInputProcessor(softwareInputs);
//
//            HashMap<String, Object> inputMap = new HashMap<>();
//            for (Iterator<String> it = json.fieldNames(); it.hasNext(); ) {
//                String field = it.next();
//                inputMap.putAll(jsonObjectToMap(field, json));
//            }
//
//            jobRequestInputProcessor.process(inputMap);
//
//            assertFalse(shouldThrowException, message);
//
//        } catch (JobProcessingException|PermissionException e) {
//            if (!shouldThrowException) {
//                fail(message, e);
//            }
//        }
//
//        return jobRequestInputProcessor.getJobInputs();
//    }

    /**
     * Creates a mock of the {@link JobRequestInputProcessor} class, passing through all the doProcessSoftwareInputValue and
     * doProcessSingleValue methods. The mock will ensure {@link JobRequestInputProcessor#canUserReadUri(String, String, URI)}
     * always returns true since the {@link PermissionManager#canUserReadUri(String, String, URI)} method is already
     * independently tested.
     *
     * @param softwareInputs the {@link SoftwareInput} instances to return when {@link Software#getInputs()} is called.
     * @return a testable mock of the {@link JobRequestInputProcessor} class.
     * @throws JobProcessingException if the init of the {@link JobRequestInputProcessor} failed.
     * @throws PermissionException if the mock of the {@link JobRequestInputProcessor#canUserReadUri(String, String, URI)} fails.
     */
    private JobRequestInputProcessor createMockJobRequestInputProcessor(List<SoftwareInput> softwareInputs) throws JobProcessingException, PermissionException {
        Software software = mock(Software.class);
        when(software.getInputs()).thenReturn(softwareInputs);

        JobRequestInputProcessor jobRequestInputProcessor = mock(JobRequestInputProcessor.class);
        when(jobRequestInputProcessor.getMapper()).thenReturn(mapper);
        when(jobRequestInputProcessor.getSoftware()).thenReturn(software);
        when(jobRequestInputProcessor.doProcessSoftwareInputValue(any(SoftwareInput.class), any(Object.class))).thenCallRealMethod();
        when(jobRequestInputProcessor.doProcessSoftwareInputValue(any(SoftwareInput.class), anyString())).thenCallRealMethod();
        when(jobRequestInputProcessor.doProcessSoftwareInputValue(any(SoftwareInput.class), any(String[].class))).thenCallRealMethod();
        when(jobRequestInputProcessor.doProcessSoftwareInputValue(any(SoftwareInput.class), any(ArrayNode.class))).thenCallRealMethod();
        when(jobRequestInputProcessor.doProcessSingleValue(any(SoftwareInput.class), anyString())).thenCallRealMethod();
        doCallRealMethod().when(jobRequestInputProcessor).process(any());
        // ignore permissino check as there are already tests for the PermissionManager#canUserReadUri(String, String, URI) method
        when(jobRequestInputProcessor.canUserReadUri(any(), any(), any())).thenReturn(true);
        jobRequestInputProcessor.jobInputs = mapper.createObjectNode();
        doNothing().when(jobRequestInputProcessor).setJobInputs(any());
        when(jobRequestInputProcessor.getJobInputs()).thenCallRealMethod();

        return jobRequestInputProcessor;
    }

    private ObjectNode _genericProcess(List<SoftwareInput> softwareInputs, ObjectNode json) throws JobProcessingException, PermissionException {
        JobRequestInputProcessor jobRequestInputProcessor = createMockJobRequestInputProcessor(softwareInputs);

        HashMap<String, Object> inputMap = new HashMap<>();
        for (Iterator<String> it = json.fieldNames(); it.hasNext(); ) {
            String field = it.next();
            inputMap.putAll(jsonObjectToMap(field, json));
        }

        // call process the inputs.
        jobRequestInputProcessor.process(inputMap);

        return jobRequestInputProcessor.getJobInputs();
    }

    /**
     * Converts a json object into a {@link Map} that can be processed for job request inputs
     *
     * @param key           the key for which the job input should be stored
     * @param jobInputs a {@link ObjectNode} containing the value(s) of the job input under test
     * @return a map of the job inputs converted to a map
     */
    private HashMap<String, Object> jsonObjectToMap(String key, ObjectNode jobInputs) {
        HashMap<String, Object> inputMap = new HashMap<>();

        // if the inputs were null, we do nothing. we will pass an empty job input map
        if (jobInputs != null) {
            if (jobInputs.isNull() || jobInputs.get(key).isNull()) {
                inputMap.put(key, null);
            } else if (jobInputs.get(key).isArray()) {
                StringBuilder sb = new StringBuilder();
                jobInputs.get(key).forEach(item -> {
                    if (sb.length() > 0) {
                        sb.append(Settings.AGAVE_SERIALIZED_LIST_DELIMITER);
                    }

                    if (!item.isNull() && !item.isMissingNode()) {
                        sb.append(item.asText());
                    }
                });
                inputMap.put(key, sb.toString());
            } else if (jobInputs.get(key).isDouble()) {
                inputMap.put(key, jobInputs.get(key).decimalValue().toPlainString());
            } else if (jobInputs.get(key).isNumber()) {
                inputMap.put(key, jobInputs.get(key).longValue());
            } else if (jobInputs.get(key).isTextual()) {
                inputMap.put(key, jobInputs.get(key).textValue());
            } else if (jobInputs.get(key).isBoolean()) {
                inputMap.put(key, jobInputs.get(key).asBoolean() ? "true" : "false");
            }
        }
        return inputMap;
    }

}
