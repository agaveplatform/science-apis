package org.iplantc.service.tags;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Convenience class to set up the environment and obtain test data.
 * 
 * @author dooley
 *
 */
public class TestDataHelper {
	
	public static String TEST_USER = "testuser";
	public static String TEST_SHAREUSER = "testshare";
	public static String TEST_OTHERUSER = "testother";
	public static String TEST_TAG = "target/test-classes/tags/tag.json";
	public static String TEST_TAGS = "target/test-classes/tags/multiple_tags.json";
	
	public static final String TEST_INTERNAL_USERNAME = "test_user";
	
	public static TestDataHelper testDataHelper;
	
    /**
     * Get a test data file from disk and deserializes to a JsonNode.
     *
     * @return An ObjectNode which can be traversed using json.org api
     * @throws IOException 
     * @throws JsonProcessingException 
     */
    public JsonNode getTestDataObject(String file) throws JsonProcessingException, IOException
    {
    	InputStream in = null;
    	try 
    	{	
    		in = new FileInputStream(file);
    		ObjectMapper mapper = new ObjectMapper();
        	return mapper.readTree(in);
    	} 
    	finally {
    		try { in.close(); } catch (Exception e) {}
    	}
    }
    
    /**
     * Get a test data file from disk and deserializes to a JSONObject.
     *
     * @return A JSONObject which can be traversed using json.org api
     * @throws IOException 
     * @throws JSONException 
     */
    public JSONObject getTestDataObjectAsJSONObject(String file) throws JSONException, IOException
    {
    	return new JSONObject(getTestDataObject(file).toString());
    }

    private TestDataHelper() 
    {
    	init();
    }
    
    public static TestDataHelper getInstance() 
    {
    	if (testDataHelper == null) {
    		testDataHelper = new TestDataHelper();
    	}
    	
    	return testDataHelper;
    }
    
    private void init() {}
}
