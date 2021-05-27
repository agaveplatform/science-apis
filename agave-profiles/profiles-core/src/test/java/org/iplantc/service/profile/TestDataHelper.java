package org.iplantc.service.profile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.POJONode;
import org.apache.commons.io.IOUtils;
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
	
	public static String TEST_INTERNAL_USER_FILE = "target/test-classes/internal_users/internal_user.json";
	
	public static TestDataHelper testDataHelper;
	
    /**
     * Get a test data file from disk and deserializes to a JSONObject.
     *
     * @return An ObjectNode which can be traversed using json.org api
     * @throws IOException 
     * @throws JsonProcessingException 
     */
    public JSONObject getTestDataObject(String file) throws JSONException, IOException
    {
    	InputStream in = null;
    	try 
    	{	
    		in = new FileInputStream(file);
    		String json = IOUtils.toString(in, "UTF-8");
    		
	    	return new JSONObject(json);
    	} 
    	finally {
    		try {
				if (in != null) {
					in.close();
				}
			} catch (Exception ignored) {}
    	}
    }

    public POJONode makeJsonObj(Object obj) 
    {
    	ObjectMapper mapper = new ObjectMapper();
//    	return mapper.getNodeFactory().POJONode(obj);
    	return (POJONode)mapper.getNodeFactory().pojoNode(obj);
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
