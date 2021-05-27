package org.iplantc.service.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;

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
	
	public static final String NOTIFICATION_CREATOR = "testuser";
	public static final String NOTIFICATION_STRANGER = "bob";
	public static final String TENANT_ADMIN = "testadmin";
	public static final String ALT_TENANT_ADMIN = "testotheradmin";
	public static final String TEST_EMAIL_NOTIFICATION = "target/test-classes/notifications/email_notif.json";
	public static final String TEST_WEBHOOK_NOTIFICATION = "target/test-classes/notifications/webhook_notif.json";
	public static final String TEST_MULTIPLE_EMAIL_NOTIFICATION = "target/test-classes/notifications/email_multi_notif.json";
	public static final String TEST_MULTIPLE_WEBHOOK_NOTIFICATION = "target/test-classes/notifications/webhook_multi_notif.json";
	public static final String TEST_REALTIME_NOTIFICATION = "target/test-classes/notifications/realtime_notif.json";
	public static final String TEST_POLICY_NOTIFICATION = "target/test-classes/notifications/realtime_policy_notif_.json";
	
	public static TestDataHelper testDataHelper;
	
    /**
     * Get a test data file from disk and deserializes to a JSONObject.
     *
     * @return An ObjectNode which can be traversed using json.org api
     * @throws IOException 
     */
    public ObjectNode getTestDataObject(String file) throws IOException
    {
    	InputStream in = null;
    	try 
    	{	
    		ObjectMapper mapper = new ObjectMapper();
    		in = new FileInputStream(file);
    		
	    	return (ObjectNode)mapper.readTree(in);
    	} 
    	finally {
    		try { in.close(); } catch (Exception e) {}
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
