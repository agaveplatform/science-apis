package org.iplantc.service.transfer.s3;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.IAnnotationTransformer;
import org.testng.annotations.ITestAnnotation;
import org.testng.reporters.Files;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Programmatically skips S3 tests when credentials are not provided for AWS. This allows for integration tests
 * to continue when users do not have a valid set of s3 keys.
 */
public class S3CheckCredentialAnnotationTransformer implements IAnnotationTransformer {

    private static final Logger log = Logger.getLogger(S3CheckCredentialAnnotationTransformer.class);

    @Override
    public void transform(ITestAnnotation annotation, Class testClass, Constructor testConstructor, Method testMethod) {

        if (testClass != null && testClass.getPackage().getName().startsWith("org.iplantc.service.transfer.s3")){
            InputStream in = null;
            boolean hasApiKeys = false;

            try {
                // get filtered s3 system file as input stream
                in = getClass().getClassLoader().getResourceAsStream("systems/storage/s3.example.com.json");

                if (in != null) {
                    // read the file contents
                    String systemJson = Files.readFile(in);

                    // parse the json. Any exception will be ignored and the tests will carry on
                    // failing on their own in a debuggable manner due to bad json
                    JSONObject json = new JSONObject(systemJson);

                    // pull out the storage.auth object from the system definition
                    JSONObject systemAuthObject = json.getJSONObject("storage").getJSONObject("auth");

                    // verify both the public and private keys are present
                    hasApiKeys = StringUtils.isNotBlank(systemAuthObject.getString("publicKey")) &&
                            StringUtils.isNotBlank(systemAuthObject.getString("privateKey"));
                }

            } catch (JSONException|IOException e) {
                log.error("Unable to read s3 system definition to check for AWS credentials.");
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (Exception ignored) {}
            }

            // disable the test if both api keys are not present
            annotation.setEnabled(hasApiKeys);
        }
    }
}
