package org.agaveplatform.service.transfers.util;

import org.apache.commons.fileupload.util.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class AgaveSchemaFactory {

    private static final Logger log = LoggerFactory.getLogger(AgaveSchemaFactory.class);

    /** Fetches the contents of the json schema file for the given resource name
     *
     * @param clazz the resource class for which to fetch the json schema definition
     * @return the json schema definition as a string or null if no resource was provided
     */
    public static String getForClass(Class clazz) {
        String json = null;

        if (clazz != null) {
            try (InputStream schemaInputStream = AgaveSchemaFactory.class.getClassLoader().getResourceAsStream("schema/" + clazz.getSimpleName() + ".json")) {
                if (schemaInputStream != null) {
                    json = Streams.asString(schemaInputStream);
                }
            } catch (IOException e) {
                log.error(String.format("Unable to read schema definition for {}: {}",
                        clazz.getSimpleName(), e.getMessage()));
            }
        }

        return json;
    }
}
