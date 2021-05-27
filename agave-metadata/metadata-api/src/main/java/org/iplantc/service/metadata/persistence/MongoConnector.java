package org.iplantc.service.metadata.persistence;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.iplantc.service.metadata.Settings;

import java.util.Arrays;

public enum MongoConnector {
    CONNECTION;

    private MongoClient client = null;

    /**
     * This function is used to create a single instance of the MongoDb connector
     * Thread and connection pooling is handled internally by MongoDb driver
     */
    MongoConnector() {
        try {
            MongoCredential credential = MongoCredential.createScramSha1Credential( Settings.METADATA_DB_USER, "api", Settings.METADATA_DB_PWD.toCharArray());
            ServerAddress serverAddress = new ServerAddress(Settings.METADATA_DB_HOST, Settings.METADATA_DB_PORT);
            client = new MongoClient(serverAddress, Arrays.asList(credential));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create a connection " + e.getMessage());
        }
    }

    /**
     * Mongo
     * @return client connection
     */
    public MongoClient getClient() {
        if (client == null) {
            throw new RuntimeException("Mongo client connection is null and cannot function");
        }
        return client;
    }
}