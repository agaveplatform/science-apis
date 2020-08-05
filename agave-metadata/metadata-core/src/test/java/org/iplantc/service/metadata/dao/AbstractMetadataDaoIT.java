package org.iplantc.service.metadata.dao;

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.*;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

public abstract class AbstractMetadataDaoIT implements IMetadataDaoIT{

    @Mock
    private MongoClient mockClient;

    @Mock
    private MongoDatabase mockDB;

    @Mock
    private MongoCollection mockCollection;

    @InjectMocks
    private MetadataDao wrapper;



    @BeforeClass
    public void beforeClass() throws Exception {
        clearCollection();
    }

    @AfterMethod
    public void afterMethod() throws Exception
    {
        clearCollection();
    }

    /**
     * Clears all permissions in the given table
     * @throws Exception
     */
    private void clearCollection() throws Exception {
        try {
            MongoCredential mongoCredential = MongoCredential.createScramSha1Credential(
                    Settings.METADATA_DB_USER, Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_PWD.toCharArray());

            mockClient = new com.mongodb.MongoClient(
                    new ServerAddress(Settings.METADATA_DB_HOST, Settings.METADATA_DB_PORT),
                    mongoCredential,
                    MongoClientOptions.builder().build());

            mockDB = mockClient.getDatabase(Settings.METADATA_DB_SCHEME);
            mockCollection = mockDB.getCollection(Settings.METADATA_DB_COLLECTION, MetadataItem.class);

            mockCollection.deleteMany(new Document());

        } catch (Exception ex) {
            throw new MetadataQueryException(ex);
        }
    }

    @Override
    public MongoCollection getCollection() {
        return null;
    }

    @Override
    public MongoCollection getDefaultCollection() {
        return null;
    }

    @Override
    public MetadataItem insert() {
        return null;
    }

}
