package org.iplantc.service.metadata.dao;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.persistence.HibernateUtil;
import org.iplantc.service.metadata.exceptions.MetadataQueryException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

import java.util.Arrays;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public abstract class AbstractMetadataPermissionDaoIT implements IMetadataPermissionDaoIT {
    protected String TEST_OWNER = "testuser";
    protected String TEST_SHARED_OWNER = "testshareuser";

    @BeforeClass
    public void beforeClass() throws Exception {
        clearMongoPermissions();
    }

    @AfterMethod
    public void afterMethod() throws Exception
    {
        clearMongoPermissions();
    }

    public void clearMongoPermissions(){
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        MongoClients mongoClients = null;
        com.mongodb.client.MongoClient mongov4Client = null;
        MongoCredential credential = MongoCredential.createScramSha1Credential(
                Settings.METADATA_DB_USER, Settings.METADATA_DB_SCHEME, Settings.METADATA_DB_PWD.toCharArray());

        mongov4Client = mongoClients.create(MongoClientSettings.builder()
//				.applyToSslSettings(builder -> builder.enabled(true))
                .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(
                        new ServerAddress(Settings.METADATA_DB_HOST, Settings.METADATA_DB_PORT))))
                .credential(credential)
                .codecRegistry(pojoCodecRegistry)
                .build());

        MongoDatabase db = mongov4Client.getDatabase(Settings.METADATA_DB_SCHEME);
        MongoCollection collection = db.getCollection("metadata_schema_permissions");
        collection.deleteMany(new Document());
    }

    /**
     * Clears all permissions in the given table
     * @throws Exception
     */
    private void clearPermissions() throws Exception {
        Session session = null;
        try
        {
            HibernateUtil.beginTransaction();
            session = HibernateUtil.getSession();
            session.clear();
            HibernateUtil.disableAllFilters();

            //noinspection JpaQlInspection
            session.createQuery("delete MetadataSchemaPermission").executeUpdate();
        }
        catch (HibernateException ex)
        {
            throw new MetadataQueryException(ex);
        }
        finally
        {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception ignore) {}
        }
    }

}
