package org.iplantc.service.metadata.dao;

import com.mongodb.client.MongoCollection;
import org.iplantc.service.metadata.model.MetadataItem;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class AbstractMetadataDaoIT implements IMetadataDaoIT{

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
