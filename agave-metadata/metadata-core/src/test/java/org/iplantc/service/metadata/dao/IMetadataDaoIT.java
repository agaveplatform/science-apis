package org.iplantc.service.metadata.dao;

import com.mongodb.client.MongoCollection;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;

import java.net.UnknownHostException;

public interface IMetadataDaoIT {

    MongoCollection getCollection();
    MongoCollection getDefaultCollection();
    MetadataItem insert();
    MetadataItem createEntity();
    void insertTest() throws MetadataStoreException, MetadataException, PermissionException, UnknownHostException;
    void removeTest() throws MetadataStoreException, MetadataException, UnknownHostException;
    void updateTest() throws MetadataStoreException, MetadataException, UnknownHostException;
}
