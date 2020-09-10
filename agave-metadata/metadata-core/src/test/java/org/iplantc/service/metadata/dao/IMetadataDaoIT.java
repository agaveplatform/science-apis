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
    void insertTest() throws  MetadataException, PermissionException;
    void insertPermissionTest() throws MetadataStoreException, MetadataException, PermissionException;
    void removeMetadataTest() throws MetadataStoreException, MetadataException, PermissionException;
    void removePermissionTest()  throws MetadataStoreException, MetadataException, PermissionException;
    void updateTest() throws MetadataStoreException, MetadataException, PermissionException;
    void updatePermissionTest() throws MetadataStoreException, MetadataException, PermissionException;
    void findTest() throws MetadataException, PermissionException;
    void findWithOffsetAndLimitTest() throws MetadataException, PermissionException;
    void findSingleMetadataItemTest() throws MetadataException, PermissionException;
    void findSingleMetadataItemNonexistentTest();
    void findPermissionTest() throws MetadataException, PermissionException;
    void findMetadataItemWithFiltersTest() throws MetadataException, PermissionException;
    void findMetadataItemWithInvalidFiltersTest() throws MetadataException, PermissionException;


}
