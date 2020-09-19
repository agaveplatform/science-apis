package org.iplantc.service.metadata.dao;

import com.mongodb.client.MongoCollection;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;

public interface IMetadataDaoIT {

    MongoCollection getCollection();
    MongoCollection getDefaultCollection();
    MetadataItem insert();
    MetadataItem createEntity();
    void insertTest() throws MetadataException, PermissionException, MetadataStoreException;
    void insertPermissionTest() throws MetadataStoreException, MetadataException, PermissionException;
    void deleteMetadataTest() throws MetadataStoreException, MetadataException, PermissionException;
    void removePermissionTest()  throws MetadataStoreException, MetadataException, PermissionException;
    void updateTest() throws MetadataStoreException, MetadataException, PermissionException;
    void updatePermissionTest() throws MetadataStoreException, MetadataException, PermissionException;
    void findTest() throws MetadataException, PermissionException, MetadataStoreException;
    void findWithOffsetAndLimitTest() throws MetadataException, PermissionException, MetadataStoreException;
    void findSingleMetadataItemTest() throws MetadataException, PermissionException, MetadataStoreException;
    void findSingleMetadataItemNonexistentTest();
    void findPermissionTest() throws MetadataException, PermissionException, MetadataStoreException;
    void findMetadataItemWithFiltersTest() throws MetadataException, PermissionException, MetadataStoreException;
    void findMetadataItemWithInvalidFiltersTest() throws MetadataException, PermissionException, MetadataStoreException;


}
