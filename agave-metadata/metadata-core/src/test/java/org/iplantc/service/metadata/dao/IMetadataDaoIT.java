package org.iplantc.service.metadata.dao;

import com.mongodb.client.MongoCollection;
import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;
import org.iplantc.service.metadata.model.MetadataItem;
import org.iplantc.service.metadata.model.MetadataPermission;

public interface IMetadataDaoIT {

    MongoCollection getCollection();

    MongoCollection getDefaultCollection();

    MetadataItem insert();

    MetadataItem createEntity();

    MetadataItem insertEntity();

    void insertTest();

    void insertNullMetadataItem() throws MetadataStoreException;

    void deleteMetadataTest(MetadataItem metadataItem, String message, int resultSize, boolean bolThrowException);

    void updateDocumentTest() throws MetadataException;

    void updatePermissionTest(MetadataPermission permission) throws MetadataStoreException;

    void findTest(MetadataItem metadataItem, String message, int findSize, boolean bolThrowException);

    void findWithOffsetAndLimitTest(int offset, int limit, int expectedSize, String message);

    void findSingleMetadataItemTest(MetadataItem metadataItem, String message, int findSize, boolean bolThrowException);

    void findSingleMetadataItemEmptyFilterTest() throws MetadataStoreException, MetadataException;

    void getPermissionTest() throws MetadataException;

    void findMetadataItemWithFiltersTest();

    void checkHasReadQueryTest(String user, boolean bolHasRead, String message) throws MetadataStoreException, MetadataException;

    void checkHasWriteQueryTest(String user, boolean bolHasWrite, String message) throws MetadataStoreException, MetadataException;
}
