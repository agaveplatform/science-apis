package org.iplantc.service.metadata.dao;

import org.iplantc.service.metadata.exceptions.MetadataException;
import org.iplantc.service.metadata.exceptions.MetadataStoreException;

public interface IMetadataPermissionDaoIT {

    String getResourceUuid();

    void persistTest() throws MetadataException, MetadataStoreException;

    void deleteTest() throws MetadataException, MetadataStoreException;

    void getByUuidTest() throws MetadataException, MetadataStoreException;

    void getByUsernameAndUuidTest() throws MetadataException, MetadataStoreException;

    void getUuidOfAllSharedMetadataSchemaItemReadableByUserTest() throws MetadataException, MetadataStoreException;

    void insertAndUpdateTest() throws MetadataException, MetadataStoreException;
}
