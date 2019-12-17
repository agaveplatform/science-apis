package org.iplantc.service.metadata.dao;

import org.iplantc.service.metadata.exceptions.MetadataException;

public interface IMetadataPermissionDaoIT {

    String getResourceUuid();

    void persistTest() throws MetadataException;

    void deleteTest() throws MetadataException;

    void getByUuidTest() throws MetadataException;

    void getByUsernameAndUuidTest() throws MetadataException;
}
