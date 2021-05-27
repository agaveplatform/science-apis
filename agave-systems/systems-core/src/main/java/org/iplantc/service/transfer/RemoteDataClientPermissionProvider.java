package org.iplantc.service.transfer;

import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.irods.IRODS;
import org.iplantc.service.transfer.irods4.IRODS4;
import org.iplantc.service.transfer.model.RemoteFilePermission;
import org.iplantc.service.transfer.model.enumerations.PermissionType;

import java.io.IOException;
import java.util.List;

/**
 * Interface for {@link RemoteDataClient} that support permission mirroring from Agave to the remote system. This is
 * not supported by every supported {@link org.iplantc.service.systems.model.RemoteSystem} as some permissions models are not sufficiently robust to
 * enable the granularity that Agave provides. Thus, we provide this interface as a baseline for mapping Agave's
 * {@link RemoteFilePermission} to the remote system permissions when possible.
 * @see IRODS4
 * @see IRODS
 */
public interface RemoteDataClientPermissionProvider
{
    List<RemoteFilePermission> getAllPermissionsWithUserFirst(String path, String username) throws RemoteDataException, IOException;

    List<RemoteFilePermission> getAllPermissions(String path) throws RemoteDataException, IOException;

    PermissionType getPermissionForUser(String username, String path) throws RemoteDataException, IOException;

    boolean hasReadPermission(String path, String username) throws RemoteDataException, IOException;

    boolean hasWritePermission(String path, String username) throws RemoteDataException, IOException;

    boolean hasExecutePermission(String path, String username) throws RemoteDataException, IOException;

    void setPermissionForUser(String username, String path, PermissionType type, boolean recursive) throws RemoteDataException, IOException;

    void setOwnerPermission(String username, String path, boolean recursive) throws RemoteDataException, IOException;

    void setReadPermission(String username, String path, boolean recursive) throws RemoteDataException, IOException;

    void removeReadPermission(String username, String path, boolean recursive) throws RemoteDataException, IOException;

    void setWritePermission(String username, String path, boolean recursive) throws RemoteDataException, IOException;

    void removeWritePermission(String username, String path, boolean recursive) throws RemoteDataException, IOException;

    void setExecutePermission(String username, String path, boolean recursive) throws RemoteDataException, IOException;

    void removeExecutePermission(String username, String path, boolean recursive) throws RemoteDataException, IOException;

    void clearPermissions(String username, String path, boolean recursive) throws RemoteDataException, IOException;

    String getPermissions(String path) throws RemoteDataException, IOException;

    boolean isPermissionMirroringRequired();
}
