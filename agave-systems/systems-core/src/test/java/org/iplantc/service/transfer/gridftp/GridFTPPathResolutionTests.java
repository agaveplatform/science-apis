package org.iplantc.service.transfer.gridftp;

import java.io.IOException;

import org.iplantc.service.transfer.AbstractPathResolutionTests;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.irods4.IRODS4;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;

@Test(enabled = false, groups= {"gridftp","path-resolution","broken"})
public class GridFTPPathResolutionTests extends AbstractPathResolutionTests
{
	@Override
	protected Class<? extends RemoteDataClient> getRemoteDataClientClass() {
		return GridFTP.class;
	}

	@Test(dataProvider="resolvePathProvider")
	@Override
	public void resolvePath(RemoteDataClient client, String beforePath,
							String resolvedPath, boolean shouldThrowException, String message) {
		super.abstractResolvePath(client, beforePath, resolvedPath, shouldThrowException, message);
	}
}
