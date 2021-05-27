package org.iplantc.service.transfer.sftp;

import org.iplantc.service.transfer.AbstractPathResolutionTests;
import org.iplantc.service.transfer.RemoteDataClient;
import org.testng.annotations.Test;

@Test(groups={"unit"})
public class SftpRelayPathResolutionTest extends AbstractPathResolutionTests
{
	@Test(dataProvider="resolvePathProvider")
	@Override
	public void resolvePath(RemoteDataClient client, String beforePath,
			String resolvedPath, boolean shouldThrowException, String message) {
		super.abstractResolvePath(client, beforePath, resolvedPath, shouldThrowException, message);
	}

	@Override
	protected Class<? extends RemoteDataClient> getRemoteDataClientClass() {
		return SftpRelay.class;
	}

}
