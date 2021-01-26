package org.iplantc.service.io.resources;

import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.common.restlet.resource.AbstractAgaveServerResource;
import org.iplantc.service.common.uri.UrlPathEscaper;
import org.iplantc.service.io.Settings;
import org.iplantc.service.systems.model.RemoteSystem;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * @author dooley
 *
 */
public abstract class AbstractFileResource extends AbstractAgaveServerResource {

//	public String getPublicLink(String path) {
//		
//		return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_IO_SERVICE) + 
//				"media/" + UrlPathEscaper.escape(path == null ? "/" : path);
//	}
	
	/**
	 * Resolves an {@code absolutePath} to an Agave absolute path for the 
	 * given {@link RemoteSystem} by resolving the path against the 
	 * {@link StorageConfig#getRootDir()}
	 * 
	 * @param system
	 * @param absolutePath the absolute path of the file item on the remote system
	 * @return
	 */
	public String getPublicLink(RemoteSystem system, String absolutePath) {
		String resolvedPath = StringUtils.removeStart(absolutePath, system.getStorageConfig().getRootDir());
		resolvedPath = "/" + StringUtils.removeEnd(resolvedPath, "/"); 
		resolvedPath = resolvedPath.replaceAll("/+", "/");
		return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_IO_SERVICE) + 
				"media/system/" + system.getSystemId() + "/" + UrlPathEscaper.escape(resolvedPath);
	}
	
	public String getMetadataLink(RemoteSystem system, String uuid) {
		String query = "{\"associationIds\":\"" + uuid + "\"}";
		try {
			query = URLEncoder.encode(query, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			query = URLEncoder.encode(query);
		}
		return TenancyHelper.resolveURLToCurrentTenant(Settings.IPLANT_METADATA_SERVICE) + "data?q=" + query;
	}
	
	@Override
	public boolean isNegotiated() {
		return true;
	}
}