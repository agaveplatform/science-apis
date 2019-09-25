/**
 *
 */
package org.iplantc.service.common.uri;

import java.net.URI;

import org.iplantc.service.common.exceptions.AgaveNamespaceException;

/**
 * Utility class to parse URLs and transform API urls into their various
 * API references.
 *
 * @author dooley
 *
 */
public class AgaveUriUtil {
    
	/**
	 * Determines whether the URI references an API resource within a given
	 * tenant. Since this may be called from worker processes, we need to
	 * explicitly provide the tenant info in the call. API resources are
	 * defined as those which have the <code>agave</code> schema or the URL
	 * is an service URL
	 *
	 * @param inputUri
	 * @return
	 * @throws AgaveNamespaceException
	 */
	public static boolean isInternalURI(URI inputUri) throws AgaveNamespaceException
	{
	    return AgaveUriRegex.matchesAny(inputUri);
	}
}
