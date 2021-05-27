/**
 *
 */
package org.iplantc.service.common.uri;

import java.net.URI;

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
	 * @param target the url to match
	 * @return true if the url matches an agave schema.
	 */
	public static boolean isInternalURI(URI target)
	{
	    return AgaveUriRegex.matchesAny(target);
	}
}
