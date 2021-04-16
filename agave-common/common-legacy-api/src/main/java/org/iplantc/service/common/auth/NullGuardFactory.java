package org.iplantc.service.common.auth;

import org.restlet.Context;
import org.restlet.Guard;
import org.restlet.data.ChallengeScheme;
import org.restlet.data.Method;

import java.util.List;

public class NullGuardFactory implements GuardFactory 
{
	public Guard createGuard(Context context, ChallengeScheme scheme,
			String realm, List<Method> unprotectedMethods)
	{
		return new NullGuard(context, scheme, realm, unprotectedMethods);
	}
	

}
