package org.iplantc.service.monitor;

import org.iplantc.service.common.auth.VerifierFactory;
import org.iplantc.service.common.restlet.AgaveStatusService;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.restlet.Context;
import org.restlet.data.ChallengeScheme;
import org.restlet.ext.jaxrs.JaxRsApplication;
import org.restlet.security.ChallengeAuthenticator;
import org.restlet.security.Verifier;

public class ServletJaxRsApplication extends JaxRsApplication 
{
	private SchedulerFactory schedulerFactory = null;

	public SchedulerFactory getSchedulerFactory() {
		return schedulerFactory;
	}

	public ServletJaxRsApplication(Context context)
    {
        super(context);

		try {
			schedulerFactory = new StdSchedulerFactory("quartz.properties");
			Scheduler sched = schedulerFactory.getScheduler();
			sched.start();
		} catch (SchedulerException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		// add basic auth
  		Verifier verifier = new VerifierFactory().createVerifier(Settings.AUTH_SOURCE);
  		ChallengeAuthenticator authenticator = new ChallengeAuthenticator(context,
  				 ChallengeScheme.HTTP_BASIC, "The Agave Platform");
		authenticator.setVerifier(verifier);

		add(new MonitorApplication());
		setStatusService(new AgaveStatusService());

		authenticator.setNext(this);
		this.setAuthenticator(authenticator);
    }
}