/**
 * 
 */
package org.iplantc.service.io;

import org.apache.log4j.Logger;
import org.iplantc.service.common.persistence.JndiSetup;
import org.iplantc.service.io.manager.FilesTransferListener;
import org.restlet.Server;
import org.restlet.data.Protocol;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author dooley
 *
 */
public class FilesEventListenerApplication extends ServerResource
{

	private static final Logger log = Logger.getLogger(FilesEventListenerApplication.class);
	ExecutorService executor = Executors.newCachedThreadPool();

	@Get
	public String healthzRoute() {
		return "ok";
	}

	public FilesEventListenerApplication() {
		super();

		setName("FileEventListener");
    }


	public static void main(String[] args) throws Exception
	{
		Server server;
		ExecutorService executor = Executors.newCachedThreadPool();

		JndiSetup.init();

		try {
			FilesTransferListener listener = new FilesTransferListener();
			executor.submit(listener);

			server = new Server(Protocol.HTTP, 80, FilesEventListenerApplication.class);
			server.start();
		}
		catch (Throwable e) {
			log.info("Shutdown request received. Cleaning up listeners...");
			log.error(e);
			executor.shutdown();
			executor.awaitTermination(30, TimeUnit.SECONDS);
		}
	}
	
}
