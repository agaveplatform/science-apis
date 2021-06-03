/**
 * 
 */
package org.iplantc.service.io;

import org.apache.log4j.Logger;
import org.iplantc.service.common.persistence.JndiSetup;
import org.iplantc.service.io.manager.FilesTransferListener;
import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.data.CharacterSet;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.routing.Router;
import org.restlet.service.MetadataService;

/**
 * @author dooley
 *
 */
public class FilesEventListenerApplication extends Application
{

	private static final Logger log = Logger.getLogger(FilesEventListenerApplication.class);

	public FilesEventListenerApplication() {
		super();
		setName("agaveFilesEventListener");
		
		MetadataService metadataService = new MetadataService();
    	metadataService.setDefaultCharacterSet(CharacterSet.UTF_8);
    	metadataService.setDefaultMediaType(MediaType.APPLICATION_JSON);
    	setMetadataService(metadataService);

    }

	@Override
	public Restlet createInboundRoot() {
        
    	Router router = new Router(getContext());


    	// Define the router for the static usage page
    	router.attach("/healthz", new Restlet(){

			public String getAttribute(String s) {
				return null;
			}

			public Representation handle() {
				return new StringRepresentation("OK");
			}

			public void setAttribute(String s, Object o) {

			}
		});

		return router;
    }

	public static void main(String[] args) throws Exception 
	{	
		JndiSetup.init();
		
//		Component component = new Component();
//        component.getServers().add(Protocol.HTTP, 80);
//        component.getDefaultHost().attach("/", new FilesEventListenerApplication());
//        component.start();

		Runnable r = new Runnable() {
			/**
			 * When an object implementing interface <code>Runnable</code> is used
			 * to create a thread, starting the thread causes the object's
			 * <code>run</code> method to be called in that separately executing
			 * thread.
			 * <p>
			 * The general contract of the method <code>run</code> is that it may
			 * take any action whatsoever.
			 *
			 * @see Thread#run()
			 */
			@Override
			public void run() {
				FilesTransferListener listener = new FilesTransferListener();
				try {
					listener.start();
				} catch (Throwable t) {
					try {
						listener.stop();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		};

		r.run();

    }
	
}
