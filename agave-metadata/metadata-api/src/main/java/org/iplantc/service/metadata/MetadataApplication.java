package org.iplantc.service.metadata;

import com.mongodb.MongoClient;
import org.apache.log4j.Logger;
import org.iplantc.service.common.restlet.AgaveApplication;
import org.iplantc.service.metadata.persistence.MongoConnector;
import org.iplantc.service.metadata.resources.*;
import org.restlet.Restlet;
import org.restlet.Router;
import org.restlet.util.Template;

/**
 * Created with IntelliJ IDEA.
 * User: wcs
 * Date: 7/30/13
 * Time: 2:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class MetadataApplication extends AgaveApplication 
{
	private static final Logger log = Logger.getLogger(MetadataApplication.class);
	
	private MongoClient mongoClient = null;
	
    /**
     * Creates a root Restlet that will receive all incoming calls.
     */
    @Override
    public synchronized Restlet createRoot() 
    {
        Router router = (Router)super.createRoot();
        router.setDefaultMatchingMode(Template.MODE_STARTS_WITH);
        return router;
    }

    @Override
    protected void mapServiceEndpoints(Router router) 
    {
        router.attach(getStandalonePrefix() + "/", MetadataDocumentationResource.class);
        
        if (!Settings.SLAVE_MODE) 
        {
        	secureEndpoint(router, "/schemas", MetadataSchemaCollection.class);
            secureEndpoint(router, "/schemas/", MetadataSchemaCollection.class);
            secureEndpoint(router, "/schemas/{schemaId}", MetadataSchemaResource.class);
            secureEndpoint(router, "/schemas/{schemaId}/", MetadataSchemaResource.class);
            secureEndpoint(router, "/schemas/{schemaId}/pems", MetadataSchemaShareResource.class);
            secureEndpoint(router, "/schemas/{schemaId}/pems/", MetadataSchemaShareResource.class);
            secureEndpoint(router, "/schemas/{schemaId}/pems/{user}", MetadataSchemaShareResource.class);
            secureEndpoint(router, "/schemas/{schemaId}/pems/{user}/", MetadataSchemaShareResource.class);
            secureEndpoint(router, "/data", MetadataCollection.class);
            secureEndpoint(router, "/data/", MetadataCollection.class);
            secureEndpoint(router, "/data/{uuid}", MetadataResource.class);
            secureEndpoint(router, "/data/{uuid}/", MetadataResource.class);
            secureEndpoint(router, "/data/{uuid}/pems", MetadataShareResource.class);
            secureEndpoint(router, "/data/{uuid}/pems/", MetadataShareResource.class);
            secureEndpoint(router, "/data/{uuid}/pems/{user}", MetadataShareResource.class);
            secureEndpoint(router, "/data/{uuid}/pems/{user}/", MetadataShareResource.class);
        }

    }
    
    @Override
	protected String getStandalonePrefix() {
		return !isStandaloneMode() ? "" : "/meta";
	}

    public MongoClient getMongoClient()
    {
        if (mongoClient == null )
        {
            this.mongoClient = MongoConnector.CONNECTION.getClient();
        }
        return mongoClient;
    }
}