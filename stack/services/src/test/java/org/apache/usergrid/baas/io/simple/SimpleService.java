package org.apache.usergrid.baas.io.simple;


import org.apache.usergrid.services.AbstractCollectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimpleService extends AbstractCollectionService {

    private static final Logger logger = LoggerFactory.getLogger( SimpleService.class );


    public SimpleService() {
        super();
        logger.info( "/simple" );
    }
}
