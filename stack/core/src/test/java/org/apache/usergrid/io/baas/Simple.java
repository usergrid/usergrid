package org.apache.usergrid.io.baas;


import org.apache.usergrid.clustering.hazelcast.HazelcastTest;
import org.apache.usergrid.persistence.TypedEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Simple extends TypedEntity {

    private static final Logger logger = LoggerFactory.getLogger( HazelcastTest.class );


    public Simple() {
        super();
        logger.info( "simple entity" );
    }
}
