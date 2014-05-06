package org.apache.usergrid.persistence.map;

import org.safehaus.guicyfig.Default;
import org.safehaus.guicyfig.GuicyFig;
import org.safehaus.guicyfig.Key;

public interface MapFig extends GuicyFig {

    public static final String READ_TIMEOUT = "usergrid.graph.read.timeout";

    /**
     * Get the read timeout (in milliseconds) that we should allow when reading from the data source
     */
    @Default("10000")
    @Key(READ_TIMEOUT)
    int getReadTimeout();

    //5MB limit for objects
    @Default( "5242880" )
    int getObjectSizeLimit();

}