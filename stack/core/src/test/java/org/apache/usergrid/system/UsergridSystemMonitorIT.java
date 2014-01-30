package org.apache.usergrid.system;


import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.apache.commons.lang.StringUtils;
import org.apache.usergrid.CoreITSuite;
import org.apache.usergrid.cassandra.Concurrent;
import org.apache.usergrid.system.UsergridSystemMonitor;
import org.apache.usergrid.utils.MapUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/** @author zznate */
@Concurrent
public class UsergridSystemMonitorIT {
    private UsergridSystemMonitor usergridSystemMonitor;


    @Before
    public void setupLocal() {
        usergridSystemMonitor = CoreITSuite.cassandraResource.getBean( UsergridSystemMonitor.class );
    }


    @Test
    public void testVersionNumber() {
        assertEquals( "0.1", usergridSystemMonitor.getBuildNumber() );
    }


    @Test
    public void testIsCassandraAlive() {
        assertTrue( usergridSystemMonitor.getIsCassandraAlive() );
    }


    @Test
    public void verifyLogDump() {
        String str = UsergridSystemMonitor.formatMessage( 1600L, MapUtils.hashMap( "message", "hello" ) );

        assertTrue( StringUtils.contains( str, "hello" ) );

        usergridSystemMonitor.maybeLogPayload( 16000L, "foo", "bar", "message", "some text" );
        usergridSystemMonitor.maybeLogPayload( 16000L, new Date() );
    }
}
