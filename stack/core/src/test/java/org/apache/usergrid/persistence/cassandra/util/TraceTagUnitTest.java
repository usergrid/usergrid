package org.apache.usergrid.persistence.cassandra.util;


import org.apache.usergrid.persistence.cassandra.util.Slf4jTraceTagReporter;
import org.apache.usergrid.persistence.cassandra.util.TaggedOpTimer;
import org.apache.usergrid.persistence.cassandra.util.TimedOpTag;
import org.apache.usergrid.persistence.cassandra.util.TraceTag;
import org.apache.usergrid.persistence.cassandra.util.TraceTagManager;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/** @author zznate */
public class TraceTagUnitTest {

    private TraceTagManager traceTagManager;
    private Slf4jTraceTagReporter traceTagReporter;
    private TaggedOpTimer taggedOpTimer;


    @Before
    public void setup() {
        traceTagManager = new TraceTagManager();
        traceTagReporter = new Slf4jTraceTagReporter();
        taggedOpTimer = new TaggedOpTimer( traceTagManager );
    }


    @Test
    public void createAttachDetach() throws Exception {
        TraceTag traceTag = traceTagManager.create( "testtag1" );
        traceTagManager.attach( traceTag );
        TimedOpTag timedOpTag = ( TimedOpTag ) taggedOpTimer.start( "op-tag-name" );
        Thread.currentThread().sleep( 500 );
        taggedOpTimer.stop( timedOpTag, "op-tag-name", true );
        assertTrue( timedOpTag.getElapsed() >= 500 );
        assertEquals( timedOpTag, traceTag.iterator().next() );
        traceTagManager.detach();
    }
}
