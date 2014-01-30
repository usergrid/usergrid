package org.apache.usergrid.persistence.cassandra.util;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;


/** @author zznate */
public class TraceTag implements Iterable<TimedOpTag> {

    private final UUID tag;
    private final String name;
    private final String traceName;
    private final List<TimedOpTag> timedOps;
    private final boolean metered;


    private TraceTag( UUID tag, String name, boolean metered ) {
        this.tag = tag;
        this.name = name;
        this.metered = metered;
        traceName = new StringBuilder( this.tag.toString() ).append( "-" ).append( this.metered ).append( "-" )
                                                            .append( this.name ).toString();
        timedOps = new ArrayList<TimedOpTag>();
    }


    public static TraceTag getInstance( UUID tag, String name ) {
        return new TraceTag( tag, name, false );
    }


    public static TraceTag getMeteredInstance( UUID tag, String name ) {
        return new TraceTag( tag, name, true );
    }


    public String getTraceName() {
        return traceName;
    }


    public void add( TimedOpTag timedOpTag ) {
        timedOps.add( timedOpTag );
    }


    public boolean getMetered() {
        return metered;
    }


    @Override
    public String toString() {
        return getTraceName();
    }


    @Override
    public Iterator iterator() {
        return timedOps.iterator();
    }


    /** The number of {@link TimedOpTag} instances currently held */
    public int getOpCount() {
        return timedOps.size();
    }


    /** Remove the currently held {@link TimedOpTag} instances */
    public void removeOps() {
        timedOps.clear();
    }
}
