package org.apache.usergrid.persistence.cassandra.util;


import java.util.UUID;

import org.apache.usergrid.utils.UUIDUtils;

import com.google.common.base.Objects;


/**
 * Simple struct holding timer information for an operation and an arbitrary tag for spanning 0 or more operations
 *
 * @author zznate
 */
public class TimedOpTag {

    private final UUID opTag;
    private final String traceTagName;
    private String tagName;
    private long elapsed = 0;
    private boolean status;


    private TimedOpTag( TraceTag trace ) {
        this.opTag = UUIDUtils.newTimeUUID();
        this.traceTagName = ( trace != null ? trace.getTraceName() : "-NONE-" );
    }


    /**
     * Get an instance with the current start timer set to 'now'
     *
     * @param traceTag can be null for single op timing
     */
    public static TimedOpTag instance( TraceTag traceTag ) {
        return new TimedOpTag( traceTag );
    }


    /** Apply tagName only if not already applied */
    public void stopAndApply( String tName, boolean opStatus ) {
        if ( elapsed == 0 ) {
            // extract from uuid and calculate
            elapsed = System.currentTimeMillis() - UUIDUtils.getTimestampInMillis( opTag );
        }
        if ( tName != null ) {
            this.tagName = tName;
            this.status = opStatus;
        }
    }


    /** Elapsed time of this op in milliseconds. */
    public long getElapsed() {
        return elapsed;
    }


    /**
     * The start time of this operation as represented by the timestamp embedded in the type-1 UUID of the opTag
     * property
     */
    public long getStart() {
        return UUIDUtils.getTimestampInMillis( opTag );
    }


    /** The tag for this specific operation */
    public UUID getOpTag() {
        return opTag;
    }


    /** A tag which may span 0 or more operations */
    public String getTraceTagName() {
        return traceTagName;
    }


    /** @return the tagName - null if never applied */
    public String getTagName() {
        return tagName;
    }


    /**
     * @return whether or not the operation was 'successful' Could still be false if {@link #stopAndApply(String,
     *         boolean)} was never called
     */
    public boolean getOpSuccessful() {
        return status;
    }


    @Override
    public String toString() {
        return Objects.toStringHelper( this ).add( "traceTag", traceTagName ).add( "opTag", opTag.toString() )
                      .add( "tagName", tagName ).add( "start", getStart() ).add( "elapsed", elapsed ).toString();
    }
}
