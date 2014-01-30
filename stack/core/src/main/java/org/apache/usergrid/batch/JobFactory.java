package org.apache.usergrid.batch;


import java.util.List;

import org.apache.usergrid.batch.repository.JobDescriptor;


/**
 * It is up to the implementation how many BulkJob instances to return, but this should be controled by the
 * BulkJobsBuilder
 *
 * @author zznate
 */
public interface JobFactory {

    /** Return one or more BulkJob ready for execution by a worker thread */
    List<Job> jobsFrom( JobDescriptor descriptor ) throws JobNotFoundException;
}
