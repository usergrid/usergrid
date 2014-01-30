package org.apache.usergrid.batch.repository;


import java.util.UUID;

import org.apache.usergrid.batch.service.JobRuntimeService;
import org.apache.usergrid.persistence.TypedEntity;
import org.apache.usergrid.persistence.entities.JobData;
import org.apache.usergrid.persistence.entities.JobStat;

import me.prettyprint.cassandra.utils.Assert;


/**
 * @author zznate
 * @author tnine
 */
public class JobDescriptor extends TypedEntity {

    private final String jobName;
    private final UUID jobId;
    private final UUID transactionId;
    private final JobData data;
    private final JobStat stats;
    private final JobRuntimeService runtime;


    public JobDescriptor( String jobName, UUID jobId, UUID transactionId, JobData data, JobStat stats,
                          JobRuntimeService runtime ) {
        Assert.notNull( jobName, "Job name cannot be null" );
        Assert.notNull( jobId != null, "A JobId is required" );
        Assert.notNull( transactionId != null, "A transactionId is required" );
        Assert.notNull( data != null, "Data is required" );
        Assert.notNull( stats != null, "Stats are required" );
        Assert.notNull( runtime != null, "A scheduler is required" );

        this.jobName = jobName;
        this.jobId = jobId;
        this.transactionId = transactionId;
        this.data = data;
        this.stats = stats;
        this.runtime = runtime;
    }


    /** @return the jobName */
    public String getJobName() {
        return jobName;
    }


    /** @return the jobId */
    public UUID getJobId() {
        return jobId;
    }


    /** @return the transactionId */
    public UUID getTransactionId() {
        return transactionId;
    }


    /** @return the data */
    public JobData getData() {
        return data;
    }


    /** @return the scheduler */
    public JobRuntimeService getRuntime() {
        return runtime;
    }


    /** @return the stats */
    public JobStat getStats() {
        return stats;
    }
}
