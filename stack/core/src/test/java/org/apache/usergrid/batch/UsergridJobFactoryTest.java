package org.apache.usergrid.batch;


import java.util.List;
import java.util.UUID;

import org.apache.usergrid.batch.Job;
import org.apache.usergrid.batch.JobNotFoundException;
import org.apache.usergrid.batch.repository.JobDescriptor;
import org.apache.usergrid.cassandra.Concurrent;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/** @author zznate */
@Concurrent()
public class UsergridJobFactoryTest {

    private static UUID jobId = UUID.randomUUID();


    @Test
    public void verifyBuildup() throws JobNotFoundException {
        JobDescriptor jobDescriptor = new JobDescriptor( "", jobId, UUID.randomUUID(), null, null, null );


        List<Job> bulkJobs = BulkTestUtils.getBulkJobFactory().jobsFrom( jobDescriptor );
        assertNotNull( bulkJobs );
        assertEquals( 1, bulkJobs.size() );
    }
}
