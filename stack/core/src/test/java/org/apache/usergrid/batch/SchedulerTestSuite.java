package org.apache.usergrid.batch;


import org.apache.usergrid.batch.AppArgsTest;
import org.apache.usergrid.batch.BulkJobExecutionUnitTest;
import org.apache.usergrid.batch.UsergridJobFactoryTest;
import org.apache.usergrid.cassandra.Concurrent;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses(
        {
                AppArgsTest.class, UsergridJobFactoryTest.class, BulkJobExecutionUnitTest.class,
        })
@Concurrent()
public class SchedulerTestSuite {}
