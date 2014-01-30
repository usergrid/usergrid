package org.apache.usergrid.rest.applications.queues;


import java.util.List;

import org.apache.usergrid.cassandra.Concurrent;
import org.apache.usergrid.rest.TestContextSetup;
import org.apache.usergrid.rest.test.resource.app.queue.Queue;
import org.apache.usergrid.rest.test.resource.app.queue.Transaction;
import org.apache.usergrid.utils.MapUtils;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.BiMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


@Concurrent()
public class QueueResourceLong1IT extends AbstractQueueResourceIT {

    @Rule
    public TestContextSetup context = new TestContextSetup( this );


    @Test
    public void transactionTimeout() throws InterruptedException {

        Queue queue = context.application().queues().queue( "org.apache.usergrid.test" );

        final int count = 2;

        for ( int i = 0; i < count; i++ ) {
            queue.post( MapUtils.hashMap( "id", i ) );
        }

        // now consume and make sure we get each message. We should receive each
        // message, and we'll use this for comparing results later
        final long timeout = 5000;

        queue = queue.withTimeout( timeout );

        TransactionResponseHandler transHandler = new TransactionResponseHandler( count );

        testMessages( queue, transHandler, new NoLastCommand() );

        long start = System.currentTimeMillis();

        transHandler.assertResults();

        List<String> originalMessageIds = transHandler.getMessageIds();
        BiMap<String, String> transactionInfo = transHandler.getTransactionToMessageId();

        // now read again, we shouldn't have any results because our timeout hasn't
        // lapsed
        IncrementHandler incrementHandler = new IncrementHandler( 0 );

        testMessages( queue, incrementHandler, new NoLastCommand() );

        incrementHandler.assertResults();

        // now sleep until our timeout expires
        Thread.sleep( timeout - ( System.currentTimeMillis() - start ) );

        // now re-read our messages, we should get them all again
        transHandler = new TransactionResponseHandler( count );

        testMessages( queue, transHandler, new NoLastCommand() );

        start = System.currentTimeMillis();

        transHandler.assertResults();

        List<String> returned = transHandler.getMessageIds();

        assertTrue( returned.size() > 0 );

        // compare the replayed messages and the make sure they're in the same order
        BiMap<String, String> newTransactions = transHandler.getTransactionToMessageId();

        for ( int i = 0; i < originalMessageIds.size(); i++ ) {
            // check the messages come back in the same order, they should
            assertEquals( originalMessageIds.get( i ), returned.get( i ) );

            assertNotNull( transactionInfo.get( originalMessageIds.get( i ) ) );
        }

        // sleep again before testing a second timeout
        Thread.sleep( timeout - ( System.currentTimeMillis() - start ) );
        // now re-read our messages, we should get them all again
        transHandler = new TransactionResponseHandler( count );

        testMessages( queue, transHandler, new NoLastCommand() );

        start = System.currentTimeMillis();

        transHandler.assertResults();

        returned = transHandler.getMessageIds();

        assertTrue( returned.size() > 0 );

        // compare the replayed messages and the make sure they're in the same order
        newTransactions = transHandler.getTransactionToMessageId();

        for ( int i = 0; i < originalMessageIds.size(); i++ ) {
            // check the messages come back in the same order, they should
            assertEquals( originalMessageIds.get( i ), returned.get( i ) );

            assertNotNull( transactionInfo.get( originalMessageIds.get( i ) ) );

            // ack the transaction we were returned
            Transaction transaction =
                    queue.transactions().transaction( newTransactions.get( originalMessageIds.get( i ) ) );
            transaction.delete();
        }

        // now sleep again we shouldn't have any messages since we acked all the
        // transactions
        Thread.sleep( timeout - ( System.currentTimeMillis() - start ) );

        incrementHandler = new IncrementHandler( 0 );

        testMessages( queue, incrementHandler, new NoLastCommand() );

        incrementHandler.assertResults();
    }
}
