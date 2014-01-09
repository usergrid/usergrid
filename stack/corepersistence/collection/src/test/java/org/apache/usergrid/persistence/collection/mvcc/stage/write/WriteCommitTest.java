package org.apache.usergrid.persistence.collection.mvcc.stage.write;


import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.usergrid.persistence.collection.CollectionScope;
import org.apache.usergrid.persistence.collection.mvcc.MvccEntitySerializationStrategy;
import org.apache.usergrid.persistence.collection.mvcc.MvccLogEntrySerializationStrategy;
import org.apache.usergrid.persistence.collection.mvcc.entity.MvccEntity;
import org.apache.usergrid.persistence.collection.mvcc.entity.MvccLogEntry;
import org.apache.usergrid.persistence.collection.mvcc.entity.Stage;
import org.apache.usergrid.persistence.collection.mvcc.stage.AbstractMvccEntityStageTest;
import org.apache.usergrid.persistence.collection.mvcc.stage.CollectionIoEvent;
import org.apache.usergrid.persistence.collection.mvcc.stage.TestEntityGenerator;
import org.apache.usergrid.persistence.model.entity.Entity;

import com.netflix.astyanax.MutationBatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.when;


/** @author tnine */
public class WriteCommitTest extends AbstractMvccEntityStageTest {

    /** Standard flow */
    @Test
    public void testStartStage() throws Exception {


        final CollectionScope context = mock( CollectionScope.class );


        //mock returning a mock mutation when we do a log entry write
        final MvccLogEntrySerializationStrategy logStrategy = mock( MvccLogEntrySerializationStrategy.class );

        final ArgumentCaptor<MvccLogEntry> logEntry = ArgumentCaptor.forClass( MvccLogEntry.class );

        final MutationBatch logMutation = mock( MutationBatch.class );

        when( logStrategy.write( same( context ), logEntry.capture() ) ).thenReturn( logMutation );


        //mock up the serialization call for the entity and returning the mutation
        final MvccEntitySerializationStrategy mvccEntityStrategy = mock( MvccEntitySerializationStrategy.class );

        final ArgumentCaptor<MvccEntity> mvccEntityCapture = ArgumentCaptor.forClass( MvccEntity.class );

        final MutationBatch mvccEntityMutation = mock( MutationBatch.class );

        when( mvccEntityStrategy.write( same( context ), mvccEntityCapture.capture() ) )
                .thenReturn( mvccEntityMutation );


        //set up the mock to return the entity from the start phase
        final Entity entity = TestEntityGenerator.generateEntity();

        final MvccEntity mvccEntityInput = TestEntityGenerator.fromEntity( entity );

        final UniqueValueSerializationStrategy uniqueValueStrategy = mock( UniqueValueSerializationStrategy.class );


        //run the stage
        WriteCommit newStage = new WriteCommit( logStrategy, mvccEntityStrategy, uniqueValueStrategy );


        Entity result = newStage.call( new CollectionIoEvent<MvccEntity>( context, mvccEntityInput ) );


        //verify the log entry is correct
        MvccLogEntry entry = logEntry.getValue();

        assertEquals( "id correct", entity.getId(), entry.getEntityId() );
        assertEquals( "version was not correct", entity.getVersion(), entry.getVersion() );
        assertEquals( "EventStage is correct", Stage.COMMITTED, entry.getStage() );


        MvccEntity written = mvccEntityCapture.getValue();

        //verify uuid and version in both the MvccEntity and the entity itself
        assertEquals( "version was correct", entity.getVersion(), written.getVersion() );
        assertSame( "Entity correct", entity, written.getEntity().get() );
        assertSame( "Entity Id is correct", entity.getId(), written.getId() );

        //now verify the output is correct

        assertSame( "Entity came from result", entity, result );
    }


    @Override
    protected void validateStage( final CollectionIoEvent<MvccEntity> event ) {
        /**
         * Write up mock mutations so we don't npe on the our operations, but rather on the input
         */
        final MvccLogEntrySerializationStrategy logStrategy = mock( MvccLogEntrySerializationStrategy.class );
        final MutationBatch logMutation = mock( MutationBatch.class );

        when( logStrategy.write( any( CollectionScope.class ), any( MvccLogEntry.class ) ) ).thenReturn( logMutation );


        final MvccEntitySerializationStrategy mvccEntityStrategy = mock( MvccEntitySerializationStrategy.class );

        final MutationBatch entityMutation = mock( MutationBatch.class );

        final UniqueValueSerializationStrategy uniqueValueStrategy = mock( UniqueValueSerializationStrategy.class );

        when( mvccEntityStrategy.write( any( CollectionScope.class ), any( MvccEntity.class ) ) )
                .thenReturn( entityMutation );

        new WriteCommit( logStrategy, mvccEntityStrategy, uniqueValueStrategy ).call( event );
    }
}


