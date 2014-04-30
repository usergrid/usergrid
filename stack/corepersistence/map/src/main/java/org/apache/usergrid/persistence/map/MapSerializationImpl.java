package org.apache.usergrid.persistence.map;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.codehaus.jackson.smile.SmileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;

import org.apache.usergrid.persistence.core.astyanax.ColumnNameIterator;
import org.apache.usergrid.persistence.core.astyanax.IdRowCompositeSerializer;
import org.apache.usergrid.persistence.core.astyanax.MultiTennantColumnFamily;
import org.apache.usergrid.persistence.core.astyanax.MultiTennantColumnFamilyDefinition;
import org.apache.usergrid.persistence.core.astyanax.OrganizationScopedRowKeySerializer;
import org.apache.usergrid.persistence.core.astyanax.ScopedRowKey;
import org.apache.usergrid.persistence.core.astyanax.StringColumnParser;
import org.apache.usergrid.persistence.core.migration.Migration;
import org.apache.usergrid.persistence.core.scope.OrganizationScope;
import org.apache.usergrid.persistence.graph.serialization.CassandraConfig;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;


/**
 *TODO: create a mapfig that will contain what you need and the upper size limit for an entity
 *TODO: create some validator methods?
 */
@Singleton
public class MapSerializationImpl<T> implements MapSerialization<T>,Migration {

    private static final Logger logger = LoggerFactory.getLogger( MapSerializationImpl.class );


    private static final byte[] HOLDER = new byte[] { 0 };

    //TODO: this needs to be upgraded to the fasterxml packages.

    SmileFactory f = new SmileFactory();
    org.codehaus.jackson.map.ObjectMapper objectMapper = new org.codehaus.jackson.map.ObjectMapper( f );


    //row key serializers
    private static final IdRowCompositeSerializer ID_SER = IdRowCompositeSerializer.get();
    private static final OrganizationScopedRowKeySerializer<Id> ROW_KEY_SER =
            new OrganizationScopedRowKeySerializer<Id>( ID_SER );

    private static final StringSerializer STRING_SERIALIZER = StringSerializer.get();


    private static final StringColumnParser PARSER = StringColumnParser.get();

    /**
     * CFs where the row key contains the source node id
     */
    private static final MultiTennantColumnFamily<OrganizationScope, Id, String> CF_SOURCE_MAP =
            new MultiTennantColumnFamily<OrganizationScope, Id, String>( "Maps", ROW_KEY_SER, STRING_SERIALIZER );

    protected final Keyspace keyspace;
    private final CassandraConfig cassandraConfig;
    private final MapFig mapFig;


    @Inject
    public MapSerializationImpl( final Keyspace keyspace, final CassandraConfig cassandraConfig, final MapFig mapFig ) {
        this.keyspace = keyspace;
        this.cassandraConfig = cassandraConfig;
        this.mapFig = mapFig;
    }


    @Override
    public MutationBatch writeMap( final MapScope mapScope, final String key, final T element ) {

        byte[] objectSerialized = null;

        try {
            objectSerialized = objectMapper.writeValueAsBytes( element );
            if ( objectSerialized.length > mapFig.getObjectSizeLimit() ) {
                logger.error( "Object is too large to be stored" );
                throw new RuntimeException( "Object is too large to be stored" );
            }
        }
        catch ( IOException e ) {
            logger.error(e.getMessage());
            //TODO: this needs to have logging inside of all the mapping impls.
            throw new RuntimeException( e );
        }

        MutationBatch batch = keyspace.prepareMutationBatch().withConsistencyLevel( cassandraConfig.getWriteCL() );

        final ScopedRowKey<OrganizationScope, Id> sourceKey = new ScopedRowKey<OrganizationScope, Id>( mapScope, mapScope.getOwner() );

        batch.withRow( CF_SOURCE_MAP, sourceKey ).putColumn( key, objectSerialized );

        return batch;
    }


    @Override
    public MutationBatch removeMapEntityFromSource( final MapScope mapScope, final String key ) {
        final ScopedRowKey<OrganizationScope, Id> rowKey = new ScopedRowKey<OrganizationScope, Id>( mapScope, mapScope.getOwner() );

        final MutationBatch batch = keyspace.prepareMutationBatch();

        batch.withRow( CF_SOURCE_MAP, rowKey ).deleteColumn( key );

        return batch;
    }


    @Override
    public Object getMapEntityFromSource( final MapScope mapScope, final String key ) {

        final ScopedRowKey<OrganizationScope, Id> sourceKey =
                new ScopedRowKey<OrganizationScope, Id>( mapScope, mapScope.getOwner() );

        final RangeBuilder rangeBuilder = new RangeBuilder().setLimit( cassandraConfig.getScanPageSize() );

        RowQuery<ScopedRowKey<OrganizationScope, Id>, String> query =
                keyspace.prepareQuery( CF_SOURCE_MAP ).getKey( sourceKey ).autoPaginate( true )
                        .withColumnRange( rangeBuilder.build() );

        Column<String> result;

        try {
            result = keyspace.prepareQuery( CF_SOURCE_MAP ).getKey( sourceKey )
                             .getColumn( key ).execute().getResult();
        }
        catch ( NotFoundException nfe ) {
            return null;
        }
        catch ( ConnectionException e ) {
            throw new RuntimeException( "Unable to connect to cassandra", e );
        }
        //TODO: is the query string the key that we want back? How do we get items out of this wacky thing.
        //TODO:why are we cutting out the timeouts of columnNameIterator

        byte[] data = result.getByteArrayValue();
        //TODO:

        //final MutationBatch batch = keyspace.prepareMutationBatch();
        //

        // batch.withRow( CF_SOURCE_MAP, sourceKey ).deleteColumn( columnNameIterator.next() );


        return new ColumnNameIterator<String, String>( query, PARSER, false );
    }


    @Override
    public Collection<MultiTennantColumnFamilyDefinition> getColumnFamilies() {

        return Arrays.asList( mapCf() );
    }


    /**
     * Helper to generate an edge definition by the type
     */
    private MultiTennantColumnFamilyDefinition mapCf() {
        return new MultiTennantColumnFamilyDefinition( CF_SOURCE_MAP, BytesType.class.getSimpleName(), UTF8Type.class.getSimpleName(), BytesType.class.getSimpleName(),
                MultiTennantColumnFamilyDefinition.CacheOption.KEYS );
    }


    private static final class ObjectSerializer<Serializable> extends AbstractSerializer<Serializable> {

        @Override
        public ByteBuffer toByteBuffer( final Serializable obj ) {
            return null;
        }


        @Override
        public Serializable fromByteBuffer( final ByteBuffer byteBuffer ) {
            return null;
        }
    }
}
