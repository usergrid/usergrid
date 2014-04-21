/*
 * Copyright 2014 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.usergrid.corepersistence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.locking.Lock;
import org.apache.usergrid.mq.Message;
import org.apache.usergrid.mq.QueueManager;
import org.apache.usergrid.mq.cassandra.QueueManagerFactoryImpl;
import org.apache.usergrid.persistence.ConnectedEntityRef;
import org.apache.usergrid.persistence.ConnectionRef;
import org.apache.usergrid.persistence.CounterResolution;
import org.apache.usergrid.persistence.DynamicEntity;
import org.apache.usergrid.persistence.Entity;
import org.apache.usergrid.persistence.EntityFactory;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.EntityRef;
import org.apache.usergrid.persistence.Identifier;
import org.apache.usergrid.persistence.IndexBucketLocator;
import org.apache.usergrid.persistence.Query;
import org.apache.usergrid.persistence.Results;
import org.apache.usergrid.persistence.RoleRef;
import org.apache.usergrid.persistence.Schema;
import org.apache.usergrid.persistence.SimpleEntityRef;
import org.apache.usergrid.persistence.SimpleRoleRef;
import org.apache.usergrid.persistence.TypedEntity;
import org.apache.usergrid.persistence.cassandra.ApplicationCF;
import org.apache.usergrid.persistence.cassandra.CassandraService;
import org.apache.usergrid.persistence.cassandra.CounterUtils;
import org.apache.usergrid.persistence.cassandra.EntityManagerFactoryImpl;
import org.apache.usergrid.persistence.cassandra.GeoIndexManager;
import org.apache.usergrid.persistence.cassandra.RelationManagerImpl;
import org.apache.usergrid.persistence.collection.CollectionScope;
import org.apache.usergrid.persistence.collection.EntityCollectionManager;
import org.apache.usergrid.persistence.collection.EntityCollectionManagerFactory;
import org.apache.usergrid.persistence.collection.OrganizationScope;
import org.apache.usergrid.persistence.collection.impl.CollectionScopeImpl;
import org.apache.usergrid.persistence.collection.impl.OrganizationScopeImpl;
import org.apache.usergrid.persistence.collection.migration.MigrationException;
import org.apache.usergrid.persistence.collection.migration.MigrationManager;
import org.apache.usergrid.persistence.entities.Application;
import org.apache.usergrid.persistence.entities.Event;
import org.apache.usergrid.persistence.entities.Group;
import org.apache.usergrid.persistence.entities.Role;
import org.apache.usergrid.persistence.entities.User;
import org.apache.usergrid.persistence.exceptions.DuplicateUniquePropertyExistsException;
import org.apache.usergrid.persistence.exceptions.RequiredPropertyNotFoundException;
import org.apache.usergrid.persistence.index.EntityCollectionIndex;
import org.apache.usergrid.persistence.index.EntityCollectionIndexFactory;
import org.apache.usergrid.persistence.index.utils.EntityMapUtils;
import org.apache.usergrid.persistence.model.entity.Id;
import org.apache.usergrid.persistence.model.entity.SimpleId;
import org.apache.usergrid.persistence.model.field.LongField;
import org.apache.usergrid.persistence.model.util.UUIDGenerator;
import org.apache.usergrid.persistence.schema.CollectionInfo;
import org.apache.usergrid.utils.ClassUtils;
import org.apache.usergrid.utils.UUIDUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.config.ConfigurationManager;
import com.yammer.metrics.annotation.Metered;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Arrays.asList;

import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.usergrid.locking.LockHelper.getUniqueUpdateLock;
import static org.apache.usergrid.persistence.Schema.COLLECTION_ROLES;
import static org.apache.usergrid.persistence.Schema.DICTIONARY_PERMISSIONS;
import static org.apache.usergrid.persistence.Schema.DICTIONARY_PROPERTIES;
import static org.apache.usergrid.persistence.Schema.DICTIONARY_SETS;
import static org.apache.usergrid.persistence.Schema.PROPERTY_CREATED;
import static org.apache.usergrid.persistence.Schema.PROPERTY_INACTIVITY;
import static org.apache.usergrid.persistence.Schema.PROPERTY_MODIFIED;
import static org.apache.usergrid.persistence.Schema.PROPERTY_NAME;
import static org.apache.usergrid.persistence.Schema.PROPERTY_TIMESTAMP;
import static org.apache.usergrid.persistence.Schema.PROPERTY_TYPE;
import static org.apache.usergrid.persistence.Schema.PROPERTY_UUID;
import static org.apache.usergrid.persistence.Schema.TYPE_APPLICATION;
import static org.apache.usergrid.persistence.Schema.TYPE_ROLE;
import static org.apache.usergrid.persistence.Schema.defaultCollectionName;
import static org.apache.usergrid.persistence.Schema.deserializeEntityProperties;
import static org.apache.usergrid.persistence.Schema.getDefaultSchema;
import static org.apache.usergrid.persistence.SimpleEntityRef.ref;
import static org.apache.usergrid.persistence.SimpleRoleRef.getIdForRoleName;
import static org.apache.usergrid.persistence.cassandra.ApplicationCF.ENTITY_COMPOSITE_DICTIONARIES;
import static org.apache.usergrid.persistence.cassandra.ApplicationCF.ENTITY_DICTIONARIES;
import static org.apache.usergrid.persistence.cassandra.ApplicationCF.ENTITY_ID_SETS;
import static org.apache.usergrid.persistence.cassandra.ApplicationCF.ENTITY_PROPERTIES;
import static org.apache.usergrid.persistence.cassandra.ApplicationCF.ENTITY_UNIQUE;
import static org.apache.usergrid.persistence.cassandra.CassandraPersistenceUtils.addDeleteToMutator;
import static org.apache.usergrid.persistence.cassandra.CassandraPersistenceUtils.addInsertToMutator;
import static org.apache.usergrid.persistence.cassandra.CassandraPersistenceUtils.addPropertyToMutator;
import static org.apache.usergrid.persistence.cassandra.CassandraPersistenceUtils.batchExecute;
import static org.apache.usergrid.persistence.cassandra.CassandraPersistenceUtils.key;
import static org.apache.usergrid.persistence.cassandra.CassandraPersistenceUtils.toStorableBinaryValue;
import static org.apache.usergrid.utils.ConversionUtils.getLong;
import static org.apache.usergrid.utils.ConversionUtils.string;
import static org.apache.usergrid.utils.ConversionUtils.uuid;
import static org.apache.usergrid.utils.UUIDUtils.getTimestampInMicros;
import static org.apache.usergrid.utils.UUIDUtils.isTimeBased;
import static org.apache.usergrid.utils.UUIDUtils.newTimeUUID;


public class CpEntityManager implements EntityManager {
    private static final Logger logger = LoggerFactory.getLogger( CpEntityManager.class );

    private static EntityCollectionManagerFactory ecmf;
    private static EntityCollectionIndexFactory ecif;
    private static EntityCollectionManager ecm;
    private static EntityCollectionIndex eci;


    private static final String SYSTEM_ORG_UUID = "b9b51240-b5d5-11e3-9ea8-11c207d6769a";
    private static final String SYSTEM_ORG_TYPE = "zzz_defaultapp_zzz";

    private static final String SYSTEM_APP_UUID = "b6768a08-b5d5-11e3-a495-10ddb1de66c4";
    private static final String SYSTEM_APP_TYPE = "zzz_defaultapp_zzz";

    private static final OrganizationScope SYSTEM_ORG_SCOPE =
            new OrganizationScopeImpl(
                    new SimpleId( UUID.fromString(SYSTEM_ORG_UUID), SYSTEM_ORG_TYPE ));

    private static final CollectionScope SYSTEM_APP_SCOPE =
            new CollectionScopeImpl(
                    SYSTEM_ORG_SCOPE.getOrganization(),
                    new SimpleId( UUID.fromString(SYSTEM_APP_UUID), SYSTEM_APP_TYPE ),
                    SYSTEM_APP_TYPE);

    private CollectionScope applicationScope = SYSTEM_APP_SCOPE;

    public static final ByteBufferSerializer be = new ByteBufferSerializer();
    public static final UUIDSerializer ue = new UUIDSerializer();
    public static final StringSerializer se = new StringSerializer();





    // TODO: eliminate need for a UUID to type map
    private final Map<UUID, String> typesByUuid = new HashMap<UUID, String>();
    private final Map<String, String> typesByCollectionNames = new HashMap<String, String>();

    @Resource
    private EntityManagerFactoryImpl emf;
    @Resource
    private QueueManagerFactoryImpl qmf;
    @Resource
    private IndexBucketLocator indexBucketLocator;

    @Resource
    private CassandraService cass;
    @Resource
    private CounterUtils counterUtils;

    private boolean skipAggregateCounters;

    public CpEntityManager() {
    }

    static {

        try {
            ConfigurationManager.loadCascadedPropertiesFromResources( "core-persistence" );

            // TODO: make CpEntityManager work in non-test environment
            Properties testProps = new Properties() {{
                put("cassandra.hosts", "localhost:" + System.getProperty("cassandra.rpc_port"));
            }};

            ConfigurationManager.loadProperties( testProps );

        } catch (IOException ex) {
            throw new RuntimeException("Error loading Core Persistence proprties", ex);
        }

        Injector injector = Guice.createInjector( new GuiceModule() );

        MigrationManager m = injector.getInstance( MigrationManager.class );
        try {
            m.migrate();
        } catch (MigrationException ex) {
            throw new RuntimeException("Error migrating Core Persistence", ex);
        }

        ecmf = injector.getInstance( EntityCollectionManagerFactory.class );
        ecif = injector.getInstance( EntityCollectionIndexFactory.class );
        ecm = ecmf.createCollectionManager( SYSTEM_APP_SCOPE );
        eci = ecif.createCollectionIndex( SYSTEM_APP_SCOPE );
    }


    public EntityManager init(
            EntityManagerFactoryImpl emf, CassandraService cass, CounterUtils counterUtils,
            UUID applicationId, boolean skipAggregateCounters ) {

        this.emf = emf;
        this.cass = cass;
        this.counterUtils = counterUtils;
        this.skipAggregateCounters = skipAggregateCounters;




        //Injector injector = Guice.createInjector( new GuiceModule() );
       // ecmf = injector.getInstance( EntityCollectionManagerFactory.class );
        // prime the application entity for the EM
        try {
            getApplication();
        }
        catch ( Exception ex ) {
            ex.printStackTrace();
        }
        return this;
    }


    CpEntityManager(CollectionScope collectionScope, EntityCollectionManager ecm, EntityCollectionIndex eci) {
        this.applicationScope = collectionScope;
        this.ecm = ecm;
        this.eci = eci;
    }


    @Override
    public Entity create(String entityType, Map<String, Object> properties) throws Exception {

        org.apache.usergrid.persistence.model.entity.Entity cpEntity =
            new org.apache.usergrid.persistence.model.entity.Entity(
                new SimpleId(UUIDGenerator.newTimeUUID(), entityType ));

        cpEntity = EntityMapUtils.fromMap( cpEntity, properties );
        cpEntity.setField(new LongField("created", cpEntity.getId().getUuid().timestamp()) );
        cpEntity.setField(new LongField("modified", cpEntity.getId().getUuid().timestamp()) );

        cpEntity = ecm.write( cpEntity ).toBlockingObservable().last();
        eci.index( cpEntity );

        Entity entity = new DynamicEntity( entityType, cpEntity.getId().getUuid() );
        entity.setUuid( cpEntity.getId().getUuid() );
        Map<String, Object> entityMap = EntityMapUtils.toMap( cpEntity );
        entity.addProperties( entityMap );

        typesByUuid.put( entity.getUuid(), entityType );

        return entity;
    }


    @Override
    public <A extends Entity> A create(
            String entityType, Class<A> entityClass, Map<String, Object> properties) throws Exception {

        //the keyword entity is derived from the Schema.java class
            if( ( entityType != null) && (entityType.startsWith( "entity") || entityType.startsWith ( "entities") ) ){
                throw new IllegalArgumentException( "Invalid entity type" );
            }
        A e = null;
        try {
            e = ( A ) create(entityType,properties); //create( entityType, ( Class<Entity> ) entityClass, properties,null);
        }catch( ClassCastException e1 ){
            logger.debug( "Unable to create typed entity", e1 );
        }
        return e;
    }


    @Override
    public <A extends TypedEntity> A create(A entity) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    @Override
    public Entity create( UUID importId, String entityType, Map<String, Object> properties) throws Exception {
        return create( entityType, properties );
    }


    @Override
    public Entity get( UUID entityId ) throws Exception {
        String type = typesByUuid.get( entityId );
        return get( entityId, type );
    }


    public Entity get( UUID entityId, String type ) throws Exception {

        Id id = new SimpleId( entityId, type );

        org.apache.usergrid.persistence.model.entity.Entity cpEntity =
            ecm.load( id ).toBlockingObservable().last();

        Entity entity = new DynamicEntity( type, cpEntity.getId().getUuid() );
        entity.setUuid( cpEntity.getId().getUuid() );
        Map<String, Object> entityMap = EntityMapUtils.toMap( cpEntity );
        entity.addProperties( entityMap );
        return entity;
    }

    @Override
    public Entity get(EntityRef entityRef) throws Exception {
        if ( entityRef == null ) {
            return null;
        }
        //return getEntity( entityRef.getUuid(), null );
        return get( entityRef.getUuid(), entityRef.getType() );
    }


    @Override
    public <A extends Entity> A get(UUID entityId, Class<A> entityClass) throws Exception {
        A e = null;
        try {
            e = ( A ) getEntity( entityId, ( Class<Entity> ) entityClass );
        }
        catch ( ClassCastException e1 ) {
            logger.error( "Unable to get typed entity: {} of class {}", new Object[] {entityId, entityClass.getCanonicalName(), e1} );
        }
        return e;
    }

    /**
     * Gets the specified entity.
     *
     * @param entityId the entity id
     * @param entityClass the entity class
     *
     * @return entity
     *
     * @throws Exception the exception
     */
    public <A extends Entity> A getEntity( UUID entityId, Class<A> entityClass ) throws Exception {

        Object entity_key = key( entityId );
        Map<String, Object> results = null;

        //gets the entity properties
        // if (entityType == null) {
        results = deserializeEntityProperties(
                cass.getAllColumns( cass.getApplicationKeyspace( getApplicationId() ), ENTITY_PROPERTIES, entity_key ) );
        // } else {
        // Set<String> columnNames = Schema.getPropertyNames(entityType);
        // results = getColumns(getApplicationKeyspace(applicationId),
        // EntityCF.PROPERTIES, entity_key, columnNames, se, be);
        // }

        if ( results == null ) {
            logger.warn( "getEntity(): No properties found for entity {}, probably doesn't exist...", entityId );
            return null;
        }

        UUID id = uuid( results.get( PROPERTY_UUID ) );
        String type = string( results.get( PROPERTY_TYPE ) );

        if ( !entityId.equals( id ) ) {

            logger.error( "Expected entity id {}, found {}. Returning null entity", new Object[]{entityId, id, new Throwable()} );
            return null;
        }

        A entity = EntityFactory.newEntity( id, type, entityClass );
        entity.setProperties( results );

        return entity;
    }


    @Override
    public Results get(Collection<UUID> entityIds, Results.Level resultsLevel) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    @Override
    public Results get(Collection<UUID> entityIds) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    @Override
    public Results get(Collection<UUID> entityIds, Class<? extends Entity> entityClass,
            Results.Level resultsLevel) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    @Override
    public Results get(Collection<UUID> entityIds, String entityType,
            Class<? extends Entity> entityClass, Results.Level resultsLevel) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    @Override
    public void update( Entity entity ) throws Exception {

        Id entityId = new SimpleId( entity.getUuid(), entity.getType() );

        org.apache.usergrid.persistence.model.entity.Entity cpEntity =
            ecm.load( entityId ).toBlockingObservable().last();

        cpEntity = EntityMapUtils.fromMap( cpEntity, entity.getProperties() );

        cpEntity = ecm.write( cpEntity ).toBlockingObservable().last();
        eci.index( cpEntity );
    }


    @Override
    public void delete(EntityRef entityRef) throws Exception {

        Id entityId = new SimpleId( entityRef.getUuid(), entityRef.getType() );

        org.apache.usergrid.persistence.model.entity.Entity entity =
            ecm.load( entityId ).toBlockingObservable().last();

        if ( entity != null ) {
            eci.deindex( entity );
            ecm.delete( entityId );
        }
    }


    @Override
    public Results searchCollection(EntityRef entityRef, String collectionName, Query query) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
//        String type = typesByCollectionNames.get( collectionName );
//		if ( type == null ) {
//			throw new RuntimeException(
//					"No type found for collection name: " + collectionName);
//		}
//
//        org.apache.usergrid.persistence.index.query.Results results = eci.execute( query );
//        return results;
    }


    @Override
    public void setApplicationId(UUID applicationId) {
        applicationScope.setOwner( new SimpleId( applicationId,applicationScope.getOwner().getType() ) );
        //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public GeoIndexManager getGeoIndexManager() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public EntityRef getApplicationRef() {
        return ref( applicationScope.getOwner().getType(), applicationScope.getOwner().getUuid() );
    }

    @Override
    public Application getApplication() throws Exception {

        Application application = get(applicationScope.getOwner().getUuid(),Application.class);
        return application;
    }

    @Override
    public void updateApplication(Application app) throws Exception {
        update( app );
        this.applicationScope = new CollectionScopeImpl( applicationScope.getOrganization(),
                new SimpleId( app.getUuid(),app.getType() ),app.getName());
    }

    @Override
    //depends on updateProperties.
    public void updateApplication(Map<String, Object> properties) throws Exception {
        //throw new UnsupportedOperationException("Not supported yet.");
    }
//Doesn't provide some of the needed Methods
//    @Override
//    public RelationManager getRelationManager(EntityRef entityRef) {
//        throw new UnsupportedOperationException("Not supported yet.");
//    }

    @Override
    public RelationManagerImpl getRelationManager( EntityRef entityRef ) {
        //RelationManagerImpl rmi = applicationContext.getBean(RelationManagerImpl.class);
        RelationManagerImpl rmi = new RelationManagerImpl();
        rmi.init( this, cass, getApplicationId(), entityRef, indexBucketLocator );
        return rmi;
    }

    @Override
    public Set<String> getApplicationCollections() throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Object> getApplicationCollectionMetadata() throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getApplicationCollectionSize(String collectionName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    @Override
    public void createApplicationCollection(String entityType) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public EntityRef getAlias(String aliasType, String alias) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public EntityRef getAlias(UUID ownerId, String collectionName, String aliasValue) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, EntityRef> getAlias(String aliasType, List<String> aliases) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, EntityRef> getAlias(UUID ownerId, String collectionName, List<String> aliases) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public EntityRef validate(EntityRef entityRef) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getType(UUID entityId) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public EntityRef getRef(UUID entityId) throws Exception {
        String entityType = getEntityType( entityId );
        if ( entityType == null ) {
            logger.warn( "Unable to get type for entity: {} ", new Object[] { entityId, new Exception()} );
            return null;
        }
        return ref( entityType, entityId );
    }

    /**
     * Gets the type.
     *
     * @param entityId the entity id
     *
     * @return entity type
     *
     * @throws Exception the exception
     */
    @Metered( group = "core", name = "EntityManager_getEntityType" )
    public String getEntityType( UUID entityId ) throws Exception {

        return get( entityId ).getType();
    }


    @Override
    public Object getProperty(EntityRef entityRef, String propertyName) throws Exception {
        Entity entity = loadPartialEntity( entityRef.getUuid(), propertyName );

        if ( entity == null ) {
            return null;
        }

        return entity.getProperty( propertyName );
    }

    @Override
    public List<Entity> getPartialEntities(Collection<UUID> ids, Collection<String> properties) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Object> getProperties(EntityRef entityRef) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setProperty(EntityRef entityRef, String propertyName, Object propertyValue) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setProperty(EntityRef entityRef, String propertyName,
            Object propertyValue, boolean override) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void updateProperties(EntityRef entityRef, Map<String, Object> properties) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteProperty(EntityRef entityRef, String propertyName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<Object> getDictionaryAsSet(EntityRef entityRef, String dictionaryName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    //TODO: this doesn't really add or do anything.
    @Override
    public void addToDictionary(EntityRef entityRef, String dictionaryName, Object elementValue) throws Exception {
        addToDictionary( entityRef, dictionaryName, elementValue, null );
    }

    @Override
    public void addToDictionary(EntityRef entityRef, String dictionaryName,
            Object elementName, Object elementValue) throws Exception {
        if ( elementValue == null ) {
            return;
        }
        ObjectMapper mapper = new ObjectMapper(  );


        ;
        ByteBuffer byteBuffer = be.fromBytes( mapper.writeValueAsBytes( elementValue ) );

        EntityRef entity = entityRef;//getRef( entityRef.getUuid() );

        UUID timestampUuid = newTimeUUID();
        Map properties;

        if(elementName instanceof String)
            properties = new HashMap<String, Object>();
        else
            properties = new HashMap<Object, Object>();

        //Map<Object,Object> derp = ( Map<Object, Object> ) elementValue;
        Map<String, Object> dictionaryNamedProperties = new HashMap<String,Object>();
        properties.put( elementName, byteBuffer);
        dictionaryNamedProperties.put( dictionaryName,properties );

        Entity ent = get( entityRef );
        ent.addProperties( dictionaryNamedProperties );

        update( ent );


    }

    @Override
    public void addSetToDictionary(EntityRef entityRef, String dictionaryName, Set<?> elementValues) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addMapToDictionary(EntityRef entityRef, String dictionaryName, Map<?, ?> elementValues) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<Object, Object> getDictionaryAsMap(EntityRef entityRef, String dictionaryName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Object getDictionaryElementValue(EntityRef entityRef, String dictionaryName, String elementName) throws Exception {
        Object value = null;

        ApplicationCF dictionaryCf = null;

        boolean entityHasDictionary = getDefaultSchema().hasDictionary( entityRef.getType(), dictionaryName );

        if ( entityHasDictionary ) {
            dictionaryCf = ENTITY_DICTIONARIES;
        }
        else {
            dictionaryCf = ENTITY_COMPOSITE_DICTIONARIES;
        }

        Class<?> dictionaryCoType = getDefaultSchema().getDictionaryValueType( entityRef.getType(), dictionaryName );
        boolean coTypeIsBasic = ClassUtils.isBasicType( dictionaryCoType );

        value = get(entityRef.getUuid());
        org.apache.usergrid.persistence.index.query.Query query = new org.apache.usergrid.persistence.index.query.Query(  );
        query.setQl( "where name = " + elementName );

        eci.execute( query );

       // get(entityRef);
       // AstyanaxKeyspaceProvider astyanaxKeyspaceProvider = new AstyanaxKeyspaceProvider( CassandraFig );

//        HColumn<ByteBuffer, ByteBuffer> result =
//                cass.getColumn( cass.getApplicationKeyspace( getApplicationId() ), dictionaryCf,
//                        key( entityRef.getUuid(), dictionaryName ),
//                        entityHasDictionary ? bytebuffer( elementName ) : DynamicComposite.toByteBuffer( elementName ),
//                        be, be );
//        if ( result != null ) {
//            if ( entityHasDictionary && coTypeIsBasic ) {
//                value = object( dictionaryCoType, result.getValue() );
//            }
//            else if ( result.getValue().remaining() > 0 ) {
//                value = Schema.deserializePropertyValueFromJsonBinary( result.getValue().slice(), dictionaryCoType );
//            }
//        }
//        else {
//            logger.info( "Results of EntityManagerImpl.getDictionaryElementValue is null" );
//        }

        return value;
    }

    @Override
    public void removeFromDictionary(EntityRef entityRef, String dictionaryName, Object elementValue) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getDictionaries(EntityRef entity) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Map<UUID, Set<String>>> getOwners(EntityRef entityRef) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isCollectionMember(EntityRef owner, String collectionName, EntityRef entity) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isConnectionMember(EntityRef owner, String connectionName, EntityRef entity) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getCollections(EntityRef entityRef) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Results getCollection(EntityRef entityRef, String collectionName, UUID startResult, int count, Results.Level resultsLevel, boolean reversed) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Results getCollection(UUID entityId, String collectionName, Query query, Results.Level resultsLevel) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Entity addToCollection(EntityRef entityRef, String collectionName, EntityRef itemRef) throws Exception {
        // TODO: eliminate need for typesByCollectionNames
        typesByCollectionNames.put( collectionName, entityRef.getType() );
        return get( entityRef );
    }

    @Override
    public Entity addToCollections(List<EntityRef> ownerEntities, String collectionName, EntityRef itemRef) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Entity createItemInCollection(EntityRef entityRef, String collectionName, String itemType, Map<String, Object> properties) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeFromCollection(EntityRef entityRef, String collectionName, EntityRef itemRef) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getCollectionIndexes(EntityRef entity, String collectionName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void copyRelationships(EntityRef srcEntityRef, String srcRelationName, EntityRef dstEntityRef, String dstRelationName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ConnectionRef createConnection(ConnectionRef connection) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ConnectionRef createConnection(EntityRef connectingEntity, String connectionType, EntityRef connectedEntityRef) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ConnectionRef createConnection(EntityRef connectingEntity, String pairedConnectionType, EntityRef pairedEntity, String connectionType, EntityRef connectedEntityRef) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ConnectionRef createConnection(EntityRef connectingEntity, ConnectedEntityRef... connections) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ConnectionRef connectionRef(EntityRef connectingEntity, String connectionType, EntityRef connectedEntityRef) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ConnectionRef connectionRef(EntityRef connectingEntity, String pairedConnectionType, EntityRef pairedEntity, String connectionType, EntityRef connectedEntityRef) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ConnectionRef connectionRef(EntityRef connectingEntity, ConnectedEntityRef... connections) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteConnection(ConnectionRef connectionRef) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getConnectionTypes(EntityRef ref) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Results getConnectedEntities(UUID entityId, String connectionType, String connectedEntityType, Results.Level resultsLevel) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Results getConnectingEntities(UUID entityId, String connectionType, String connectedEntityType, Results.Level resultsLevel) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Results getConnectingEntities(UUID uuid, String connectionType, String entityType, Results.Level level, int count) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Results searchConnectedEntities(EntityRef connectingEntity, Query query) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getConnectionIndexes(EntityRef entity, String connectionType) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, String> getRoles() throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void resetRoles() throws Exception {

        try {
            createRole( "admin", "Administrator", 0 );
        }
        catch ( Exception e ) {
            logger.error( "Could not create admin role, may already exist", e );
        }

        try {
            createRole( "default", "Default", 0 );
        }
        catch ( Exception e ) {
            logger.error( "Could not create default role, may already exist", e );
        }

        try {
            createRole( "guest", "Guest", 0 );
        }
        catch ( Exception e ) {
            logger.error( "Could not create guest role, may already exist", e );
        }

        try {
            grantRolePermissions( "default", Arrays.asList( "get,put,post,delete:/**" ) );
        }
        catch ( Exception e ) {
            logger.error( "Could not populate default role", e );
        }

        try {
            grantRolePermissions( "guest", Arrays.asList( "post:/users", "post:/devices", "put:/devices/*" ) );
        }
        catch ( Exception e ) {
            logger.error( "Could not populate guest role", e );
        }
    }
    public EntityRef roleRef( String roleName ) {
        return ref( TYPE_ROLE, getIdForRoleName( roleName ) );
    }

    @Override
    public Entity createRole(String roleName, String roleTitle, long inactivity) throws Exception {
        UUID timestampUuid = newTimeUUID();
        Mutator<ByteBuffer> batch = createMutator( cass.getApplicationKeyspace( getApplicationId() ), be );
        batchCreateRole( batch, null, roleName, roleTitle, inactivity, null, timestampUuid );
        batchExecute( batch, CassandraService.RETRY_COUNT );
        return get( roleRef( roleName ) );
    }

    @Override
    public void grantRolePermission(String roleName, String permission) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void grantRolePermissions(String roleName, Collection<String> permissions) throws Exception {
        roleName = roleName.toLowerCase();
        long timestamp = cass.createTimestamp();
        Mutator<ByteBuffer> batch = createMutator( cass.getApplicationKeyspace( getApplicationId() ), be );
        for ( String permission : permissions ) {
            permission = permission.toLowerCase();
            addInsertToMutator( batch, ApplicationCF.ENTITY_DICTIONARIES, getRolePermissionsKey( roleName ), permission,
                    ByteBuffer.allocate( 0 ), timestamp );
        }
        batchExecute( batch, CassandraService.RETRY_COUNT );
    }

    public Object getRolePermissionsKey( String roleName ) {
        return key( getIdForRoleName( roleName ), DICTIONARY_PERMISSIONS );
    }


    @Override
    public void revokeRolePermission(String roleName, String permission) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getRolePermissions(String roleName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteRole(String roleName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, String> getGroupRoles(UUID groupId) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Entity createGroupRole(UUID groupId, String roleName, long inactivity) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void grantGroupRolePermission(UUID groupId, String roleName, String permission) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void revokeGroupRolePermission(UUID groupId, String roleName, String permission) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getGroupRolePermissions(UUID groupId, String roleName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteGroupRole(UUID groupId, String roleName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getUserRoles(UUID userId) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addUserToRole(UUID userId, String roleName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeUserFromRole(UUID userId, String roleName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getUserPermissions(UUID userId) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void grantUserPermission(UUID userId, String permission) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void revokeUserPermission(UUID userId, String permission) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, String> getUserGroupRoles(UUID userId, UUID groupId) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addUserToGroupRole(UUID userId, UUID groupId, String roleName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeUserFromGroupRole(UUID userId, UUID groupId, String roleName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Results getUsersInGroupRole(UUID groupId, String roleName, Results.Level level) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void incrementAggregateCounters(UUID userId, UUID groupId, String category, String counterName, long value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Results getAggregateCounters(UUID userId, UUID groupId, String category, String counterName, CounterResolution resolution, long start, long finish, boolean pad) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Results getAggregateCounters(UUID userId, UUID groupId, UUID queueId, String category, String counterName, CounterResolution resolution, long start, long finish, boolean pad) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Results getAggregateCounters(Query query) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public EntityRef getUserByIdentifier(Identifier identifier) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public EntityRef getGroupByIdentifier(Identifier identifier) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getCounterNames() throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Long> getEntityCounters(UUID entityId) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Long> getApplicationCounters() throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void incrementAggregateCounters(UUID userId, UUID groupId, String category, Map<String, Long> counters) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isPropertyValueUniqueForEntity(String entityType, String propertyName, Object propertyValue) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <A extends Entity> A get(EntityRef entityRef, Class<A> entityClass) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Role> getRolesWithTitles(Set<String> roleNames) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getRoleTitle(String roleName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Role> getUserRolesWithTitles(UUID userId) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Role> getGroupRolesWithTitles(UUID userId) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addGroupToRole(UUID userId, String roleName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeGroupFromRole(UUID userId, String roleName) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getGroupPermissions(UUID groupId) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void grantGroupPermission(UUID groupId, String permission) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void revokeGroupPermission(UUID groupId, String permission) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <A extends Entity> A batchCreate(Mutator<ByteBuffer> m, String entityType, Class<A> entityClass, Map<String, Object> properties, UUID importId, UUID timestampUuid) throws Exception {
        String eType = Schema.normalizeEntityType( entityType );

        Schema schema = getDefaultSchema();

        boolean is_application = TYPE_APPLICATION.equals( eType );

        if ( ( ( getApplicationId() == null ) || getApplicationId().equals( UUIDUtils.ZERO_UUID ) ) && !is_application ) {
            return null;
        }

        long timestamp = getTimestampInMicros( timestampUuid );

        UUID itemId = UUIDUtils.newTimeUUID();

        if ( is_application ) {
            itemId = getApplicationId();
        }
        if ( importId != null ) {
            itemId = importId;
        }
        boolean emptyPropertyMap = false;
        if ( properties == null ) {
            properties = new TreeMap<String, Object>( CASE_INSENSITIVE_ORDER );
        }
        if ( properties.isEmpty() ) {
            emptyPropertyMap = true;
        }

        if ( importId != null ) {
            if ( isTimeBased( importId ) ) {
                timestamp = UUIDUtils.getTimestampInMicros( importId );
            }
            else if ( properties.get( PROPERTY_CREATED ) != null ) {
                timestamp = getLong( properties.get( PROPERTY_CREATED ) ) * 1000;
            }
        }

        if ( entityClass == null ) {
            entityClass = ( Class<A> ) Schema.getDefaultSchema().getEntityClass( entityType );
        }

        Set<String> required = schema.getRequiredProperties( entityType );

        if ( required != null ) {
            for ( String p : required ) {
                if ( !PROPERTY_UUID.equals( p ) && !PROPERTY_TYPE.equals( p ) && !PROPERTY_CREATED.equals( p )
                        && !PROPERTY_MODIFIED.equals( p ) ) {
                    Object v = properties.get( p );
                    if ( schema.isPropertyTimestamp( entityType, p ) ) {
                        if ( v == null ) {
                            properties.put( p, timestamp / 1000 );
                        }
                        else {
                            long ts = getLong( v );
                            if ( ts <= 0 ) {
                                properties.put( p, timestamp / 1000 );
                            }
                        }
                        continue;
                    }
                    if ( v == null ) {
                        throw new RequiredPropertyNotFoundException( entityType, p );
                    }
                    else if ( ( v instanceof String ) && isBlank( ( String ) v ) ) {
                        throw new RequiredPropertyNotFoundException( entityType, p );
                    }
                }
            }
        }

        // Create collection name based on entity: i.e. "users"
        String collection_name = Schema.defaultCollectionName( eType );
        // Create collection key based collection name
        String bucketId = indexBucketLocator.getBucket( getApplicationId(), IndexBucketLocator.IndexType.COLLECTION, itemId, collection_name );

        Object collection_key = key( getApplicationId(), Schema.DICTIONARY_COLLECTIONS, collection_name, bucketId );

        CollectionInfo collection = null;

        if ( !is_application ) {
            // Add entity to collection


            if ( !emptyPropertyMap ) {
                addInsertToMutator( m, ENTITY_ID_SETS, collection_key, itemId, null, timestamp );
            }

            // Add name of collection to dictionary property
            // Application.collections
            addInsertToMutator( m, ENTITY_DICTIONARIES, key( getApplicationId(), Schema.DICTIONARY_COLLECTIONS ),
                    collection_name, null, timestamp );

            addInsertToMutator( m, ENTITY_COMPOSITE_DICTIONARIES, key( itemId, Schema.DICTIONARY_CONTAINER_ENTITIES ),
                    asList( TYPE_APPLICATION, collection_name, getApplicationId() ), null, timestamp );
        }

        if ( emptyPropertyMap ) {
            return null;
        }
        properties.put( PROPERTY_UUID, itemId );
        properties.put( PROPERTY_TYPE, Schema.normalizeEntityType( entityType, false ) );

        if ( importId != null ) {
            if ( properties.get( PROPERTY_CREATED ) == null ) {
                properties.put( PROPERTY_CREATED, timestamp / 1000 );
            }

            if ( properties.get( PROPERTY_MODIFIED ) == null ) {
                properties.put( PROPERTY_MODIFIED, timestamp / 1000 );
            }
        }
        else {
            properties.put( PROPERTY_CREATED, timestamp / 1000 );
            properties.put( PROPERTY_MODIFIED, timestamp / 1000 );
        }

        // special case timestamp and published properties
        // and dictionary their timestamp values if not set
        // this is sure to break something for someone someday

        if ( properties.containsKey( PROPERTY_TIMESTAMP ) ) {
            long ts = getLong( properties.get( PROPERTY_TIMESTAMP ) );
            if ( ts <= 0 ) {
                properties.put( PROPERTY_TIMESTAMP, timestamp / 1000 );
            }
        }

        A entity = EntityFactory.newEntity( itemId, eType, entityClass );
        logger.info( "Entity created of type {}", entity.getClass().getName() );

        if ( Event.ENTITY_TYPE.equals( eType ) ) {
            Event event = ( Event ) entity.toTypedEntity();
            for ( String prop_name : properties.keySet() ) {
                Object propertyValue = properties.get( prop_name );
                if ( propertyValue != null ) {
                    event.setProperty( prop_name, propertyValue );
                }
            }
            Message message = storeEventAsMessage( m, event, timestamp );
            incrementEntityCollection( "events", timestamp );

            entity.setUuid( message.getUuid() );
            return entity;
        }

        for ( String prop_name : properties.keySet() ) {

            Object propertyValue = properties.get( prop_name );

            if ( propertyValue == null ) {
                continue;
            }


            if ( User.ENTITY_TYPE.equals( entityType ) && "me".equals( prop_name ) ) {
                throw new DuplicateUniquePropertyExistsException( entityType, prop_name, propertyValue );
            }

            entity.setProperty( prop_name, propertyValue );

            batchSetProperty( m, entity, prop_name, propertyValue, true, true, timestampUuid );
        }

        if ( !is_application ) {
            incrementEntityCollection( collection_name, timestamp );
        }

        return entity;
    }

    @Override
    public void batchCreateRole(Mutator<ByteBuffer> batch, UUID groupId, String roleName, String roleTitle, long inactivity, RoleRef roleRef, UUID timestampUuid) throws Exception {
        long timestamp = getTimestampInMicros( timestampUuid );

        if ( roleRef == null ) {
            roleRef = new SimpleRoleRef( groupId, roleName );
        }
        if ( roleTitle == null ) {
            roleTitle = roleRef.getRoleName();
        }

        EntityRef ownerRef = null;
        if ( roleRef.getGroupId() != null ) {
            ownerRef = new SimpleEntityRef( Group.ENTITY_TYPE, roleRef.getGroupId() );
        }
        else {
            ownerRef = new SimpleEntityRef( Application.ENTITY_TYPE, getApplicationId() );
        }

        Map<String, Object> properties = new TreeMap<String, Object>( CASE_INSENSITIVE_ORDER );
        properties.put( PROPERTY_TYPE, Role.ENTITY_TYPE );
        properties.put( "group", roleRef.getGroupId() );
        properties.put( PROPERTY_NAME, roleRef.getApplicationRoleName() );
        properties.put( "roleName", roleRef.getRoleName() );
        properties.put( "title", roleTitle );
        properties.put( PROPERTY_INACTIVITY, inactivity );

        Entity role = batchCreate( batch, Role.ENTITY_TYPE, null, properties, roleRef.getUuid(), timestampUuid );

        addInsertToMutator( batch, ENTITY_DICTIONARIES, key( ownerRef.getUuid(), Schema.DICTIONARY_ROLENAMES ),
                roleRef.getRoleName(), roleTitle, timestamp );

        addInsertToMutator( batch, ENTITY_DICTIONARIES, key( ownerRef.getUuid(), Schema.DICTIONARY_ROLETIMES ),
                roleRef.getRoleName(), inactivity, timestamp );

        addInsertToMutator( batch, ENTITY_DICTIONARIES, key( ownerRef.getUuid(), DICTIONARY_SETS ),
                Schema.DICTIONARY_ROLENAMES, null, timestamp );

        if ( roleRef.getGroupId() != null ) {
            getRelationManager( ownerRef ).batchAddToCollection( batch, COLLECTION_ROLES, role, timestampUuid );
        }
    }

    /**
     * Batch dictionary property.
     *
     * @param batch The batch to set the property into
     * @param entity The entity that owns the property
     * @param propertyName the property name
     * @param propertyValue the property value
     * @param timestampUuid The update timestamp as a uuid
     *
     * @return batch
     *
     * @throws Exception the exception
     */
    @Override
    public Mutator<ByteBuffer> batchSetProperty(Mutator<ByteBuffer> batch, EntityRef entity, String propertyName, Object propertyValue, UUID timestampUuid) throws Exception {
        return this.batchSetProperty( batch, entity, propertyName, propertyValue, false, false, timestampUuid );
    }

    @Override
    public Mutator<ByteBuffer> batchSetProperty(Mutator<ByteBuffer> batch, EntityRef entity, String propertyName, Object propertyValue, boolean force, boolean noRead, UUID timestampUuid) throws Exception {
        long timestamp = getTimestampInMicros( timestampUuid );

        // propertyName = propertyName.toLowerCase();

        boolean entitySchemaHasProperty = getDefaultSchema().hasProperty( entity.getType(), propertyName );

        propertyValue = getDefaultSchema().validateEntityPropertyValue( entity.getType(), propertyName, propertyValue );

        Schema defaultSchema = Schema.getDefaultSchema();

        if ( PROPERTY_TYPE.equalsIgnoreCase( propertyName ) && ( propertyValue != null ) ) {
            if ( "entity".equalsIgnoreCase( propertyValue.toString() ) || "dynamicentity"
                    .equalsIgnoreCase( propertyValue.toString() ) ) {
                String errorMsg =
                        "Unable to dictionary entity type to " + propertyValue + " because that is not a valid type.";
                logger.error( errorMsg );
                throw new IllegalArgumentException( errorMsg );
            }
        }

        if ( entitySchemaHasProperty ) {

            if ( !force ) {
                if ( !defaultSchema.isPropertyMutable( entity.getType(), propertyName ) ) {
                    return batch;
                }

                // Passing null for propertyValue indicates delete the property
                // so if required property, exit
                if ( ( propertyValue == null ) && defaultSchema.isRequiredProperty( entity.getType(), propertyName ) ) {
                    return batch;
                }
            }


            /**
             * Unique property, load the old value and remove it, check if it's not a duplicate
             */
            if ( defaultSchema.getEntityInfo( entity.getType() ).isPropertyUnique( propertyName ) ) {

                Lock lock = getUniqueUpdateLock( cass.getLockManager(), getApplicationId(), propertyValue, entity.getType(),
                        propertyName );

                try {
                    lock.lock();

                    if ( !isPropertyValueUniqueForEntity( entity.getUuid(), entity.getType(), propertyName,
                            propertyValue ) ) {
                        throw new DuplicateUniquePropertyExistsException( entity.getType(), propertyName,
                                propertyValue );
                    }


                    String collectionName = Schema.defaultCollectionName( entity.getType() );

                    uniquePropertyDelete( batch, collectionName, entity.getType(), propertyName, propertyValue,
                            entity.getUuid(), timestamp - 1 );
                    uniquePropertyWrite( batch, collectionName, propertyName, propertyValue, entity.getUuid(),
                            timestamp );
                }
                finally {
                    lock.unlock();
                }
            }
        }

        if ( getDefaultSchema().isPropertyIndexed( entity.getType(), propertyName ) ) {
            //this call is incorrect.  The current entity is NOT the head entity
            getRelationManager( entity )
                    .batchUpdatePropertyIndexes( batch, propertyName, propertyValue, entitySchemaHasProperty, noRead,
                            timestampUuid );
        }


        if ( propertyValue != null ) {
            // Set the new value
            addPropertyToMutator( batch, key( entity.getUuid() ), entity.getType(), propertyName, propertyValue,
                    timestamp );

            if ( !entitySchemaHasProperty ) {
                // Make a list of all the properties ever dictionary on this
                // entity
                addInsertToMutator( batch, ENTITY_DICTIONARIES, key( entity.getUuid(), DICTIONARY_PROPERTIES ),
                        propertyName, null, timestamp );
            }
        }
        else {
            addDeleteToMutator( batch, ENTITY_PROPERTIES, key( entity.getUuid() ), propertyName, timestamp );
        }

        return batch;
    }
    /**
     * Returns true if the property is unique, and the entity can be saved.  If it's not unique, false is returned
     *
     * @return True if this entity can safely "own" this property name and value unique combination
     */
    @Metered( group = "core", name = "EntityManager_isPropertyValueUniqueForEntity" )
    public boolean isPropertyValueUniqueForEntity( UUID ownerEntityId, String entityType, String propertyName,
                                                   Object propertyValue ) throws Exception {

        if ( !getDefaultSchema().isPropertyUnique( entityType, propertyName ) ) {
            return true;
        }

        if ( propertyValue == null ) {
            return true;
        }

        /**
         * Doing this in a loop sucks, but we need to account for possibly having more than 1 entry in the index due
         * to corruption.  We need to allow them to update, otherwise
         * both entities will be unable to update and must be deleted
         */

        Set<UUID> ownerEntityIds = getUUIDsForUniqueProperty( getApplicationId(), entityType, propertyName, propertyValue );

        //if there are no entities for this property, we know it's unique.  If there are,
        // we have to make sure the one we were passed is in the set.  otherwise it belongs
        //to a different entity
        return ownerEntityIds.size() == 0 || ownerEntityIds.contains( ownerEntityId );
    }

    /**
     * Gets the entity info. If no propertyNames are passed it loads the ENTIRE entity!
     *
     * @param entityId the entity id
     * @param propertyNames the property names
     *
     * @return DynamicEntity object holding properties
     *
     * @throws Exception the exception
     */
    @Metered( group = "core", name = "EntityManager_loadPartialEntity" )
    public DynamicEntity loadPartialEntity( UUID entityId, String... propertyNames ) throws Exception {

        List<HColumn<String, ByteBuffer>> results = null;
        if ( ( propertyNames != null ) && ( propertyNames.length > 0 ) ) {
            Set<String> column_names = new TreeSet<String>( CASE_INSENSITIVE_ORDER );

            column_names.add( PROPERTY_TYPE );
            column_names.add( PROPERTY_UUID );

            for ( String propertyName : propertyNames ) {
                column_names.add( propertyName );
            }

            results = cass.getColumns( cass.getApplicationKeyspace( getApplicationId() ), ENTITY_PROPERTIES, key( entityId ),
                    column_names, se, be );
        }
        else {
            results = cass.getAllColumns( cass.getApplicationKeyspace( getApplicationId() ), ENTITY_PROPERTIES,
                    key( entityId ) );
        }

        Map<String, Object> entityProperties = deserializeEntityProperties( results );
        if ( entityProperties == null ) {
            return null;
        }

        String entityType = ( String ) entityProperties.get( PROPERTY_TYPE );
        UUID id = ( UUID ) entityProperties.get( PROPERTY_UUID );

        return new DynamicEntity( entityType, id, entityProperties );
    }

    /**
     * Return all UUIDs that have this unique value
     *
     * @param ownerEntityId The entity id that owns this entity collection
     * @param collectionName The entity collection name
     * @param propertyName The name of the unique property
     * @param propertyValue The value of the unique property
     */
    private Set<UUID> getUUIDsForUniqueProperty( UUID ownerEntityId, String collectionName, String propertyName,
                                                 Object propertyValue ) throws Exception {


        String collectionNameInternal = defaultCollectionName( collectionName );

        Object key = createUniqueIndexKey( ownerEntityId, collectionNameInternal, propertyName, propertyValue );

        List<HColumn<ByteBuffer, ByteBuffer>> cols =
                cass.getColumns( cass.getApplicationKeyspace( getApplicationId() ), ENTITY_UNIQUE, key, null, null, 2,
                        false );


        //No columns at all, it's unique
        if ( cols.size() == 0 ) {
            return Collections.emptySet();
        }

        //shouldn't happen, but it's an error case
        if ( cols.size() > 1 ) {
            logger.error( "INDEX CORRUPTION: More than 1 unique value exists for entities in ownerId {} of type {} on "
                            + "property {} with value {}",
                    new Object[] { ownerEntityId, collectionNameInternal, propertyName, propertyValue } );
        }

        /**
         * Doing this in a loop sucks, but we need to account for possibly having more than 1 entry in the index due
         * to corruption.  We need to allow them to update, otherwise
         * both entities will be unable to update and must be deleted
         */

        Set<UUID> results = new HashSet<UUID>( cols.size() );

        for ( HColumn<ByteBuffer, ByteBuffer> col : cols ) {
            results.add( ue.fromByteBuffer( col.getName() ) );
        }

        return results;
    }

    /**
     * Create a row key for the entity of the given type with the name and value in the property.  Used for fast unique
     * index lookups
     */
    private Object createUniqueIndexKey( UUID ownerId, String collectionName, String propertyName, Object value ) {
        return key( ownerId, collectionName, propertyName, value );
    }

    /** Add this unique index to the delete */
    private void uniquePropertyDelete( Mutator<ByteBuffer> m, String collectionName, String entityType,
                                       String propertyName, Object propertyValue, UUID entityId, long timestamp )
            throws Exception {
        //read the old value and delete it

        Object oldValue = getProperty( new SimpleEntityRef( entityType, entityId ), propertyName );

        //we have an old value.  If the new value is empty, we want to delete the old value.  If the new value is
        // different we want to delete, otherwise we don't issue the delete
        if ( oldValue != null && ( propertyValue == null || !oldValue.equals( propertyValue ) ) ) {
            Object key = createUniqueIndexKey( getApplicationId(), collectionName, propertyName, oldValue );

            addDeleteToMutator( m, ENTITY_UNIQUE, key, timestamp, entityId );
        }
    }

    /** Add this unique index to the delete */
    private void uniquePropertyWrite( Mutator<ByteBuffer> m, String collectionName, String propertyName,
                                      Object propertyValue, UUID entityId, long timestamp ) throws Exception {
        Object key = createUniqueIndexKey( getApplicationId(), collectionName, propertyName, propertyValue );

        addInsertToMutator( m, ENTITY_UNIQUE, key, entityId, null, timestamp );
    }

    @Override
    public Mutator<ByteBuffer> batchUpdateDictionary(Mutator<ByteBuffer> batch, EntityRef entity, String dictionaryName, Object elementValue, Object elementCoValue, boolean removeFromDictionary, UUID timestampUuid) throws Exception {
        long timestamp = getTimestampInMicros( timestampUuid );

        // dictionaryName = dictionaryName.toLowerCase();
        if ( elementCoValue == null ) {
            elementCoValue = ByteBuffer.allocate( 0 );
        }

        boolean entityHasDictionary = getDefaultSchema().hasDictionary( entity.getType(), dictionaryName );

        // Don't index dynamic dictionaries not defined by the schema
        if ( entityHasDictionary ) {
            RelationManagerImpl relationManager = getRelationManager( entity );
            //relationManager.set
            relationManager
                    .batchUpdateSetIndexes( batch, dictionaryName, elementValue, removeFromDictionary, timestampUuid);
        }

        ApplicationCF dictionary_cf = entityHasDictionary ? ENTITY_DICTIONARIES : ENTITY_COMPOSITE_DICTIONARIES;

        if ( elementValue != null ) {
            if ( !removeFromDictionary ) {
                // Set the new value

                elementCoValue = toStorableBinaryValue( elementCoValue, !entityHasDictionary );

                addInsertToMutator( batch, dictionary_cf, key( entity.getUuid(), dictionaryName ),
                        entityHasDictionary ? elementValue : asList( elementValue ), elementCoValue, timestamp );

                if ( !entityHasDictionary ) {
                    addInsertToMutator( batch, ENTITY_DICTIONARIES, key( entity.getUuid(), DICTIONARY_SETS ),
                            dictionaryName, null, timestamp );
                }
            }
            else {
                addDeleteToMutator( batch, dictionary_cf, key( entity.getUuid(), dictionaryName ),
                        entityHasDictionary ? elementValue : asList( elementValue ), timestamp );
            }
        }

        return batch;
    }

    @Override
    public Mutator<ByteBuffer> batchUpdateDictionary(Mutator<ByteBuffer> batch, EntityRef entity, String dictionaryName, Object elementValue, boolean removeFromDictionary, UUID timestampUuid) throws Exception {
        return batchUpdateDictionary( batch, entity, dictionaryName, elementValue, null, removeFromDictionary,
                timestampUuid );
    }

    @Override
    public Mutator<ByteBuffer> batchUpdateProperties(Mutator<ByteBuffer> batch, EntityRef entity, Map<String, Object> properties, UUID timestampUuid) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> getDictionaryNames(EntityRef entity) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void insertEntity(String type, UUID entityId) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteEntity(UUID entityId) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public UUID getApplicationId() {
        return applicationScope.getOwner().getUuid();
    }

    @Override
    public IndexBucketLocator getIndexBucketLocator() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public CassandraService getCass() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Message storeEventAsMessage( Mutator<ByteBuffer> m, Event event, long timestamp ) {

        counterUtils.addEventCounterMutations( m, getApplicationId(), event, timestamp );

        QueueManager q = qmf.getQueueManager( getApplicationId());

        Message message = new Message();
        message.setType( "event" );
        message.setCategory( event.getCategory() );
        message.setStringProperty( "message", event.getMessage() );
        message.setTimestamp( timestamp );
        q.postToQueue( "events", message );

        return message;
    }

    private void incrementEntityCollection( String collection_name, long cassandraTimestamp ) {
        try {
            incrementAggregateCounters( null, null, null, new String( "application.collection." + collection_name ),
                    1L, cassandraTimestamp );
        }
        catch ( Exception e ) {
            logger.error( "Unable to increment counter application.collection: {}.", new Object[]{ collection_name, e} );
        }
        try {
            incrementAggregateCounters( null, null, null, "application.entities", 1L, cassandraTimestamp );
        }
        catch ( Exception e ) {
            logger.error( "Unable to increment counter application.entities for collection: {} with timestamp: {}", new Object[]{collection_name, cassandraTimestamp,e} );
        }
    }

    private void incrementAggregateCounters( UUID userId, UUID groupId, String category, String counterName, long value,
                                             long cassandraTimestamp ) {
        // TODO short circuit
        if ( !skipAggregateCounters ) {
            Mutator<ByteBuffer> m = createMutator( cass.getApplicationKeyspace( getApplicationId() ), be );

            counterUtils
                    .batchIncrementAggregateCounters( m, getApplicationId(), userId, groupId, null, category, counterName,
                            value, cassandraTimestamp / 1000, cassandraTimestamp );

            batchExecute( m, CassandraService.RETRY_COUNT );
        }
    }
}
