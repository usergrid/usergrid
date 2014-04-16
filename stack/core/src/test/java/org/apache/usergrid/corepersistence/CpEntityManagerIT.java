package org.apache.usergrid.corepersistence;


import java.util.UUID;

import org.junit.Test;

import org.apache.usergrid.persistence.collection.CollectionScope;
import org.apache.usergrid.persistence.collection.EntityCollectionManager;
import org.apache.usergrid.persistence.collection.EntityCollectionManagerFactory;
import org.apache.usergrid.persistence.collection.OrganizationScope;
import org.apache.usergrid.persistence.collection.impl.CollectionScopeImpl;
import org.apache.usergrid.persistence.collection.impl.OrganizationScopeImpl;
import org.apache.usergrid.persistence.entities.Application;
import org.apache.usergrid.persistence.index.EntityCollectionIndex;
import org.apache.usergrid.persistence.index.EntityCollectionIndexFactory;
import org.apache.usergrid.persistence.model.entity.SimpleId;

import com.google.inject.Guice;
import com.google.inject.Injector;

import static org.junit.Assert.assertEquals;


/**
 *
 *
 */
public class CpEntityManagerIT {


        private static EntityCollectionManagerFactory ecmf;
        private static EntityCollectionIndexFactory ecif;
        private EntityCollectionManager ecm;
        private EntityCollectionIndex eci;

        private static final String SYSTEM_ORG_UUID = "b9b51240-b5d5-11e3-9ea8-11c207d6769a";
        private static final String SYSTEM_ORG_TYPE = "zzz_defaultapp_zzz";

        private static final String SYSTEM_APP_UUID = "b6768a08-b5d5-11e3-a495-10ddb1de66c4";
        private static final String SYSTEM_APP_TYPE = "zzz_defaultapp_zzz";

        private static final OrganizationScope SYSTEM_ORG_SCOPE =
                new OrganizationScopeImpl(
                        new SimpleId( UUID.fromString( SYSTEM_ORG_UUID ), SYSTEM_ORG_TYPE ));

        private static final CollectionScope SYSTEM_APP_SCOPE =
                new CollectionScopeImpl(
                        SYSTEM_ORG_SCOPE.getOrganization(),
                        new SimpleId( UUID.fromString(SYSTEM_APP_UUID), SYSTEM_APP_TYPE ),
                        SYSTEM_APP_TYPE);

    static {

//        try {
//            ConfigurationManager.loadCascadedPropertiesFromResources( "core-persistence" );
//
//            // TODO: make CpEntityManager work in non-test environment
//            Properties testProps = new Properties() {{
//                put("cassandra.hosts", "localhost:" + System.getProperty("cassandra.rpc_port"));
//            }};
//
//            ConfigurationManager.loadProperties( testProps );
//
//        } catch (IOException ex) {
//            throw new RuntimeException("Error loading Core Persistence proprties", ex);
//        }

        Injector injector = Guice.createInjector( new GuiceModule() );

//        MigrationManager m = injector.getInstance( MigrationManager.class );
//        try {
//            m.migrate();
//        } catch (MigrationException ex) {
//            throw new RuntimeException("Error migrating Core Persistence", ex);
//        }

        ecmf = injector.getInstance( EntityCollectionManagerFactory.class );
        ecif = injector.getInstance( EntityCollectionIndexFactory.class );
    }
//        {
//            Injector injector = Guice.createInjector( new GuiceModule() );
//
//            ecmf = injector.getInstance( EntityCollectionManagerFactory.class );
//            ecif = injector.getInstance( EntityCollectionIndexFactory.class );
//            ecm = ecmf.createCollectionManager( SYSTEM_APP_SCOPE );
//            eci = ecif.createCollectionIndex( SYSTEM_APP_SCOPE );
//        }
    //functions that need testing
    //getApplication
    //updateApplication
    //getApplicationRef
    //getApplicationId


    @Test
    public void testApplication(){

       //ecm = ecmf.createCollectionManager( SYSTEM_APP_SCOPE );
        //eci = ecif.createCollectionIndex( SYSTEM_APP_SCOPE );

        CpEntityManager cpEntityManager =
                new CpEntityManager( SYSTEM_APP_SCOPE, ecm, eci );

        Application app = null;
        try {
            app = cpEntityManager.getApplication();
        }
        catch ( Exception e ) {
            e.printStackTrace();
            assert(false);
        }
        assertEquals( UUID.fromString(SYSTEM_APP_UUID),app.getUuid());
    }
}
