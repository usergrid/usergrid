package org.apache.usergrid.persistence.map.guice;


import org.safehaus.guicyfig.GuicyFigModule;

import org.apache.usergrid.persistence.collection.guice.CollectionModule;
import org.apache.usergrid.persistence.core.migration.Migration;
import org.apache.usergrid.persistence.graph.serialization.CassandraConfig;
import org.apache.usergrid.persistence.graph.serialization.impl.CassandraConfigImpl;
import org.apache.usergrid.persistence.map.MapFactory;
import org.apache.usergrid.persistence.map.MapFig;
import org.apache.usergrid.persistence.map.MapManager;
import org.apache.usergrid.persistence.map.MapManagerImpl;
import org.apache.usergrid.persistence.map.MapSerialization;
import org.apache.usergrid.persistence.map.MapSerializationImpl;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.Multibinder;


/**
 *
 *
 */
public class MapModule extends AbstractModule {
    @Override
    protected void configure() {
        //configure collections and our core astyanax framework
        install(new CollectionModule());


        bind( CassandraConfig.class).to( CassandraConfigImpl.class );

        //install our configuration
        install (new GuicyFigModule( MapFig.class ));

        // bind( MapScope.class ).to( MapScopeImpl.class );
        bind( MapManager.class).to( MapManagerImpl.class );
        bind( MapSerialization.class ).to( MapSerializationImpl.class );

        // create a guice factory for getting our collection manager
        install( new FactoryModuleBuilder().implement( MapManager.class, MapManagerImpl.class )
                                           .build( MapFactory.class ) );

        //do multibindings for migrations
        Multibinder<Migration> migrationBinding = Multibinder.newSetBinder( binder(), Migration.class );
        migrationBinding.addBinding().to( MapSerialization.class );

    }
}
