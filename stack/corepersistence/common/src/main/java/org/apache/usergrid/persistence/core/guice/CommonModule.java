/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.usergrid.persistence.core.guice;


import org.safehaus.guicyfig.GuicyFigModule;

import org.apache.usergrid.persistence.core.astyanax.AstyanaxKeyspaceProvider;
import org.apache.usergrid.persistence.core.astyanax.CassandraConfig;
import org.apache.usergrid.persistence.core.astyanax.CassandraConfigImpl;
import org.apache.usergrid.persistence.core.astyanax.CassandraFig;
import org.apache.usergrid.persistence.core.consistency.AsyncProcessorFactory;
import org.apache.usergrid.persistence.core.consistency.AsyncProcessorFactoryImpl;
import org.apache.usergrid.persistence.core.consistency.ConsistencyFig;
import org.apache.usergrid.persistence.core.consistency.LocalTimeoutQueueFactory;
import org.apache.usergrid.persistence.core.consistency.TimeService;
import org.apache.usergrid.persistence.core.consistency.TimeServiceImpl;
import org.apache.usergrid.persistence.core.consistency.TimeoutQueueFactory;
import org.apache.usergrid.persistence.core.migration.MigrationManager;
import org.apache.usergrid.persistence.core.migration.MigrationManagerFig;
import org.apache.usergrid.persistence.core.migration.MigrationManagerImpl;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.netflix.astyanax.Keyspace;


/**
 * Simple module for configuring our core services.  Cassandra etc
 *
 */
public abstract class CommonModule extends AbstractModule {


    @Override
    protected void configure() {
        //noinspection unchecked
        install( new GuicyFigModule(
                MigrationManagerFig.class,
                CassandraFig.class,
                ConsistencyFig.class) );

             // bind our keyspace to the AstyanaxKeyspaceProvider
        bind( Keyspace.class ).toProvider( AstyanaxKeyspaceProvider.class ).asEagerSingleton();

        // bind our migration manager
        bind( MigrationManager.class ).to( MigrationManagerImpl.class );

        bind( TimeService.class ).to( TimeServiceImpl.class );

        bind( CassandraConfig.class ).to( CassandraConfigImpl.class );

        bind(AsyncProcessorFactory.class).to( AsyncProcessorFactoryImpl.class ).asEagerSingleton();

        bindTimeoutQueueFactory();


    }


    /**
     * Bind the timeoutqueue factory to an implementation
     * @return
     */
    protected abstract void bindTimeoutQueueFactory();


}
