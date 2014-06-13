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
package org.apache.usergrid.persistence.collection.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.persistence.collection.CollectionScope;
import org.apache.usergrid.persistence.collection.EntityCollectionManager;
import org.apache.usergrid.persistence.collection.guice.Write;
import org.apache.usergrid.persistence.collection.guice.WriteUpdate;
import org.apache.usergrid.persistence.collection.mvcc.entity.MvccEntity;
import org.apache.usergrid.persistence.collection.mvcc.entity.MvccValidationUtils;
import org.apache.usergrid.persistence.collection.mvcc.stage.CollectionIoEvent;
import org.apache.usergrid.persistence.collection.mvcc.stage.EntityUpdateEvent;
import org.apache.usergrid.persistence.collection.mvcc.stage.delete.MarkCommit;
import org.apache.usergrid.persistence.collection.mvcc.stage.delete.MarkStart;
import org.apache.usergrid.persistence.collection.mvcc.stage.load.Load;
import org.apache.usergrid.persistence.collection.mvcc.stage.write.RollbackAction;
import org.apache.usergrid.persistence.collection.mvcc.stage.write.WriteCommit;
import org.apache.usergrid.persistence.collection.mvcc.stage.write.WriteOptimisticVerify;
import org.apache.usergrid.persistence.collection.mvcc.stage.write.WriteStart;
import org.apache.usergrid.persistence.collection.mvcc.stage.write.WriteUniqueVerify;
import org.apache.usergrid.persistence.collection.service.UUIDService;
import org.apache.usergrid.persistence.core.consistency.AsyncProcessor;
import org.apache.usergrid.persistence.core.consistency.AsyncProcessorFactory;
import org.apache.usergrid.persistence.core.consistency.AsynchronousMessage;
import org.apache.usergrid.persistence.core.util.ValidationUtils;
import org.apache.usergrid.persistence.model.entity.Entity;
import org.apache.usergrid.persistence.model.entity.Id;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.functions.FuncN;
import rx.schedulers.Schedulers;


/**
 * Simple implementation.  Should perform  writes, delete and load.
 *
 * TODO: maybe refactor the stage operations into their own classes for clarity and organization?
 *
 * @author tnine
 */
public class EntityCollectionManagerImpl implements EntityCollectionManager {

    private static final Logger log = LoggerFactory.getLogger( EntityCollectionManagerImpl.class );

    private final CollectionScope collectionScope;
    private final UUIDService uuidService;


    //start stages
    private final WriteStart writeStart;
    private final WriteStart writeUpdate;
    private final WriteUniqueVerify writeVerifyUnique;
    private final WriteOptimisticVerify writeOptimisticVerify;
    private final WriteCommit writeCommit;
    private final RollbackAction rollback;

    //load stages
    private final Load load;


    //delete stages
    private final MarkStart markStart;
    private final MarkCommit markCommit;

    private final AsyncProcessor<EntityUpdateEvent> entityUpdate;


    @Inject
    public EntityCollectionManagerImpl( final UUIDService uuidService, @Write final WriteStart writeStart,
                                        @WriteUpdate final WriteStart writeUpdate,
                                        final WriteUniqueVerify writeVerifyUnique,
                                        final WriteOptimisticVerify writeOptimisticVerify,
                                        final WriteCommit writeCommit, final RollbackAction rollback, final Load load,
                                        final MarkStart markStart, final MarkCommit markCommit,
                                        @Assisted final CollectionScope collectionScope,
                                        final AsyncProcessorFactory factory) {

        Preconditions.checkNotNull( uuidService, "uuidService must be defined" );

        MvccValidationUtils.validateCollectionScope( collectionScope );

        this.writeStart = writeStart;
        this.writeUpdate = writeUpdate;
        this.writeVerifyUnique = writeVerifyUnique;
        this.writeOptimisticVerify = writeOptimisticVerify;
        this.writeCommit = writeCommit;
        this.rollback = rollback;

        this.load = load;
        this.markStart = markStart;
        this.markCommit = markCommit;

        this.uuidService = uuidService;
        this.collectionScope = collectionScope;

        this.entityUpdate = factory.getProcessor( EntityUpdateEvent.class );
    }


    @Override
    public Observable<Entity> write( final Entity entity ) {

        //do our input validation
        Preconditions.checkNotNull( entity, 
            "Entity is required in the new stage of the mvcc write" );

        final Id entityId = entity.getId();

        ValidationUtils.verifyIdentity( entityId );


        // create our observable and start the write
        CollectionIoEvent<Entity> writeData = new CollectionIoEvent<Entity>( collectionScope, entity );

        Observable<CollectionIoEvent<MvccEntity>> observable = stageRunner( writeData,writeStart );

        // execute all validation stages concurrently.  Needs refactored when this is done.  
        // https://github.com/Netflix/RxJava/issues/627
        // observable = Concurrent.concurrent( observable, Schedulers.io(), new WaitZip(), 
        //                  writeVerifyUnique, writeOptimisticVerify );

        observable.doOnNext( new Action1<CollectionIoEvent<MvccEntity>>() {
            @Override
            public void call( final CollectionIoEvent<MvccEntity> mvccEntityCollectionIoEvent ) {
                //Queue future write here (verify)
            }
        } ).map( writeCommit ).doOnNext( new Action1<Entity>() {
            @Override
            public void call( final Entity entity ) {
                //fork background processing here (start)

                //post-processing to come later. leave it empty for now.
            }
        } ).doOnError( rollback );


        // return the commit result.
        return observable.map( writeCommit ).doOnError( rollback );
    }


    @Override
    public Observable<Void> delete( final Id entityId ) {

        Preconditions.checkNotNull( entityId, "Entity id is required in this stage" );
        Preconditions.checkNotNull( entityId.getUuid(), "Entity id is required in this stage" );
        Preconditions.checkNotNull( entityId.getType(), "Entity type is required in this stage" );

        return Observable.from( new CollectionIoEvent<Id>( collectionScope, entityId ) ).subscribeOn( Schedulers.io() )
                         .map( markStart ).map( markCommit );
    }


    @Override
    public Observable<Entity> load( final Id entityId ) {

        Preconditions.checkNotNull( entityId, "Entity id required in the load stage" );
        Preconditions.checkNotNull( entityId.getUuid(), "Entity id uuid required in load stage" );
        Preconditions.checkNotNull( entityId.getType(), "Entity id type required in load stage" );

        return Observable.from( new CollectionIoEvent<Id>( collectionScope, entityId ) ).subscribeOn( Schedulers.io() )
                         .map( load );
    }

    @Override
    public Observable<Entity> update( final Entity entity ) {

        log.debug( "Starting update process" );

        //do our input validation
        Preconditions.checkNotNull( entity, "Entity is required in the new stage of the mvcc write" );

        final Id entityId = entity.getId();

        Preconditions.checkNotNull( entityId, "The entity id is required to be set for an update operation" );

        Preconditions
                .checkNotNull( entityId.getUuid(), "The entity id uuid is required to be set for an update operation" );

        Preconditions
                .checkNotNull( entityId.getType(), "The entity id type required to be set for an update operation" );

//        if ( entity.getVersion() != null ) {
//            //validate version
//        }
//        else {
//            final UUID version = uuidService.newTimeUUID();
//
//            EntityUtils.setVersion( entity, version );
//        }

        // fire the stages
        // TODO use our own Schedulers.io() to help with multitenancy here.
        // TODO writeOptimisticVerify and writeVerifyUnique should be concurrent to reduce wait time
        // these 3 lines could be done in a single line, but they are on multiple lines for clarity

        // create our observable and start the write
        CollectionIoEvent<Entity> writeData = new CollectionIoEvent<Entity>( collectionScope, entity );
        //
        Observable<CollectionIoEvent<MvccEntity>> observable = stageRunner( writeData,writeUpdate );


        // execute all validation stages concurrently.  Needs refactored when this is done.
        // https://github.com/Netflix/RxJava/issues/627
        // observable = Concurrent.concurrent( observable, Schedulers.io(), new WaitZip(),
        //                  writeVerifyUnique, writeOptimisticVerify );

        return observable.doOnNext( new Action1<CollectionIoEvent<MvccEntity>>() {
            @Override
            public void call( final CollectionIoEvent<MvccEntity> mvccEntityCollectionIoEvent ) {
                //Queue future write here (verify)

            }
        } ).map( writeCommit ).doOnNext( new Action1<Entity>() {
            @Override
            public void call( final Entity entity ) {
                //TODO: find more suitable timeout, 20 is just a random number.
                //TODO: We should be setting the verification before the writeCommit, then starting the event post write commit.

                log.debug( "sending entity to the queue" );
                final AsynchronousMessage<EntityUpdateEvent>
                        event = entityUpdate.setVerification( new EntityUpdateEvent( collectionScope, entityId ), 20 );
                //fork background processing here (start)
                entityUpdate.start( event );

                //post-processing to come later. leave it empty for now.
            }
        } ).doOnError( rollback );
        //        }

        //return observable.map(writeCommit).doOnError( rollback );
    }

    // fire the stages
    public Observable<CollectionIoEvent<MvccEntity>> stageRunner( CollectionIoEvent<Entity> writeData,WriteStart writeState ) {


        // TODO use our own Schedulers.io() to help with multitenancy here.
        // TODO writeOptimisticVerify and writeVerifyUnique should be concurrent to reduce wait time
        // these 3 lines could be done in a single line, but they are on multiple lines for clarity

        return Observable.from( writeData ).subscribeOn( Schedulers.io() ).map( writeState ).flatMap(
                new Func1<CollectionIoEvent<MvccEntity>, Observable<CollectionIoEvent<MvccEntity>>>() {

                    @Override
                    public Observable<CollectionIoEvent<MvccEntity>> call(
                            final CollectionIoEvent<MvccEntity> mvccEntityCollectionIoEvent ) {

                        // do the unique and optimistic steps in parallel

                        // unique function.  Since there can be more than 1 unique value in this 
                        // entity the unique verify step itself is multiple parallel executions.
                        // This is why we use "flatMap" instead of "map", which allows the
                        // WriteVerifyUnique stage to execute multiple verification steps in 
                        // parallel and zip the results

                        Observable<CollectionIoEvent<MvccEntity>> unique =
                                Observable.from( mvccEntityCollectionIoEvent ).subscribeOn( Schedulers.io() )
                                          .flatMap( writeVerifyUnique );


                        // optimistic verification
                        Observable<CollectionIoEvent<MvccEntity>> optimistic =
                                Observable.from( mvccEntityCollectionIoEvent ).subscribeOn( Schedulers.io() )
                                          .map( writeOptimisticVerify );

                        // zip the results
                        // TODO: Should the zip only return errors here, and if errors are present, 
                        // we throw during the zip phase?  I couldn't find "

                        return Observable.zip( unique, optimistic,
                                new Func2<CollectionIoEvent<MvccEntity>, CollectionIoEvent<MvccEntity>, 
                                        CollectionIoEvent<MvccEntity>>() {

                                    @Override
                                    public CollectionIoEvent<MvccEntity> call(
                                            final CollectionIoEvent<MvccEntity> mvccEntityCollectionIoEvent,
                                            final CollectionIoEvent<MvccEntity> mvccEntityCollectionIoEvent2 ) {

                                        return mvccEntityCollectionIoEvent;
                                    }
                                } );
                    }
                } );
    }




    /**
     * Class that validates all results are equal then proceeds
     */
    private static class WaitZip<R> implements FuncN<R> {


        private WaitZip() {
        }


        @Override
        public R call( final Object... args ) {

            for ( int i = 0; i < args.length - 1; i++ ) {
                assert args[i] == args[i + 1];
            }

            return ( R ) args[0];
        }
    }
}
