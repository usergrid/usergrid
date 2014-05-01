package org.apache.usergrid.persistence.map;


import java.io.Serializable;

import org.apache.usergrid.persistence.core.hystrix.HystrixGraphObservable;

import com.google.inject.Inject;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import rx.Observable;
import rx.functions.Func1;
import rx.schedulers.Schedulers;


/**
 *
 *
 */
public class MapManagerImpl<T extends Serializable> implements MapManager<T> {

    private final MapScope mapScope;

    private final MapSerialization mapSerialization;


    @Inject
    public MapManagerImpl( MapScope mapScope, final MapSerialization mapSerialization ) {
        this.mapScope = mapScope;
        this.mapSerialization = mapSerialization;
    }


    @Override
    public Observable<String> put( final String key, final T element ) {

        return HystrixGraphObservable
                .user( Observable.just( key ).subscribeOn( Schedulers.io() ).map( new Func1<String, String>() {
                            @Override
                            public String call( final String key ) {
                                final MutationBatch mutation = mapSerialization.writeMap( mapScope, key, element );
                                try {
                                    mutation.execute();
                                }
                                catch ( ConnectionException e ) {
                                    throw new RuntimeException( "Unable to connect to cassandra", e );
                                }

                                return key;
                            }
                        } )
                     );
    }


    @Override
    public Object get( final String key ) {

        return  mapSerialization.getMapEntityFromSource(mapScope,key  );

    }


    @Override
    public Observable<String> delete( final String key ) {
        return HystrixGraphObservable
                .user( Observable.just( key ).subscribeOn( Schedulers.io() ).map( new Func1<String, String>() {
                            @Override
                            public String call( final String key ) {
                                final MutationBatch mutation =
                                        mapSerialization.removeMapEntityFromSource( mapScope, key );

                                try {
                                    mutation.execute();
                                }
                                catch ( ConnectionException e ) {
                                    throw new RuntimeException( "Unable to connect to cassandra", e );
                                }

                                return key;
                            }
                        } )
                     );
    }


    @Override
    public Observable<String> keys() {

        return null;
    }
}
