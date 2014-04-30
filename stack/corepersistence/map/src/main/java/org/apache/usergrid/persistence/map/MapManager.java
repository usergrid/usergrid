package org.apache.usergrid.persistence.map;


import java.io.Serializable;

import rx.Observable;


/**
 * Note that each map should have a maximum of approximately 250k elements for faster performance
 *
 */
public interface MapManager<T extends Serializable> {

    /**
     * WRite the element to the map
     * @param key
     * @param element
     * @param <T>
     * @return
     */
    public Observable<String> put(String key, T element);


    /**
     * Gets the entry from the map
     * @param key
     * @return
     */
    public Observable<T> get(String key);


    /**
     * Deletes the entity from the map
     * @param key
     * @return
     */
    public Observable<String> delete(String key);


    /**
     * Returns an observable of keys
     * @return
     */
    public Observable<String> keys();

    /**
     * EdgeMetaSerialization and it's impl
     */

}