package org.apache.usergrid.persistence.map;


import org.apache.usergrid.persistence.core.migration.Migration;

import com.netflix.astyanax.MutationBatch;


/**
 * Simple interface for serialization of a Map.
 *
 */
public interface MapSerialization<T> extends Migration {
    /**
     * Writes a map into the mutation
     * @param mapScope
     * @param element
     * @return
     */
    MutationBatch writeMap ( MapScope mapScope, String key ,T element);

    /**
     * Removes a map entity from the column family
     * @param mapScope
     * @param key
     * @return
     */
    MutationBatch removeMapEntityFromSource ( MapScope mapScope, String key );

    /**
     * Allows you to get a map element out from a column family.
     * @param mapScope
     * @param key
     * @return
     */
    Object getMapEntityFromSource ( MapScope mapScope, String key );
}
