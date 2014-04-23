package org.apache.usergrid.persistence.map;


import java.io.Serializable;


/**
 *What would a map api need?
 * setting and retrieving elements from a map
 *Instance of a map manager returned from factory.
 */
public interface MapFactory {

    /**
     * Get the map with the given scope
     * @param scope
     * @return
     */
    public <T extends Serializable> MapManager<T> getMap(MapScope<T> scope);

}
