package org.apache.usergrid.persistence.map;


import org.apache.usergrid.persistence.model.entity.Id;


/**
 *
 *
 */
public class MapScopeImpl implements MapScope {
    private final String name;
    private final Id owner;
    private final Id organization;
    Class entityClass;


    public MapScopeImpl (){
        name= null;
        owner = null;
        organization = null;

    }

    public MapScopeImpl( final String name, final Id owner, final Id organization,Class storedEntityClass ){
        this.name = name;
        this.owner = owner;
        this.organization = organization;
        this.entityClass = storedEntityClass;

    }


    @Override
    public String getName() {
        return name;
    }


    @Override
    public Id getOwner() {
        return owner;
    }


        @Override
        public Class getEntryClass() {
            return entityClass;
        }


    @Override
    public Id getOrganization() {
        return organization;
    }
}
