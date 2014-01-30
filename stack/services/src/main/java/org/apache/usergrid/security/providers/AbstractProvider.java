package org.apache.usergrid.security.providers;


import java.util.Map;

import org.apache.usergrid.management.ManagementService;
import org.apache.usergrid.persistence.EntityManager;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;


/** @author zznate */
public abstract class AbstractProvider implements SignInAsProvider {

    protected EntityManager entityManager;
    protected ManagementService managementService;
    protected Client client;


    AbstractProvider( EntityManager entityManager, ManagementService managementService ) {
        this.entityManager = entityManager;
        this.managementService = managementService;
        ClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getFeatures().put( JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE );
        client = Client.create( clientConfig );
    }


    abstract void configure();

    abstract Map<String, Object> userFromResource( String externalToken );

    public abstract Map<Object, Object> loadConfigurationFor();

    public abstract void saveToConfiguration( Map<String, Object> config );


    /** Encapsulates the dictionary lookup for any configuration required */
    protected Map<Object, Object> loadConfigurationFor( String providerKey ) {
        try {
            return entityManager.getDictionaryAsMap( entityManager.getApplication(), providerKey );
        }
        catch ( Exception ex ) {
            ex.printStackTrace();
        }
        return null;
    }


    protected void saveToConfiguration( String providerKey, Map<String, Object> config ) {
        try {
            entityManager.addMapToDictionary( entityManager.getApplication(), providerKey, config );
        }
        catch ( Exception ex ) {
            ex.printStackTrace();
        }
    }
}
