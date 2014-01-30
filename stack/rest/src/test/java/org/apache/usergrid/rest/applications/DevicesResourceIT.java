package org.apache.usergrid.rest.applications;


import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.usergrid.cassandra.Concurrent;
import org.apache.usergrid.rest.AbstractRestIT;
import org.codehaus.jackson.JsonNode;
import org.junit.Test;

import com.sun.jersey.api.client.UniformInterfaceException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


@Concurrent()
public class DevicesResourceIT extends AbstractRestIT {

    @Test
    public void putWithUUIDShouldCreateAfterDelete() {

        Map<String, String> payload = new HashMap<String, String>();
        UUID uuid = UUID.randomUUID();
        payload.put( "name", "foo" );

        String path = "devices/" + uuid;

        JsonNode response = appPath( path ).put( JsonNode.class, payload );

        // create
        JsonNode entity = getEntity( response, 0 );
        assertNotNull( entity );
        String newUuid = entity.get( "uuid" ).getTextValue();
        assertEquals( uuid.toString(), newUuid );

        // delete
        response = appPath( path ).delete( JsonNode.class );
        assertNotNull( getEntity( response, 0 ) );

        // check deleted
        try {
            response = appPath( path ).get( JsonNode.class );
            fail( "should get 404 error" );
        }
        catch ( UniformInterfaceException e ) {
            assertEquals( 404, e.getResponse().getStatus() );
        }

        // create again
        response = appPath( path ).put( JsonNode.class, payload );
        entity = getEntity( response, 0 );
        assertNotNull( entity );

        // check existence
        response = appPath( path ).get( JsonNode.class );
        assertNotNull( getEntity( response, 0 ) );
    }
}
