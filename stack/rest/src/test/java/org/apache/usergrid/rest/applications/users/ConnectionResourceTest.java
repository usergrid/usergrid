package org.apache.usergrid.rest.applications.users;


import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.MediaType;

import org.apache.usergrid.rest.AbstractRestIT;
import org.apache.usergrid.rest.TestContextSetup;
import org.apache.usergrid.rest.test.resource.CustomCollection;
import org.codehaus.jackson.JsonNode;
import org.junit.Rule;
import org.junit.Test;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;

import static org.apache.usergrid.utils.MapUtils.hashMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * // TODO: Document this
 *
 * @author ApigeeCorporation
 * @since 4.0
 */
public class ConnectionResourceTest extends AbstractRestIT {
    @Rule
    public TestContextSetup context = new TestContextSetup( this );


    @Test
    public void connectionsQueryTest() {


        CustomCollection activities = context.collection( "peeps" );

        Map stuff = hashMap( "type", "chicken" );

        activities.create( stuff );


        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put( "username", "todd" );

        Map<String, Object> objectOfDesire = new LinkedHashMap<String, Object>();
        objectOfDesire.put( "codingmunchies", "doritoes" );

        resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users" ).queryParam( "access_token", access_token )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                .post( JsonNode.class, payload );

        payload.put( "username", "scott" );


        resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users" ).queryParam( "access_token", access_token )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                .post( JsonNode.class, payload );
    /*finish setting up the two users */


        ClientResponse toddWant = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/todd/likes/peeps" )
                .queryParam( "access_token", access_token ).accept( MediaType.TEXT_HTML )
                .type( MediaType.APPLICATION_JSON_TYPE ).post( ClientResponse.class, objectOfDesire );

        assertEquals( 200, toddWant.getStatus() );

        JsonNode node =
                resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/peeps" ).queryParam( "access_token", access_token )
                        .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                        .get( JsonNode.class );

        String uuid = node.get( "entities" ).get( 0 ).get( "uuid" ).getTextValue();


        try {
            node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/users/scott/likes/" + uuid )
                    .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                    .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );
            assert ( false );
        }
        catch ( UniformInterfaceException uie ) {
            assertEquals( 404, uie.getResponse().getClientResponseStatus().getStatusCode() );
            return;
        }
    }


    @Test
    public void connectionsLoopbackTest() {

        CustomCollection things = context.collection( "things" );

        UUID thing1Id = getEntityId( things.create( hashMap( "name", "thing1" ) ), 0 );

        UUID thing2Id = getEntityId( things.create( hashMap( "name", "thing2" ) ), 0 );


        //create the connection
        things.entity( thing1Id ).connection( "likes" ).entity( thing2Id ).post();


        //org.apache.usergrid.test we have the "likes" in our connection meta data response

        JsonNode response = things.entity( "thing1" ).get();

        String url = getEntity( response, 0 ).get( "metadata" ).get( "connections" ).get( "likes" ).asText();


        assertNotNull( "Connection url returned in entity", url );

        //trim off the start /
        url = url.substring( 1 );


        //now that we know the URl is correct, follow it

        response = context.collection( url ).get();

        UUID returnedUUID = getEntityId( response, 0 );

        assertEquals( thing2Id, returnedUUID );


        //now follow the loopback, which should be pointers to the other entity

        url = getEntity( response, 0 ).get( "metadata" ).get( "connecting" ).get( "likes" ).asText();

        assertNotNull( "Incoming edge URL provited", url );

        //trim off the start /
        url = url.substring( 1 );

        //now we should get thing1 from the loopback url

        response = context.collection( url ).get();

        UUID returned = getEntityId( response, 0 );

        assertEquals( "Should point to thing1 as an incoming entity connection", thing1Id, returned );
    }


    @Test
    public void connectionsUUIDTest() {

        CustomCollection things = context.collection( "things" );

        UUID thing1Id = getEntityId( things.create( hashMap( "name", "thing1" ) ), 0 );

        UUID thing2Id = getEntityId( things.create( hashMap( "name", "thing2" ) ), 0 );


        //create the connection
        things.entity( thing1Id ).connection( "likes" ).entity( thing2Id ).post();


        //org.apache.usergrid.test we have the "likes" in our connection meta data response

        JsonNode response = things.entity( "thing1" ).get();

        String url = getEntity( response, 0 ).get( "metadata" ).get( "connections" ).get( "likes" ).asText();


        assertNotNull( "Connection url returned in entity", url );

        //trim off the start /
        url = url.substring( 1 );


        //now that we know the URl is correct, follow it

        response = context.collection( url ).get();

        UUID returnedUUID = getEntityId( response, 0 );

        assertEquals( thing2Id, returnedUUID );

        //get on the collection works, now get it directly by uuid

        //now we should get thing1 from the loopback url

        response = things.entity( thing1Id ).connection( "likes" ).entity( thing2Id ).get();

        UUID returned = getEntityId( response, 0 );

        assertEquals( "Should point to thing2 as an entity connection", thing2Id, returned );
    }
}
