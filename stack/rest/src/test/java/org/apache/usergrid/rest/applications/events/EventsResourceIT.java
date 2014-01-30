package org.apache.usergrid.rest.applications.events;


import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.usergrid.cassandra.Concurrent;
import org.apache.usergrid.rest.AbstractRestIT;
import org.codehaus.jackson.JsonNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


@Concurrent
public class EventsResourceIT extends AbstractRestIT {

    private static Logger log = LoggerFactory.getLogger( EventsResourceIT.class );


    @Test
    public void testEventPostandGet() {

        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put( "timestamp", 0 );
        payload.put( "category", "advertising" );
        payload.put( "counters", new LinkedHashMap<String, Object>() {
            {
                put( "ad_clicks", 5 );
            }
        } );

        JsonNode node =
                resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/events" ).queryParam( "access_token", access_token )
                        .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                        .post( JsonNode.class, payload );

        assertNotNull( node.get( "entities" ) );
        String advertising = node.get( "entities" ).get( 0 ).get( "uuid" ).asText();

        payload = new LinkedHashMap<String, Object>();
        payload.put( "timestamp", 0 );
        payload.put( "category", "sales" );
        payload.put( "counters", new LinkedHashMap<String, Object>() {
            {
                put( "ad_sales", 20 );
            }
        } );

        node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/events" ).queryParam( "access_token", access_token )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                .post( JsonNode.class, payload );

        assertNotNull( node.get( "entities" ) );
        String sales = node.get( "entities" ).get( 0 ).get( "uuid" ).asText();

        payload = new LinkedHashMap<String, Object>();
        payload.put( "timestamp", 0 );
        payload.put( "category", "marketing" );
        payload.put( "counters", new LinkedHashMap<String, Object>() {
            {
                put( "ad_clicks", 10 );
            }
        } );

        node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/events" ).queryParam( "access_token", access_token )
                .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE )
                .post( JsonNode.class, payload );

        assertNotNull( node.get( "entities" ) );
        String marketing = node.get( "entities" ).get( 0 ).get( "uuid" ).asText();

        String lastId = null;

        // subsequent GETs advertising
        for ( int i = 0; i < 3; i++ ) {

            node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/events" ).queryParam( "access_token", access_token )
                    .accept( MediaType.APPLICATION_JSON ).type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

            logNode( node );
            assertEquals( "Expected Advertising", advertising, node.get( "messages" ).get( 0 ).get( "uuid" ).asText() );
            lastId = node.get( "last" ).asText();
        }

        // check sales event in queue
        node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/events" ).queryParam( "last", lastId )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        logNode( node );
        assertEquals( "Expected Sales", sales, node.get( "messages" ).get( 0 ).get( "uuid" ).asText() );
        lastId = node.get( "last" ).asText();


        // check marketing event in queue
        node = resource().path( "/org.apache.usergrid.test-organization/org.apache.usergrid.test-app/events" ).queryParam( "last", lastId )
                .queryParam( "access_token", access_token ).accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON_TYPE ).get( JsonNode.class );

        logNode( node );
        assertEquals( "Expected Marketing", marketing, node.get( "messages" ).get( 0 ).get( "uuid" ).asText() );
    }
}
