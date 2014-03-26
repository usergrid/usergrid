/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.usergrid.security.providers;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.usergrid.management.ManagementService;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.Query;
import org.apache.usergrid.persistence.Results;
import org.apache.usergrid.persistence.entities.User;
import org.apache.usergrid.security.tokens.exceptions.BadTokenException;
import org.apache.usergrid.utils.JsonUtils;

import org.springframework.web.client.RestTemplate;

import static org.apache.usergrid.utils.ListUtils.anyNull;


/**
 * Foursquare sign-in-as implementation
 *
 * @author zznate
 */
public class FoursquareProvider extends AbstractProvider {

    private Logger logger = LoggerFactory.getLogger( FoursquareProvider.class );


    FoursquareProvider( EntityManager entityManager, ManagementService managementService, RestTemplate restTemplate ) {
        super( entityManager, managementService, restTemplate );
    }


    @Override
    void configure() {
        // TODO
        // config params: url, version
    }


    @Override
    Map<String, Object> userFromResource( String externalToken ) {
        Map<String, String> params = new HashMap<String, String>();
        params.put("oauth_token", externalToken);
        params.put("v", "20140318");
        try {
            String json = restTemplate.getForObject("https://api.foursquare.com/v2/users/self?oauth_token={oauth_token}&v={v}", String.class, params);
            Map<String, Object> data = (Map<String, Object>) JsonUtils.parse(json);
            Map<String, Object> response = (Map<String, Object>) data.get("response");
            Map<String, Object> user = (Map<String, Object>) response.get("user");
            return user;
        } catch (Exception e) {
        }
        return null;
    }


    @Override
    public Map<Object, Object> loadConfigurationFor() {
        return loadConfigurationFor( "foursquareProvider" );
    }


    @Override
    public void saveToConfiguration( Map<String, Object> config ) {
        saveToConfiguration( "foursquareProvider", config );
    }

    @Override
    public User createOrAuthenticate( String externalToken ) throws BadTokenException {
        Map<String, Object> fq_user = userFromResource(externalToken);

        String fq_user_id = ( String ) fq_user.get( "id" );
        String fq_user_username = ( String ) fq_user.get( "id" );
        String fq_user_email = ( String ) ( ( Map<?, ?> ) fq_user.get( "contact" ) ).get( "email" );
        String fq_user_picture = ( String ) ( ( Map<?, ?> ) fq_user.get( "photo" ) ).get( "suffix" );
        String fq_user_name = new String( "" );

        // Grab the last check-in so we can store that as the user location
        Map<String, Double> location = new LinkedHashMap<String, Double>();
        try {
            Map<String, Object> fq_location =
                    ( Map<String, Object> ) ( ( Map<?, ?> ) ( ( Map<?, ?> ) ( ( ArrayList<?> ) ( ( Map<?, ?> ) fq_user
                            .get("checkins") ).get( "items" ) ).get( 0 ) ).get( "venue" ) ).get( "location" );
            location.put( "latitude", ( Double ) fq_location.get( "lat" ) );
            location.put( "longitude", ( Double ) fq_location.get( "lng" ) );

            if ( logger.isDebugEnabled() ) {
                logger.debug( JsonUtils.mapToFormattedJsonString( location ) );
            }
        } catch (Exception e) { }

        // Only the first name is guaranteed to be here
        try {
            fq_user_name = ( String ) fq_user.get( "firstName" ) + " " + ( String ) fq_user.get( "lastName" );
        }
        catch ( NullPointerException e ) {
            fq_user_name = ( String ) fq_user.get( "firstName" );
        }

        User user = null;
        try {
            if ( ( fq_user != null ) && !anyNull( fq_user_id, fq_user_name ) ) {

                Results r = entityManager.searchCollection( entityManager.getApplicationRef(), "users",
                        Query.findForProperty( "foursquare.id", fq_user_id ) );

                if ( r.size() > 1 ) {
                    logger.error( "Multiple users for FQ ID: " + fq_user_id );
                    throw new BadTokenException( "multiple users with same Foursquare ID" );
                }

                if ( r.size() < 1 ) {
                    Map<String, Object> properties = new LinkedHashMap<String, Object>();

                    properties.put( "foursquare", fq_user );
                    properties.put( "username", fq_user_username != null ? fq_user_username : "fq_" + fq_user_id );
                    properties.put( "name", fq_user_name );
                    if ( fq_user_email != null ) {
                        properties.put( "email", fq_user_email );
                    }
                    properties.put( "picture", "https://is0.4sqi.net/userpix_thumbs" + fq_user_picture );
                    properties.put( "activated", true );
                    properties.put( "location", location );

                    user = entityManager.create( "user", User.class, properties );
                }
                else {
                    user = ( User ) r.getEntity().toTypedEntity();
                    Map<String, Object> properties = new LinkedHashMap<String, Object>();

                    properties.put( "foursquare", fq_user );
                    properties.put( "picture", "https://is0.4sqi.net/userpix_thumbs" + fq_user_picture );
                    properties.put( "location", location );
                    entityManager.updateProperties( user, properties );

                    user.setProperty( "foursquare", fq_user );
                    user.setProperty( "picture", "https://is0.4sqi.net/userpix_thumbs" + fq_user_picture );
                    user.setProperty( "location", location );
                }
            }
            else {
                throw new BadTokenException( "Unable to confirm Foursquare access token" );
            }
        }
        catch ( Exception ex ) {
            throw new BadTokenException( "Could not create or update Foursquare user", ex );
        }

        return user;
    }
}
