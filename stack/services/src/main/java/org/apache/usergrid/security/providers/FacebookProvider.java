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


import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.usergrid.management.ManagementService;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.Identifier;
import org.apache.usergrid.persistence.Query;
import org.apache.usergrid.persistence.Results;
import org.apache.usergrid.persistence.entities.User;
import org.apache.usergrid.security.tokens.exceptions.BadTokenException;
import org.apache.usergrid.utils.JsonUtils;
import org.springframework.web.client.RestTemplate;

import static org.apache.usergrid.persistence.Schema.PROPERTY_MODIFIED;
import static org.apache.usergrid.utils.ListUtils.anyNull;


/**
 * Provider implementation for sign-in-as with facebook
 *
 * @author zznate
 */
public class FacebookProvider extends AbstractProvider {
    private static final String DEF_API_URL = "https://graph.facebook.com/me";
    private static final String DEF_PICTURE_URL = "http://graph.facebook.com/%s/picture";

    private Logger logger = LoggerFactory.getLogger( FacebookProvider.class );

    private String apiUrl = DEF_API_URL;
    private String pictureUrl = DEF_PICTURE_URL;

    FacebookProvider( EntityManager entityManager, ManagementService managementService, RestTemplate restTemplate) {
        super( entityManager, managementService, restTemplate );
    }


    @Override
    void configure() {
        try {
            Map config = loadConfigurationFor( "facebookProvider" );
            if ( config != null ) {
                String foundApiUrl = ( String ) config.get( "api_url" );
                if ( foundApiUrl != null ) {
                    apiUrl = foundApiUrl;
                }
                String foundPicUrl = ( String ) config.get( "pic_url" );
                if ( foundPicUrl != null ) {
                    pictureUrl = foundApiUrl;
                }
            }
        }
        catch ( Exception ex ) {
            ex.printStackTrace();
        }
    }


    @Override
    public Map<Object, Object> loadConfigurationFor() {
        return loadConfigurationFor( "facebookProvider" );
    }


    /** Configuration parameters we look for: <ul> <li>api_url</li> <li>pic_url</li> </ul> */
    @Override
    public void saveToConfiguration( Map<String, Object> config ) {
        saveToConfiguration( "facebookProvider", config );
    }


    @Override
    Map<String, Object> userFromResource( String externalToken ) {
        return null;
    }

    private FacebookUser getUser( String access_token ) {
        FacebookUser fb_user = null;
        Map<String, String> params = new HashMap<String, String>();
        params.put("access_token", access_token);
        try {
            fb_user = restTemplate.getForObject(apiUrl+"?access_token={access_token}", FacebookUser.class, params);
        } catch (Exception e) {
        }
        return fb_user;
    }

    @Override
    public User createOrAuthenticate( String externalToken ) throws BadTokenException {

        FacebookUser fb_user = getUser( externalToken );
        Preconditions.checkNotNull(fb_user, "Invalid facebook user");

        if ( logger.isDebugEnabled() ) {
            logger.debug( JsonUtils.mapToFormattedJsonString( fb_user ) );
        }

        User user = null;

        if ( ( fb_user != null ) && !anyNull( fb_user.id, fb_user.name ) ) {

            Results r = null;
            try {
                r = entityManager.searchCollection( entityManager.getApplicationRef(), "users",
                        Query.findForProperty( "facebook.id", fb_user.id ) );
            }
            catch ( Exception ex ) {
                throw new BadTokenException( "Could not lookup user for that Facebook ID", ex );
            }
            if ( r.size() > 1 ) {
                logger.error( "Multiple users for FB ID: " + fb_user.id );
                throw new BadTokenException( "multiple users with same Facebook ID" );
            }

            if ( r.size() < 1 ) {
                Map<String, Object> properties = new LinkedHashMap<String, Object>();

                properties.put( "facebook", fb_user );
                properties.put( "username", "fb_" + fb_user.id );
                properties.put( "name", fb_user.name );
                properties.put( "picture", String.format( pictureUrl, fb_user.id ) );

                if ( fb_user.email != null ) {
                    try {
                        user = managementService.getAppUserByIdentifier( entityManager.getApplication().getUuid(),
                                Identifier.fromEmail( fb_user.email ) );
                    }
                    catch ( Exception ex ) {
                        throw new BadTokenException(
                                "Could not find existing user for this applicaiton for email: " + fb_user.email, ex );
                    }
                    // if we found the user by email, unbind the properties from above
                    // that will conflict
                    // then update the user
                    if ( user != null ) {
                        properties.remove( "username" );
                        properties.remove( "name" );
                        try {
                            entityManager.updateProperties( user, properties );
                        }
                        catch ( Exception ex ) {
                            throw new BadTokenException( "Could not update user with new credentials", ex );
                        }
                        user.setProperty( PROPERTY_MODIFIED, properties.get( PROPERTY_MODIFIED ) );
                    }
                    else {
                        properties.put( "email", fb_user.email );
                    }
                }
                if ( user == null ) {
                    properties.put( "activated", true );
                    try {
                        user = entityManager.create( "user", User.class, properties );
                    }
                    catch ( Exception ex ) {
                        throw new BadTokenException( "Could not create user for that token", ex );
                    }
                }
            }
            else {
                user = ( User ) r.getEntity().toTypedEntity();
                Map<String, Object> properties = new LinkedHashMap<String, Object>();

                properties.put( "facebook", fb_user );
                properties.put( "picture", String.format( pictureUrl, fb_user.id ) );
                try {
                    entityManager.updateProperties( user, properties );
                    user.setProperty( PROPERTY_MODIFIED, properties.get( PROPERTY_MODIFIED ) );
                    user.setProperty( "facebook", fb_user );
                    user.setProperty( "picture", String.format( pictureUrl, fb_user.id ) );
                }
                catch ( Exception ex ) {
                    throw new BadTokenException( "Could not update user properties", ex );
                }
            }
        }
        else {
            throw new BadTokenException( "Unable to confirm Facebook access token" );
        }

        return user;
    }

    @JsonIgnoreProperties(ignoreUnknown=true)
    static class FacebookUser {
        @JsonProperty("id") String id;
        @JsonProperty("name") String name;
        @JsonProperty("username") String username;
        @JsonProperty("email") String email;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }
    }
}
