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


import java.util.Map;

import org.apache.usergrid.management.ManagementService;
import org.apache.usergrid.persistence.EntityManager;
import org.springframework.web.client.RestTemplate;


/** @author zznate */
public abstract class AbstractProvider implements SignInAsProvider {

    protected EntityManager entityManager;
    protected ManagementService managementService;
    protected RestTemplate restTemplate;


    AbstractProvider( EntityManager entityManager, ManagementService managementService, RestTemplate restTemplate) {
        this.entityManager = entityManager;
        this.managementService = managementService;
        this.restTemplate = restTemplate;
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
