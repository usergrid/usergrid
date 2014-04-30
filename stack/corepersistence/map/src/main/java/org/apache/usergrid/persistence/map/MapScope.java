/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.usergrid.persistence.map;


import org.apache.usergrid.persistence.core.scope.OrganizationScope;
import org.apache.usergrid.persistence.model.entity.Id;


/**
 * A scope to use when creating the map manager. Data encapsulated within instances of a scope are mutually exclusive from instances with other ids and
 * names.
 */
public interface MapScope extends OrganizationScope {

    /**
     * @return The name of the collection. If you use pluralization for you names vs types,
     * you must keep the consistent or you will be unable to load data
     */
    public String getName();

    /**
     * Get the owner of the map
     * @return
     */
    public Id getOwner();

    //    /**
    //     * Get the class type of entries
    //     * @return
    //     */
    //    public Class getEntryClass();

}
