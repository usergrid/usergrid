/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  The ASF licenses this file to You
 * under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.  For additional information regarding
 * copyright in this work, please see the NOTICE file in the top level
 * directory of this distribution.
 */
package org.apache.usergrid.persistence.collection.mvcc.changelog;


import java.util.List;
import java.util.UUID;

import org.apache.usergrid.persistence.collection.mvcc.entity.MvccEntity;

/**
 * This change log generator takes one or more entity versions and generates the change-log. 
 */
public interface ChangeLogGenerator {

    /**
     * This change log generator takes one or more entity versions and generates the change-log.
     * The log is designed to be bring an index to a "current" state and allow for is allows 
     * for retrieving all deleted properties, all new properties, and all properties that have 
     * not changes, but now have a newer version. Changes should be ordered from lowest time 
     * uuid to highest timeuuid.
     *
     * @param versions Versions of the entity to be considered. 
     *     Must be ordered from lowest time UUID to highest time UUID.
     * 
     * @param minVersion Properties of versions older than this should be discarded 
     *     and properties newer should be retained. 
     * 
     * @return Change-log entries ordered by version, ordered from lowest time 
     *     uuid to highest timeuuid.
     */
    List<ChangeLogEntry> getChangeLog( List<MvccEntity> versions, UUID minVersion );
}
