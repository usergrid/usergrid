/*******************************************************************************
 * Copyright 2012 Apigee Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.usergrid.services;


import org.apache.usergrid.cassandra.Concurrent;
import org.apache.usergrid.persistence.Entity;
import org.apache.usergrid.services.ServiceAction;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;


@Concurrent()
public class GroupServiceIT extends AbstractServiceIT {
    @Test
    public void testGroups() throws Exception {
        app.put( "path", "org.apache.usergrid.test/org.apache.usergrid.test" );
        app.put( "title", "Test group" );

        Entity group = app.testRequest( ServiceAction.POST, 1, "groups" ).getEntity();
        assertNotNull( group );

        app.testRequest( ServiceAction.GET, 1, "groups", "org.apache.usergrid.test", "org.apache.usergrid.test" );

        app.testRequest( ServiceAction.GET, 0, "groups", "org.apache.usergrid.test", "org.apache.usergrid.test", "messages" );

        app.testRequest( ServiceAction.GET, 1, "groups" );

        app.put( "username", "edanuff" );
        app.put( "email", "ed@anuff.com" );

        Entity user = app.testRequest( ServiceAction.POST, 1, "users" ).getEntity();
        assertNotNull( user );

        app.testRequest( ServiceAction.GET, 0, "groups", "org.apache.usergrid.test", "org.apache.usergrid.test", "users" );

        app.testRequest( ServiceAction.POST, 1, "groups", "org.apache.usergrid.test", "org.apache.usergrid.test", "users", user.getUuid() );

        app.testRequest( ServiceAction.GET, 1, "groups", "org.apache.usergrid.test", "org.apache.usergrid.test", "users" );

        app.testRequest( ServiceAction.GET, 1, "users", user.getUuid(), "groups" );

        app.testRequest( ServiceAction.GET, 0, "users", user.getUuid(), "activities" );

        app.testRequest( ServiceAction.GET, 0, "groups", group.getUuid(), "users", user.getUuid(), "activities" );
    }


    @Test
    public void testPermissions() throws Exception {
        app.put( "path", "mmmeow" );

        Entity group = app.create( "group" );
        assertNotNull( group );

        app.createGroupRole( group.getUuid(), "admin", 0 );
        app.createGroupRole( group.getUuid(), "author", 0 );

        app.grantGroupRolePermission( group.getUuid(), "admin", "users:access:*" );
        app.grantGroupRolePermission( group.getUuid(), "admin", "groups:access:*" );
        app.grantGroupRolePermission( group.getUuid(), "author", "assets:access:*" );

        app.testDataRequest( ServiceAction.GET, "groups", group.getUuid(), "rolenames" );
        app.testDataRequest( ServiceAction.GET, "groups", group.getUuid(), "roles", "admin", "permissions" );
        app.testDataRequest( ServiceAction.GET, "groups", group.getUuid(), "roles", "author", "permissions" );
    }
}
