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
package org.apache.usergrid.management.cassandra;

import org.apache.usergrid.ServiceITSetup;
import org.apache.usergrid.ServiceITSetupImpl;
import org.apache.usergrid.ServiceITSuite;
import org.apache.usergrid.cassandra.CassandraResource;
import org.apache.usergrid.cassandra.ClearShiroSubject;
import org.apache.usergrid.cassandra.Concurrent;
import org.apache.usergrid.management.OrganizationInfo;
import org.apache.usergrid.management.UserInfo;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Created by ApigeeCorporation on 7/8/14.
 */
@Concurrent
public class ImportServiceIT {

    private static final Logger LOG = LoggerFactory.getLogger(ExportServiceIT.class);

    private static CassandraResource cassandraResource = ServiceITSuite.cassandraResource;

    // app-level data generated only once
    private static UserInfo adminUser;
    private static OrganizationInfo organization;
    private static UUID applicationId;

    @Rule
    public ClearShiroSubject clearShiroSubject = new ClearShiroSubject();

    @ClassRule
    public static final ServiceITSetup setup = new ServiceITSetupImpl( cassandraResource );


    @BeforeClass
    public static void setup() throws Exception {
        LOG.info( "in setup" );
        adminUser = setup.getMgmtSvc().createAdminUser( "pooja", "Pooja Jain", "pooja@jain.com", "test", false, false );
        organization = setup.getMgmtSvc().createOrganization( "pooja-organization", adminUser, true );
        applicationId = setup.getMgmtSvc().createApplication( organization.getUuid(), "pooja-application" ).getId();
    }

    

}
