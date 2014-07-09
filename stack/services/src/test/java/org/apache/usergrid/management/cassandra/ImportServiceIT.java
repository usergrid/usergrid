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
