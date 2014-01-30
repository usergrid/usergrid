package org.apache.usergrid.security.providers;


import java.util.Map;
import java.util.UUID;

import org.apache.usergrid.ServiceITSetup;
import org.apache.usergrid.ServiceITSetupImpl;
import org.apache.usergrid.ServiceITSuite;
import org.apache.usergrid.cassandra.ClearShiroSubject;
import org.apache.usergrid.cassandra.Concurrent;
import org.apache.usergrid.management.OrganizationInfo;
import org.apache.usergrid.management.UserInfo;
import org.apache.usergrid.persistence.entities.Application;
import org.apache.usergrid.persistence.entities.User;
import org.apache.usergrid.security.providers.PingIdentityProvider;
import org.apache.usergrid.utils.MapUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;


/** @author zznate */
@Ignore
@Concurrent()
public class PingIdentityProviderIT {
    private static UserInfo adminUser;
    private static OrganizationInfo organization;
    private static UUID applicationId;

    @Rule
    public ClearShiroSubject clearShiroSubject = new ClearShiroSubject();

    @ClassRule
    public static ServiceITSetup setup = new ServiceITSetupImpl( ServiceITSuite.cassandraResource );


    @BeforeClass
    public static void setup() throws Exception {
        adminUser = setup.getMgmtSvc()
                         .createAdminUser( "pinguser", "Ping User", "ping-user@usergrid.com", "org.apache.usergrid.test", false, false );
        organization = setup.getMgmtSvc().createOrganization( "ping-organization", adminUser, true );
        applicationId = setup.getMgmtSvc().createApplication( organization.getUuid(), "ping-application" ).getId();
    }


    @Test
    public void verifyLiveConnect() throws Exception {
        Application application = setup.getEmf().getEntityManager( applicationId ).getApplication();
        Map pingProps = MapUtils.hashMap( "api_url", "" ).map( "client_secret", "" )
                                .map( "client_id", "dev.app.appservicesvalidation" );

        PingIdentityProvider pingProvider =
                ( PingIdentityProvider ) setup.getProviderFactory().pingident( application );
        pingProvider.saveToConfiguration( pingProps );
        pingProvider.configure();
        User user = pingProvider.createOrAuthenticate( "u0qoW7TS9eT8Vmt7UzrEWrhHbhDK" );
        assertNotNull( user );
    }
}
