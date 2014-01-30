package org.apache.usergrid.tools;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.usergrid.management.UserInfo;

import static org.apache.usergrid.utils.JsonUtils.mapToFormattedJsonString;


/** @author zznate */
public class UserManager extends ToolBase {

    @Override
    public Options createOptions() {
        Options options = super.createOptions();
        options.addOption( "u", "username", true, "The username to lookup" );
        options.addOption( "p", "password", true, "The password to set for the user" );
        return options;
    }


    @Override
    public void runTool( CommandLine line ) throws Exception {
        startSpring();
        String userName = line.getOptionValue( "u" );

        UserInfo userInfo = managementService.findAdminUser( userName );
        if ( userInfo == null ) {
            logger.info( "user {} not found", userName );
            return;
        }

        logger.info( mapToFormattedJsonString( userInfo ) );


        if ( line.hasOption( "p" ) ) {
            String password = line.getOptionValue( "p" );
            managementService.setAdminUserPassword( userInfo.getUuid(), password );
            logger.info( "new password match?: " + managementService
                    .verifyAdminUserPassword( userInfo.getUuid(), password ) );
        }
    }
}
