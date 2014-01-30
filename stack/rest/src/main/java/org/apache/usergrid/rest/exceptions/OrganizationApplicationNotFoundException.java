package org.apache.usergrid.rest.exceptions;


import javax.ws.rs.core.UriInfo;

import org.apache.usergrid.rest.ApiResponse;
import org.apache.usergrid.rest.ServerEnvironmentProperties;

import static org.apache.usergrid.utils.JsonUtils.mapToJsonString;


/** @author zznate */
public class OrganizationApplicationNotFoundException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private ApiResponse apiResponse;


    public OrganizationApplicationNotFoundException( String orgAppName, UriInfo uriInfo,
                                                     ServerEnvironmentProperties properties ) {
        super( "Could not find application for " + orgAppName + " from URI: " + uriInfo.getPath() );
        apiResponse = new ApiResponse( properties );

        apiResponse.setError( this );
    }


    public ApiResponse getApiResponse() {
        return apiResponse;
    }


    public String getJsonResponse() {
        return mapToJsonString( apiResponse );
    }
}
