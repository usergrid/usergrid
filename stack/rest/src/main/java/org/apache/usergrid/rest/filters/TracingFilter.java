package org.apache.usergrid.rest.filters;


import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

import org.apache.usergrid.persistence.cassandra.util.TraceTag;
import org.apache.usergrid.persistence.cassandra.util.TraceTagManager;
import org.apache.usergrid.persistence.cassandra.util.TraceTagReporter;
import org.apache.usergrid.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;


/**
 * Attach and detach trace tags at start and end of request scopes
 *
 * @author zznate
 */
@Component
public class TracingFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private Logger logger = LoggerFactory.getLogger( TracingFilter.class );

    @Autowired
    private TraceTagManager traceTagManager;
    @Autowired
    private TraceTagReporter traceTagReporter;


    @Context
    private HttpServletRequest httpServletRequest;


    @Override
    public ContainerRequest filter( ContainerRequest request ) {
        if ( !traceTagManager.getTraceEnabled() && !traceTagManager.getExplicitOnly() ) {
            return request;
        }
        String traceId;
        if ( traceTagManager.getExplicitOnly() ) {
            // if we are set in explicit mode and the header is not present, leave.
            String id = httpServletRequest.getHeader( "XX-TRACE-ID" );
            if ( StringUtils.isBlank( id ) ) {
                return request;
            }
            traceId = id.concat( "-REST-" ).concat( request.getPath( true ) );
        }
        else {
            traceId = "TRACE-".concat( request.getPath( true ) );
        }
        TraceTag traceTag = traceTagManager.create( traceId );
        traceTagManager.attach( traceTag );

        return request;
    }


    @Override
    public ContainerResponse filter( ContainerRequest request, ContainerResponse response ) {
        if ( traceTagManager.isActive() ) {
            TraceTag traceTag = traceTagManager.detach();
            traceTagReporter.report( traceTag );
        }
        return response;
    }
}
