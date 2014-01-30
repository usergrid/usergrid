package org.apache.usergrid.rest.exceptions;


import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;


/** @author zznate */
@Provider
public class OrganizationApplicationNotFoundExceptionMapper
        extends AbstractExceptionMapper<OrganizationApplicationNotFoundException> {
    @Override
    public Response toResponse( OrganizationApplicationNotFoundException e ) {
        return super.toResponse( Response.Status.BAD_REQUEST, e );
    }
}
