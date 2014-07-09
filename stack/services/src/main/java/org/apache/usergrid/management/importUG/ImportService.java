package org.apache.usergrid.management.importUG;

import org.apache.usergrid.batch.JobExecution;

import java.util.Map;
import java.util.UUID;

/**
 * Performs all functions related to importing
 */
public interface ImportService {

    /**
     * Schedules the import to execute
     */
    UUID schedule( Map<String,Object> json) throws Exception;

    /**
     * Perform the import to the external resource
     */
    void doImport( JobExecution jobExecution ) throws Exception;

    /**
     * Returns the current state of the service.
     */
    String getState( UUID state ) throws Exception;

    String getErrorMessage( UUID appId, UUID state ) throws Exception;

}
