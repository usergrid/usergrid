package org.apache.usergrid.management.importUG;

import org.apache.usergrid.batch.JobExecution;
import org.apache.usergrid.batch.service.SchedulerService;
import org.apache.usergrid.management.ApplicationInfo;
import org.apache.usergrid.management.ManagementService;
import org.apache.usergrid.persistence.EntityManager;
import org.apache.usergrid.persistence.EntityManagerFactory;
import org.apache.usergrid.persistence.entities.Import;
import org.apache.usergrid.persistence.entities.JobData;
import org.codehaus.jackson.JsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.usergrid.persistence.cassandra.CassandraService.MANAGEMENT_APPLICATION_ID;

/**
 * Created by ApigeeCorporation on 7/8/14.
 */
public class ImportServiceImpl implements ImportService {

    private static final Logger logger = LoggerFactory.getLogger(ImportServiceImpl.class);
    public static final String IMPORT_ID = "importId";
    public static final String IMPORT_JOB_NAME = "importJob";

    //dependency injection
    private SchedulerService sch;

    //injected the Entity Manager Factory
    protected EntityManagerFactory emf;

    //inject Management Service to access Organization Data
    private ManagementService managementService;

    //Amount of time that has passed before sending another heart beat in millis
    public static final int TIMESTAMP_DELTA = 5000;

    private JsonFactory jsonFactory = new JsonFactory();

    @Override
    public UUID schedule(Map<String, Object> config) throws Exception {

        ApplicationInfo defaultImportApp = null;

        if ( config == null ) {
            logger.error( "import information cannot be null" );
            return null;
        }

        EntityManager em = null;
        try {
            em = emf.getEntityManager( MANAGEMENT_APPLICATION_ID );
            Set<String> collections = em.getApplicationCollections();
            if ( !collections.contains( "imports" ) ) {
                em.createApplicationCollection( "imports" );
            }
        }
        catch ( Exception e ) {
            logger.error( "application doesn't exist within the current context" );
            return null;
        }

        Import importUG = new Import();

        //update state
        try {
            importUG = em.create( importUG );
        }
        catch ( Exception e ) {
            logger.error( "Import entity creation failed" );
            return null;
        }

        importUG.setState( Import.State.CREATED );
        em.update( importUG );

        //set data to be transferred to exportInfo
        JobData jobData = new JobData();
        jobData.setProperty( "importInfo", config );
        jobData.setProperty( IMPORT_ID, importUG.getUuid() );

        long soonestPossible = System.currentTimeMillis() + 250; //sch grace period

        //schedule job
        sch.createJob( IMPORT_JOB_NAME, soonestPossible, jobData );

        //update state
        importUG.setState( Import.State.SCHEDULED );
        em.update( importUG );

        return importUG.getUuid();
    }

    @Override
    public void doImport(JobExecution jobExecution) throws Exception {

    }

    @Override
    public String getState(UUID state) throws Exception {
        return null;
    }

    @Override
    public String getErrorMessage(UUID appId, UUID state) throws Exception {
        return null;
    }
}
