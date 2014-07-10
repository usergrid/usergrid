package org.apache.usergrid.management.importUG;

import org.apache.usergrid.batch.JobExecution;
import org.apache.usergrid.batch.service.SchedulerService;
import org.apache.usergrid.management.ApplicationInfo;
import org.apache.usergrid.management.OrganizationInfo;
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
import java.io.File;

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

        if (config == null) {
            logger.error("import information cannot be null");
            return null;
        }

        EntityManager em = null;
        try {
            em = emf.getEntityManager(MANAGEMENT_APPLICATION_ID);
            Set<String> collections = em.getApplicationCollections();
            if (!collections.contains("imports")) {
                em.createApplicationCollection("imports");
            }
        } catch (Exception e) {
            logger.error("application doesn't exist within the current context");
            return null;
        }

        Import importUG = new Import();

        //update state
        try {
            importUG = em.create(importUG);
        } catch (Exception e) {
            logger.error("Import entity creation failed");
            return null;
        }

        importUG.setState(Import.State.CREATED);
        em.update(importUG);

        //set data to be transferred to exportInfo
        JobData jobData = new JobData();
        jobData.setProperty("importInfo", config);
        jobData.setProperty(IMPORT_ID, importUG.getUuid());

        long soonestPossible = System.currentTimeMillis() + 250; //sch grace period

        //schedule job
        sch.createJob(IMPORT_JOB_NAME, soonestPossible, jobData);

        //update state
        importUG.setState(Import.State.SCHEDULED);
        em.update(importUG);

        return importUG.getUuid();
    }

    @Override
    public String getState(UUID state) throws Exception {
        return null;
    }

    @Override
    public String getErrorMessage(UUID appId, UUID state) throws Exception {
        return null;
    }

    public void doImport(JobExecution jobExecution) throws Exception {

        Map<String, Object> config = ( Map<String, Object> ) jobExecution.getJobData().getProperty( "importInfo" );
        Object s3PlaceHolder = jobExecution.getJobData().getProperty( "s3Import" );
        S3Import s3Import = null;

        if ( config == null ) {
            logger.error( "Import Information passed through is null" );
            return;
        }
        //get the entity manager for the application, and the entity that this Import corresponds to.
        UUID importId = ( UUID ) jobExecution.getJobData().getProperty( IMPORT_ID );

        EntityManager em = emf.getEntityManager( MANAGEMENT_APPLICATION_ID );
        Import importUG = em.get( importId, Import.class );

        //update the entity state to show that the job has officially started.
        importUG.setState(Import.State.STARTED);
        em.update( importUG );
        try {
            if ( s3PlaceHolder != null ) {
                s3Import = ( S3Import ) s3PlaceHolder;
            }
            else {
                s3Import =  new S3ImportImpl();
            }
        }
        catch ( Exception e ) {
            logger.error( "S3Import doesn't exist" );
            importUG.setErrorMessage(e.getMessage());
            importUG.setState(Import.State.FAILED);
            em.update( importUG );
            return;
        }

        if ( config.get( "organizationId" ) == null ) {
            logger.error( "No organization could be found" );
            importUG.setState(Import.State.FAILED);
            em.update( importUG );
            return;
        }
        else if ( config.get( "applicationId" ) == null ) {
            //import All the applications from an organization
            try {
                //importApplicationsFromOrg((UUID) config.get("organizationId"), config, jobExecution, s3Import);
            }
            catch ( Exception e ) {
                importUG.setErrorMessage(e.getMessage());
                importUG.setState( Import.State.FAILED );
                em.update( importUG );
                return;
            }
        }
        else if ( config.get( "collectionName" ) == null ) {
            //imports an Application from a single organization
            try {
                //importApplicationFromOrg((UUID) config.get("organizationId"),
                       // (UUID) config.get("applicationId"), config, jobExecution, s3Import);
            }
            catch ( Exception e ) {
                importUG.setErrorMessage( e.getMessage() );
                importUG.setState( Import.State.FAILED );
                em.update( importUG );
                return;
            }
        }
        else {
            try {
                //imports a single collection from an app org combo
                try {
                    importCollectionFromOrgApp((UUID) config.get("applicationId"), config, jobExecution,s3Import);
                }
                catch ( Exception e ) {
                    importUG.setErrorMessage( e.getMessage() );
                    importUG.setState( Import.State.FAILED );
                    em.update( importUG );
                    return;
                }
            }
            catch ( Exception e ) {
                //if for any reason the backing up fails, then update the entity with a failed state.
                importUG.setErrorMessage(e.getMessage());
                importUG.setState( Import.State.FAILED );
                em.update( importUG );
                return;
            }
        }
        importUG.setState( Import.State.FINISHED );
        em.update( importUG );

    }

    /**
     * Exports a specific collection from an org-app combo.
     */
    //might be confusing, but uses the /s/ inclusion or exclusion nomenclature.
    private void importCollectionFromOrgApp( UUID applicationUUID, final Map<String, Object> config,
                                             final JobExecution jobExecution, S3Import s3Import ) throws Exception {

        //retrieves export entity
        Import importUG = getImportEntity(jobExecution);
        ApplicationInfo application = managementService.getApplicationInfo( applicationUUID );
        OrganizationInfo organization = managementService.getOrganizationForApplication(applicationUUID);
        String appFileName = prepareInputFileName("application", application.getName(),organization.getName(),
                (String) config.get("collectionName"));

        File ephemeral = fileTransfer( importUG, appFileName, config, s3Import );
        //collectionExportAndQuery( applicationUUID, config, export, jobExecution );


    }

    public File fileTransfer( Import importUG, String appFileName, Map<String, Object> config,
                              S3Import s3Import ) {
        File ephemeral;
        try {
          ephemeral   =  s3Import.copyFromS3(config, appFileName );
        }
        catch ( Exception e ) {
            importUG.setErrorMessage(e.getMessage());
            importUG.setState(Import.State.FAILED);
            return null;
        }
        return ephemeral;
    }

    public Import getImportEntity( final JobExecution jobExecution ) throws Exception {

        UUID importId = ( UUID ) jobExecution.getJobData().getProperty( IMPORT_ID );
        EntityManager importManager = emf.getEntityManager( MANAGEMENT_APPLICATION_ID );

        return importManager.get( importId, Import.class );
    }

    /**
     * @param type just a label such us: organization, application.
     *
     * @return the file name concatenated with the type and the name of the collection
     */
    protected String prepareInputFileName( String type, String name, String organizationName, String CollectionName ) {
        StringBuilder str = new StringBuilder();
        str.append(organizationName);
        str.append("/");
        str.append( name );
        str.append( "." );
        if ( CollectionName != null ) {
            str.append( CollectionName );
            str.append( "." );
        }
        //TODO: check how to give regex in filename
        //TODO: confirm how to give the org folder in path
        str.append("*");
        str.append( ".json" );

        String inputFileName = str.toString();

        return inputFileName;
    }
}
