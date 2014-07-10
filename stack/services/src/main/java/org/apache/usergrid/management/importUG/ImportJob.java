package org.apache.usergrid.management.importUG;

import org.apache.usergrid.batch.JobExecution;
import org.apache.usergrid.batch.job.OnlyOnceJob;
import org.apache.usergrid.persistence.entities.JobData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by ApigeeCorporation on 7/8/14.
 */
@Component("importJob")
public class ImportJob extends OnlyOnceJob {

    public static final String IMPORT_ID = "importId";
    private static final Logger logger = LoggerFactory.getLogger(ImportJob.class);

    @Autowired
    ImportService importService;

    public ImportJob() {
        logger.info( "ImportJob created " + this );
    }

    @Override
    protected void doJob(JobExecution jobExecution) throws Exception {
        logger.info( "execute ImportJob {}", jobExecution );

        JobData jobData = jobExecution.getJobData();
        if ( jobData == null ) {
            logger.error( "jobData cannot be null" );
            return;
        }

        jobExecution.heartbeat();
        try {
            importService.doImport( jobExecution );
        }
        catch ( Exception e ) {
            logger.error( "Import Service failed to complete job" );
            logger.error(e.getMessage());
            return;
        }

        logger.info( "executed ImportJob process completed" );
    }

    @Override
    protected long getDelay(JobExecution execution) throws Exception {
        return 100;
    }
}
