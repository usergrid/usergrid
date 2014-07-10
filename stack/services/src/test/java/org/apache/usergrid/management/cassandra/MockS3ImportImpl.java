package org.apache.usergrid.management.cassandra;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.usergrid.management.importUG.S3Import;

/**
 * Created by ApigeeCorporation on 7/8/14.
 */
public class MockS3ImportImpl implements S3Import{
    private final String filename;

    public MockS3ImportImpl (String filename) {
        this.filename = filename;
    }

    @Override
    public File copyFromS3(final Map<String,Object> exportInfo, String filename ) {

        File verfiedData = new File( this.filename );
        try {
            FileUtils.copyFile(filename, verfiedData);
        }
        catch ( IOException e ) {
            e.printStackTrace();
        }
    }

    @Override
    public String getFilename () {
        return filename;
    }
}
