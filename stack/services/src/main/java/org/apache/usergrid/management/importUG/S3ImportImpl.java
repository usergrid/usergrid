package org.apache.usergrid.management.importUG;

import java.io.File;
import java.util.Map;

/**
 * Created by ApigeeCorporation on 7/8/14.
 */
public class S3ImportImpl implements S3Import {
    @Override
    public File copyFromS3(Map<String, Object> importInfo, String filename) {
        return new File("ephemeral");
    }

    @Override
    public String getFilename() {
        return null;
    }
}
