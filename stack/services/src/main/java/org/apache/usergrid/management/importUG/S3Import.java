package org.apache.usergrid.management.importUG;

import java.util.Map;
import java.io.File;

/**
 * Created by ApigeeCorporation on 7/8/14.
 */
public interface S3Import {
    File copyFromS3(Map<String,Object> importInfo, String filename );

    String getFilename ();
}
