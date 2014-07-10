package org.apache.usergrid.management.importUG;

import java.io.File;
import java.util.Map;

/**
 * Created by ApigeeCorporation on 7/8/14.
 */
public interface S3Import {
    File copyFromS3(Map<String,Object> exportInfo, String filename );

    String getFilename ();

}
