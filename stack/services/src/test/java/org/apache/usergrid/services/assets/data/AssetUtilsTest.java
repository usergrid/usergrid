package org.apache.usergrid.services.assets.data;


import java.util.UUID;

import org.apache.usergrid.persistence.entities.Asset;
import org.apache.usergrid.services.assets.data.AssetUtils;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;


/** @author zznate */
public class AssetUtilsTest {

    private static UUID appId = new UUID( 0, 1 );


    @Test
    public void buildPathOk() {
        Asset asset = new Asset();
        asset.setPath( "path/to/file" );
        asset.setUuid( UUID.randomUUID() );

        String path = AssetUtils.buildAssetKey( appId, asset );

        assertEquals( 73, path.length() );
        assertEquals( appId.toString(), path.substring( 0, 36 ) );
    }


    @Test(expected = IllegalArgumentException.class)
    public void verifyErrorsOkAssetId() {
        Asset asset = new Asset();
        AssetUtils.buildAssetKey( appId, asset );
    }


    @Test(expected = IllegalArgumentException.class)
    public void verifyErrorsOkNullAppId() {
        Asset asset = new Asset();
        asset.setUuid( UUID.randomUUID() );
        AssetUtils.buildAssetKey( null, asset );
    }
}
