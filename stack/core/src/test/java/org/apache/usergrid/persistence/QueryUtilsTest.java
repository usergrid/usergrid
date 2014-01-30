package org.apache.usergrid.persistence;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.usergrid.cassandra.Concurrent;
import org.apache.usergrid.persistence.QueryUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/** @author zznate */
@Concurrent()
public class QueryUtilsTest {

    private static final String FAKE_QL = "select color from cat";

    private Map<String, List<String>> params = new HashMap<String, List<String>>();


    @Test
    public void extractQueryAlias() {
        params.put( QueryUtils.PARAM_QL, Arrays.asList( FAKE_QL ) );
        String query = QueryUtils.queryStrFrom( params );
        assertEquals( FAKE_QL, query );
        params.clear();

        params.put( QueryUtils.PARAM_Q, Arrays.asList( FAKE_QL ) );
        query = QueryUtils.queryStrFrom( params );
        assertEquals( FAKE_QL, query );
        params.clear();

        params.put( QueryUtils.PARAM_QUERY, Arrays.asList( FAKE_QL ) );
        query = QueryUtils.queryStrFrom( params );
        assertEquals( FAKE_QL, query );
        params.clear();
    }
}
