package org.apache.usergrid.cassandra;


import org.apache.usergrid.cassandra.SchemaManager;
import org.junit.Ignore;


/** @author zznate */
@Ignore
public class FakeSchemaManager implements SchemaManager {
    @Override
    public void create() {

    }


    @Override
    public boolean exists() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public void populateBaseData() {

    }


    @Override
    public void destroy() {

    }
}
