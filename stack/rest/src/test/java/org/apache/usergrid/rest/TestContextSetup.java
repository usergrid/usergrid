package org.apache.usergrid.rest;


import org.apache.usergrid.rest.test.resource.TestContext;
import org.apache.usergrid.rest.test.security.TestAdminUser;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.sun.jersey.test.framework.JerseyTest;


/**
 * A self configuring TestContext which sets itself up it implements TestRule. With a @Rule annotation, an instance of
 * this Class as a public member in any org.apache.usergrid.test class or abstract org.apache.usergrid.test class will auto svcSetup itself before each org.apache.usergrid.test.
 */
public class TestContextSetup extends TestContext implements TestRule {

    public TestContextSetup( JerseyTest test ) {
        super( test );
    }


    public Statement apply( Statement base, Description description ) {
        return statement( base, description );
    }


    private Statement statement( final Statement base, final Description description ) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                before( description );
                try {
                    base.evaluate();
                }
                finally {
                    cleanup();
                }
            }
        };
    }


    protected void cleanup() {
        // might want to do something here later
    }


    protected void before( Description description ) {
        String testClass = description.getTestClass().getName();
        String methodName = description.getMethodName();
        String name = testClass + "." + methodName;

        TestAdminUser testAdmin = new TestAdminUser( name, name + "@usergrid.com", name + "@usergrid.com" );
        withOrg( name ).withApp( methodName ).withUser( testAdmin ).initAll();
    }
}
