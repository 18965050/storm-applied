package com.hp.schutale.storm.ch3;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalTopologyRunnerTest {

    private LocalTopologyRunner topologyRunner;

    @Before
    public void setUp() {
        topologyRunner = new LocalTopologyRunner();
    }

    @After
    public void tearDown() {
        topologyRunner = null;
    }

    @Test
    public void testRunTopology() {
        topologyRunner.runTopology();
    }

}
