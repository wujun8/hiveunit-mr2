package com.inmobi.hive.test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.HiveSQLException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HiveSuiteTest {
    
    private final static Logger log = LoggerFactory.getLogger(HiveSuiteTest.class);
    private static HiveTestSuite testSuite;
    
    @BeforeClass
    public static void setUp() throws Exception {
        log.debug("In HiveSuiteTest setup method");
        testSuite = new HiveTestSuite();
        testSuite.createTestCluster();
    }
    
    @AfterClass
    public static void tearDown() throws Exception {
        log.debug("In HiveSuiteTest tearDown method");
        testSuite.shutdownTestCluster();
    }
    
    @Test
    public void testManagedTable() throws Throwable {
        File inputRawData = new File("src/test/resources/files/weather.txt");
        String inputRawDataAbsFilePath = inputRawData.getAbsolutePath();
        Map<String, String> params = new HashMap<String, String>();
        params.put("WEATHER_DATA", inputRawDataAbsFilePath);
        
        List<String> results = testSuite.executeScript("src/test/resources/scripts/managed_weather.hql", params);
        assertEquals(1, results.size());
        assertEquals("2", results.get(0));
    }
    
    @Test(expected = HiveSQLException.class)
    public void testInvalidSyntax() throws Throwable {
        List<String> results = testSuite.executeScript("src/test/resources/scripts/invalid.hql");
    }

    @Test
    public void testTableJoin() throws Throwable {
        File inputRawData = new File("src/test/resources/files/emps.csv");
        Map<String, String> params = new HashMap<String, String>();
        params.put("EMP_DATA", inputRawData.getAbsolutePath());
        inputRawData = new File("src/test/resources/files/dept_emp.csv");
        params.put("DEPT_EMP_DATA", inputRawData.getAbsolutePath());
        List<String> results = testSuite.executeScript("src/test/resources/scripts/join_test.hql",params);
        assertEquals(1, results.size());
        assertEquals("11", results.get(0));
    }

    @Test
    public void testExternalTable() throws Throwable {
        FileSystem fs = testSuite.getFS();
        Path homeDir = fs.getHomeDirectory();
        String rawHdfsDirPath = homeDir + "/testing/input";
        Path rawHdfsData = new Path(rawHdfsDirPath + "/weather.txt");
        File inputRawData = new File("src/test/resources/files/weather.txt");
        String inputRawDataAbsFilePath = inputRawData.getAbsolutePath();
        Path inputData = new Path(inputRawDataAbsFilePath);
        fs.copyFromLocalFile(inputData, rawHdfsData);
        
        Map<String, String> params = new HashMap<String, String>();
        params.put("WEATHER_DATA", rawHdfsDirPath);
        List<String> results = testSuite.executeScript("src/test/resources/scripts/external_weather.hql", params);
        
        assertEquals(2, results.size());
        assertTrue(results.contains("1950\t22\t1"));
        assertTrue(results.contains("1949\t111\t1"));
    }

}
