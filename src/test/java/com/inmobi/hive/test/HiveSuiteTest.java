package com.inmobi.hive.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.inmobi.hive.test.HiveTestSuite;

public class HiveSuiteTest {
    
    private HiveTestSuite testSuite;
    
    @Before
    public void setUp() throws Exception {
        testSuite = new HiveTestSuite();
        testSuite.createTestCluster();
    }
    
    @After
    public void tearDown() throws Exception {
        testSuite.shutdownTestCluster(false);
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

    private void printResult(List<String> results) {
        System.out.println("*************************");
        for (String s : results) {
            System.out.println(s);
        }
        System.out.println("*************************");
    }

    @Test
    public void testManagedTableUrl() throws Throwable {
        File inputRawData = new File("src/test/resources/files/url.txt");
        String inputRawDataAbsFilePath = inputRawData.getAbsolutePath();
        File inputRawData2 = new File("src/test/resources/files/url2.txt");
        String inputRawDataAbsFilePath2 = inputRawData2.getAbsolutePath();
        Map<String, String> params = new HashMap<String, String>();
        params.put("TABLE_DATA", inputRawDataAbsFilePath);
        params.put("TABLE_DATA2", inputRawDataAbsFilePath2);

        List<String> results = testSuite.executeScript("src/test/resources/scripts/managed_url.hql", params);
        printResult(results);
    }

    @Test
    public void testQueryUrl() {
        List<String> results = testSuite.executeStatements("select * from url where dt='20170101'");
        printResult(results);
    }

    @Test
    public void testQueryInIncrements() {
        List<String> results = testSuite.executeStatements("select t1.url from url t1 where t1.dt='20170101' and not exists (select t2.url from url t2 where t2.dt='20170102' and t1.url=t2.url)");
        printResult(results);
    }

}
