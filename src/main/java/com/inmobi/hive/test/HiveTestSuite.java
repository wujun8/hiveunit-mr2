package com.inmobi.hive.test;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.service.cli.HiveSQLException;

/*
 * This class is the primary interface to create new test cases using the 
 * minicluster and mini hive server.  The basic sequence a test case would 
 * need to follow is:
 * 
 *   HiveTestSuite testSuite = testSuite = new HiveTestSuite();
 *   testSuite.createTestCluster();
 *   testSuite.executeScript(<some_script>);
 *   ...
 *   testSuite.shutdownTestCluster();
 *   
 * In addition, a test case can directly reference HDFS by retrieving the 
 * FileSystem object:
 * 
 *   FileSystem fs = testSuite.getFS();
 *   fs.copyFromLocalFile(inputData, rawHdfsData);
 * 
 */
public class HiveTestSuite {
    
    private HiveTestCluster cluster;
    
    public void createTestCluster() {
        cluster = new HiveTestCluster();
        try {
            cluster.start();
        } catch (Exception e) {
            throw new RuntimeException("Unable to start test cluster", e);
        }
    }
    
    public void shutdownTestCluster(boolean clean) {
        if (cluster == null) {
            return;
        }
        try {
            cluster.stop();
            if (clean) {
                cluster.clean();
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to stop test cluster", e);
        }
    }
    
    public List<String> executeScript(String scriptFile) {
        return executeScript(scriptFile, null, null);
    }
    
    public List<String> executeScript(String scriptFile, Map<String, String> params) {
        return executeScript(scriptFile, params, null);
    }
    
    public List<String> executeScript(String scriptFile, Map<String, String> params, List<String> excludes) {
        HiveScript hiveScript = new HiveScript(scriptFile, params, excludes);
        return executeStatements(hiveScript.getStatements());
    }

    public List<String> executeStatements(List<String> statements) {
        if (cluster == null) {
            throw new IllegalStateException("No active cluster to run script with");
        }
        List<String> results = null;
        try {
            results = cluster.executeStatements(statements);
        } catch (HiveSQLException e) {
            throw new RuntimeException("Unable to execute script", e);
        }
        return results;
    }

    public List<String> executeStatements(String... statements) {
        return executeStatements(Lists.newArrayList(statements));
    }
    
    public FileSystem getFS() {
        if (cluster == null) {
            return null;
        }
        return this.cluster.getFS();
    }

}
