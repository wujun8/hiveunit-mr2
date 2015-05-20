## hiveunit-mr2: A framework to test Hive scripts with YARN and MR2.

### Usage

The hiveunit-mr2 framework provides a convenient wrapper around the existing Mini Hive Server and MiniCluster testing tools that are currently being used by the developers of YARN and MR2. It will allow you to easily fire up a MiniCluster, a Mini Hive Server, including an internal Derby database in a test case that will allow you to test your Hive QL scripts. 

The basic sequence of commands a test case would need to follow is:
  
    HiveTestSuite testSuite = testSuite = new HiveTestSuite();
    testSuite.createTestCluster();
    List<String> results = testSuite.executeScript(<some_script>);
    assertEquals(2, results.size());

    ...

    testSuite.shutdownTestCluster();

Refer to the test case, com.inmobi.hive.test.HiveSuiteTest, for a more complete example.  

The executeScript() method can take two additional parameters: params and excludes.  The params is a HashMap that will allow for parameters to be substituted using the Hive convention of ${VAR_NAME}, where you would use  ${VAR_NAME} in your script, and then create a hashmap with VAR_NAME as the key and the substitution value as the data.  The excludes is a List of Strings that allow you to exclude entire lines from your script.  This is primarily intended for the ADD JAR commands.  It is just easier to exclude those lines and ensure the needed jars are on the classpath at the project level.  

In addition, a test case can directly reference HDFS by retrieving the  FileSystem object:
  
    FileSystem fs = testSuite.getFS();
    // setup staged data here
    fs.copyFromLocalFile(inputData, rawHdfsData);


Finally, the use of MiniCluster to test MapReduce programs is also provided along with the Hive testing, because MiniCluster is required.  So it would be possible to also test any MapReduce programs as well, refer to the test case com.inmobi.hadoop.BasicMRTest for a more concrete example.  

### Credit 

This project was inspired by an initial Hive Testing framework built by 
Edward Capiolli (https://github.com/edwardcapriolo/hive_test).

Special appreciation to the development teams that have put countless hours into the the new releases and improvements to Hadoop and Hive.  They have really improved the testing tools.  

### Contribute!

Contributions are welcome! You can contribute by
 * starring this repo!
 * adding new features
 * enhancing existing code
 * testing
 * enhancing documentation
 * bringing suggestions and reporting bugs
 * spreading the word / telling us how you use it!

