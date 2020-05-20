# spark-hello-world
An example Spark 2.4.5 application written in Scala and setup using Maven.

# Getting Started

Make sure you have JDK 1.8 installed. I recommend also installing an IDE like IntelliJ (the following will be IntelliJ specific).

To start, clone this repo to your PC:
```
git clone https://github.com/kitmenke/spark-hello-world.git
```

Open the project in IntelliJ:

 1. From the IntelliJ splash screen, click IMPORT (not open)
 1. Navigate to the spark-hello-world/pom.xml and double click it
 1. Click next
 1. Click next
 1. Click next
 1. Make sure JDK 1.8 is selected. If it is not, click the green plus and add it. Then click next.
 1. Click finish
 
Give IntelliJ a few minutes to download all of the project dependencies. You'll see the progress bar in the bottom right going crazy.

# Running the Structured Streaming App

The structured streaming app requires a Kafka cluster. The data on the topic is from the [Amazon Customer Reviews Dataset](https://registry.opendata.aws/amazon-reviews/)
and looks like the following example:

```json
{
  "marketplace": "US",
  "customer_id": 1,
  "review_id": "R26ZK6XLDT8DDS",
  "product_id": "B000L70MQO",
  "product_parent": 216773674,
  "product_title": "Product 1",
  "product_category": "Toys",
  "star_rating": 5,
  "helpful_votes": 1,
  "total_votes": 4,
  "vine": "N",
  "verified_purchase": "Y",
  "review_headline": "Five Stars",
  "review_body": "Cool.",
  "review_date": "2015-01-12T00:00:00.000-06:00"
}
```

Open up MyStreamingApp.scala from src/main/scala/com/kitmenke/spark.

You will need to specify the Kafka bootstrap servers as the first argument.

# Running the Batch App

Open up MyApp.scala from src/main/scala/com/kitmenke/spark.

Right click on MyApp and choose "Run MyApp" to run the Spark app locally. If all goes well, you should see the following output in the log:
```
(to,3)
(new,3)
(the,3)
(enterprise,1)
(space,1)
(are,1)
(mission,1)
(starship,1)
(its,1)
(civilizations,1)
(gone,1)
(one,1)
(strange,1)
(no,1)
(before,1)
(life,1)
(voyages,1)
(worlds,1)
(of,1)
(where,1)
(seek,1)
(go,1)
(and,1)
(out,1)
(frontier,1)
(final,1)
(these,1)
(has,1)
(boldly,1)
(explore,1)
(continuing,1)
```

Congratulations! You just ran a Spark application!

# Known Issues

## MacOS can't assign requested address

Problems running locally?

```
spark.conf.set("spark.driver.host", "localhost")
```

## Connecting to a single node cluster

To write data to a single ndoe cluster, you'll need the following config:
```
spark.conf.set("spark.hadoop.dfs.client.use.datanode.hostname", "true")
spark.conf.set("spark.hadoop.fs.defaultFS", "hdfs://quickstart.cloudera:8020")
```

## WinUtils

You may experience the following error on Windows:
```
2018-01-23 17:00:21 ERROR Shell:396 - Failed to locate the winutils binary in the hadoop binary path
java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
	at org.apache.hadoop.util.Shell.getQualifiedBinPath(Shell.java:378)
	at org.apache.hadoop.util.Shell.getWinUtilsPath(Shell.java:393)
	at org.apache.hadoop.util.Shell.<clinit>(Shell.java:386)
	at org.apache.hadoop.util.StringUtils.<clinit>(StringUtils.java:79)
	at org.apache.hadoop.security.Groups.parseStaticMapping(Groups.java:116)
	at org.apache.hadoop.security.Groups.<init>(Groups.java:93)
	at org.apache.hadoop.security.Groups.<init>(Groups.java:73)
	at org.apache.hadoop.security.Groups.getUserToGroupsMappingService(Groups.java:293)
	at org.apache.hadoop.security.UserGroupInformation.initialize(UserGroupInformation.java:283)
	at org.apache.hadoop.security.UserGroupInformation.ensureInitialized(UserGroupInformation.java:260)
	at org.apache.hadoop.security.UserGroupInformation.loginUserFromSubject(UserGroupInformation.java:789)
	at org.apache.hadoop.security.UserGroupInformation.getLoginUser(UserGroupInformation.java:774)
	at org.apache.hadoop.security.UserGroupInformation.getCurrentUser(UserGroupInformation.java:647)
	at org.apache.spark.util.Utils$$anonfun$getCurrentUserName$1.apply(Utils.scala:2430)
	at org.apache.spark.util.Utils$$anonfun$getCurrentUserName$1.apply(Utils.scala:2430)
	at scala.Option.getOrElse(Option.scala:121)
	at org.apache.spark.util.Utils$.getCurrentUserName(Utils.scala:2430)
	at org.apache.spark.SparkContext.<init>(SparkContext.scala:295)
	at org.apache.spark.SparkContext$.getOrCreate(SparkContext.scala:2509)
	at org.apache.spark.sql.SparkSession$Builder$$anonfun$6.apply(SparkSession.scala:909)
	at org.apache.spark.sql.SparkSession$Builder$$anonfun$6.apply(SparkSession.scala:901)
	at scala.Option.getOrElse(Option.scala:121)
	at org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:901)
	at com.kitmenke.spark.MyApp$.main(MyApp.scala:22)
	at com.kitmenke.spark.MyApp.main(MyApp.scala)
```

This error is mostly harmless although annoying. To fix it, you'll need to download Hadoop binaries for windows. Luckily, there is a repo with these already.

```
git clone https://github.com/steveloughran/winutils
```

Then, in your IntelliJ run configuration, add an environment variable named `HADOOP_HOME` with the value as the path to wherever you cloned winutils, for example: `C:\Users\USERNAME\Code\winutils\hadoop-2.7.1`

# Other Stuff

For a more complicated project, you may run into conflicts between dependencies. In that case, switch to use the maven-shade-plugin.

    <!-- Alternative to maven-assembly-plugin -->
    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.1.0</version>
        <configuration>
            <createDependencyReducedPom>false</createDependencyReducedPom>
            <keepDependenciesWithProvidedScope>false</keepDependenciesWithProvidedScope>
        </configuration>
        <executions>
            <execution>
                <phase>package</phase>
                <goals>
                    <goal>shade</goal>
                </goals>
            </execution>
        </executions>
    </plugin>
    
For Cloudera's distribution of Hadoop, add the following to your pom.xml
    
    <!-- Cloudera specific dependencies -->
    <repositories>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
        </repository>
    </repositories>
    
