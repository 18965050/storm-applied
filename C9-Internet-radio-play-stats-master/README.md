Pre-requisites for running project:

*  Have an installation of Maven.  For more information, see http://maven.apache.org/
*  Have an installation of JDK version 6 or later.  For more information, see http://www.oracle.com/technetwork/java/javaee/overview/index.html

To run this topology in local mode:

*  Compile the project source, producing a jar file.  Do this by running the following on the command-line:

```
mvn clean package
```

*  Execute the main class in the jar file by running the following:

```
java -jar target/internet-radio-play-stats-1.0.0-jar-with-dependencies.jar
```

*  Sit back and watch the topology continually process messages based on the data in playlogs.txt.


Output of our drpc query will appear amongst regular output shortly before cluster shutdown in standard out. Search for "RESULTS" in your terminal. To find the number of "Classic Rock", "Post Punk" and "Punk" songs played that resulted from our never ending playlog.

Output can be read thusly:

```
["FULL QUERY", "Genre", Play Count]

RESULTS
==========================================================================
[["Classic Rock,Punk,Post Punk","Punk",846],["Classic Rock,Punk,Post Punk","Post Punk",121],["Classic Rock,Punk,Post Punk","Classic Rock",null]]
==========================================================================
```

so in the above

Punk: 846 plays
Post Punk: 121 plays
Classic Rock: No plays

To Run this topology in remote mode:

* Replace pom.xml with pom.xml.remote
* Compile the project, producing a jar as per normal
* Deploy to your storm cluster (see Chapter 5 for how to do this)
* Create a DRPC client such like the following and run it against your cluster:

```
    DRPCClient client = new DRPCClient("YOUR_REMOTE_DRPC_SERVER_NAME_HERE", YOUR_REMOTE_DRPC_SERVER_PORT_HERE);
    try {
      String result = client.execute("count-request-by-tag", "Classic Rock,Punk,Post Punk");
      System.out.println(result);
    } catch (TException e) {
      // thrift error
    } catch (DRPCExecutionException e) {
      // drpc execution error
    }
```

