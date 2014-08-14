GraphAware Neo4j ChangeFeed
===========================

[![Build Status](https://travis-ci.org/graphaware/neo4j-changefeed.png)](https://travis-ci.org/graphaware/neo4j-changefeed) | <a href="http://graphaware.com/downloads/" target="_blank">Downloads</a> | <a href="http://graphaware.com/site/changefeed/latest/apidocs/" target="_blank">Javadoc</a> | Latest Release: 2.1.3.11.3

GraphAware ChangeFeed is a [GraphAware](https://github.com/graphaware/neo4j-framework) Runtime Module that keeps track of changes made to the graph.

Getting the Software
--------------------

### Server Mode

When using Neo4j in the <a href="http://docs.neo4j.org/chunked/stable/server-installation.html" target="_blank">standalone server</a> mode,
you will need the <a href="https://github.com/graphaware/neo4j-framework" target="_blank">GraphAware Neo4j Framework</a> and GraphAware Neo4j ChangeFeed .jar files (both of which you can <a href="http://graphaware.com/downloads/" target="_blank">download here</a>) dropped
into the `plugins` directory of your Neo4j installation. After a change in neo4.properties (described later) and Neo4j restart, you will be able to use the REST APIs of the ChangeFeed.

### Embedded Mode / Java Development

Java developers that use Neo4j in <a href="http://docs.neo4j.org/chunked/stable/tutorials-java-embedded.html" target="_blank">embedded mode</a>
and those developing Neo4j <a href="http://docs.neo4j.org/chunked/stable/server-plugins.html" target="_blank">server plugins</a>,
<a href="http://docs.neo4j.org/chunked/stable/server-unmanaged-extensions.html" target="_blank">unmanaged extensions</a>,
GraphAware Runtime Modules, or Spring MVC Controllers can include use the ChangeFeed as a dependency for their Java project.

#### Releases

Releases are synced to <a href="http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22changefeed%22" target="_blank">Maven Central repository</a>. When using Maven for dependency management, include the following dependency in your pom.xml.

    <dependencies>
        ...
        <dependency>
            <groupId>com.graphaware.neo4j</groupId>
            <artifactId>changefeed</artifactId>
            <version>2.1.3.11.3</version>
        </dependency>
        ...
    </dependencies>

#### Snapshots

To use the latest development version, just clone this repository, run `mvn clean install` and change the version in the
dependency above to 2.1.3.11.4-SNAPSHOT.

#### Note on Versioning Scheme

The version number has two parts. The first four numbers indicate compatibility with Neo4j GraphAware Framework.
 The last number is the version of the ChangeFeed library. For example, version 2.1.2.10.2 is version 2 of the ChangeFeed
 compatible with GraphAware Neo4j Framework 2.1.2.10.

Setup and Configuration
=======================

### Server Mode

Edit neo4j.properties to register the ChangeFeed module:

```
com.graphaware.runtime.enabled=true

#CFM becomes the module ID:
com.graphaware.module.CFM.1=com.graphaware.module.changefeed.ChangeFeedModuleBootstrapper

#optional, default is 100:
com.graphaware.module.CFM.maxChanges=100

#optional, default is 10000 (10 seconds):
com.graphaware.module.CFM.pruneDelay=10000

#optional, default is 10;
com.graphaware.module.CFM.pruneWhenExceeded=10
```

Note that "CFM" becomes the module ID. It is possible to register the ChangeFeed module multiple times with different
configurations, provided that their IDs are different. This ID is important for querying the feed (read on).

`com.graphaware.module.CFM.maxChanges` limits the total number of changes tracked. The default is 100.
Note that for efficiency, the total number of changes at any given point may be 10 (by default) more than the maxChanges
set but will eventually constrain the size to 100. The default value can be changed by setting the `com.graphaware.module.CFM.pruneWhenExceeded`
configuration value. Finally, `com.graphaware.module.CFM.pruneDelay` specifies in milliseconds, how frequently the changes
 will be checked for pruning. The default is 10 seconds.

### Embedded Mode / Java Development

To use the ChangeFeed programmatically, register the module like this

```java
 GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);  //where database is an instance of GraphDatabaseService
 ChangeFeedModule module = new ChangeFeedModule("CFM", ChangeFeedConfiguration.defaultConfiguration(), database);
 runtime.registerModule(module);
 runtime.start();
```

Alternatively:
```java
 GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(pathToDb)
    .loadPropertiesFromFile(this.getClass().getClassLoader().getResource("neo4j.properties").getPath())
    .newGraphDatabase();
 
 //make sure neo4j.properties contain the lines mentioned in previous section
```

Using GraphAware ChangeFeed
===========================

### Server Mode

In Server Mode, the ChangeFeed is accessible via the REST API.

You can issue GET requests to `http://your-server-address:7474/graphaware/changefeed/{moduleId}` to get a list of changes
made to the graph, most recent change first. {moduleId} is the module ID the ChangeFeed Module was registered with. You
can omit this part of the URL, in which case "CFM" is assumed as the default value.

 Two parameters can be added to each request, `uuid` and `limit`, where `uuid` is the uuid of the last change set the client
 has already seen, so only changes later than the given one will be returned. `limit` is the maximum
 number of changes to return, most recent change first. A GET request using these parameters would be issued to the following
 URL: `http://your-server-address:7474/graphaware/changefeed/{moduleId}?uuid={uuid}&limit={limit}`

The REST API returns a JSON array of changesets. A changeset contains the following:

* uuid - the uuid of the changeset
* timestamp - timestamp of the changeset (represented as the number of milliseconds since 1/1/1970)
* changes - an array of Strings representing each modification to the graph that occurred in the same transaction

e.g.
```json
[
    {
        "uuid": "376de020-20b3-11e4-83b0-f0b4792288ef",
        "timestamp": 1405411937335,
        "changes": [
            "Created node (:Person {name: Doe})"
        ]
    },
    {
        "uuid": "376de021-20b3-11e4-83b0-f0b4792288ef",
        "timestamp": 1405411933210,
        "changes": [
            "Created node (:Person {name: John})"
        ]
    }
]
```

*NOTE*: Please note that timestamps are assigned at the instant when the transaction starts committing.
Consequently, the order does not represent the order in which the transactions have been committed. 

### Java API

To use the Java API, please instantiate `CachingGraphChangeReader` and use one of its methods for getting the changes.

```
GraphChangeReader reader = new CachingGraphChangeReader(database);
Collection<ChangeSet> changes = reader.getAllChanges();
```

In case more than one ChangeFeed Module is registered or a single one with ID different than "CFM" is registered, then
the ID must be specified when constructing the reader.

```
GraphChangeReader reader = new CachingGraphChangeReader(database, "ModuleID");
Collection<ChangeSet> changes = reader.getAllChanges();
```

Please refer to Javadoc for more detail.

Limitations
-----------

Note that Node IDs and Relationship IDs are not exposed by the change feed. This is a deliberate choice as it is not a
good practice to expose internal IDs outside of Neo4j. Please use custom identifiers (such as UUIDs) instead.

Also note that the contents of the changes are human-, rather than machine-readable. This will be changed in future
versions.

Both functionality and performance of ChangeFeed have been extensively tested on single-machine deployments. 

License
-------

Copyright (c) 2014 GraphAware

GraphAware is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program.
If not, see <http://www.gnu.org/licenses/>.
