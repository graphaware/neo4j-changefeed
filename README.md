GraphAware Neo4j ChangeFeed
================

GraphAware ChangeFeed is a [GraphAware](https://github.com/graphaware/neo4j-framework) Runtime Module that keeps track of changes made to the graph.

Getting the Software
--------------------

### Server Mode

When using Neo4j in the <a href="http://docs.neo4j.org/chunked/stable/server-installation.html" target="_blank">standalone server</a> mode,
you will need the <a href="https://github.com/graphaware/neo4j-framework" target="_blank">GraphAware Neo4j Framework</a> and GraphAware Neo4j Changefeed .jar files (both of which you can <a href="http://graphaware.com/downloads/" target="_blank">download here</a>) dropped
into the `plugins` directory of your Neo4j installation. 

Edit neo4j.properties to register the ChangeFeed module:

com.graphaware.runtime.enabled=true
com.graphaware.module.CFM.1=com.graphaware.module.changefeed.ChangeFeedModuleBootstrapper
com.graphaware.module.CFM.maxChanges=100

com.graphaware.module.CFM.maxChanges limits the total number of changes tracked. The default is 100.
Note that for efficiency, the total number of changes at any given point may be 10 more than the maxChanges set but will eventually constrain the size to 100.

### Embedded Mode / Java Development

Java developers that use Neo4j in <a href="http://docs.neo4j.org/chunked/stable/tutorials-java-embedded.html" target="_blank">embedded mode</a>
and those developing Neo4j <a href="http://docs.neo4j.org/chunked/stable/server-plugins.html" target="_blank">server plugins</a>,
<a href="http://docs.neo4j.org/chunked/stable/server-unmanaged-extensions.html" target="_blank">unmanaged extensions</a>,
GraphAware Runtime Modules, or Spring MVC Controllers can include use the ChangeFeed as a dependency for their Java project.

To use the ChangeFeed programmatically, register the module like this

```java
 GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
 ChangeFeedModule module = new ChangeFeedModule("CFM", new ChangeFeedConfiguration(100), database);    //where 100 is the maxChanges configuration
 runtime.registerModule(module);
 runtime.start();
 ChangeReader changeReader = new GraphChangeReader(database);
```

Alternatively:
```java
 GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(pathToDb)
    .loadPropertiesFromFile(this.getClass().getClassLoader().getResource("neo4j.properties").getPath())
    .newGraphDatabase();
 
 //make sure neo4j.properties contain the lines mentioned in previous section
```

Using GraphAware ChangeFeed
--------------------------

The ChangeFeed is accessible via the REST or Java API. 

### REST API

When deployed in server mode, there are two URLs that you can issue GET requests to:
* `http://your-server-address:7474/graphaware/changefeed/` to get a list of changes made to the graph, most recent change first.
* `http://your-server-address:7474/graphaware/changefeed?since={sequence}&limit={limit}` to get a list of changes made to the graph after a particular change (most recent change first).
 {sequence} is optional and must be replaced with the sequence number of the change if used. The change represented by this {sequence} will NOT be returned.
 {limit} is optional and if used, must be replaced with the maximum number of changes to return.

The REST API a JSON array of changesets. A changeset contains the following:

* sequence - the sequence number of the changeset
* timestamp - timestamp of the changeset (represented as the number of milliseconds since 1/1/1970)
* changes - an array of Strings representing each modification to the graph that occurred in the same transaction

e.g.
```json
[
    {
        "sequence": 2,
        "timestamp": 1405411937335,
        "changes": [
            "Created node (:Person {name: Doe})"
        ]
    },
    {
        "sequence": 1,
        "timestamp": 1405411933210,
        "changes": [
            "Created node (:Person {name: John})"
        ]
    }
]
```

*NOTE*: Please note that timestamps and sequence numbers are assigned at the instant when the transaction starts committing.
Consequently, the order does not represent the order in which the transactions have been committed. Also, there can
be missing sequence numbers if a transaction failed to commit for whatever reason.

### Java API

Java API has the same functionality as the rest API. Please refer to the Javadoc (link it)

License
-------

Copyright (c) 2014 GraphAware

GraphAware is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program.
If not, see <http://www.gnu.org/licenses/>.
