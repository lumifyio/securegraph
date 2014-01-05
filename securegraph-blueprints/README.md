
Setting up Gremlin command line interface
-----------------------------------------

* [Download](https://github.com/tinkerpop/gremlin/wiki/Downloads) and extract Gremlin
* Create a file called `gremlin-sg-accumulo.config` with the following contents:

```
storage=com.altamiracorp.securegraph.accumulo.blueprints.AccumuloSecureGraphBlueprintsGraphFactory
storage.graph.tableName=sg
storage.graph.accumuloInstanceName=sg
storage.graph.zookeeperServers=localhost
storage.graph.username=root
storage.graph.password=password
storage.graph.autoFlush=true

storage.graph.search=com.altamiracorp.securegraph.elasticsearch.ElasticSearchSearchIndex
storage.graph.search.locations=localhost
storage.graph.search.indexName=sg

storage.graph.serializer=com.altamiracorp.securegraph.accumulo.serializer.JavaValueSerializer

storage.graph.idgenerator=com.altamiracorp.securegraph.id.UUIDIdGenerator

storage.visibilityProvider=com.altamiracorp.securegraph.blueprints.DefaultVisibilityProvider

storage.authorizationsProvider=com.altamiracorp.securegraph.blueprints.DefaultAuthorizationsProvider
```

* Create a file called `gremlin-sg.script` with the following contents:

```
g = com.altamiracorp.securegraph.blueprints.SecureGraphBlueprintsFactory.open('gremlin-sg-accumulo.config')
```

* Run `mvn package -DskipTests` from the root of securegraph.
* Run

```
cp securegraph-core/target/securegraph-core-*.jar ${GREMLIN_HOME}/lib
cp securegraph-blueprints/target/securegraph-blueprints-*.jar ${GREMLIN_HOME}/lib
cp securegraph-accumulo/target/securegraph-accumulo-*.jar ${GREMLIN_HOME}/lib
cp securegraph-accumulo-blueprints/target/securegraph-accumulo-blueprints-*.jar ${GREMLIN_HOME}/lib
cp securegraph-elasticsearch/target/securegraph-elasticsearch-*.jar ${GREMLIN_HOME}/lib
```

* Copy other dependencies accumulo, hadoop, etc. to ${GREMLIN_HOME}/lib
* Run `${GREMLIN_HOME}/bin/gremlin.sh gremlin-sg.script`