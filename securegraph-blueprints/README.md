
Setting up Gremlin command line interface
-----------------------------------------

1. [Download](https://github.com/tinkerpop/gremlin/wiki/Downloads) and extract Gremlin (tested with 2.6.0)
1. Create a file called `gremlin-sg-accumulo.config` with the following contents:

        storage=org.securegraph.accumulo.blueprints.AccumuloSecureGraphBlueprintsGraphFactory
        storage.graph.useServerSideElementVisibilityRowFilter=false
        storage.graph.tableNamePrefix=sg
        storage.graph.accumuloInstanceName=sg
        storage.graph.zookeeperServers=localhost
        storage.graph.username=root
        storage.graph.password=password
        storage.graph.autoFlush=true

        storage.graph.search=org.securegraph.elasticsearch.ElasticSearchSearchIndex
        storage.graph.search.locations=localhost
        storage.graph.search.indexName=sg

        storage.graph.serializer=org.securegraph.accumulo.serializer.JavaValueSerializer

        storage.graph.idgenerator=org.securegraph.id.UUIDIdGenerator

        storage.visibilityProvider=org.securegraph.blueprints.DefaultVisibilityProvider

        storage.authorizationsProvider=org.securegraph.accumulo.blueprints.AccumuloAuthorizationsProvider
        storage.authorizationsProvider.auths=auth1,auth2

1. Create a file called `gremlin-sg.script` with the following contents:

        g = org.securegraph.blueprints.SecureGraphBlueprintsFactory.open('gremlin-sg-accumulo.config')

1. Run `mvn package -DskipTests` from the root of securegraph.
1. Run

        cp securegraph-core/target/securegraph-core-*.jar ${GREMLIN_HOME}/lib
        cp securegraph-blueprints/target/securegraph-blueprints-*.jar ${GREMLIN_HOME}/lib
        cp securegraph-accumulo/target/securegraph-accumulo-*.jar ${GREMLIN_HOME}/lib
        cp securegraph-accumulo-blueprints/target/securegraph-accumulo-blueprints-*.jar ${GREMLIN_HOME}/lib
        cp securegraph-elasticsearch/target/securegraph-elasticsearch-*.jar ${GREMLIN_HOME}/lib
        cp securegraph-elasticsearch-base/target/securegraph-elasticsearch-*.jar ${GREMLIN_HOME}/lib

1. Copy other dependencies accumulo, hadoop, etc. to ${GREMLIN_HOME}/lib

        cp ~/.m2/repository/org/apache/accumulo/accumulo-core/1.5.2/accumulo-core-1.5.2.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/accumulo/accumulo-fate/1.5.2/accumulo-fate-1.5.2.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/accumulo/accumulo-trace/1.5.2/accumulo-trace-1.5.2.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/commons-io/commons-io/2.4/commons-io-2.4.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/hadoop/hadoop-client/0.23.10/hadoop-client-0.23.10.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/hadoop/hadoop-common/0.23.10/hadoop-common-0.23.10.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/hadoop/hadoop-core/0.20.2/hadoop-core-0.20.2.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/hadoop/hadoop-auth/0.23.10/hadoop-auth-0.23.10.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/thrift/libthrift/0.9.0/libthrift-0.9.0.jar ${GREMLIN_HOME}/lib

        cp ~/.m2/repository/org/elasticsearch/elasticsearch/1.2.0/elasticsearch-1.2.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-analyzers-common/4.9.0/lucene-analyzers-common-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-codecs/4.9.0/lucene-codecs-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-core/4.9.0/lucene-core-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-grouping/4.9.0/lucene-grouping-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-highlighter/4.9.0/lucene-highlighter-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-join/4.9.0/lucene-join-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-memory/4.9.0/lucene-memory-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-queries/4.9.0/lucene-queries-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-queryparser/4.9.0/lucene-queryparser-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-sandbox/4.9.0/lucene-sandbox-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-spatial/4.9.0/lucene-spatial-4.9.0.jar ${GREMLIN_HOME}/lib
        cp ~/.m2/repository/org/apache/lucene/lucene-suggest/4.9.0/lucene-suggest-4.9.0.jar ${GREMLIN_HOME}/lib

        rm lucene-core-3.6.2.jar

1. Run `${GREMLIN_HOME}/bin/gremlin.sh gremlin-sg.script`
1. Test is out:
        
        v = g.addVertex()
        g.V

Setting up Rexster
------------------

1. Download Rexster and unzip

        curl -O -L http://tinkerpop.com/downloads/rexster/rexster-server-2.4.0.zip > rexster-server-2.4.0.zip
        unzip rexster-server-2.4.0.zip

1. Run maven just like in the gremlin section

1. Copy the secure graph jars just like in the gremlin section

1. Copy the dependencies just like in the gremlin section to `${REXSTER_HOME}/lib`

1. Edit `${REXSTER_HOME}/config/rexster.xml` and add the following to the graphs element

        <graph>
            <graph-name>securegraph</graph-name>
            <graph-type>org.securegraph.accumulo.blueprints.AccumuloSecureGraphRexsterGraphConfiguration</graph-type>
            <storage>org.securegraph.accumulo.blueprints.AccumuloSecureGraphBlueprintsGraphFactory</storage>
            <graph-useServerSideElementVisibilityRowFilter>false</graph-useServerSideElementVisibilityRowFilter>
            <graph-accumuloInstanceName>accumulo</graph-accumuloInstanceName>
            <graph-username>root</graph-username>
            <graph-password>password</graph-password>
            <graph-tableNamePrefix>sg</graph-tableNamePrefix>
            <graph-zookeeperServers>192.168.33.10,192.168.33.10</graph-zookeeperServers>
            <graph-serializer>org.securegraph.accumulo.serializer.JavaValueSerializer</graph-serializer>
            <graph-idgenerator>org.securegraph.id.UUIDIdGenerator</graph-idgenerator>
            <graph-search>org.securegraph.elasticsearch.ElasticSearchSearchIndex</graph-search>
            <graph-search-locations>192.168.33.10</graph-search-locations>
            <graph-search-indexName>securegraph</graph-search-indexName>
            <visibilityProvider>org.securegraph.blueprints.DefaultVisibilityProvider</visibilityProvider>
            <authorizationsProvider>org.securegraph.accumulo.blueprints.AccumuloAuthorizationsProvider</authorizationsProvider>
            <authorizationsProvider-auths>auth1,auth2</authorizationsProvider-auths>
            <extensions>
                <allows>
                    <allow>tp:gremlin</allow>
                </allows>
            </extensions>
        </graph>
