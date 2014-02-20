
Setting up Gremlin command line interface
-----------------------------------------

1. [Download](https://github.com/tinkerpop/gremlin/wiki/Downloads) and extract Gremlin
1. Create a file called `gremlin-sg-accumulo.config` with the following contents:

        storage=com.altamiracorp.securegraph.accumulo.blueprints.AccumuloSecureGraphBlueprintsGraphFactory
        storage.graph.useServerSideElementVisibilityRowFilter=false
        storage.graph.tableNamePrefix=sg
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

        storage.authorizationsProvider=com.altamiracorp.securegraph.accumulo.blueprints.AccumuloAuthorizationsProvider
        storage.authorizationsProvider.auths=auth1,auth2

1. Create a file called `gremlin-sg.script` with the following contents:

        g = com.altamiracorp.securegraph.blueprints.SecureGraphBlueprintsFactory.open('gremlin-sg-accumulo.config')

1. Run `mvn package -DskipTests` from the root of securegraph.
1. Run

        cp securegraph-core/target/securegraph-core-*.jar ${GREMLIN_HOME}/lib
        cp securegraph-blueprints/target/securegraph-blueprints-*.jar ${GREMLIN_HOME}/lib
        cp securegraph-accumulo/target/securegraph-accumulo-*.jar ${GREMLIN_HOME}/lib
        cp securegraph-accumulo-blueprints/target/securegraph-accumulo-blueprints-*.jar ${GREMLIN_HOME}/lib
        cp securegraph-elasticsearch/target/securegraph-elasticsearch-*.jar ${GREMLIN_HOME}/lib

1. Copy other dependencies accumulo, hadoop, etc. to ${GREMLIN_HOME}/lib

        accumulo-core-1.5.0.jar
        accumulo-fate-1.5.0.jar
        accumulo-trace-1.5.0.jar
        commons-io-2.4.jar
        hadoop-client-0.23.10.jar
        hadoop-common-0.23.10.jar
        hadoop-core-0.20.2.jar
        hadoop-auth-0.23.10.jar
        guava-14.0.1.jar
        libthrift-0.9.0.jar

        elasticsearch-0.90.0.jar
        lucene-analyzers-common-4.2.1.jar
        lucene-codecs-4.2.1.jar
        lucene-core-4.2.1.jar
        lucene-grouping-4.2.1.jar
        lucene-highlighter-4.2.1.jar
        lucene-join-4.2.1.jar
        lucene-memory-4.2.1.jar
        lucene-queries-4.2.1.jar
        lucene-queryparser-4.2.1.jar
        lucene-sandbox-4.2.1.jar
        lucene-spatial-4.2.1.jar
        lucene-suggest-4.2.1.jar

        rm lucene-core-3.6.2.jar

1. Run `${GREMLIN_HOME}/bin/gremlin.sh gremlin-sg.script`

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
            <graph-type>com.altamiracorp.securegraph.accumulo.blueprints.AccumuloSecureGraphRexsterGraphConfiguration</graph-type>
            <storage>com.altamiracorp.securegraph.accumulo.blueprints.AccumuloSecureGraphBlueprintsGraphFactory</storage>
            <graph-useServerSideElementVisibilityRowFilter>false</graph-useServerSideElementVisibilityRowFilter>
            <graph-accumuloInstanceName>accumulo</graph-accumuloInstanceName>
            <graph-username>root</graph-username>
            <graph-password>password</graph-password>
            <graph-tableNamePrefix>sg</graph-tableNamePrefix>
            <graph-zookeeperServers>192.168.33.10,192.168.33.10</graph-zookeeperServers>
            <graph-serializer>com.altamiracorp.securegraph.accumulo.serializer.JavaValueSerializer</graph-serializer>
            <graph-idgenerator>com.altamiracorp.securegraph.id.UUIDIdGenerator</graph-idgenerator>
            <graph-search>com.altamiracorp.securegraph.elasticsearch.ElasticSearchSearchIndex</graph-search>
            <graph-search-locations>192.168.33.10</graph-search-locations>
            <graph-search-indexName>securegraph</graph-search-indexName>
            <visibilityProvider>com.altamiracorp.securegraph.blueprints.DefaultVisibilityProvider</visibilityProvider>
            <authorizationsProvider>com.altamiracorp.securegraph.accumulo.blueprints.AccumuloAuthorizationsProvider</authorizationsProvider>
            <authorizationsProvider-auths>auth1,auth2</authorizationsProvider-auths>
            <extensions>
                <allows>
                    <allow>tp:gremlin</allow>
                </allows>
            </extensions>
        </graph>
