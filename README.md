# neo4j-elasticsearch-plugin

## Introduction

This neo4j plugin enables you to transfer data to elasticsearch cluster while performing neo4j data changes.

## Usage

1. run `mvn clean install -DskipTests` to install

1. copy installed jar file into `$NEO4J_HOME/plugins/`

1. edit `$NEO4J_HOME/conf/neo4j.conf`

    ```properties
    ## required, the host of elasticsearch cluster
    neo4j.plugin.elasticsearch.host=http://localhost:9200
    ## required, the index name to store data
    neo4j.plugin.elasticsearch.indexName=index_test
    ## optional, the number of shards of the specific index name, default 5
    neo4j.plugin.elasticsearch.numberOfShards=5
    ## optional, the number of replicas of the specific index name, default 1
    neo4j.plugin.elasticsearch.numberOfReplicas=1
    ## optional, elasticsearch discovery mode, default false
    neo4j.plugin.elasticsearch.discovery=false
    ## optional, whether to sync nodes or not, default true
    neo4j.plugin.elasticsearch.syncNodes=true
    ## optional, whether to sync relationships or not, default false
    neo4j.plugin.elasticsearch.syncRelationships=false
    ## optional, transfer data asynchronously, default true
    neo4j.plugin.elasticsearch.executeAsync=true
    ```

1. start neo4j
