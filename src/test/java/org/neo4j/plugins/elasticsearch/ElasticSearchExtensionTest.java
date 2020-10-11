package org.neo4j.plugins.elasticsearch;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

public class ElasticSearchExtensionTest {

    @Test
    public void testInit() throws Throwable {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ElasticSearchExtension extension = new ElasticSearchExtension.Builder()
                .graphDatabaseService(db)
                .host("http://localhost:9200")
                .indexName("index_test")
                .numberOfShards(3)
                .numberOfReplicas(0)
                .discovery(false)
                .syncNodes(true)
                .syncRelationships(false)
                .executeAsync(true)
                .build();
        extension.init();
    }
}
