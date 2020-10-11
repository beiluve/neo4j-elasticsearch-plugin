package org.neo4j.plugins.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ElasticSearchExtension extends LifecycleAdapter {

    private final static Logger logger = Logger.getLogger(ElasticSearchExtension.class.getName());

    private final GraphDatabaseService graphDatabaseService;

    private final String host;

    private final String indexName;

    private final Integer numberOfShards;

    private final Integer numberOfReplicas;

    private final Boolean syncNodes;

    private final Boolean syncRelationships;

    private final Boolean executeAsync;

    private final Boolean discovery;

    private ElasticSearchExtension(Builder builder) {
        this.graphDatabaseService = builder.graphDatabaseService;
        this.host = builder.host;
        this.indexName = builder.indexName.toLowerCase();
        this.numberOfShards = builder.numberOfShards;
        this.numberOfReplicas = builder.numberOfReplicas;
        this.syncNodes = builder.syncNodes;
        this.syncRelationships = builder.syncRelationships;
        this.executeAsync = builder.executeAsync;
        this.discovery = builder.discovery;
    }

    public static class Builder {

        private GraphDatabaseService graphDatabaseService;

        private String host;

        private String indexName;

        private Integer numberOfShards;

        private Integer numberOfReplicas;

        private Boolean discovery;

        private Boolean syncNodes;

        private Boolean syncRelationships;

        private Boolean executeAsync;

        public Builder graphDatabaseService(GraphDatabaseService graphDatabaseService) {
            this.graphDatabaseService = graphDatabaseService;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder numberOfShards(Integer numberOfShards) {
            this.numberOfShards = numberOfShards;
            return this;
        }

        public Builder numberOfReplicas(Integer numberOfReplicas) {
            this.numberOfReplicas = numberOfReplicas;
            return this;
        }

        public Builder discovery(Boolean discovery) {
            this.discovery = discovery;
            return this;
        }

        public Builder syncNodes(Boolean syncNodes) {
            this.syncNodes = syncNodes;
            return this;
        }

        public Builder syncRelationships(Boolean syncRelationships) {
            this.syncRelationships = syncRelationships;
            return this;
        }

        public Builder executeAsync(Boolean executeAsync) {
            this.executeAsync = executeAsync;
            return this;
        }

        public ElasticSearchExtension build() {
            return new ElasticSearchExtension(this);
        }
    }

    private JestClient jestClient;

    private ElasticSearchEventHandler elasticSearchEventHandler;

    @Override
    public void init() throws Throwable {
        // get JestClient
        jestClient = JestHttpClientFactory.getClient(host, discovery);
        // whether the specific index name exists
        if (existsIndex(indexName)) {
            logger.info("ElasticSearch Index: [" + indexName + "] already exists.");
        } else {
            // create specific index
            Map<String, Object> settings = new HashMap<>(4);
            settings.put("number_of_shards", numberOfShards);
            settings.put("number_of_replicas", numberOfReplicas);
            if (createIndex(indexName, settings)) {
                logger.info("ElasticSearch Index: [" + indexName + "] created.");
            } else {
                logger.log(Level.WARNING, "ElasticSearch Index: [" + indexName + "] create failed.");
                return;
            }
        }

        // build ElasticSearchEventHandler
        elasticSearchEventHandler = new ElasticSearchEventHandler.Builder()
                .jestClient(jestClient)
                .indexName(indexName)
                .syncNodes(syncNodes)
                .syncRelationships(syncRelationships)
                .executeAsync(executeAsync)
                .build();

        // register ElasticSearchEventHandler to GraphDatabaseService
        graphDatabaseService.registerTransactionEventHandler(elasticSearchEventHandler);
        logger.info("Neo4j elasticsearch plugin registered!");
    }

    @Override
    public void shutdown() throws Throwable {
        this.graphDatabaseService.unregisterTransactionEventHandler(elasticSearchEventHandler);
        this.jestClient.close();
        logger.info("Neo4j elasticsearch plugin shutdown!");
    }

    private boolean existsIndex(String indexName) throws IOException {
        JestResult result = jestClient.execute(new IndicesExists.Builder(indexName).build());
        return result.isSucceeded();
    }

    private boolean createIndex(String indexName, Map<String, Object> settings) throws IOException {
        final JestResult result = jestClient.execute(new CreateIndex.Builder(indexName).settings(settings).build());
        return result.isSucceeded();
    }
}
