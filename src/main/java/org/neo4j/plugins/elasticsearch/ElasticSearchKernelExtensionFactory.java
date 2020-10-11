package org.neo4j.plugins.elasticsearch;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class ElasticSearchKernelExtensionFactory
        extends KernelExtensionFactory<ElasticSearchKernelExtensionFactory.Dependencies> {

    private static final String SERVICE_NAME = "NEO4J_ELASTICSEARCH_INTEGRATION";

    public ElasticSearchKernelExtensionFactory() {
        super(SERVICE_NAME);
    }

    @Override
    public Lifecycle newInstance(KernelContext kernelContext, Dependencies dependencies) {
        Config config = dependencies.getConfig();
        return new ElasticSearchExtension.Builder()
                .graphDatabaseService(dependencies.getGraphDatabaseService())
                .host(config.get(ElasticSearchSettings.HOST))
                .indexName(config.get(ElasticSearchSettings.INDEX_NAME))
                .numberOfShards(config.get(ElasticSearchSettings.NUMBER_OF_SHARDS))
                .numberOfReplicas(config.get(ElasticSearchSettings.NUMBER_OF_REPLICAS))
                .discovery(config.get(ElasticSearchSettings.DISCOVERY))
                .syncNodes(config.get(ElasticSearchSettings.SYNC_NODES))
                .syncRelationships(config.get(ElasticSearchSettings.SYNC_RELATIONSHIPS))
                .executeAsync(config.get(ElasticSearchSettings.EXECUTE_ASYNC))
                .build();
    }

    /**
     * Dependencies that holds {@link GraphDatabaseService} and {@link Config}
     * for creating new kernel extension instance.
     */
    public interface Dependencies {

        /**
         * Return the {@link GraphDatabaseService} instance.
         *
         * @return GraphDatabaseService
         */
        GraphDatabaseService getGraphDatabaseService();

        /**
         * Return the {@link Config} instance.
         *
         * @return Config
         */
        Config getConfig();
    }
}
