package org.egov.estokafka;

import lombok.extern.slf4j.Slf4j;
import org.egov.estokafka.config.AppProperties;
import org.egov.estokafka.connector.ElasticsearchConnector;
import org.egov.estokafka.producer.Producer;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;

import java.net.InetAddress;
import java.util.List;

@Slf4j
public class ESToKafkaApplication {

    public static void main(String args[]) {

        AppProperties appProperties = new AppProperties();
        ElasticsearchConnector esConnector = new ElasticsearchConnector(appProperties);

        esConnector.fetchAllRecordsAndPushToKafka();

    }

}
