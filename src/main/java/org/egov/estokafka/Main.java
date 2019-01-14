package org.egov.estokafka;

import lombok.extern.slf4j.Slf4j;
import org.egov.estokafka.config.AppProperties;
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
public class Main {

    public static void main(String args[]) {

        AppProperties appProperties = new AppProperties();
        Producer producer = new Producer();

//        ElasticsearchConnector elasticsearchConnector = new ElasticsearchConnector(appProperties);
//        ESScrollResponse esScrollResponse;
//
//        esScrollResponse = elasticsearchConnector.startScroll(appProperties.getSourceIndex());
//
//        Integer numberOfRecordsProduced = 0;
//
//        while (esScrollResponse.getHitsContent().length() > 0) {
//            producer.pushBulk(appProperties.getDestinationTopic(), esScrollResponse.getHitsContent());
//            numberOfRecordsProduced += esScrollResponse.getHitsContent().length();
//            log.info("Total Records Produced : " + numberOfRecordsProduced);
//            esScrollResponse = elasticsearchConnector.getNextScroll(esScrollResponse.getScrollId());
//        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        Client client = null;
        try {
            client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        } catch (Exception e) {
            log.error(e.getMessage());
        }

        SearchResponse scrollResp = client.prepareSearch(appProperties.getSourceIndex())
                .setQuery(QueryBuilders.matchAllQuery())
                .setScroll(new TimeValue(appProperties.getScrollTime()))
                .setSize(appProperties.getBatchSize()).get();

        Long numberOfRecordsProduced = 0L;
        do {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                producer.push(appProperties.getDestinationTopic(), new JSONObject(hit.getSourceAsMap()));
            }
            numberOfRecordsProduced += scrollResp.getHits().getHits().length;
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(appProperties.getScrollTime()))
                    .execute().actionGet();

        } while(scrollResp.getHits().getHits().length != 0);

        log.info("Total Records Produced : " + numberOfRecordsProduced);


        //////////////////////////////////////////////////////////////////////////////////////

//        CardinalityAggregationBuilder cardinalityAggregationBuilder =
//                AggregationBuilders
//                        .cardinality("agg")
//                        .field("actor.id.keyword");
//
//        SearchResponse searchResponse = client.prepareSearch(appProperties.getSourceIndex()).addAggregation
//                (cardinalityAggregationBuilder).get();
//
//        Cardinality agg = searchResponse.getAggregations().get("agg");
//
//        log.info("Number of Distinct User Ids : " + agg.getValue());
//
//        Integer numberOfDistinctUserIds = Math.toIntExact(agg.getValue());
//
//
//        SearchResponse scrollResp1 = client.prepareSearch(appProperties.getSourceIndex()).addAggregation
//                (AggregationBuilders.terms("distinct_uid").field("actor.id.keyword").size(numberOfDistinctUserIds))
//                .get();
//
//        Terms userIds = scrollResp1.getAggregations().get("distinct_uid");
//
//        for (Terms.Bucket entry : userIds.getBuckets()) {
//            System.out.println(entry.getKey());      // Term
//            System.out.println(entry.getDocCount()); // Doc count
//        }

    }

}
