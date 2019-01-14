package org.egov.estokafka.connector;

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
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;

import java.net.InetAddress;

@Slf4j
public class ElasticsearchConnector {

    private AppProperties appProperties;

    private Integer batchSize;
    private Integer scrollTime;

    private Client client;
    private Producer producer;


    public ElasticsearchConnector(AppProperties appProperties) {
        this.appProperties = appProperties;

        batchSize = appProperties.getBatchSize();
        scrollTime = appProperties.getScrollTime();

        producer = new Producer();

        try {
            client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(appProperties.getEsHost()),
                            Integer.parseInt(appProperties.getEsPort())));
        } catch (Exception e) {
            log.error(e.getMessage());
        }

    }

    public void fetchAllRecordsAndPushToKafka() {

        SearchResponse scrollResp = client.prepareSearch(appProperties.getSourceIndex())
                .setQuery(QueryBuilders.matchAllQuery())
                .setScroll(new TimeValue(scrollTime))
                .setSize(batchSize).get();

        Long numberOfRecordsProduced = 0L;
        do {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                producer.push(appProperties.getDestinationTopic(), new JSONObject(hit.getSourceAsMap()));
            }
            numberOfRecordsProduced += scrollResp.getHits().getHits().length;
            log.info("Reocrds produced : " + numberOfRecordsProduced);
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(scrollTime))
                    .execute().actionGet();

        } while(scrollResp.getHits().getHits().length != 0);

        log.info("Total Records Produced : " + numberOfRecordsProduced);

    }

}
