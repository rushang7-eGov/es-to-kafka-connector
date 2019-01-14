package org.egov.estokafka.connector;

import lombok.extern.slf4j.Slf4j;
import org.egov.estokafka.config.AppProperties;
import org.egov.estokafka.models.ESScrollResponse;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

@Slf4j
public class ElasticsearchConnector {

    private AppProperties appProperties;

    private Integer batchSize;
    private Integer scrollTime;

    private String esURL;
    private String matchAllQueryContent;


    public ElasticsearchConnector(AppProperties appProperties) {
        this.appProperties = appProperties;
        batchSize = appProperties.getBatchSize();
        scrollTime = appProperties.getScrollTime();
        esURL = appProperties.getEsURL();

        matchAllQueryContent = "{\"size\":" + batchSize + ",\"query\":{\"match_all\":{}}}";
    }


    public ESScrollResponse startScroll(String sourceIndex) {
        ESScrollResponse esScrollResponse = new ESScrollResponse();

        try {
            URL queryURL = new URL(esURL + sourceIndex + "_search?scroll=" + scrollTime + "m");
            String response = executeQuery(queryURL, matchAllQueryContent);
            JSONArray hitsContent = getHitsContent(response);
            String scrollId = new JSONObject(response).getString("_scroll_id");
            esScrollResponse = ESScrollResponse.builder().scrollId(scrollId).hitsContent(hitsContent).build();

        } catch (MalformedURLException e) {
            log.error(e.getMessage());
        }

        return esScrollResponse;
    }

    public ESScrollResponse getNextScroll(String sccrollId) {
        ESScrollResponse esScrollResponse = new ESScrollResponse();

        try {
            URL queryURL = new URL(esURL + "_search/scroll");
            String queryContent = "{\"scroll\":\"" + scrollTime + "m\",\"scroll_id\":\"" + sccrollId + "\"}";
            String response = executeQuery(queryURL, queryContent);
            JSONArray hitsContent = getHitsContent(response);
            String scrollId = new JSONObject(response).getString("_scroll_id");
            esScrollResponse = ESScrollResponse.builder().scrollId(scrollId).hitsContent(hitsContent).build();

        } catch (MalformedURLException e) {
            log.error(e.getMessage());
        }

        return esScrollResponse;
    }

    private JSONArray getHitsContent(String esResponse) {
        JSONArray hitsContent = new JSONArray();

        JSONObject esResponseContent = new JSONObject(esResponse);
        JSONArray jsonArray = esResponseContent.getJSONObject("hits").getJSONArray("hits");

        for(Object hit : jsonArray) {
            hitsContent.put(((JSONObject) hit).getJSONObject("_source"));
        }

        return hitsContent;
    }


    private String executeQuery(URL url, String queryContent) {
        String esResponse = null;

        try {
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");

            connection.setDoOutput(true);
            OutputStream connectionOutputStream =  connection.getOutputStream();

            connectionOutputStream.write(queryContent.getBytes());

            if(connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();
                while ((inputLine = in .readLine()) != null) {
                    response.append(inputLine);
                }
                in .close();
                esResponse = response.toString();
            } else {
                log.info("Error in Elasticsearch Query");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        return esResponse;
    }

}
