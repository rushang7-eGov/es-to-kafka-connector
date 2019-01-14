package org.egov.estokafka.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.json.JSONArray;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ESScrollResponse {

    private String scrollId;

    private JSONArray hitsContent;

}
