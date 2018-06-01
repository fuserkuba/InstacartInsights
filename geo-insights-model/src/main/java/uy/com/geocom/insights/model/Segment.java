package uy.com.geocom.insights.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Segment {
    protected List<Long> itemIds;
    /**
     * e.g.: [{"recency",[{"count",10.0},{"mean",53.3},{"stddev",53.3}]},
     *        {"frequency",[{"min",18.0},{"50%",24.3},{"max",92.3}]},
     *        {"monetary",[{"count",10.0},{"min",163.0},{"25%",176.4},{"50%",178.3},{"75%",180.53},{"max",192.8}]}]
     */
    protected Map<String,Map<String,Double>> statisticsValuesByFeature;
    // TODO: Definir si mantenemos Object o especificamos que es Client
    protected Object typicalItem;
    protected Map<String,Object> typicalItemFeatures;

}
