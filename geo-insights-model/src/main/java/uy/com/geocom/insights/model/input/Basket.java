package uy.com.geocom.insights.model.input;

import lombok.*;
import uy.com.geocom.insights.model.output.SegmentItem;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;


/**
 * Cesta de compra
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Basket implements SegmentItem {
    protected String id;
    protected LocalDateTime time;
    protected Long clientId;
    protected Double charge;
    protected String paymentMethod;
    protected Long posId;

    @Override
    public Map<String, String> getFeatures() {
        Map<String, String> features=new HashMap<String, String>();
        features.put("id",id);
        features.put("time",time.toString());
        features.put("clientId",clientId.toString());
        features.put("charge",charge.toString());
        features.put("paymentMethod",paymentMethod.toString());
        features.put("posId",posId.toString());
        return features;
    }
}
