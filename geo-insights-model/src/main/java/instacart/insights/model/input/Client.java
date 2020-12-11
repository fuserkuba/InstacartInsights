package instacart.insights.model.input;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import instacart.insights.model.output.SegmentItem;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO: Incorporar otra información demográfica de interés
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Client implements SegmentItem {
    protected String id;
    protected String sex;
    protected Integer age;
    protected String locality;

    @Override
    public Map<String, String> getFeatures() {
        Map<String, String> features = new HashMap<String, String>();
        features.put("id", id);
        features.put("sex", sex);
        features.put("age", age.toString());
        features.put("locality", locality);
        return features;
    }
}
