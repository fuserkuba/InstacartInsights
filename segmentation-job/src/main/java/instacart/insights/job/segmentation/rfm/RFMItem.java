package instacart.insights.job.segmentation.rfm;

import lombok.Data;
import instacart.insights.model.output.SegmentItem;

import java.util.HashMap;
import java.util.Map;

@Data
public class RFMItem implements SegmentItem {
    protected Double recency;
    protected Double frequency;
    protected Double monetary;

    @Override
    public Map<String, String> getFeatures() {
        Map<String, String> features = new HashMap<String, String>();

        features.put(RFMSchema.recency.name(), recency.toString());
        features.put(RFMSchema.frequency.name(), frequency.toString());
        features.put(RFMSchema.monetary.name(), monetary.toString());

        return features;
    }
}
