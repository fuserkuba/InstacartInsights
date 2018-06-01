package uy.com.geocom.insights.model.output;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Analysis result
 */
public abstract class Insight {
    protected List<String> datasetIds;
    protected Map<String,String> params;
    // Textual description
    protected String description;
}
