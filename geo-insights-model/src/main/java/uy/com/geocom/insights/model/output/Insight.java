package uy.com.geocom.insights.model.output;

import lombok.*;

import java.util.List;
import java.util.Map;

/**
 * Analysis result
 */
@Setter
@ToString
public abstract class Insight {
    protected List<String> datasetIds;
    protected Map<String,String> params;
    // Textual description
    protected String description;
}
