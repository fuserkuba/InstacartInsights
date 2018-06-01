package uy.com.geocom.insights.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Analysis result
 */
//TODO: Incorporar las anotaciones lombok que correspondan
public abstract class Insight {
    protected String datasetId;
    protected Map<String,Object> params;
    // Textual description
    protected String description;
}
