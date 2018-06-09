package uy.com.geocom.insight.dataextractor;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "insights.extractor", ignoreUnknownFields = false)
public class InsightExtractorProperties {

    private String path;

}
