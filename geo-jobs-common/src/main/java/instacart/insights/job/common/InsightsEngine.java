package instacart.insights.job.common;

import instacart.insights.model.output.Insight;

public interface InsightsEngine {
    public Insight mineInsight(String... parameters);
}
