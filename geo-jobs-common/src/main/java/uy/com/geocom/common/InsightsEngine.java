package uy.com.geocom.common;

import uy.com.geocom.insights.model.output.Insight;

public interface InsightsEngine {
    public Insight mineInsight(String... parameters);
}
