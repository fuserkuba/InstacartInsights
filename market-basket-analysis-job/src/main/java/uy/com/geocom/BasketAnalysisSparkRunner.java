package uy.com.geocom;

import org.apache.spark.sql.SparkSession;
import uy.com.geocom.common.InsightsEngine;
import uy.com.geocom.insights.model.output.Insight;

public class BasketAnalysisSparkRunner {
    public static void main(String[] args) {

        if (args.length < 5) {
            System.err.println("Usage: BasketAnalysisEngine <fileBaskets filePurchases support confidence outputPath>");
            System.exit(1);
        }
        InsightsEngine engine = new BasketAnalysisEngine(SparkSession.builder()
                .appName("BasketAnalysisEngine")
                .getOrCreate());
        //Execute InsightsEngine
        Insight insight = engine.mineInsight(args);
        //TODO Write insight
    }
}
