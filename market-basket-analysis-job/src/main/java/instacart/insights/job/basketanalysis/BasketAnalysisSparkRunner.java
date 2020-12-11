package instacart.insights.job.basketanalysis;

import instacart.insights.job.common.InsightsEngine;
import instacart.insights.job.common.Utils;
import instacart.insights.model.output.Insight;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import instacart.insights.model.output.BasketsAnalysis;

public class BasketAnalysisSparkRunner {

    protected static Logger logger=Logger.getLogger(BasketAnalysisSparkRunner.class);

    public static void main(String[] args) {

        logger.setLevel(Level.INFO);

        if (args.length < 5) {
            System.err.println("Usage: BasketAnalysisEngine <fileProducts filePurchases support confidence outputPath>");
            System.exit(1);
        }
        InsightsEngine engine = new BasketAnalysisEngine(SparkSession.builder()
                .appName("BasketAnalysisEngine")
                .getOrCreate());
        //Execute InsightsEngine
        Insight insight = engine.mineInsight(args);

        logger.info("BasketAnalysis Insight mined!!!");

        Utils.printTypicalBaskets(logger,((BasketsAnalysis)insight).getTypicalBasketsOrderedBySupport());
        Utils.printAssociationRules(logger,((BasketsAnalysis)insight).getBasketsRulesOrderedByRelevance());

        //TODO Write insight to outputPath

    }
}
