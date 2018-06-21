package uy.com.geocom.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import uy.com.geocom.insights.model.output.AssociationRule;
import uy.com.geocom.insights.model.output.FrequentItemSet;

import java.util.Arrays;
import java.util.List;

public class Utils {

    public static Dataset<Row> readDataSetFromFile(SparkSession spark, String dataPath, StructType structType) {
        if (dataPath.contains(".csv")) {
            return spark.read()
                    .schema(structType)
                    .option("header", true)
                    .option("dateFormat", "yyyy-MM-dd")
                    .csv(dataPath);
        } else if (dataPath.contains(".json")) {
            return spark.read().schema(structType).json(dataPath);
        } else if (dataPath.contains(".parquet")) {
            return spark.read().parquet(dataPath);
        } else
            //Tab Separated Files (TSV)
            return spark.read().option("sep", "t").csv(dataPath);
    }

    public static void describeDataSet(Logger logger, Dataset dataset, String title, int rows) {
        //display schema of data
        logger.info("------------".concat(title + " (").concat(dataset.count() + " rows) ------------"));
        dataset.printSchema();
        dataset.show(rows);
    }

    public static void printTypicalBaskets(Logger logger, List<FrequentItemSet> typicalBaskets) {
        for (FrequentItemSet frequentItemSet : typicalBaskets) {
            System.out.println(frequentItemSet.getRank() + " - " + frequentItemSet.getFreq()
                    + " -> (" + frequentItemSet.getItems().length + ") " + Arrays.toString(frequentItemSet.getItems()));
        }
    }

    public static void printAssociationRules(Logger logger, List<AssociationRule> rules) {
        for (AssociationRule rule : rules) {
            System.out.println(Arrays.toString(rule.getAntecedentItemIds())
                    + " --> " + rule.getConsequentItemId()
                    + " rank: " + rule.getRank()
                    + " confidence: " + rule.getConfidence()
                    + " relevance: " + rule.getRelevance());
        }
    }
}
