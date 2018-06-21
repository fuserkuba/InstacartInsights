package uy.com.geocom;

import lombok.Builder;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import uy.com.geocom.common.ProductSchema;
import uy.com.geocom.insights.model.input.Product;
import uy.com.geocom.insights.model.output.AssociationRule;
import uy.com.geocom.insights.model.output.BasketsAnalysis;
import uy.com.geocom.insights.model.output.FrequentItemSet;

import java.util.List;

import static org.apache.spark.sql.functions.*;

@Data
@Builder
public class BasketAnalysisCreator {
    protected Dataset<Row> freqItemSets;
    protected Dataset<Row> associationRules;
    protected Dataset<Product> productDataset;

    public BasketsAnalysis createInsight() {
        freqItemSets.printSchema();
        //Transform freqItemSets in FrequentItemSet
        List<FrequentItemSet> typicalBaskets = freqItemSets.sort(desc(FreqItemSetsSchema.freq.name()))
                .withColumn(FreqItemSetsSchema.rank.name(), monotonically_increasing_id())
                .as(Encoders.bean(FrequentItemSet.class))
                .collectAsList();

        for (FrequentItemSet frequentItemSet : typicalBaskets) {
            System.out.println(frequentItemSet.getRank() + " - " + frequentItemSet.getFreq()
                    + " -> (" + frequentItemSet.getItems().size()+") "+frequentItemSet.getItems().toString());
        }
        //Transform associationRules in AssociationRule(s)
        UserDefinedFunction itemId = udf(
                (String[] ids) -> new String(ids[0])
                , DataTypes.StringType);
        //Extract consequentID
        associationRules=associationRules.withColumn(AssociationRuleSchema.consequentItemId.name()
                ,itemId.apply(col(AssociationRuleSchema.consequent.name())));
        //Get consequentItem profit
        associationRules=associationRules
                .join(productDataset.select(ProductSchema.id.name(),ProductSchema.profit.name())
                        ,associationRules.col(AssociationRuleSchema.consequentItemId.name())
                        .equalTo(productDataset.col(ProductSchema.id.name())),"left");
        //
        associationRules.printSchema();
        associationRules.show(15);
        //Compute relevance
        UserDefinedFunction relevance= udf(
                (Double profit,String confidence)
                        -> new Double(profit*Double.parseDouble(confidence))
                ,DataTypes.DoubleType);
        //

        List<AssociationRule> rules=associationRules
                .select(col(AssociationRuleSchema.antecedent.name())
                        .as(AssociationRuleSchema.antecedentItemIds.name()),
                        col(AssociationRuleSchema.consequentItemId.name()),
                        col(AssociationRuleSchema.confidence.name()),
                        relevance.apply(col(ProductSchema.profit.name())
                                ,col(AssociationRuleSchema.confidence.name()))
                        .as(AssociationRuleSchema.relevance.name()))
                .sort(desc(AssociationRuleSchema.relevance.name()))
                .withColumn(AssociationRuleSchema.rank.name(),monotonically_increasing_id())
                .as(Encoders.bean(AssociationRule.class))
                .collectAsList();

        return BasketsAnalysis.builder()
                .typicalBasketsOrderedBySupport(typicalBaskets)
                .basketsRulesOrderedByRelevance(rules)
                .build();
    }
    public void persistDatasets(){
        String savePath="/mnt/data/MINING/instacart4insights";

        freqItemSets.write().mode(SaveMode.Overwrite).option("sep", ",").option("header", true).csv(savePath.concat("freqItemSets.csv"));
        System.out.println("------------".concat("freqItemSets dataset (").concat(freqItemSets.count() + " rows). Saved ------------"));

        associationRules.write().mode(SaveMode.Overwrite).option("sep", ",").option("header", true).csv(savePath.concat("associationRules.csv"));
        System.out.println("------------".concat("associationRules dataset (").concat(associationRules.count() + " rows). Saved ------------"));

    }
    public boolean loadDatasets(){
        return true;
    }
}



