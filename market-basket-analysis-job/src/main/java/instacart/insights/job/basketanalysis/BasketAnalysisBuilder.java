package instacart.insights.job.basketanalysis;

import instacart.insights.job.common.ProductSchema;
import instacart.insights.model.input.Product;
import lombok.Builder;
import lombok.Data;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;
import instacart.insights.job.basketanalysis.schema.AssociationRuleSchema;
import instacart.insights.job.basketanalysis.schema.FreqItemSetsSchema;
import instacart.insights.model.output.AssociationRule;
import instacart.insights.model.output.BasketsAnalysis;
import instacart.insights.model.output.FrequentItemSet;

import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Data
@Builder
public class BasketAnalysisBuilder {
    protected Dataset<Row> freqItemSets;
    protected Dataset<Row> associationRules;
    protected Dataset<Product> productDataset;

    public BasketsAnalysis createInsight(List<String> dataSetsPaths, Map<String, String> params) {

        //Transform freqItemSets in FrequentItemSet
        List<FrequentItemSet> typicalBaskets = freqItemSets.sort(desc(FreqItemSetsSchema.freq.name()))
                .withColumn(FreqItemSetsSchema.rank.name(), monotonically_increasing_id())
                .as(Encoders.bean(FrequentItemSet.class))
                .collectAsList();

        //Transform associationRules in AssociationRule(s)

        //Extract consequentID from consequent WrappedArray
        UserDefinedFunction itemId = udf(
                (WrappedArray<String> ids) -> JavaConversions
                        .asJavaCollection(ids)
                        .iterator().next()
                , DataTypes.StringType);
        //For Compute relevance
        UserDefinedFunction relevance = udf(
                (Double profit, Double confidence)
                        -> new Double(profit * confidence)
                , DataTypes.DoubleType);

        //Extract consequentID
        associationRules = associationRules
                .withColumn(AssociationRuleSchema.consequentItemId.name()
                        , itemId.apply(col(AssociationRuleSchema.consequent.name())));

        List<AssociationRule> rules = associationRules
                //Get consequentItem profit
                .join(productDataset.select(ProductSchema.id.name(), ProductSchema.profit.name())
                ,associationRules.col(AssociationRuleSchema.consequentItemId.name())
                        .equalTo(productDataset.col(ProductSchema.id.name())), "left")
                .select(col(AssociationRuleSchema.antecedent.name())
                                .as(AssociationRuleSchema.antecedentItemIds.name()),
                        col(AssociationRuleSchema.consequentItemId.name()),
                        col(AssociationRuleSchema.confidence.name()),
                        //Compute relevance
                        relevance.apply(col(ProductSchema.profit.name())
                                , col(AssociationRuleSchema.confidence.name()))
                                .as(AssociationRuleSchema.relevance.name()))
                //Add rank by relevance in descending order
                .sort(desc(AssociationRuleSchema.relevance.name()))
                .withColumn(AssociationRuleSchema.rank.name(), monotonically_increasing_id())
                .as(Encoders.bean(AssociationRule.class)) //Convert to AssociationRule
                .collectAsList();

        //Create Insight
        BasketsAnalysis basketsAnalysis = BasketsAnalysis.builder()
                .typicalBasketsOrderedBySupport(typicalBaskets)
                .basketsRulesOrderedByRelevance(rules)
                .build();
        basketsAnalysis.setDatasetIds(dataSetsPaths);
        basketsAnalysis.setParams(params);

        return basketsAnalysis;
    }

}



