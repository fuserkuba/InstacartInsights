package uy.com.geocom.insights.job.segmentation.rfm;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uy.com.geocom.insights.job.segmentation.DataTransformer;
import uy.com.geocom.insights.job.common.BasketSchema;
import uy.com.geocom.insights.model.output.SegmentItem;
import uy.com.geocom.insights.model.output.StatisticMeasure;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class DataTransformer4RFM extends DataTransformer {


    @Override
    public Dataset<Row> transformDataset(Dataset basketDataset) {
        transformedDataset = basketDataset.groupBy(col(BasketSchema.clientId.name()).alias(RFMSchema.clientId.name()))
                .agg(max(col(BasketSchema.time.name())).as("last"),     //Date of last purchased basket
                        count(BasketSchema.id.name()).as(RFMSchema.frequency.name()),     //Number of baskets for Frequency
                        sum(BasketSchema.charge.name()).as(RFMSchema.monetary.name()))   //Sum of charges for Monetary
                .withColumn(RFMSchema.recency.name()
                        , datediff(current_date(), col("last")))     //Days from last purchased basket for Recency
                .drop("last");
        return transformedDataset;
    }

    public SegmentItem createTypicalItem(Map<String, Map<StatisticMeasure, Double>> statisticsValuesByFeature){
        SegmentItem segmentItem=new RFMItem();
        //
        StatisticMeasure measure=StatisticMeasure.mean;
        //Recency
        ((RFMItem) segmentItem).setRecency(
                statisticsValuesByFeature
                        .get(RFMSchema.recency.name())
                        .get(measure));
        //Frequency
        ((RFMItem) segmentItem).setFrequency(
                statisticsValuesByFeature
                        .get(RFMSchema.frequency.name())
                        .get(measure));
        //Monetary
        ((RFMItem) segmentItem).setMonetary(
                statisticsValuesByFeature
                        .get(RFMSchema.monetary.name())
                        .get(measure));
        //
        return segmentItem;
    }
}
