package uy.com.geocom.insights.job.segmentation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uy.com.geocom.insights.model.output.SegmentItem;
import uy.com.geocom.insights.model.output.StatisticMeasure;

import java.util.Map;

public abstract class DataTransformer {

    protected Dataset<Row> transformedDataset;

    public abstract Dataset<Row> transformDataset(Dataset dataset);

    public Dataset<Row> getTransformedDataset() {
        return transformedDataset;
    }

    public abstract SegmentItem createTypicalItem(Map<String, Map<StatisticMeasure, Double>> statisticsValuesByFeature);
}
