package instacart.insights.job.segmentation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import instacart.insights.model.output.SegmentItem;
import instacart.insights.model.output.StatisticMeasure;

import java.util.Map;

public abstract class DataTransformer {

    protected Dataset<Row> transformedDataset;

    public abstract Dataset<Row> transformDataset(Dataset dataset);

    public Dataset<Row> getTransformedDataset() {
        return transformedDataset;
    }

    public abstract SegmentItem createTypicalItem(Map<String, Map<StatisticMeasure, Double>> statisticsValuesByFeature);
}
