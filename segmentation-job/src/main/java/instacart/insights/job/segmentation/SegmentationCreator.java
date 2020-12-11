package instacart.insights.job.segmentation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import instacart.insights.model.output.Segment;
import instacart.insights.model.output.Segmentation;
import instacart.insights.model.output.StatisticMeasure;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SegmentationCreator {
    protected Segmentation segmentationInsight;
    protected Model clusteringModel;
    protected Dataset<Row> clusteredDataset;
    protected List<String> features;

    public Segmentation createSegmentationInsightFromClustering(DataTransformer dataTransformer, Map<String, String> params, String[] datasetPaths, String segmentationPath) {

        segmentationInsight = new Segmentation();
        segmentationInsight.setParams(params);
        segmentationInsight.setDatasetIds(Arrays.asList(datasetPaths));

        // Evaluate clustering by computing Silhouette score
        segmentationInsight.setSilhouetteValue(new ClusteringEvaluator()
                .setPredictionCol(SegmentationEngine.SEGMENTS_COLUMN)
                .evaluate(clusteredDataset));
        // Evaluate clustering by computing Within Set Sum of Squared Errors.
        if (clusteringModel instanceof KMeansModel)
            segmentationInsight.setWssseValue(((KMeansModel) clusteringModel).computeCost(clusteredDataset));
        else
            segmentationInsight.setWssseValue(0);

        //Get segments
        //clusteredDataset.printSchema();
        //clusteredDataset.groupBy(SegmentationEngine.SEGMENTS_COLUMN).count().orderBy(desc("count")).show();
        //
        List<String> segmentsNames = clusteredDataset.groupBy(SegmentationEngine.SEGMENTS_COLUMN).count()
                .select(SegmentationEngine.SEGMENTS_COLUMN)
                .as(Encoders.STRING())
                .collectAsList();
        Map<String, Segment> segments = new HashMap<String, Segment>();
        for (String segmentName : segmentsNames) {
            Segment segment = buildSegment(clusteredDataset
                    .where(col(SegmentationEngine.SEGMENTS_COLUMN)
                            .equalTo(segmentName))
                    .summary(), dataTransformer);
            segments.put(segmentName, segment);
        }
        segmentationInsight.setSegments(segments);
        segmentationInsight.setSegmentedDatasetPath(segmentationPath);

        return segmentationInsight;
    }

    protected Segment buildSegment(Dataset<Row> segmentSummary, DataTransformer dataTransformer) {

        Map<String, Map<StatisticMeasure, Double>> statisticsValuesByFeature = new HashMap<String, Map<StatisticMeasure, Double>>();
        Double count=0.0;

        for (String feature : features) {

            Map<StatisticMeasure, Double> statisticsValues = new HashMap<>();
            //All values
            List<Row> statistics = segmentSummary.select(feature).collectAsList();
            //Count
            count = Double.parseDouble(statistics.get(0).getString(0));
            statisticsValues.put(StatisticMeasure.count, count);
            //Mean
            Double mean = Double.parseDouble(statistics.get(1).getString(0));
            statisticsValues.put(StatisticMeasure.mean, mean);
            //StdDev
            Double stdDev = Double.parseDouble(statistics.get(2).getString(0));
            statisticsValues.put(StatisticMeasure.stdDev, stdDev);
            //Min
            Double min = Double.parseDouble(statistics.get(3).getString(0));
            statisticsValues.put(StatisticMeasure.min, min);
            //lowerQuartile
            Double lowerQuartile = Double.parseDouble(statistics.get(4).getString(0));
            statisticsValues.put(StatisticMeasure.lowerQuartile, lowerQuartile);
            //Median
            Double median = Double.parseDouble(statistics.get(5).getString(0));
            statisticsValues.put(StatisticMeasure.median, median);
            //UpperQuartile
            Double upperQuartile = Double.parseDouble(statistics.get(6).getString(0));
            statisticsValues.put(StatisticMeasure.upperQuartile, upperQuartile);
            //Max
            Double max = Double.parseDouble(statistics.get(7).getString(0));
            statisticsValues.put(StatisticMeasure.max, max);

            statisticsValuesByFeature.put(feature, statisticsValues);
            //
        }

        return Segment.builder()
                .statisticsValuesByFeature(statisticsValuesByFeature)
                .totalItems(count.longValue())
                .typicalItem(dataTransformer.createTypicalItem(statisticsValuesByFeature))
                .build();
    }


}
