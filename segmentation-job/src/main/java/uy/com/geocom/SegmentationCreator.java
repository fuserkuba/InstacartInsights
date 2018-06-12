package uy.com.geocom;

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
import scala.collection.JavaConverters;
import scala.collection.Seq;
import uy.com.geocom.insights.model.output.Segment;
import uy.com.geocom.insights.model.output.Segmentation;
import uy.com.geocom.insights.model.output.StatisticMeasure;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SegmentationCreator {
    protected Segmentation segmentationInsight;
    protected Model clusteringModel;
    protected Dataset<Row> clusteredDataset;
    protected List<String> features;

    public Segmentation createSegmentationInsightFromClustering(Map<String, String> params, String[] datasetPaths) {

        segmentationInsight = new Segmentation();
        segmentationInsight.setParams(params);
        segmentationInsight.setDatasetIds(Arrays.asList(datasetPaths));

        // Evaluate clustering by computing Silhouette score
        segmentationInsight.setSilhouetteValue(new ClusteringEvaluator().evaluate(clusteredDataset));
        // Evaluate clustering by computing Within Set Sum of Squared Errors.
        if (clusteringModel instanceof KMeansModel)
            segmentationInsight.setWssseValue(((KMeansModel) clusteringModel).computeCost(clusteredDataset));
        else
            segmentationInsight.setWssseValue(0);

        //Get segments
        String segmentColumn = "prediction";
        //
        clusteredDataset.printSchema();
        clusteredDataset.groupBy(segmentColumn).count().orderBy(desc("count")).show();
        //
        List<String> segmentsNames = clusteredDataset.groupBy(segmentColumn).count()
                .select(segmentColumn)
                .as(Encoders.STRING())
                .collectAsList();
        //Features names as Scala Seq
        Seq<String> featuresTailNames=JavaConverters
                .asScalaIteratorConverter(features.subList(1,features.size())
                        .iterator())
                .asScala().toSeq();
        Map<String,Segment> segments=new HashMap<String, Segment>();
        for (String segmentName : segmentsNames) {
            Segment segment = buildSegment(segmentName,
                    clusteredDataset.where(col(segmentColumn)
                            .equalTo(segmentName))
                            .select(features.get(0),featuresTailNames)
                            .summary());
            segments.put(segmentName,segment);
        }

        //TODO: obtener resultados y transformarlos al formato de Segmentation
        return segmentationInsight;
    }

    protected Segment buildSegment(String segmentName, Dataset<Row> segmentSummary) {
        final String summaryColumn="summary";
        Map<String,Map<StatisticMeasure,Double>> statisticsValuesByFeature=new HashMap<String, Map<StatisticMeasure, Double>>();
        segmentSummary.show(20);
        for(String feature:features){
            Map<StatisticMeasure,Double> statisticsValues=new HashMap<>();
            statisticsValues.put(StatisticMeasure.count,0.0);
            segmentSummary.where(col(summaryColumn).equalTo(StatisticMeasure.count.name()))
                    .select(feature).as(Encoders.STRING());
        }
        return Segment.builder()
                .totalItems(segmentSummary.count())
                .build();
    }

}
