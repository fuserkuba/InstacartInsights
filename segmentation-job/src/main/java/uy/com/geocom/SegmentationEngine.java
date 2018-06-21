package uy.com.geocom;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.QuantileDiscretizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.*;
import uy.com.geocom.common.BasketSchema;
import uy.com.geocom.common.Utils;
import uy.com.geocom.insights.model.input.Basket;
import uy.com.geocom.insights.model.output.Segmentation;
import uy.com.geocom.rfm.DataTransformer4RFM;
import uy.com.geocom.rfm.RFMSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 */
public class SegmentationEngine {
    public final static String SEGMENTS_COLUMN = "segment";
    protected final static int basketsPathArgsIndex = 0;
    protected final static int segmentsPathArgsIndex = 1;
    protected static SparkSession spark;
    protected static Map<String, String> params = new HashMap<String, String>();

    protected static Logger logger;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: SegmentationEngine <fileBaskets segmentationPath>");
            System.exit(1);
        }
        spark = SparkSession.builder().appName("SegmentationEngine").getOrCreate();
        //only error logs
        logger =Logger.getLogger(SegmentationEngine.class);
        logger.info("Engine ready!!!");
        //ETL
        DataTransformer dataTransformer = extractDataSets(args);
        //Prepare segmentation
        String[] features = RFMSchema.getDataColumns();
        int[] k_values = new int[]{10, 9, 8, 7, 6, 5, 4, 3};
        //Create clusters
        SegmentationCreator segmentationCreator = clusterDataset(dataTransformer.getTransformedDataset(), features, k_values);
        //Prepare result
        params.put("features", ArrayUtils.toString(features));
        params.put("k_values", ArrayUtils.toString(k_values));

        Segmentation segmentation = segmentationCreator.createSegmentationInsightFromClustering(dataTransformer, params, args, args[segmentsPathArgsIndex]);


        //Write clusters
        segmentationCreator.clusteredDataset
                .select(BasketSchema.clientId.name(), SEGMENTS_COLUMN)
                .orderBy(SEGMENTS_COLUMN)
                .write().mode(SaveMode.Overwrite)
                .option("sep", ",").option("header", true)
                .csv(segmentation.getSegmentedDatasetPath());

        ObjectMapper mapper = new ObjectMapper();
        try (OutputStream stream = Files.newOutputStream(Paths.get(segmentation.getSegmentedDatasetPath() + ".json"), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            mapper.writeValue(stream, segmentation);
        } catch (IOException e) {
            spark.log().error("Error trying to write segmentation file", e);
        }

    }

    private static DataTransformer extractDataSets(String[] paths) {
        String basketsPath = paths[basketsPathArgsIndex];
        //read data sets

        Dataset<Basket> basketDataset = Utils.readDataSetFromFile(spark, basketsPath, BasketSchema.getSchema())
                .as(Encoders.bean(Basket.class));
        //describe data sets
       // Utils.describeDataSet(logger, basketDataset, "Baskets", 10);
        //Transform input datasets
        DataTransformer dataTransformer = new DataTransformer4RFM();
        dataTransformer.transformDataset(basketDataset);
        return dataTransformer;
    }


    private static SegmentationCreator clusterDataset(Dataset<Row> items, String[] inputCols, int[] k_values) {

       // Utils.describeDataSet(logger, items, "Items for clustering", 10);

        ArrayList<String> features = new ArrayList<String>(inputCols.length);
        for (String cols : inputCols) {
            features.add(cols.concat("_Discretized"));
        }
        String[] featuresTransformed = features.toArray(new String[0]);

        //Features transformation
        QuantileDiscretizer discretizer = new QuantileDiscretizer()
                .setInputCols(inputCols)
                .setOutputCols(featuresTransformed)
                .setNumBucketsArray(new int[]{10, 10, 10});

        //Select Features
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featuresTransformed)
                .setOutputCol("features");

        //Set Kmeans
        KMeans kMeans = new KMeans().setSeed(1L);
        kMeans.setPredictionCol(SEGMENTS_COLUMN);
        //Prepare pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{discretizer, assembler, kMeans});

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(kMeans.k(), k_values)
                .build();
        //CrossValidator
        CrossValidator cv = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(new ClusteringEvaluator().setPredictionCol(SEGMENTS_COLUMN))
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(2)  // Use 3+ in practice
                .setParallelism(2);  // Evaluate up to 2 parameter settings in parallel

        // Run cross-validation, and choose the best set of parameters.
        CrossValidatorModel cvModel = cv.fit(items);

        Dataset<Row> crossValidatorPredictions = cvModel.transform(items);
        //Obtain bestModel
        KMeansModel bestKMeans = (KMeansModel) ((PipelineModel) cvModel.bestModel()).stages()[2];
        //Centers
        org.apache.spark.ml.linalg.Vector[] centers = bestKMeans.clusterCenters();
        //Show
        //crossValidatorPredictions.show(20);

        return SegmentationCreator.builder()
                .clusteredDataset(crossValidatorPredictions)
                .clusteringModel(bestKMeans)
                .features(Arrays.asList(inputCols))
                .build();
    }

}
