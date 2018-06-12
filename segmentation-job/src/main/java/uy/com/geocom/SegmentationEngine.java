package uy.com.geocom;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.QuantileDiscretizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.*;
import uy.com.geocom.common.*;
import uy.com.geocom.insights.model.input.Basket;
import uy.com.geocom.insights.model.input.Client;
import uy.com.geocom.insights.model.input.Product;
import uy.com.geocom.insights.model.input.Purchase;
import uy.com.geocom.insights.model.output.Segmentation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

/**
 * Hello world!
 *
 */
public class SegmentationEngine
{
    protected static SparkSession spark;
    protected static Map<String,String> params=new HashMap<String, String>();
    protected final static int basketsPathArgsIndex=0;
    protected final static int clientsPathArgsIndex=1;
    protected final static int segmentsPathArgsIndex=2;

    public static void main( String[] args )
    {
        if (args.length < 3) {
            System.err.println("Usage: SegmentationEngine <fileBaskets fileClients segmentationPath>");
            System.exit(1);
        }
        spark = SparkSession.builder().appName("SegmentationEngine").getOrCreate();
        //only error logs
        spark.sparkContext().setLogLevel("ERROR");
        //ETL
        DataTransfomer dataTransfomer= extractDataSets(args);
        //Prepare segmentation
        String[] features = RFMSchema.getDataColumns();
        int[] k_values=new int[]{10, 9, 8, 7, 6, 5, 4, 3};
        //Create clusters
        SegmentationCreator segmentationCreator= clusterDataset(dataTransfomer.getClientsRFM(),features, k_values);
        //Utils.describeDataSet(spark.log(), clusteredDataset,"Clients segmented",10);
        //Prepare result
        params.put("features",ArrayUtils.toString(features));
        params.put("k_values",ArrayUtils.toString(k_values));

        Segmentation segmentation=segmentationCreator.createSegmentationInsightFromClustering(params,args);

        //TODO: write Segmentation to files

    }
    private static DataTransfomer extractDataSets(String[] paths){
        String basketsPath = paths[basketsPathArgsIndex];
        String clientsPath = paths[clientsPathArgsIndex];
        //read data sets

        Dataset<Basket> basketDataset=Utils.readDataSetFromFile(spark, basketsPath,BasketSchema.getSchema())
                .as(Encoders.bean(Basket.class));
        Dataset<Client> clientDataset=Utils.readDataSetFromFile(spark, clientsPath, ClientSchema.getSchema())
                .as(Encoders.bean(Client.class));
        //describe data sets
        Utils.describeDataSet(spark.log(),basketDataset,"Baskets",10);

        //Transform input datasets
        DataTransfomer dataTransfomer=new DataTransfomer();
        dataTransfomer.calculateClientsRFM(basketDataset);
        return dataTransfomer;
    }


    private static SegmentationCreator clusterDataset(Dataset<Row> items, String[] inputCols, int[] k_values) {

        Utils.describeDataSet(spark.log(),items,"Items for clustering",10);


        ArrayList<String> features= new ArrayList<String>(inputCols.length);
        for(String cols:inputCols){
             features.add(cols.concat("_Discretized"));
        }
        String[] featuresTransformed= features.toArray(new String[0]);

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
        //Prepare pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{discretizer, assembler, kMeans});

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(kMeans.k(), k_values)
                .build();
        //CrossValidator
        CrossValidator cv = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(new ClusteringEvaluator())
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(2)  // Use 3+ in practice
                .setParallelism(2);  // Evaluate up to 2 parameter settings in parallel

        // Run cross-validation, and choose the best set of parameters.
        CrossValidatorModel cvModel = cv.fit(items);
        Dataset<Row> crossValidatorPredictions = cvModel.transform(items);
        //Obtain bestModel
        KMeansModel bestKMeans = (KMeansModel) ((PipelineModel) cvModel.bestModel()).stages()[2];
        //Explain
        System.out.println("Parameters: " + cvModel.explainParams());
        System.out.println("clusters: " + bestKMeans.getK());
        //Centers
        org.apache.spark.ml.linalg.Vector[] centers = bestKMeans.clusterCenters();
        System.out.println("Cluster Centers: ".concat(ArrayUtils.toString(features)));
        for (Vector center : centers) {
            System.out.println(center);
        }

        return SegmentationCreator.builder()
                .clusteredDataset(crossValidatorPredictions)
                .clusteringModel(bestKMeans)
                .features(Arrays.asList(inputCols))
                .build();
    }

}