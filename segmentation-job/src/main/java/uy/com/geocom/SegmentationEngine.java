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

    public static void main( String[] args )
    {
        if (args.length < 5) {
            System.err.println("Usage: SegmentationEngine <fileBaskets fileProducts filePurchases fileClients filePOS>");
            System.exit(1);
        }
        spark = SparkSession.builder().appName("SegmentationEngine").getOrCreate();
        //only error logs
        spark.sparkContext().setLogLevel("ERROR");
        //ETL
        DataTransfomer dataTransfomer= extractDataSets(args);
        //Prepare segmentation
        String[] features = RFMSchema.getSchema().fieldNames();
        int[] k_values=new int[]{10, 9, 8, 7, 6, 5, 4, 3};
        //Create clustergins
        Dataset<Row> clusteredDataset=segmentDataset(dataTransfomer.getClientsRFM(),features, k_values);
        Utils.describeDataSet(clusteredDataset,"Clients segmented",10);
        //Prepare result
        params.put("features",ArrayUtils.toString(features));
        params.put("k_values",ArrayUtils.toString(k_values));
        SegmentationCreator segmentationCreator=new SegmentationCreator();
        segmentationCreator.createSegmentationInsightFromClustering(params,args);
        Segmentation segmentation=segmentationCreator.getSegmentationInsight();
        //TODO: write Segmentation to files

    }
    private static DataTransfomer extractDataSets(String[] paths){
        String basketsPath = paths[0];
        String productsPath = paths[1];
        String purchasesPath = paths[2];
        String clientsPath = paths[3];
        String posPath = paths[4];
        //read data sets

        Dataset<Basket> basketDataset=Utils.readDataSetFromFile(spark, basketsPath,BasketSchema.getSchema())
                .as(Encoders.bean(Basket.class));
        Dataset<Product> productDataset=Utils.readDataSetFromFile(spark, productsPath,ProductSchema.getSchema())
                .as(Encoders.bean(Product.class));
        Dataset<Purchase> purchaseDataset=Utils.readDataSetFromFile(spark, purchasesPath,PurchaseSchema.getSchema())
                .as(Encoders.bean(Purchase.class));
        Dataset<Client> clientDataset=Utils.readDataSetFromFile(spark, clientsPath, ClientSchema.getSchema())
                .as(Encoders.bean(Client.class));
        //describe data sets
        Utils.describeDataSet(basketDataset,"Baskets",10);
        //Utils.describeDataSet(productDataset,"Products",10);
        //Utils.describeDataSet(purchaseDataset,"Purchases",10);
        //Utils.describeDataSet(clientDataset,"Clients",10);

        //Transform input datasets
        DataTransfomer dataTransfomer=new DataTransfomer();
        dataTransfomer.calculateClientsRFM(basketDataset);
        return dataTransfomer;
    }


    private static Dataset<Row> segmentDataset(Dataset<Row> items, String[] features, int[] k_values) {

        Utils.describeDataSet(items,"Items for clutering",10);

        //Remove first fields (expet to be "id")
        String[]inputCols= (String[]) ArrayUtils.remove(items.schema().fieldNames(),0);

        //Features transformation
        QuantileDiscretizer discretizer = new QuantileDiscretizer()
                .setInputCols(inputCols)
                .setOutputCols(features)
                .setNumBucketsArray(new int[]{10, 10, 10});

        //Select Features
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(features)
                .setOutputCol("features");

        //Set Kmeans
        KMeans kMeans = new KMeans().setSeed(1L);
        //Prepare pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{discretizer, assembler, kMeans});

        // Evaluate clustering by computing Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator();

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
        System.out.println("CrossValidator Clustered data");
        crossValidatorPredictions.printSchema();
        crossValidatorPredictions.groupBy("prediction").count().orderBy(desc("count")).show();

        double silhouette = evaluator.evaluate(crossValidatorPredictions);
        System.out.println("CrossValidator: Silhouette with squared euclidean distance = " + silhouette);

        //Describe segments
        //TODO Eliminar después de preparar la salida
        for (int i = 0; i < bestKMeans.getK(); i++) {
            System.out.println("------------".concat("Cluster (" + i + ") ------------"));
            crossValidatorPredictions.drop("features")
                    .filter(col("prediction").equalTo(i))
                    .summary().show();
        }

        return crossValidatorPredictions.withColumnRenamed("prediction", "segment");
    }

}
