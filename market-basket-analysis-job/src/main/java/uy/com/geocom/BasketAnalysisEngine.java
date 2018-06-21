package uy.com.geocom;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uy.com.geocom.common.*;
import uy.com.geocom.insights.model.input.Basket;
import uy.com.geocom.insights.model.input.Product;
import uy.com.geocom.insights.model.input.Purchase;
import uy.com.geocom.insights.model.output.Insight;

import static org.apache.spark.sql.functions.*;

public class BasketAnalysisEngine implements InsightsEngine {

    protected final SparkSession spark;
    protected final int productsPathArgsIndex = 0;
    protected final int purchasesPathArgsIndex = 1;
    protected final int supportArgsIndex = 2;
    protected final int confidenceArgsIndex = 3;
    protected final String ITEMS_COLUMN = "items";
    protected final String K_COLUMN = "k";
    protected Dataset<Row> purchaseItems;
    protected Dataset<Product> productDataset;


    public BasketAnalysisEngine(SparkSession spark) {
        this.spark = spark;
        //only error logs
        this.spark.sparkContext().setLogLevel("ERROR");
        //testing logging
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("uy.com.geocom").setLevel(Level.OFF);
        Logger.getLogger("uy.com.geocom").setLevel(Level.DEBUG);
        //info for jobs logs
        spark.log().debug("Testing in debug");
        spark.log().info("Testing in info");

    }

    @Override
    public Insight mineInsight(String... parameters) {
        //Extract and Transform input data
        extractDataSets(parameters);
        //Find Rules
        double support = Double.parseDouble(parameters[supportArgsIndex]);
        double confidence = Double.parseDouble(parameters[confidenceArgsIndex]);
        //
        BasketAnalysisCreator basketAnalysisCreator = findRules(purchaseItems, support, confidence);
        //describe rules
        Dataset<Row> productsRules = basketAnalysisCreator.getAssociationRules();
        Utils.describeDataSet(spark.log(), productsRules, "Rules", 20);
        basketAnalysisCreator.persistDatasets();
        //construct Insight
        basketAnalysisCreator.setProductDataset(productDataset);
        return basketAnalysisCreator.createInsight();
    }

    private void extractDataSets(String[] paths) {
        String productsPath = paths[productsPathArgsIndex];
        String purchasesPath = paths[purchasesPathArgsIndex];
        //read data sets
        productDataset = Utils.readDataSetFromFile(spark, productsPath, ProductSchema.getSchema())
                .as(Encoders.bean(Product.class));
        Dataset<Purchase> purchaseDataset = Utils.readDataSetFromFile(spark, purchasesPath, PurchaseSchema.getSchema())
                .as(Encoders.bean(Purchase.class));
        //describe data sets
        Utils.describeDataSet(spark.log(), productDataset, "Products", 10);
        Utils.describeDataSet(spark.log(), purchaseDataset, "Purchases", 10);
        //Transform input datasets
        transformPurchaseDataset(productDataset, purchaseDataset);
        Utils.describeDataSet(spark.log(), this.purchaseItems, "Purchase Items", 10);
    }

    private void transformPurchaseDataset(Dataset<Product> productDataset, Dataset<Purchase> purchaseDataset) {
        this.purchaseItems = purchaseDataset
                .groupBy(PurchaseSchema.basketId.name())
                .agg(collect_set(PurchaseSchema.productId.name()).as(ITEMS_COLUMN),
                        count(PurchaseSchema.productId.name()).as(K_COLUMN));
    }

    private BasketAnalysisCreator findRules(Dataset<Row> basketItems, double support, double confidence) {

        spark.log().info("------------FPM Analysis started with min_support: " + support + " and min_confidence: " + confidence);

        FPGrowthModel model = new FPGrowth()
                .setItemsCol(ITEMS_COLUMN)
                .setMinSupport(support)
                .setMinConfidence(confidence)
                .fit(basketItems);


        // Display frequent itemsets.
        Dataset<Row> freqItemsets = model.freqItemsets().sort(desc("freq"));
        //
        freqItemsets.show();
        // Display generated association rules.
        Dataset<Row> associationRules = model.associationRules()
                .sort(desc(AssociationRuleSchema.confidence.name()));
        //
        associationRules.show(100);

        // transform examines the input items against all the association rules and summarize the
        // consequents as prediction
        //model.transform(basketItems).show(50);
        //
        return BasketAnalysisCreator.builder()
                .freqItemSets(freqItemsets)
                .associationRules(associationRules)
                .build();
    }

}
