package uy.com.geocom;

import lombok.extern.java.Log;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uy.com.geocom.common.BasketSchema;
import uy.com.geocom.common.InsightsEngine;
import uy.com.geocom.common.PurchaseSchema;
import uy.com.geocom.common.Utils;
import uy.com.geocom.insights.model.input.Basket;
import uy.com.geocom.insights.model.input.Purchase;
import uy.com.geocom.insights.model.output.BasketsAnalysis;
import uy.com.geocom.insights.model.output.Insight;

import java.util.logging.Level;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.desc;

@Log
public class BasketAnalysisEngine implements InsightsEngine {

    protected final SparkSession spark;
    protected final int basketsPathArgsIndex = 0;
    protected final int purchasesPathArgsIndex = 1;
    protected final int supportArgsIndex = 2;
    protected final int confidenceArgsIndex = 3;
    protected final String ITEMS_COLUMN = "items";
    protected final String K_COLUMN = "k";
    protected Dataset<Row> purchaseItems;
    protected Dataset<Row> productsRules;


    public BasketAnalysisEngine(SparkSession spark) {
        this.spark = spark;
        //only error logs
        this.spark.sparkContext().setLogLevel("ERROR");
        //info for jobs logs
        log.setLevel(Level.INFO);
    }

    @Override
    public Insight mineInsight(String... parameters) {
        //Extract and Transform input data
        extractDataSets(parameters);
        //Find Rules
        double support=Double.parseDouble(parameters[supportArgsIndex]);
        double confidence=Double.parseDouble(parameters[confidenceArgsIndex]);
        productsRules=findRules(purchaseItems,support, confidence);
        //describe rules
        Utils.describeDataSet(spark.log(),productsRules,"Rules",20);
        return new BasketsAnalysis();
    }

    private void extractDataSets(String[] paths) {
        String basketsPath = paths[basketsPathArgsIndex];
        String purchasesPath = paths[purchasesPathArgsIndex];
        //read data sets
        Dataset<Basket> basketDataset = Utils.readDataSetFromFile(spark, basketsPath, BasketSchema.getSchema())
                .as(Encoders.bean(Basket.class));
        Dataset<Purchase> purchaseDataset = Utils.readDataSetFromFile(spark, purchasesPath, PurchaseSchema.getSchema())
                .as(Encoders.bean(Purchase.class));
        //describe data sets
        Utils.describeDataSet(spark.log(), basketDataset, "Baskets", 10);
        Utils.describeDataSet(spark.log(), purchaseDataset, "Purchases", 10);
        //Transform input datasets
        transformPurchaseDataset(basketDataset, purchaseDataset);
        Utils.describeDataSet(spark.log(), this.purchaseItems, "Purchase Items", 10);

    }

    private void transformPurchaseDataset(Dataset<Basket> basketDataset, Dataset<Purchase> purchaseDataset) {
        this.purchaseItems = purchaseDataset
                .groupBy(PurchaseSchema.basketId.name())
                .agg(collect_set(PurchaseSchema.productId.name()).as(ITEMS_COLUMN),
                        count(PurchaseSchema.productId.name()).as(K_COLUMN));
    }

    private Dataset<Row> findRules(Dataset<Row> basketItems, double support, double confidence) {

        log.log(Level.INFO,"------------FPM Analysis started with min_support: " + support + " and min_confidence: " + confidence);

        FPGrowthModel model = new FPGrowth()
                .setItemsCol(ITEMS_COLUMN)
                .setMinSupport(support)
                .setMinConfidence(confidence)
                .fit(basketItems);

        // Display frequent itemsets.
        model.freqItemsets().sort(desc("freq")).show();
        // Display generated association rules.
        Dataset<Row> dsRules = model.associationRules().sort(desc("confidence"));
        dsRules.show();
        return dsRules;
        // transform examines the input items against all the association rules and summarize the
        // consequents as prediction
        //model.transform(itemsDF).show();
    }
}
