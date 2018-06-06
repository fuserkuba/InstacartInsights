package uy.com.geocom;

import org.apache.spark.sql.*;
import uy.com.geocom.common.*;
import uy.com.geocom.insights.model.input.Basket;
import uy.com.geocom.insights.model.input.Client;
import uy.com.geocom.insights.model.input.Product;
import uy.com.geocom.insights.model.input.Purchase;

/**
 * Hello world!
 *
 */
public class SegmentationEngine
{
    private static SparkSession spark;

    public static void main( String[] args )
    {
        if (args.length < 5) {
            System.err.println("Usage: SegmentationEngine <fileBaskets fileProducts filePurchases fileClients filePOS>");
            System.exit(1);
        }
        spark = SparkSession.builder().appName("SegmentationEngine").getOrCreate();
        //only error logs
        spark.sparkContext().setLogLevel("ERROR");

        String basketsPath = args[0];
        String productsPath = args[1];
        String purchasesPath = args[2];
        String clientsPath = args[3];
        String posPath = args[4];
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
        Utils.describeDataSet(productDataset,"Products",10);
        Utils.describeDataSet(purchaseDataset,"Purchases",10);
        Utils.describeDataSet(clientDataset,"Clients",10);
    }

}
