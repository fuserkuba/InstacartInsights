package uy.com.geocom;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.math.BigInt;


import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneOffset;


import static org.apache.spark.sql.functions.*;

/**
 * Hello world!
 */
public class ETLInputData {
    private static SparkSession spark;


    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: ETLInput <file>");
            System.exit(1);
        }

        spark = SparkSession.builder().appName("ETLInput").getOrCreate();
        //only error logs
        spark.sparkContext().setLogLevel("ERROR");
        //directory
        String input_directory = args[0].concat("/");
        String output_directory = args[1].concat("/");
        etl_instacart_to_geo_insight(input_directory, output_directory);
    }


    private static void etl_instacart_to_geo_insight(String dataPath, String savePath) {
        //Products
        Dataset<Row> dsAisles = readDataSet(dataPath.concat("aisles.csv"))
                .withColumnRenamed("aisle_id","id");
        Dataset<Row> dsDepartments = readDataSet(dataPath.concat("departments.csv"))
                .withColumnRenamed("department_id","id");
        Dataset<Row> products=readDataSet(dataPath.concat("products.csv"))
                .join(dsAisles, col("aisle_id").equalTo(dsAisles.col("id")), "left")
                .join(dsDepartments, col("department_id").equalTo(dsDepartments.col("id")), "left")
                .withColumnRenamed("aisle", "category")
                .drop("aisle_id").drop("department_id").drop("id")
                .withColumnRenamed("product_id","id")
                .withColumnRenamed("product_name","name")
                .withColumn("type",lit("groceries"))
                .withColumn("brand",lit("instacart"))
                .withColumn("profit",rand());
        describeDataSet(products, "Products", 10);
        //Purchases
        Dataset<Row> purchases = readDataSet(dataPath.concat("order_products__train.csv"))
                .union(readDataSet(dataPath.concat("order_products__prior.csv")))
                .withColumnRenamed("order_id", "basketId")
                .withColumnRenamed("product_id","productId")
                .withColumnRenamed("add_to_cart_order","units")
                .withColumn("price"
                        ,udf((Integer product_id) -> new Double(Math.random())*product_id, DataTypes.DoubleType)
                                .apply(col("productId")))
                .withColumn("context",lit("Instacart Gorcery Data Set"))
                .drop("reordered");
        describeDataSet(purchases, "Purchases", 10);

        //Baskets
        UserDefinedFunction addDaysToCurrentDateTime= udf(
                (Double days) -> new Date(
                        Date.from(LocalDateTime.now()
                                .minusDays(Math.round(days))
                                .toInstant(ZoneOffset.UTC))
                                .getTime())
                ,DataTypes.DateType);

        Dataset<Row> baskets=readDataSet(dataPath.concat("orders.csv"))
                .withColumnRenamed("order_id","id")
                .withColumnRenamed("user_id","clientId")
                .na().fill(0)
                .select(col("id")
                        ,addDaysToCurrentDateTime.apply(col("days_since_prior_order")).name("time")
                        ,col("clientId")
                        ,lit("cash").name("paymentMethod")
                        ,spark_partition_id().as("posId")
                        ,udf((Integer orderNumber) -> new Double(Math.random()*10)*orderNumber, DataTypes.DoubleType)
                                .apply(col("order_number")).name("charge"));
        describeDataSet(baskets, "Baskets", 10);

        //Client
        Dataset<Row> clients=readDataSet(dataPath.concat("orders.csv"))
                .groupBy("user_id").count()
                .withColumnRenamed("user_id","id")
                .select(col("id"),col("count"));

        String value=clients.select("count").summary("mean").first().getString(1);
        Double ordersMean=Double.parseDouble(value);

        clients=clients
                .withColumn("sex"
                ,udf((Long quantity) -> quantity> ordersMean? "F": "M",DataTypes.StringType)
                .apply(col("count")))
                .withColumn("age"
                ,udf((Integer id) -> new Double(Math.random()*100).intValue(), DataTypes.IntegerType)
                .apply(col("id")))
                .withColumn("locality",lit("Montevideo"))
                .drop("count");
        describeDataSet(clients, "Clients", 10);

        baskets.write().mode(SaveMode.Overwrite).option("sep", ",").option("header", true).csv(savePath.concat("baskets.csv"));
        System.out.println("------------".concat("Baskets dataset (").concat(baskets.count() + " rows). Saved ------------"));

        purchases.write().mode(SaveMode.Overwrite).option("sep", ",").option("header", true).csv(savePath.concat("purchases.csv"));
        System.out.println("------------".concat("Purchases dataset (").concat(purchases.count() + " rows). Saved ------------"));

        clients.write().mode(SaveMode.Overwrite).option("sep", ",").option("header", true).csv(savePath.concat("clients.csv"));
        System.out.println("------------".concat("Clients dataset (").concat(clients.count() + " rows). Saved ------------"));

        products.write().mode(SaveMode.Overwrite).option("sep", ",").option("header", true).csv(savePath.concat("products.csv"));
        System.out.println("------------".concat("Products dataset (").concat(products.count() + " rows). Saved ------------"));

    }
    private static Dataset<Row> readDataSet(String dataPath) {
        return spark.read()
                .option("sep", ",")
                .option("header", true)
                .option("inferSchema", true)
                .csv(dataPath).toDF();

    }
    private static void describeDataSet(Dataset dataset, String title, int rows) {
        //display schema of data
        System.out.println("------------".concat(title + " (").concat(dataset.count() + " rows) ------------"));
        dataset.printSchema();
        dataset.show(rows);
    }
}
