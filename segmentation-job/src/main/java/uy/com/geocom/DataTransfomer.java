package uy.com.geocom;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uy.com.geocom.common.BasketSchema;
import uy.com.geocom.common.ClientSchema;
import uy.com.geocom.insights.model.input.Basket;

import static org.apache.spark.sql.functions.*;

public class DataTransfomer {

    private Dataset<Row> clientsRFM;

    public Dataset<Row> calculateClientsRFM(Dataset<Basket> basketDataset) {

        clientsRFM = basketDataset.groupBy(col(BasketSchema.clientId.name()).alias(RFMSchema.clientId.name()))
                .agg(max(col(BasketSchema.time.name())).as("last"),     //Date of last purchased basket
                        count(BasketSchema.id.name()).as(RFMSchema.frequency.name()),     //Number of baskets for Frequency
                        sum(BasketSchema.charge.name()).as(RFMSchema.monetary.name()))   //Sum of charges for Monetary
                .withColumn(RFMSchema.recency.name()
                        , datediff(current_date(), col("last")))     //Days from last purchased basket for Recency
                .drop("last");
        return clientsRFM;
    }

    public Dataset<Row> getClientsRFM() {
        return clientsRFM;
    }
}
