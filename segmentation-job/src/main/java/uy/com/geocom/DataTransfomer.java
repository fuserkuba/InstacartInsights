package uy.com.geocom;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uy.com.geocom.insights.model.input.Basket;
import uy.com.geocom.insights.model.input.Client;
import uy.com.geocom.insights.model.input.Purchase;

public class DataTransfomer {

    private Dataset<Row> clientsRFM;

    public Dataset<Row> calculateClientsRFM(Dataset<Client> clientDataset, Dataset<Basket> basketDataset, Dataset<Purchase> purchaseDataset){
        // TODO : transformar datos desde los dominios definidos hasta la entrada del algortimo de clustering
        clientsRFM=clientDataset.toDF();

        return clientsRFM;
    }

    public Dataset<Row> getClientsRFM() {

        return clientsRFM;
    }
}
