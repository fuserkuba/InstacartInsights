package uy.com.geocom.insights.model.input;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Relación entre la cesta y el producto adquirido
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Purchase {
    protected String productId;
    protected String basketId;
    protected Double price;
    protected Integer units;
    //Contexto de la compra: promoción, descuentos, navidades
    protected String context;

    public static StructType getSchema(){
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("productId", DataTypes.StringType, false),
                DataTypes.createStructField("basketId", DataTypes.StringType, true),
                DataTypes.createStructField("units", DataTypes.IntegerType, true),
                DataTypes.createStructField("price", DataTypes.DoubleType, true),
                DataTypes.createStructField("context", DataTypes.StringType, true)
        });
    }
}
