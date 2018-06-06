package uy.com.geocom.insights.model.input;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Product {
    protected String id;
    protected String name;
    protected String category;
    protected String department;
    protected String type;
    protected String brand;
    protected Double profit;

    public static StructType getSchema(){
        return DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("category", DataTypes.StringType, true),
                DataTypes.createStructField("department", DataTypes.StringType, true),
                DataTypes.createStructField("type", DataTypes.StringType, true),
                DataTypes.createStructField("brand", DataTypes.StringType, true),
                DataTypes.createStructField("profit", DataTypes.DoubleType, true),
        });
    }
}
