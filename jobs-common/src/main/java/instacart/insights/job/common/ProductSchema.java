package instacart.insights.job.common;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ProductSchema {

    public static final StructField id = DataTypes.createStructField("id", DataTypes.StringType, false);
    public static final StructField name = DataTypes.createStructField("name", DataTypes.StringType, true);
    public static final StructField category = DataTypes.createStructField("category", DataTypes.StringType, true);
    public static final StructField department = DataTypes.createStructField("department", DataTypes.StringType, true);
    public static final StructField type = DataTypes.createStructField("type", DataTypes.StringType, true);
    public static final StructField brand = DataTypes.createStructField("brand", DataTypes.StringType, true);
    public static final StructField profit = DataTypes.createStructField("profit", DataTypes.DoubleType, true);

    public static StructType getSchema() {
        return DataTypes.createStructType(new StructField[]{id, name, category, department, type, brand, profit});
    }
}
