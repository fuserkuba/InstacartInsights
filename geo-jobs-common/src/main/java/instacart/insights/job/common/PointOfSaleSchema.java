package instacart.insights.job.common;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class PointOfSaleSchema {
    public static final StructField id = DataTypes.createStructField("id", DataTypes.LongType, false);
    public static final StructField name = DataTypes.createStructField("name", DataTypes.StringType, true);
    public static final StructField location = DataTypes.createStructField("location", DataTypes.StringType, true);

    public static StructType getSchema() {
        return DataTypes.createStructType(new StructField[]{id, name, location});
    }
}
