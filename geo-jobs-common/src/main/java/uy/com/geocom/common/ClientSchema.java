package uy.com.geocom.common;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ClientSchema {

    public static final StructField id = DataTypes.createStructField("id", DataTypes.StringType, false);
    public static final StructField sex = DataTypes.createStructField("sex", DataTypes.StringType, true);
    public static final StructField age = DataTypes.createStructField("age", DataTypes.IntegerType, true);
    public static final StructField locality = DataTypes.createStructField("locality", DataTypes.StringType, true);

    public static StructType getSchema() {
        return DataTypes.createStructType(new StructField[]{id, sex, age, locality});
    }
}
