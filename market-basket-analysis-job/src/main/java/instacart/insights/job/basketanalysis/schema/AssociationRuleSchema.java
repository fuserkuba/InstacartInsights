package instacart.insights.job.basketanalysis.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class AssociationRuleSchema {
    public static final StructField antecedent = DataTypes.createStructField("antecedent", DataTypes.createArrayType(DataTypes.StringType), true);
    public static final StructField consequent = DataTypes.createStructField("consequent", DataTypes.createArrayType(DataTypes.StringType), true);
    public static final StructField confidence = DataTypes.createStructField("confidence", DataTypes.DoubleType, true);

    public static final StructField antecedentItemIds = DataTypes.createStructField("antecedentItemIds", DataTypes.StringType, true);
    public static final StructField consequentItemId = DataTypes.createStructField("consequentItemId", DataTypes.StringType, true);
    public static final StructField relevance = DataTypes.createStructField("relevance", DataTypes.DoubleType, true);
    public static final StructField rank = DataTypes.createStructField("rank", DataTypes.LongType, true);

    public static StructType getSchema() {
        return DataTypes.createStructType(new StructField[]{antecedent, consequent, confidence});
    }
}
