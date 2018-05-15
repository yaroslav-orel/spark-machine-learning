import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class PrepareForMLApp {

    private static final String APP_NAME = "Prepare-Dataset";
    private static final String DATASET_NAME = "dataset.csv";
    private static final String ML_READY_DATASET_PATH = "ml-ready.csv";

    public static void main(String[] args) {
        val session = SparkUtil.initSparkSession(APP_NAME);
        registerUDFs(session);

        val initialDataset = SparkUtil.getDataset(session, getSchema(), MiscUtil.getFilePath(DATASET_NAME));
        val preparedDS = getPreparedDataset(initialDataset);

        val lightweightDS = preparedDS
                .drop("Date")
                .drop("avg(Outdoor Temp)")
                .drop("avg(Indoor Temp)")
                .drop("avg(Energy Consumption)");

        SparkUtil.saveCSV(lightweightDS, MiscUtil.getFilePath(ML_READY_DATASET_PATH));
    }

    private static Dataset<Row> getPreparedDataset(Dataset<Row> initialDataset) {
        val transformedDS = initialDataset
                .withColumn("Date", callUDF("timestampToDate", initialDataset.col("Timestamp")))
                .withColumn("Week Day", callUDF("timestampToWeekDay", initialDataset.col("Timestamp")))
                .withColumn("Day Period", callUDF("timestampToDayPeriod", initialDataset.col("Timestamp")))
                .groupBy("Date", "Week Day", "Day Period")
                .avg("Outdoor Temp", "Indoor Temp", "Energy Consumption");

        return transformedDS
                .withColumn("Norm Indoor", callUDF("normalizeIndoorTemp", transformedDS.col("avg(Indoor Temp)")))
                .withColumn("Norm Outdoor", callUDF("normalizeOutdoorTemp", transformedDS.col("avg(Outdoor Temp)")));
    }

    public static StructType getSchema() {
        return new StructType()
                    .add("Timestamp", DataTypes.StringType)
                    .add("Outdoor Temp", DataTypes.DoubleType)
                    .add("Indoor Temp", DataTypes.DoubleType)
                    .add("Energy Consumption", DataTypes.DoubleType);
    }

    public static void registerUDFs(SparkSession session) {
        session.udf().register("timestampToDate", UDFs.timestampToDate, StringType);
        session.udf().register("timestampToWeekDay", UDFs.timestampToWeekDay, IntegerType);
        session.udf().register("timestampToDayPeriod", UDFs.timestampToDayPeriod, IntegerType);

        session.udf().register("normalizeIndoorTemp", UDFs.normalizeIndoorTemp, DoubleType);
        session.udf().register("normalizeOutdoorTemp", UDFs.normalizeOutdoorTemp, DoubleType);
    }

}
