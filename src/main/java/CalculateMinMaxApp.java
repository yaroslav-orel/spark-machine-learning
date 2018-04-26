import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

public class CalculateMinMaxApp {

    private static final String APP_NAME = "Calculate mix-max";
    private static final String DATASET_NAME = "miniset.csv";
    private static final String ML_READY_DATASET_PATH = "min-max-miniset.csv";

    public static void main(String[] args) {
        val session = SparkUtil.initSparkSession(APP_NAME);
        PrepareForMLApp.registerUDFs(session);

        val initialDataset = SparkUtil.getDataset(session, PrepareForMLApp.getSchema(), MiscUtil.getFilePath(DATASET_NAME));
        val preparedDS = getPreparedDataset(initialDataset);


        SparkUtil.saveCSV(preparedDS, MiscUtil.getFilePath(ML_READY_DATASET_PATH));
    }

    private static Dataset<Row> getPreparedDataset(Dataset<Row> initialDataset) {
        val transformedDS = initialDataset
                .withColumn("Date", callUDF("timestampToDate", initialDataset.col("Timestamp")))
                .withColumn("Week Day", callUDF("timestampToWeekDay", initialDataset.col("Timestamp")))
                .withColumn("Day Period", callUDF("timestampToDayPeriod", initialDataset.col("Timestamp")))
                .groupBy("Date", "Week Day", "Day Period")
                .avg("Outdoor Temp", "Indoor Temp", "Energy Consumption");

        return transformedDS.agg(min("avg(Indoor Temp)"), min("avg(Outdoor Temp)"),
                max("avg(Indoor Temp)"), max("avg(Outdoor Temp)"));
    }
}
