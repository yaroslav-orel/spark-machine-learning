import lombok.NonNull;
import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static java.lang.ClassLoader.getSystemClassLoader;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class SparkUtil {

    private static final String SPARK_MASTER = "spark://yorel-VirtualBox:7077";

    public static SparkSession initSparkSession(String appName){
        val conf = new SparkConf(true)
                .setAppName(appName)
                .setMaster(SPARK_MASTER)
                .set("spark.executor.memory", "6g");

        return SparkSession.builder().sparkContext(new SparkContext(conf)).getOrCreate();
    }

    public static Dataset<Row> getDataset(SparkSession session, StructType schema, String path) {
        return session.read().option("header", true).schema(schema).csv(path);
    }

    public static void saveCSV(Dataset<Row> dataset, String path){
        dataset
                .coalesce(1)
                .write()
                .option("header", "true")
                .csv(path);
    }

}
