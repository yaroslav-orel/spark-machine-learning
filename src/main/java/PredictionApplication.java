import lombok.val;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Scanner;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.functions.callUDF;

public class PredictionApplication {

    private static final String APP_NAME = "Predict";

    public static void main(String[] args) {
        val scanner = new Scanner(System.in);
        System.out.println("Enter week day");
        Integer weekDay = scanner.nextInt();
        System.out.println("Enter day period");
        Integer dayPeriod = scanner.nextInt();
        System.out.println("Enter outdoor temperature");
        Double outdoorTemp = scanner.nextDouble();
        Double normOutdoor = (outdoorTemp - UDFs.MIN_TEMP)/(UDFs.MAX_TEMP - UDFs.MIN_TEMP);

        val session = SparkUtil.initSparkSession(APP_NAME);
        session.udf().register("disnornalizeIndoorTemp", UDFs.disnornalizeIndoorTemp, DataTypes.DoubleType);
        val row = RowFactory.create(weekDay, dayPeriod, normOutdoor);

        val rowToPredict = session.createDataFrame(asList(row), getSchema());
        val pipelineModel = PipelineModel.load("ml-model");
        val prediction = pipelineModel.transform(rowToPredict);
        val prettifiedDS = prediction
                .withColumn("Outdoor Temp", callUDF("disnornalizeIndoorTemp", prediction.col("Norm Outdoor")))
                .withColumn("Predicted Indoor Temp", callUDF("disnornalizeIndoorTemp", prediction.col("prediction")))
                .drop("rawFeatures").drop("features").drop("prediction").drop("Norm Outdoor");
        prettifiedDS.show();
    }

    private static StructType getSchema(){
        return new StructType()
                .add("Week Day", DataTypes.IntegerType)
                .add("Day Period", DataTypes.IntegerType)
                .add("Norm Outdoor", DataTypes.DoubleType);
    }
}
