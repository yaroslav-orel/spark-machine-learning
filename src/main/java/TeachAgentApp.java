import lombok.experimental.var;
import lombok.val;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

import static java.util.Arrays.asList;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class TeachAgentApp {

    private static final String APP_NAME = "Teach-Agent";
    private static final String DATASET_NAME = "ml-ready.csv";
    private static final String PREDICTION_DATASET_PATH = "predictions1.csv";

    public static void main(String[] args) throws IOException {
        val session = SparkUtil.initSparkSession(APP_NAME);
        session.udf().register("disnornalizeIndoorTemp", UDFs.disnornalizeIndoorTemp, DoubleType);
        var initialDataset = SparkUtil.getDataset(session, getSchema(), MiscUtil.getFilePath(DATASET_NAME));

        val splitDatasets = initialDataset.randomSplit(new double[]{0.7, 0.3});
        val trainDataset = splitDatasets[0];
        val testDataset = splitDatasets[1];

        val pipeline = buildPipeline(initialDataset.columns());
        val pipelineModel = pipeline.fit(trainDataset);
        pipelineModel.save("ml-model");

        val predictions = pipelineModel.transform(testDataset);

        val withoutFeatures = predictions.drop("rawFeatures").drop("features");
        /*val withConvertedIndoorTemp = withoutFeatures
                .withColumn("Indoor Temp", callUDF("disnornalizeIndoorTemp", withoutFeatures.col("prediction")));*/
        SparkUtil.saveCSV(withoutFeatures, MiscUtil.getFilePath(PREDICTION_DATASET_PATH));
    }

    private static StructType getSchema() {
        return new StructType()
                    .add("Week Day", DataTypes.IntegerType)
                    .add("Day Period", DataTypes.IntegerType)
                    .add("Norm Indoor", DataTypes.DoubleType)
                    .add("Norm Outdoor", DataTypes.DoubleType);
    }

    private static Pipeline buildPipeline(String[] cols){
        val featureCols = ArrayUtils.removeElement(cols, "Norm Indoor");

        val vectorAssembler = new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("rawFeatures");

        val vectorIndexer = new VectorIndexer()
                .setInputCol("rawFeatures")
                .setOutputCol("features")
                .setMaxCategories(7);

        val gbt = new GBTRegressor()
                .setLabelCol("Norm Indoor");

        val paramGrid = new ParamGridBuilder()
                .addGrid(gbt.maxDepth(), new int[]{2,5})
                .addGrid(gbt.maxIter(), new int[]{10, 100})
                .build();

        val evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol(gbt.getLabelCol())
                .setPredictionCol(gbt.getPredictionCol());

        val  cv = new CrossValidator().setEstimator(gbt).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid);

        return new Pipeline().setStages(new PipelineStage[]{vectorAssembler, vectorIndexer, cv});
    }
}
