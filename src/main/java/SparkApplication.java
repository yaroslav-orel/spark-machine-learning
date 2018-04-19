import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkApplication {

    public static void main(String[] args) {
        val session = initSparkSession();
    }

    private static SparkSession initSparkSession(){
        val conf = new SparkConf(true)
                .setAppName("Machine-Learning")
                .setMaster("spark://yorel-VirtualBox:7077");
        return SparkSession.builder().sparkContext(new SparkContext(conf)).getOrCreate();
    }
}
