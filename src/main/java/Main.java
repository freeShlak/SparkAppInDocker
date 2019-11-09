import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {
    public static void main(String[] args) {
        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");

        SparkConf conf = new SparkConf()
                .setAppName("Example of spark docker application.")
                .setMaster(sparkMasterUrl);

        try (JavaSparkContext sparkContext = new JavaSparkContext(conf)) {
            // Write your code for execution here.
        }
    }
}
