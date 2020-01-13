import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws IOException {
        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
        String appName = "Tonality definition spark project";

        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(sparkMasterUrl);

        // Unchecked configuration
        StructType schema = new StructType()
                .add("term", StringType.productPrefix(), false)
                .add("value", DoubleType.productPrefix(), false);

        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        SQLContext sqlContext = new SQLContext(session);
        Dataset<Row> dataset = sqlContext.read()
                .format("csv")
                .option("delimiter", ";")
                .option("header", "true")
                .schema(schema)
                .load("./emo_dict.csv");


        try (JavaSparkContext sparkContext = new JavaSparkContext(conf)) {
            File inputDir = new File("/app/input");
            if (!inputDir.isDirectory()) {
                throw new IllegalStateException("Input directory doesn't exist.");
            }

            File[] inputFiles = inputDir.listFiles();
            if (inputFiles == null || inputFiles.length == 0) {
                throw new IllegalStateException("Input directory is empty.");
            }

            RussianLuceneMorphology morphology = new RussianLuceneMorphology();

            for (File file : inputFiles) {
                List<String> content = readInputFile(file);

                List<String> firstForms = sparkContext.parallelize(content)
                        .flatMap((s) -> Arrays.asList(s.split("\\s")).iterator())
                        .map((s) -> s.toLowerCase().replaceAll("[^а-яё]", ""))
                        .filter((s) -> s.length() > 0)
                        .collect().parallelStream()
                        .map((s) -> morphology.getNormalForms(s).get(0))
                        .collect(Collectors.toList());

                sparkContext.parallelize(firstForms)
                        .filter((s) -> s.length() > 3)
                        .mapToPair((s) -> new Tuple2<>(s, 1))
                        .reduceByKey(Integer::sum)
                        // Unchecked mapping
                        .mapToPair((pair) -> new Tuple2<>(pair._1, Double.parseDouble(
                                dataset.select("value")
                                        .where("term = " + pair._1).toString()) * pair._2));
            }
        }
    }

    public static List<String> readInputFile(File file) throws IOException {
        return Files.lines(file.toPath()).collect(Collectors.toList());
    }
}
