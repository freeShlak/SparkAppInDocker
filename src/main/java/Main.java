import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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
                        .mapToPair((s) -> new Tuple2<>(s, 1))
                        .reduceByKey(Integer::sum)
                        .collectAsMap().forEach((key, value) -> System.out.println(key + " : " + value));
            }
        }
    }

    public static List<String> readInputFile(File file) throws IOException {
        return Files.lines(file.toPath()).collect(Collectors.toList());
    }
}
