import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName(Properties.appName)
                .setMaster(Properties.sparkMasterUrl);


        FileManager fileManager = new FileManager();

        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext javaContext = new JavaSparkContext(session.sparkContext());

        Dictionary dictionary = new Dictionary(session);
        RussianLuceneMorphology morphology = new RussianLuceneMorphology();

        for (File file : fileManager.getInputFiles()) {
            List<String> content = fileManager.readLines(file);

            List<String> firstForms = javaContext.parallelize(content)
                    .flatMap((s) -> Arrays.asList(s.split("\\s")).iterator())
                    .map((s) -> s.toLowerCase().replaceAll("[^а-яё]", ""))
                    .filter((s) -> s.length() > 0)
                    .collect().parallelStream()
                    .map((s) -> morphology.getNormalForms(s).get(0))
                    .collect(Collectors.toList());
        }
    }
}
