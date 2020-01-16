import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.*;
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
            FileInformation fileInformation = fileManager.readLines(file);

            List<String> firstForms = javaContext.parallelize(fileInformation.getContent())
                    .flatMap((s) -> Arrays.asList(s.split("\\s+")).iterator())
                    .map((s) -> s.toLowerCase().replaceAll("[^а-яё]", ""))
                    .filter((s) -> s.length() > 0)
                    .collect().parallelStream()
                    .map((s) -> morphology.getNormalForms(s).get(0))
                    .collect(Collectors.toList());

            Map<String, Long> counts = javaContext.parallelize(firstForms)
                    .mapToPair((s) -> new Tuple2<>(s, 1L))
                    .reduceByKey(Long::sum)
                    .mapToPair((p) -> new Tuple2<>(p._2, p._1))
                    .sortByKey()
                    .mapToPair((p) -> new Tuple2<>(p._2, p._1))
                    .collectAsMap();

            Map<Long, String> order = javaContext.parallelize(firstForms)
                    .zipWithIndex()
                    .mapToPair((p) -> new Tuple2<>(p._2, p._1))
                    .sortByKey()
                    .collectAsMap();

            Map<String, Double> rates = counts.entrySet().parallelStream()
                    .map((ent) -> new AbstractMap.SimpleEntry<>(ent.getKey(), dictionary.getRate(ent.getKey())))
                    .filter((ent) -> ent.getValue().isPresent())
                    .map((ent) -> new  AbstractMap.SimpleEntry<>(ent.getKey(), ent.getValue().get()))
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

            Map<Long, Double> data = order.entrySet().parallelStream()
                    .map((ent) -> new AbstractMap.SimpleEntry<>(ent.getKey(), rates.get(ent.getValue())))
                    .filter((ent) -> ent.getValue() != null)
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

            double average = rates.entrySet().parallelStream()
                    .map((ent) -> new AbstractMap.SimpleEntry<>(ent.getValue(), counts.get(ent.getKey())))
                    .map((ent) -> new AbstractMap.SimpleEntry<>(ent.getKey() * ent.getValue(), ent.getValue()))
                    .reduce((ent1, ent2) -> new AbstractMap.SimpleEntry<>(
                            ent1.getKey() + ent2.getKey(),
                            ent1.getValue() + ent2.getValue()))
                    .map((ent) -> ent.getKey() / ent.getValue())
                    .get();

            StringBuilder buffer = new StringBuilder();
            buffer.append("Average tonality: ").append(average).append("\n\n");
            buffer.append("All tonalities: ").append("\n");
            data.forEach((k, v) -> buffer.append(k).append(";").append(v).append("\n"));

            fileManager.writeOutputFile(fileInformation, buffer);
        }
    }
}
