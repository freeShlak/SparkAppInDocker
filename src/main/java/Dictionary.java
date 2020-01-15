import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Dictionary implements Serializable {
    private final static StructType schema = new StructType(new StructField[]{
            new StructField("term", DataTypes.StringType, true, Metadata.empty()),
            new StructField("value", DataTypes.DoubleType, true, Metadata.empty())
    });

    private final Dataset<Row> dataset;

    public Dictionary(SparkSession session) {
        try {
            dataset = session.createDataFrame(
                    readDictionaryRows(new File(Properties.dictionaryName)),
                    schema
            );
        } catch (IOException e) {
            throw new IllegalStateException("Can't read dictionary.");
        }
    }

    public Optional<Double> getRate(String word) {
        try {
            return Optional.of(dataset
                    .select("value")
                    .where(String.format("term = \"%s\"", word))
                    .first().getDouble(0));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private List<Row> readDictionaryRows(File file) throws IOException {
        return Files.lines(file.toPath()).skip(1).map(s -> {
            String[] columns = s.split(";");
            Object[] selected = new Object[2];
            selected[0] = columns[0];
            selected[1] = Double.parseDouble(columns[2]);
            return selected;
        }).map(RowFactory::create).collect(Collectors.toList());
    }
}
