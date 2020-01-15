import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

public class FileManager {
    private File[] inputFiles;
    private File output;

    public FileManager() {
        File input = new File(Properties.inputDirectory);
        output = new File(Properties.outputDirectory);

        if (!input.isDirectory()) {
            throw new IllegalStateException("Input directory doesn't exist.");
        }

        File[] inputFiles = input.listFiles();
        if (inputFiles == null || inputFiles.length == 0) {
            throw new IllegalStateException("Input directory is empty.");
        }

        this.inputFiles = inputFiles;


        if (!output.isDirectory()) {
            throw new IllegalStateException("Output directory doesn't exist.");
        }
    }

    public void writeOutputFile(String fileName, StringBuilder builder) throws IOException {
        File out = new File(Properties.outputDirectory + "/" + fileName);
        if (out.createNewFile()) {
            try (FileWriter writer = new FileWriter(out)) {
                writer.write(builder.toString());
            }
        }
    }

    public File[] getInputFiles() {
        return inputFiles;
    }

    public List<String> readLines(File file) {
        try {
            return Files.lines(file.toPath()).collect(Collectors.toList());
        } catch (IOException e) {
            throw new IllegalStateException("Incorrect file: " + file.getName());
        }
    }
}
