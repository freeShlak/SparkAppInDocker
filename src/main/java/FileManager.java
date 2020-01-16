import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
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

    public void writeOutputFile(FileInformation fileInformation, StringBuilder contentBuilder) throws IOException {
        File out = new File(Properties.outputDirectory + "/" + fileInformation.getFile().getName());

        if(out.exists()) {
            if (!out.delete()) {
                throw new IOException("Can't delete old output file.");
            }
        }

        if (out.createNewFile()) {
            try (FileWriter writer = new FileWriter(out)) {
                StringBuilder headersBuilder = new StringBuilder();
                fileInformation.getHeaders().forEach((s) -> headersBuilder.append(s).append("\n"));
                headersBuilder.append("\n");

                writer.write(headersBuilder.toString());
                writer.write(contentBuilder.toString());
            }
        }
    }

    public File[] getInputFiles() {
        return inputFiles;
    }

    public FileInformation readLines(File file) {
        try {
            List<String> headers = new LinkedList<>();
            List<String> content = Files.lines(file.toPath()).collect(Collectors.toList());
            int i = 0;
            do {
                headers.add(content.get(i));
            }  while (i < content.size() && !content.get(i++).matches("^-{5,}$"));
            content.removeAll(headers);

            return new FileInformation(file, headers, content);
        } catch (IOException e) {
            throw new IllegalStateException("Incorrect file: " + file.getName());
        }
    }
}
