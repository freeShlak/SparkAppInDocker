import java.io.File;
import java.util.LinkedList;
import java.util.List;

public class FileInformation {
    private File file;
    private List<String> headers;
    private List<String> content;

    public FileInformation(File file, List<String> headers, List<String> content) {
        this.file = file;
        this.headers = new LinkedList<>(headers);
        this.content = new LinkedList<>(content);
    }

    public List<String> getHeaders() {
        return headers;
    }

    public List<String> getContent() {
        return content;
    }

    public File getFile() {
        return file;
    }
}
