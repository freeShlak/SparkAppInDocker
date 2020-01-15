public class Properties {
    public static final String appName = "Tonality definition spark project";
    public static final String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
    public static final String dictionaryName = System.getenv("DICTIONARY_NAME");

    public static final String inputDirectory = "/app/input";
    public static final String outputDirectory = "/app/output";
}
