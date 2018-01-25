package task5_callLogs;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CallLogsHandler {

    public static final int TOP_N = 5;
    public static final String CALL_LOGS_CSV = "src\\main\\resources\\callLogs.csv";
    public static final String CALL_LOGS_PARQUET = "src\\main\\resources\\callLogs.parquet";

    public static void main(String[] args) {
        SparkSession sparkSession = getSparkSession();
        Dataset<Row> callLogsFromCSV = readFromCSV(sparkSession);
        calculateTopByDuration(callLogsFromCSV, sparkSession);
        Dataset<Row> callLogsFromParquet = readFromParquet(sparkSession);
        calculateTopByDuration(callLogsFromParquet, sparkSession);
    }

    private static Dataset<Row> readFromCSV(SparkSession sparkSession) {
        long startTime = System.currentTimeMillis();
        Dataset<Row> callLogs = sparkSession
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(CALL_LOGS_CSV);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Reading from csv file:" + totalTime + " ms");
        return callLogs;
    }

    private static Dataset<Row> readFromParquet(SparkSession sparkSession) {
        long startTime = System.currentTimeMillis();
        Dataset<Row> callLogs = sparkSession
                .read()
                .parquet(CALL_LOGS_PARQUET);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Reading from parquet file:" + totalTime + " ms");
        return callLogs;
    }

    private static void calculateTopByDuration(Dataset<Row> callLogs, SparkSession sparkSession) {
        callLogs.createOrReplaceTempView("callLogs");
        Dataset<Row> sumDurationsByFrom = sparkSession.sql("SELECT from, sum(duration) as sumDuration FROM callLogs GROUP BY from ORDER BY sumDuration");
        Dataset<Row> topOutgoingCalls = sumDurationsByFrom.limit(TOP_N);
        topOutgoingCalls.show();
    }

    private static SparkSession getSparkSession() {
        System.setProperty("hadoop.home.dir", "C:/winutils/");
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("Call Logs")
                .getOrCreate();
        return sparkSession;
    }
}
