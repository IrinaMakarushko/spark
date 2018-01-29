package task6_statistics;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static task6_statistics.StatisticsUtils.*;
import static task6_statistics.TestingConstants.*;

public class HypothesisTesting {

    public static void main(String[] args) {
        SparkSession sparkSession = getSparkSession();
        Dataset<Double> scores = getStudentsScores(sparkSession);
        int countOfIntervals = (int) ((MAX_SCORE - MIN_SCORE) / SCORE_INTERVAL);
        Vector vectorEmpiric = getVectorWithEmpiricFrequences(scores, countOfIntervals);
        Vector vectorTheoretical = getVectorWithTeoreticalFrequences(countOfIntervals);
        caclChiSqTest(vectorEmpiric, vectorTheoretical);
        sparkSession.close();
    }

    private static Dataset<Double> getStudentsScores(SparkSession sparkSession) {
        Dataset<Row> students = sparkSession.read().parquet(STUDENT_PARQUET_FILENAME);
        Dataset<Double> scores = students.map((MapFunction<Row, Double>) row -> row.getAs("score"), Encoders.DOUBLE());
        return scores;
    }

    private static SparkSession getSparkSession() {
        System.setProperty("hadoop.home.dir", "C:/winutils/");
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("Top TopRevenueFinder")
                .getOrCreate();
        return sparkSession;
    }
}
