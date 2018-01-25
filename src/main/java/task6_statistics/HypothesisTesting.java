package task6_statistics;


import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

import static task6_statistics.StatisticsUtils.*;
import static task6_statistics.TestingConstants.*;

public class HypothesisTesting {

    public static void main(String[] args) {
        SparkSession sparkSession = getSparkSession();
        List<Double> scores = getStudentsScores(sparkSession);
        int countOfIntervals = (int) ((MAX_SCORE - MIN_SCORE) / SCORE_INTERVAL);
        Vector vectorEmpiric = getVectorWithEmpiricFrequences(scores, countOfIntervals);
        Vector vectorTheoretical = getVectorWithTeoreticalFrequences(countOfIntervals);
        caclChiSqTest(vectorEmpiric, vectorTheoretical);
        sparkSession.close();
    }

    private static List<Double> getStudentsScores(SparkSession sparkSession) {
        JavaDoubleRDD empiricNormalDistribution = getNormalDistribution(sparkSession, MEAN, DISPERSION, COUNT_STUDENT);
        List<Student> students = StudentGenerator.generateStudentList(empiricNormalDistribution.take(COUNT_STUDENT));
        return students.stream().map(Student::getScore).collect(Collectors.toList());
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
