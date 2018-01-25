package task6_statistics;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.mllib.random.RandomRDDs.normalJavaRDD;
import static task6_statistics.TestingConstants.*;

public class StatisticsUtils {

    public static JavaDoubleRDD getNormalDistribution(SparkSession sparkSession, double mean, double dispersion, int count) {
        // Generate a random double RDD. Values drawn from the standard normal distribution `N(0, 1)`, evenly distributed in 10 partitions.
        JavaDoubleRDD standardNormal = normalJavaRDD(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()), count, 10);
        // Apply a transform to get a random double RDD following `N(5.5, 2.25)`.
        // 3*S = 4.5 -> S=1.5 => D = 2.25
        return standardNormal.mapToDouble(x -> mean + Math.sqrt(dispersion) * x);
    }

    public static Vector getVectorWithEmpiricFrequences(List<Double> scores, int countOfIntervals) {
        double[] frequences = new double[countOfIntervals];
        scores.forEach(value -> {
            if (value > MIN_SCORE && value < MAX_SCORE) {
                int index = (int) ((value - MIN_SCORE) / SCORE_INTERVAL);
                frequences[index] = frequences[index] + 1;
            } else {
                frequences[0] = frequences[0] + 1;
            }
        });
        return Vectors.dense(frequences);
    }

    public static Vector getVectorWithTeoreticalFrequences(int countOfIntervals) {
        List<Double> teoreticalFrequences = teoreticalFrequences(countOfIntervals);
        double[] teoreticalFrequencesArray = new double[teoreticalFrequences.size()];
        for (int i = 0; i < teoreticalFrequences.size(); i++) {
            teoreticalFrequencesArray[i] = teoreticalFrequences.get(i);
        }
        return Vectors.dense(teoreticalFrequencesArray);
    }

    private static List<Double> teoreticalFrequences(int countOfIntervals) {
        List<Double> result = new ArrayList<>();
        double middleInterval = MIN_SCORE + SCORE_INTERVAL / 2;
        for (int i = 0; i < countOfIntervals; i++) {
            double ti = (middleInterval - MEAN) / Math.sqrt(DISPERSION);
            middleInterval += SCORE_INTERVAL;
            double teoreticalValue = SCORE_INTERVAL * COUNT_STUDENT * fi(ti) / MEAN;
            result.add(teoreticalValue);
        }
        return result;
    }

    private static double fi(double t) {
        return Math.exp(-(t * t) / 2) / (Math.sqrt(2 * Math.PI));
    }

    public static void caclChiSqTest(Vector vec, Vector vecTheoretical) {
        // compute the goodness of fit. If a second vector to test against is not supplied
        // as a parameter, the test runs against a uniform distribution.
        ChiSqTestResult goodnessOfFitTestResult = Statistics.chiSqTest(vec, vecTheoretical);
        // summary of the test including the p-value, degrees of freedom, test statistic,
        // the method used, and the null hypothesis.
        System.out.println(goodnessOfFitTestResult + "\n");
    }
}
