package task6_statistics;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static task6_statistics.StatisticsUtils.getNormalDistribution;
import static task6_statistics.TestingConstants.*;

public class StudentGenerator {

    private static List<Student> generateStudentList(List<Double> scores) {
        List<Student> students = new ArrayList<>();
        for (int i = 0; i < scores.size(); i++) {
            Student student = new Student();
            student.setId((long) i);
            student.setName("Name" + i);
            student.setLastName("Last Name" + i);
            student.setScore(scores.get(i));
            students.add(student);
        }
        return students;
    }

    public static void writeStudentsToFile() {
        SparkSession sparkSession = getSparkSession();
        JavaDoubleRDD scores = getNormalDistribution(sparkSession, MEAN, DISPERSION, COUNT_STUDENT);
        List<Student> students = generateStudentList(scores.take(COUNT_STUDENT));
        Encoder encoder = Encoders.bean(Student.class);
        sparkSession.createDataset(students, encoder).write().parquet(STUDENT_PARQUET_FILENAME);
        sparkSession.close();
    }

    private static SparkSession getSparkSession() {
        System.setProperty("hadoop.home.dir", "C:/winutils/");
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("Students ")
                .getOrCreate();
        return sparkSession;
    }
}
