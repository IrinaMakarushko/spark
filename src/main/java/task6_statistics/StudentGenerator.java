package task6_statistics;

import java.util.ArrayList;
import java.util.List;

public class StudentGenerator {

    public static List<Student> generateStudentList(List<Double> scores) {
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
}
