import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class LargeDatasetGenerator {
    public static void main(String[] args) {
        int numberOfPoints = 40000;
        int minValue = 0;
        int maxValue = 5000;
        String fileName = "BigDataset.csv";

        generateDataset(numberOfPoints, minValue, maxValue, fileName);
        System.out.println("Dataset generated successfully: " + fileName);
    }

    public static void generateDataset(int numPoints, int min, int max, String fileName) {
        Random random = new Random();

        try (FileWriter writer = new FileWriter(fileName)) {

            for (int i = 0; i < numPoints; i++) {
                int x = random.nextInt(max - min + 1) + min;
                int y = random.nextInt(max - min + 1) + min;
                writer.write(x + "," + y + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
