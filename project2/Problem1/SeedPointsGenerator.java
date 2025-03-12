import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class SeedPointsGenerator {
    public static void main(String[] args) {
        int k = 3; // Example seed count, can be changed as needed
        int minValue = 0;
        int maxValue = 7000;
        String fileName = "seeds_3.csv";

        generateSeedPoints(k, minValue, maxValue, fileName);
        System.out.println("Seed points file generated successfully: " + fileName);
    }

    public static void generateSeedPoints(int k, int min, int max, String fileName) {
        Random random = new Random();

        try (FileWriter writer = new FileWriter(fileName)) {

            for (int i = 0; i < k; i++) {
                int x = random.nextInt(max - min + 1) + min;
                int y = random.nextInt(max - min + 1) + min;
                writer.write(x + "," + y + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
