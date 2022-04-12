import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Utils {

    // codul de eroare in caz ca nu se poate citi un caracter
    public static final int ERRORCODE = -1;

    // directorul de baza al programului
    public static final String baseDirectory = Paths.get(System.getProperty("user.dir")).getParent().toString() + "/";

    // lista pentru a stoca numerele fibonacci deja calculate
    public static final List<Integer> fibonacciNumbers = Collections.synchronizedList(new ArrayList<>());
    // initializam cu primele doua numere
    static {
        fibonacciNumbers.add(0);
        fibonacciNumbers.add(1);
    }

    // functie de baza pentru a afla un numar fibonacci
    public static int getFibonacciNumber(int order) throws InterruptedException {
        if (order < fibonacciNumbers.size()) {
            return fibonacciNumbers.get(order);
        }
        int newFibonacciNumber = -1;
        newFibonacciNumber = getFibonacciRecursive(order - 1) + getFibonacciRecursive(order - 2);
        return newFibonacciNumber;
    }

    // functie ajutatoare blocanta, in caz ca un numar fibonacci nu a fost aflat,
    // doar un thread poate sa coboare in recursivitate pentru a completa lista
    // fibonacciNumbers cu numerele fibonacci aflate
    private synchronized static int getFibonacciRecursive(int order) {

        if (order < fibonacciNumbers.size()) {
            return fibonacciNumbers.get(order);
        }

        int newFibonacciNumber = -1;
            newFibonacciNumber = getFibonacciRecursive(order - 1) + getFibonacciRecursive(order - 2);
            fibonacciNumbers.add(newFibonacciNumber);
        return newFibonacciNumber;
    }

    // fiind data calea catre un fisier, functia extrage numele fisierului
    public static String extractFileName(String path) {
        String[] splitPath = path.split("/");
        return splitPath[splitPath.length - 1];
    }

    // functie pentru citirea input file
    public static MyPair<Integer, ArrayList<String>> readInputFiles(String fileName) {
        Scanner sc = null;
        int chunkDimension = 0;
        int totalNumberOfFiles = 0;
        ArrayList<String> fileNames = new ArrayList<>();

        try {
            sc = new Scanner(new FileReader(baseDirectory + fileName));
            chunkDimension = Integer.parseInt(sc.nextLine());
            totalNumberOfFiles = Integer.parseInt(sc.nextLine());
            for (int i = 0; i < totalNumberOfFiles; i++) {
                fileNames.add(sc.nextLine());
            }

        } catch (FileNotFoundException e) {
            System.out.println("Input file " + baseDirectory + fileName + " not found");
            System.exit(1);
        } finally {
            assert sc != null;
            sc.close();
        }
        return new MyPair<>(chunkDimension, fileNames);
    }

    // functie pentru a scrie rezultatele dintr-o lista de Future intr-un fisier specificat
     public static <T> void writeToFile(String fileOutput, List<Future<T>> list) throws IOException, ExecutionException, InterruptedException {
         BufferedWriter writer = new BufferedWriter(new FileWriter(fileOutput));
         for (Future<T> elem : list) {
             String line = elem.get().toString() + "\n";
             writer.write(line);
         }
         writer.close();
     }

    // functie pentru obtinerea marimii fisierului dat ca parametru
    public static long getFileSize(String fileName) {
        long size = 0;
        Path path = Paths.get(baseDirectory + fileName);
        try {
            size = Files.size(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return size;
    }
}
