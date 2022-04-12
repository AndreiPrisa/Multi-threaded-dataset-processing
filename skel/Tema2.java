import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Tema2 {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }

        // extragem argumentele
        int numberOfWorkers = Integer.parseInt(args[0]);
        String inputFile = args[1];
        String outputFile = args[2];

        ExecutorService executorService = Executors.newFixedThreadPool(numberOfWorkers);
        ArrayList<MyMapTask> mapTasks = new ArrayList<>();

        // citim datele din fisierul de intrare
        MyPair<Integer, ArrayList<String>> fileSpecifications = Utils.readInputFiles(inputFile);

        int chunkDimension = fileSpecifications.getFirst();
        ArrayList<String> fileNames = fileSpecifications.getSecond();

        // hashMap pentru retinerea ordinii fisierelor
        HashMap<String, Integer> fileOrder = new HashMap<>();
        for (int i = 0; i < fileNames.size(); i++) {
            fileOrder.put(fileNames.get(i), i);
        }

        // generam taskurile Map
        for (String currentFileName : fileNames) {
            long totalOffset = 0;

            // luam dimensiunea fisierului curent
            long currentFileSize = Utils.getFileSize(currentFileName);

            // generam taskuri pentru fragmente de chunkDimension biti
            while (totalOffset < currentFileSize) {
                mapTasks.add(new MyMapTask(currentFileName, (int) totalOffset,
                        (int) Math.min(chunkDimension, currentFileSize - totalOffset)));
                totalOffset += chunkDimension;
            }
        }

        // executam taskurile Map
        List<Future<MapOutput>> futureMapList = executorService.invokeAll(mapTasks);

        // map cu cheia numele fisierului si valoare o lista cu rezultatele Map pe
        // fragmentele din fisierul respectiv
        HashMap<String,ArrayList<MapOutput>> reduceInputMap = new HashMap<>();

        // grupam rezultatele din urma Map
        for (Future<MapOutput> future : futureMapList) {
            String fileName = future.get().getDocumentName();
            if (!reduceInputMap .containsKey(fileName)) {
                reduceInputMap.put(fileName, new ArrayList<>());
            }
            reduceInputMap.get(fileName).add(future.get());
        }

        ArrayList<MyReduceTask> reduceTasks = new ArrayList<>();

        // generam taskurile de Reduce
        for (String fileName : fileNames) {
            reduceTasks.add(new MyReduceTask(fileName, reduceInputMap.get(fileName)));
        }

        // executam taskurile de Reduce
        List<Future<ReduceOutput>> futureReduceList = executorService.invokeAll(reduceTasks);

        // inchidem executorService, intrucat am executat toate taskurile
        executorService.shutdown();

        // sortam rezultatele de dupa Reduce conform specificatiilor
        futureReduceList.sort((o1, o2) -> {
            try {
                ReduceOutput ro1 = o1.get();
                ReduceOutput ro2 = o2.get();

                if (ro1.getRank() > ro2.getRank()) {
                    return -1;
                } else if (ro1.getRank() < ro2.getRank()) {
                    return 1;
                } else return fileOrder.get(ro1.getDocumentName()) - fileOrder.get(ro2.getDocumentName());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return 0;
        });

        // scriem in fisierul de output
        try {
            Utils.writeToFile(outputFile, futureReduceList);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
