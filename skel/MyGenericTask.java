import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

// clasa generica pentru Task-uri
public abstract class MyGenericTask<T> implements Callable<T> {


    @Override
    public final T call() {
        return execute();
    }

    public abstract T execute();
}

class TestTask extends MyGenericTask<Integer> {
    Integer id;

    public TestTask(Integer id) {
        this.id = id;
    }

    @Override
    public Integer execute() {
        return id;
    }
}

// clasa abstracta pentru MapTaskuri
// generateInput returneaza inputul asupra caruia se
// aplica operatia map
// in cazul nostru particular, generateInput ajusteaza fragmentul
// si in sparge in cuvinte, iar map genereaza MapOutput ce
// contine rezultatele cerute
abstract class GenericMapTask<I, O> extends MyGenericTask<O> {

    public abstract O map(I input);
    public abstract I generateInput();
    @Override
    public final O execute() {
        return map(generateInput());
    }
}

// clasa abstracta pentru ReduceTaskuri
// reduceInput corespunde etapei de combinare
// process corespunde etai de prelucrare
abstract class GenericReduceTask<I, O> extends MyGenericTask<O> {

    public abstract I reduceInput();
    public abstract O process(I input);
    @Override
    public final O execute() {
        I newInput = reduceInput();
        return process(newInput);
    }
}

class MyReduceTask extends GenericReduceTask<MapOutput, ReduceOutput> {
    String documentName;
    ArrayList<MapOutput> mapResults;

    public MyReduceTask(String documentName, ArrayList<MapOutput> mapResults) {
        this.documentName = documentName;
        this.mapResults = mapResults;
    }

    @Override
    public MapOutput reduceInput() {
        HashMap<Integer, Integer> reducedMap = new HashMap<>();
        ArrayList<String> longestWordsUpdated = new ArrayList<>();
        int maxLength = 0;

        for (MapOutput mapOutput : mapResults) {
            int length = 0;
            if (mapOutput.getLongestWords() != null) {
                length = mapOutput.getLongestWords().get(0).length();
            }
            maxLength = Math.max(maxLength, length);
            HashMap<Integer, Integer> currentFrequencyMap = mapOutput.getFrequencyMap();

            for (Map.Entry<Integer, Integer> entry : currentFrequencyMap.entrySet()) {
                int key = entry.getKey();
                int frequency = entry.getValue();
                if (!reducedMap.containsKey(key)) {
                    reducedMap.put(key, frequency);
                } else {
                    int currentVal = reducedMap.get(key);
                    reducedMap.put(key, currentVal + frequency);
                }
            }
        }
        for (MapOutput mapOutput : mapResults) {
            ArrayList<String> currentLongestWords = mapOutput.getLongestWords();
            if (currentLongestWords != null && currentLongestWords.get(0).length() == maxLength) {
                longestWordsUpdated.addAll(currentLongestWords);
            }
        }
        return new MapOutput(documentName, reducedMap, longestWordsUpdated);
    }

    @Override
    public ReduceOutput process(MapOutput input) {
        int totalSum = 0;
        int totalWordsCount = 0;

        HashMap<Integer, Integer> reducedMap = input.getFrequencyMap();

        try {

            // calculam simultan numarul total de cuvinte si numaratorul formulei
            for (Map.Entry<Integer, Integer> entry : reducedMap.entrySet()) {
                int length = entry.getKey();
                int numberOfOccurrences = entry.getValue();
                totalWordsCount += numberOfOccurrences;
                totalSum += Utils.getFibonacciNumber(length + 1) * numberOfOccurrences;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        float rank = ((float) totalSum) / totalWordsCount;
        int lengthOfLongestWord = input.getLongestWords().get(0).length();
        int numberOfLongestWords = input.getLongestWords().size();
        return new ReduceOutput(documentName, rank, lengthOfLongestWord, numberOfLongestWords);
    }
}

class MyMapTask extends GenericMapTask<String[], MapOutput> {
    // delimitatori
    public final String delimiters = ";:/?~\\.,><`[]{}()!@#$%^&-_+'=*\"| \t\r\n";

    // expresie regulata pentru a impartii fragmentul in cuvinte
    public final String regexExpression =
            "[;:/?~.,><`\\[\\]{}()!@#$%^&\\-_+'=*\"| \t\r\n]+";

    String documentName;
    Integer offset;
    Integer  size;

    public MyMapTask(String documentName, Integer  offset, Integer  size) {
        this.documentName = documentName;
        this.offset = offset;
        this.size = size;
    }

    @Override
    public MapOutput map(String[] input) {

        // map cu cheie lungimea cuvantului si valoare o lista cu acele
        // cuvinte de lungimea indicata de cheie
        HashMap<Integer, ArrayList<String>> wordMap = new HashMap<>();

        // map cu lungimile cuvintelor si cate sunt acestea
        HashMap<Integer, Integer> lengthMap = new HashMap<>();
        
        // lista cu cele mai lungi cuvinte
        ArrayList<String> longestWords;
        
        
        int maxLength = 0;

        for (String word : input) {

            if (word.length() == 0) {
                continue;
            }

            // completam map-ul
            if (!wordMap.containsKey(word.length())) {
                wordMap.put(word.length(), new ArrayList<>());
            }
            wordMap.get(word.length()).add(word);
            maxLength = Math.max(maxLength, word.length());
        }

        // completam lengthMap cu lungimile cuvintelor si cate cuvinte au acea lungime
        for (Map.Entry<Integer, ArrayList<String>> entry : wordMap.entrySet()) {
            int length = entry.getKey();
            int numberOfWords = entry.getValue().size();
            lengthMap.put(length, numberOfWords);

        }

        // completam lista cu cele mai lungi cuvinte din fragment
        longestWords = wordMap.get(maxLength);

        return new MapOutput(documentName, lengthMap, longestWords);
    }

    @Override
    public String[] generateInput() {
        int start = offset;
        int end = offset + size - 1;

        // variabila utilizata pentru a citi caractere (este -1 daca nu se mai poate citi)
        int code;

        // cuvintele extrase din fragment
        String[] words = new String[0];

        // completam cu continutul fragmentului
        StringBuilder fileContent = new StringBuilder();

        try {
            BufferedReader br = new BufferedReader( new InputStreamReader(new FileInputStream(Utils.baseDirectory + documentName)));

            // daca nu suntem la inceputul fisierului, citim
            // si caracterul de dinainte sa ne dam seama daca suntem
            // sau nu in mijlocul unui cuvant;
            if (start > 0) {
                long skipped = br.skip(start - 1);
                char c = (char) br.read();

                if (!delimiters.contains(Character.toString(c))) {

                    // suntem in mijlocul cuvantului, inaintam pana la urmatorul cuvant
                    do {
                        code =  br.read();
                        c = (char) code;
                        start++;

                        // daca am terminat de citit cuvantul la mijlocul caruia
                        // suntem, iesim din while
                        if (delimiters.contains(Character.toString(c))) {
                            fileContent.append(c);
                            break;
                        }
                        // cat timp avem ce sa citim
                    } while (code != Utils.ERRORCODE);
                }
            }

            code = 0;

            // citim chunk-ul destinat noua
            while (start <= end) {
                code = br.read();
                if (code != Utils.ERRORCODE) {
                    fileContent.append((char) code);
                } else {
                    break;
                }
                start++;
            }

            // daca suntem in mijlocul cuvantului, il citim pana la capat
            if (code != Utils.ERRORCODE && !delimiters.contains(Character.toString((char) code))) {
                while ((code = br.read()) != Utils.ERRORCODE && !delimiters.contains(Character.toString((char) code))) {
                    fileContent.append((char) code);
                }
            }
            br.close();

            // impartim fragmentul in cuvinte
            words = fileContent.toString().split(regexExpression);
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        return words;
    }

}

