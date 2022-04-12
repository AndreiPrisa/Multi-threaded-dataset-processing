import java.util.ArrayList;
import java.util.HashMap;

// aici avem clase ajutatoare
public class MyPair<U, V> {
    private final U first;
    private final V second;

    public MyPair(U first, V second) {
        this.first = first;
        this.second = second;
    }

    public U getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }
}

// clasa pentru iesirea operatiei de Reduce
class ReduceOutput {
    private final String documentName;
    private final float rank;
    private final int maxWordLength;
    private final int numberOfMaxLengthWords;

    public ReduceOutput(String documentName, float rank, int maxWordLength, int numberOfMaxLengthWords) {
        this.documentName = documentName;
        this.rank = rank;
        this.maxWordLength = maxWordLength;
        this.numberOfMaxLengthWords = numberOfMaxLengthWords;
    }

    public String getDocumentName() {
        return documentName;
    }

    public float getRank() {
        return rank;
    }

    public int getMaxWordLength() {
        return maxWordLength;
    }

    public int getNumberOfMaxLengthWords() {
        return numberOfMaxLengthWords;
    }

    @Override
    public String toString() {
        return Utils.extractFileName(this.documentName) + "," + String.format("%.2f", this.rank) + "," +
                this.maxWordLength + "," + this.numberOfMaxLengthWords;

    }
}

class MyTriplet<U, V, T> {
    private final U first;
    private final V second;
    private final T third;

    public MyTriplet(U first, V second, T third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public U getFirst() {
        return first;
    }

    public V getSecond() {
        return second;
    }

    public T getThird() {
        return third;
    }
}

// clasa pentru iesirea operatiei de Map
class MapOutput extends MyTriplet<String, HashMap<Integer, Integer>, ArrayList<String>> {
    public MapOutput(String documentName, HashMap<Integer, Integer> frequencyMap, ArrayList<String> longestWords) {
        super(documentName, frequencyMap, longestWords);
    }

    public String getDocumentName() {
        return getFirst();
    }

    public HashMap<Integer, Integer> getFrequencyMap() {
        return getSecond();
    }

    public ArrayList<String> getLongestWords() {
        return getThird();
    }
}