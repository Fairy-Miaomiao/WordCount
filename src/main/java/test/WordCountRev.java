package test;

public class WordCountRev implements Comparable<WordCountRev>{
    private int key;
    private String value;
    public WordCountRev(Integer count, String word){
        this.key = count;
        this.value = word;
    }
    public int compareTo(WordCountRev o) {
        if(key == o.getKey()){
            return value.compareTo(o.getValue());
        }else {
            return key - o.getKey();
        }
    }
    public String getValue() {
        return value;
    }
    public int getKey() {
        return key;
    }
    public void setValue(String value) {
        this.value = value;
    }
}
