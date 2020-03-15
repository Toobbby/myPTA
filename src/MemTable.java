import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class MemTable {
    HashMap<Integer,Record> tuples;

    public MemTable(){

    }
    public MemTable(BufferedReader r)  {
        String line = null;
        try {
            while ((line = r.readLine()) != null) {
                write(new Record(line));
            }
        }catch (IOException e){
            e.getStackTrace();
        }
    }
    public int size(){
        return tuples.size();
    }

    public void write(Record t){
        tuples.put(t.ID,t);
    }
    //return null if not found
    public Record read(int id){
        return tuples.getOrDefault(id,null);
    }

    //flush current records and create new memtable
    public ArrayList<Record> flush(){
        StringBuilder s=new StringBuilder();
        ArrayList<Integer> keys=new ArrayList<Integer>(tuples.keySet());
        Collections.sort(keys);
        ArrayList<Record> res=new ArrayList<>();
        for (Integer k:keys){
            res.add(tuples.get(k));
        }
        tuples=new HashMap<>();
        return res;
    }
}
