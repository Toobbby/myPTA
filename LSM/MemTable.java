

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;

public class MemTable {
    HashMap<Integer,Record> tuples;
    UUID id;
    public MemTable(UUID uuid){
        tuples=new HashMap<>();
        id=uuid;
    }
    public MemTable(BufferedReader r)  {
        tuples=new HashMap<>();
        String line = null;
        try {
            line=r.readLine();
            id=UUID.fromString(line);
            while ((line = r.readLine()) != null) {
                write(new Record(line));
            }
            r.close();
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
    public void delete(int id){
        tuples.put(id,new Record(id,true));
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

    public Iterator<Record> iterate(){
        return tuples.values().iterator();
    }
}
