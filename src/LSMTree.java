
import java.io.*;
import java.lang.reflect.Field;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class LSMTree {
    MemTable memTable;
    int levels;
    int maxSize;
    String fileBaseDir;
    ArrayList<SSTable> level0;
    //start from level 1
    ArrayList<TreeSet<SSTable>> SStables;
    ArrayList<Integer> level_size;
    ArrayList<Integer> last_chosen;
    MessageDigest md;
    public LSMTree(int maxSize, String dir){
        memTable=new MemTable();
        this.maxSize=maxSize;
        levels=-1;
        fileBaseDir=dir;
        level_size=new ArrayList<>();
        last_chosen=new ArrayList<>();
        try {
            md=MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        level0=new ArrayList<>();
        SStables=new ArrayList<>();
    }

    public void write(Record t){
        //if Memtable is not full add to Memtable
        memTable.write(t);

        if (memTable.size()>=maxSize){
            //need flush to level0
           if (level0.size()==0){
               //create first sstable
               level0=new ArrayList<>();
               levels=0;
               File f=new File(fileBaseDir+"/level0");
               f.mkdirs();
               String filename = randomFileName();
               String loc=fileBaseDir+"/level0/"+filename;
               level0.add(new SSTable(loc,memTable.flush()));
           }else{
               String filename = randomFileName();
               String loc=fileBaseDir+"/level0/"+filename;
               level0.add(new SSTable(loc,memTable.flush()));
           }
           if (level0.size()>=4){
               compaction();
           }
        }
    }


    public Record read(int id) throws Exception {
        Record res=memTable.read(id);
        if (res==null){
            //loop in level 0
            for(SSTable s: level0){
                if (id>=s.begin&&id<=s.end) {
                    //could be optimized
                    MemTable file = new MemTable(s.read());
                    res = file.read(id);
                    if (res != null) return res;
                }
            }
            //from level1
            for (TreeSet<SSTable> level: SStables){
                SSTable temp=new SSTable();
                temp.begin=id;
                SSTable file=level.floor(temp);
                if (file==null||file.end<id) return null;
                MemTable table=new MemTable(file.read());
                res = table.read(id);
                if (res != null) return res;
            }
        }
        return res;
    }

    public String randomFileName(){
        return Integer.toHexString(new Random().nextInt(1000000000));
    }


    public void compaction(){
        //compaction will always start from level0 and propagate to deeper level;
        //level0 compaction is treated specially
        compactLevel0();
        int level=1;
        //check if we need to continue compaction
        while (level_size.get(level-1)>=maxSize*Math.pow(10,level)){
            compactLevel(level);
            ++level;
        }
    }

    //compact level and write to level+1
    public void compactLevel(int level){
        //pick one SStable from level
        SSTable temp=new SSTable();
        temp.begin=last_chosen.get(level-1)+1;
        SSTable chosen=SStables.get(level-1).floor(temp);
        if (chosen==null)chosen=SStables.get(0).first();
        last_chosen.set(level-1,chosen.end);
        SStables.get(level-1).remove(chosen);
        level_size.set(level-1,level_size.get(level-1)-chosen.size);
        ArrayList<SSTable> toMerge=new ArrayList<>();
        if (SStables.size()==level){
            //Cloud be Improved

            //no SStable in next level create first one
            File f=new File(fileBaseDir+"/level"+(level+1));
            f.mkdirs();
            SStables.add(new TreeSet<>(new Comparator<SSTable>() {
                @Override
                public int compare(SSTable o1, SSTable o2) {
                    return o1.begin-o2.begin;
                }
            }));
            last_chosen.add(0);
            MemTable buffer=new MemTable(chosen.read());
            String filename = randomFileName();
            String loc=fileBaseDir+"/level"+(level+1)+"/"+filename;
            SStables.get(level).add(new SSTable(loc,buffer.flush()));
            level_size.add(chosen.size);
            f=new File(chosen.loc);
            if (!f.delete()){
                System.out.println("File deletion failed");
            }
        }else {
            //find overlap and merge
            SSTable tempBegin=new SSTable();tempBegin.begin=chosen.begin;
            SSTable tempEnd=new SSTable();tempEnd.begin=chosen.end;
            SortedSet<SSTable> overlapped=SStables.get(level).subSet(tempBegin,tempEnd);
            toMerge.add(chosen);
            toMerge.addAll(overlapped);
            mergeAndWrite(toMerge,level+1);
        }
        delete(toMerge);
    }


    public void compactLevel0(){
        ArrayList<SSTable> toCompact=new ArrayList<>();
        SSTable chosen=level0.get(3);
        toCompact.add(chosen);level0.remove(3);

        for (int i=2;i>=0;--i){
            SSTable s=level0.get(i);
            if (chosen.overlap(s)){
                toCompact.add(s);level0.remove(s);
            }
        }

        if (SStables.size()!=0){
            int max=Integer.MIN_VALUE,min=Integer.MAX_VALUE;
            for (SSTable s:toCompact) {
                max = Math.max(max, s.end);
                min = Math.min(min, s.begin);
            }
            SSTable start=new SSTable();start.begin=min;
            SSTable end=new SSTable();end.begin=max;
            TreeSet<SSTable> level1=SStables.get(0);
            SortedSet<SSTable> overlaped=level1.subSet(start,end);
            toCompact.addAll(overlaped);
            ArrayList<SSTable> temp=new ArrayList<>();
            for(SSTable toMerge:overlaped){
                level_size.set(0,level_size.get(0)-toMerge.size);
                temp.add(toMerge);
            }
            level1.removeAll(temp);
            mergeAndWrite(toCompact,1);

        } else {
            //add first level One
            File f=new File(fileBaseDir+"/level1");
            f.mkdirs();
            SStables.add(new TreeSet<>(new Comparator<SSTable>() {
                @Override
                public int compare(SSTable o1, SSTable o2) {
                    return o1.begin-o2.begin;
                }
            }));
            level_size.add(0);
            last_chosen.add(0);
            mergeAndWrite(toCompact,1);
        }
        //finish merge and discard old sstables
        delete(toCompact);
    }

    public void delete(ArrayList<SSTable> toCompact){
        for (SSTable s:toCompact){
            File f=new File(s.loc);
            if(!f.delete())
            {
                System.out.println("Failed to delete the file");
            }
        }
    }

    //merge file in level-1 and level and write to level
    public void mergeAndWrite(ArrayList<SSTable> filesToMerge,int level) {
        //filesToMerge need to add to list by time order. ie index 0 is newest value
        HashMap<Integer,Record> mergedTuples=new HashMap<>();
        for (int i=filesToMerge.size()-1;i>=0;--i){
            SSTable ss=filesToMerge.get(i);
            MemTable temp=new MemTable(ss.read());
            mergedTuples.putAll(temp.tuples);
        }
        ArrayList<Integer> keys=new ArrayList<Integer>(mergedTuples.keySet());
        Collections.sort(keys);
        ArrayList<Record> buffer=new ArrayList<>();
        for (Integer k:keys){
            buffer.add(mergedTuples.get(k));
            if (buffer.size()==maxSize){
                String loc=fileBaseDir+"/level"+level+"/"+randomFileName();
                SStables.get(level-1).add(new SSTable(loc,buffer));
                level_size.set(level-1,level_size.get(level-1)+maxSize);
                buffer=new ArrayList<>();
            }
        }
        if (!buffer.isEmpty()){
            String loc=fileBaseDir+"/level"+level+"/"+randomFileName();
            SStables.get(level-1).add(new SSTable(loc,buffer));
            level_size.set(level-1,level_size.get(level-1)+buffer.size());
        }
    }

    class SSTable implements Comparable<SSTable>{
        String loc;
        int begin;
        int end;
        int size;
        //Since SSTable is immutable, it writes the file when constructing it.
        public SSTable(String loc,ArrayList<Record> tuples){
            this.loc=loc;
            begin=tuples.get(0).ID;
            end=tuples.get(tuples.size()-1).ID;
            size=tuples.size();
            try {
                BufferedWriter w=new BufferedWriter(new FileWriter(loc));
                for (Record r:tuples) {
                    w.write(r.toString());
                    w.write("\n");
                }
                w.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        public SSTable(){ }

        public  BufferedReader read()  {
            try {
                return new BufferedReader(new FileReader(loc));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }

        public boolean overlap(SSTable o2){
            return (begin<=o2.end&&end>=o2.end)||(end>=o2.begin&&begin<=o2.begin)||
                    (begin>=o2.begin&&end<=o2.end)||(begin<=o2.begin&&end>=o2.end);
        }
        @Override
        public int compareTo(SSTable o) {
            return begin-o.begin;
        }
    }
}
