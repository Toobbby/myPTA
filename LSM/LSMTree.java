package LSM;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;

public class LSMTree {
    String tableName;
    MemTable memTable;
    int levels;
    int maxSize;
    String fileBaseDir;
    ArrayList<SSTable> level0;
    //start from level 1
    ArrayList<TreeSet<SSTable>> SStables;
    ArrayList<Integer> level_size;
    ArrayList<Integer> last_chosen;
    LSMBuffer<String, MemTable> buffer;
    String logPath;
    int number=0;
    public LSMTree(String tableName, int maxSize, String dir, LSMBuffer<String, MemTable> b, String logPath){
        this.logPath = logPath;
        buffer=b;
        this.tableName = tableName;
        memTable=new MemTable(randomFileName());
        this.maxSize=maxSize;
        levels=-1;
        fileBaseDir=dir;
        level_size=new ArrayList<>();
        last_chosen=new ArrayList<>();
        level0=new ArrayList<>();
        SStables=new ArrayList<>();
     //   tableInitialization(fileBaseDir);
    }


    public void flushMemtable() throws IOException {
        if (memTable.size()>=maxSize){
            //need flush to level0
            if (level0.size()==0){
                //create first sstable
                level0=new ArrayList<>();
                levels=0;
                File f=new File(fileBaseDir+"/level0");
                f.mkdirs();
                String filename=randomFileName();
                String loc=fileBaseDir+"/level0/"+filename;
                SSTable newSSTable = new SSTable(loc,memTable.flush(),filename);
                level0.add(newSSTable);
                logWriter("create  " + "L-0 " + "K " + newSSTable.id.substring(0,2) + newSSTable.begin +" "+ newSSTable.id.substring(0,2) + newSSTable.end, this.logPath);
            }else{
                String filename=randomFileName();
                String loc=fileBaseDir+"/level0/"+filename;
                SSTable newSSTable = new SSTable(loc,memTable.flush(),filename);
                level0.add(newSSTable);
                logWriter("create  " + "L-0 " + "K " + newSSTable.id.substring(0,2) + newSSTable.begin +" "+ newSSTable.id.substring(0,2) + newSSTable.end, this.logPath);

            }
            if (level0.size()>=4){
                compaction();
            }
        }
    }

    public void deleteRecord(int id) throws IOException {
        Record r=null;
        try {
            r=readID(id);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (r!=null) {
            memTable.delete(id);
            //may need to flush
            flushMemtable();
        }
    }


    public void write(Record t) throws IOException {
        //if Memtable is not full add to Memtable
        memTable.write(t);
        logWriter("Written: " + t.toString(), this.logPath);
        flushMemtable();
    }


    public Record readID(int id) throws Exception {
        Record res=memTable.read(id);
        if (res==null){
            //loop in level 0
            for(SSTable s: level0){
                if (id>=s.begin&&id<=s.end) {
                    //could be optimized
                    MemTable file=loadOrGetMemtable(s, 0);
                    res = file.read(id);
                    if (res != null) return res;
                }
            }
            //from level1
            for(int i = 0; i < SStables.size(); i++){
                TreeSet<SSTable> level = SStables.get(i);
                SSTable temp=new SSTable();
                temp.begin=id;
                SSTable file=level.floor(temp);
                if (file==null||file.end<id) return null;
                MemTable table=loadOrGetMemtable(file, i + 1);
                res = table.read(id);
                if (res != null) return res;
            }
        }
        if (res != null && res.Phone.equals("delete")) return null;
        return res;
    }

    
    public MemTable loadOrGetMemtable(SSTable sstable, int level) throws IOException {
        if (buffer.containsKey(sstable.id.toString())){
            return buffer.get(sstable.id.toString());
        }else {
            MemTable res=new MemTable(sstable.read());
            buffer.put(sstable.id.toString(),res);
            logWriter("Swap in  " + "L-" + level +" K " + sstable.id.substring(0,2) + sstable.begin +" "+ sstable.id.substring(0,2) + sstable.end, this.logPath);
            return res;
        }
    }
    //naive implementation
    //other way is to build a index using area code
    public ArrayList<Record> readAreaCode(String areaCode) throws IOException {
        ArrayList<Record> res=new ArrayList<>();
        Iterator<Record> i=memTable.iterate();
        Record tempRecord;
        MemTable tempMentable;
        while (i.hasNext()){
            tempRecord=i.next();
            if (tempRecord.getPhone().startsWith(areaCode)){
                res.add(tempRecord);
                logWriter("MRead: " + tempRecord.toString(), this.logPath);
            }
        }
        //loop in level 0
        for(SSTable s: level0){
                //could be optimized
                tempMentable = loadOrGetMemtable(s, 0);
                i=tempMentable.iterate();
                while (i.hasNext()){
                    tempRecord=i.next();
                    if (tempRecord.getPhone().startsWith(areaCode)){
                        res.add(tempRecord);
                        logWriter("MRead: " + tempRecord.toString(), this.logPath);
                    }
                }
        }
        //from level1
        for(int j = 0; j < SStables.size(); j++){
            TreeSet<SSTable> level = SStables.get(j);
            for(SSTable s: level){
                //could be optimized
                tempMentable = loadOrGetMemtable(s, j + 1);
                i=tempMentable.iterate();
                while (i.hasNext()){
                    tempRecord=i.next();
                    if (tempRecord.getPhone().startsWith(areaCode)){
                        res.add(tempRecord);
                        logWriter("MRead: " + tempRecord.toString(), this.logPath);
                    }
                }
            }
        }
        return res;
    }

    public String randomFileName(){
        return tableName+"-"+number++;
    }


    public void compaction() throws IOException {
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
    public void compactLevel(int level) throws IOException {
        //pick one SStable from level
        SSTable temp=new SSTable();
        temp.begin=last_chosen.get(level-1)+1;
        SSTable chosen=SStables.get(level-1).floor(temp);
        if (chosen==null)chosen=SStables.get(level - 1).first();
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
            //move chosen to next level
            String filename=chosen.loc.split("/level"+level+"/")[1];
            String loc=fileBaseDir+"/level"+(level+1)+"/"+filename;
            try {
                Files.move(Paths.get(chosen.loc),Paths.get(loc),StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                e.printStackTrace();
            }
            SStables.get(level).add(chosen);
            level_size.add(chosen.size);
        }else {
            buffer.remove(chosen.id.toString());
            //find overlap and merge
            SSTable tempBegin=new SSTable();tempBegin.begin=chosen.begin;
            SSTable tempEnd=new SSTable();tempEnd.begin=chosen.end;
            SortedSet<SSTable> overlapped=SStables.get(level).subSet(tempBegin,tempEnd);
            for (SSTable s:overlapped){
                buffer.remove(s.id.toString());
            }
            toMerge.add(chosen);
            toMerge.addAll(overlapped);
            mergeAndWrite(toMerge,level+1);
        }
        delete(toMerge);
    }

    public void deleteTable(){
        for(SSTable s: level0){
            //could be optimized
            s.delete();
            File f=new File(fileBaseDir+"/level0");
            f.delete();
        }
        //from level1
        for (int i=0;i< SStables.size();++i){
            TreeSet<SSTable> level=SStables.get(i);
            for(SSTable s: level){
               s.delete();
            }
            File f=new File(fileBaseDir+"/level"+(i+1));
            f.delete();
        }
    }



    public void compactLevel0() throws IOException {
        ArrayList<SSTable> toCompact=new ArrayList<>();
        SSTable chosen=level0.get(3);
        toCompact.add(chosen);level0.remove(3);
        buffer.remove(chosen.id.toString());
        for (int i=2;i>=0;--i){
            SSTable s=level0.get(i);
            if (chosen.overlap(s)){
                buffer.remove(s.id.toString());
                toCompact.add(s);
                level0.remove(s);
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
                buffer.remove(toMerge.id.toString());
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
            s.delete();
        }
    }


    public void mergeAndWrite(ArrayList<SSTable> filesToMerge,int level) throws IOException {
        //filesToMerge need to add to list by time order. ie index 0 is newest value
        HashMap<Integer,SSTable> mergedTuples=new HashMap<>();
        for (int i=filesToMerge.size()-1;i>=0;--i){
            SSTable ss=filesToMerge.get(i);
            String[] getssLevel = ss.loc.split("/");
            String sslevel = getssLevel[getssLevel.length - 2];
            String number = sslevel.substring(5);
            MemTable temp=loadOrGetMemtable(ss, Integer.parseInt(number));
            for (Integer integer : temp.tuples.keySet()) {
                mergedTuples.put(integer,ss);
            }
        }
        ArrayList<Integer> keys=new ArrayList<Integer>(mergedTuples.keySet());
        Collections.sort(keys);
        ArrayList<Record> buffer=new ArrayList<>();
        SSTable start=new SSTable(),end=new SSTable();
        start.begin=keys.get(0);
        for (Integer k:keys){
            if (start.begin==-1){
                start.begin=k;
            }
            end.begin=k;
            SSTable recordLoc=mergedTuples.get(k);
            String[] getLevel = recordLoc.loc.split("/");
            String mylevel = getLevel[getLevel.length - 2];
            String number = mylevel.substring(5);
            MemTable source=loadOrGetMemtable(recordLoc, Integer.parseInt(number));
            Record r=source.read(k);
            if (r.Phone.equals("delete")&&level== SStables.size()){
                continue;
            }
            buffer.add(source.read(k));
            if (buffer.size()==maxSize||(level!=1&&SStables.get(level-2).subSet(start,end).size()>10)){
                String filename=randomFileName();
                String loc=fileBaseDir+"/level"+level+"/"+filename;
                SStables.get(level-1).add(new SSTable(loc,buffer,filename));
                level_size.set(level-1,level_size.get(level-1)+maxSize);
                buffer=new ArrayList<>();
                start.begin=-1;
            }
        }
        if (!buffer.isEmpty()){
            String filename=randomFileName();
            String loc=fileBaseDir+"/level"+level+"/"+filename;
            SStables.get(level-1).add(new SSTable(loc,buffer,filename));
            level_size.set(level-1,level_size.get(level-1)+buffer.size());
        }
    }

    public void tableInitialization(String fileBaseDir){

        File table = new File(fileBaseDir);
        if(table.list().length == 0){
            return;
        }
        ArrayList<String> level0paths = getFile(fileBaseDir + "/level0");
        level_size.add(0,0);
        for(int i = 0; i < level0paths.size(); i++){
            SSTable temp = new SSTable();
            temp.loc = level0paths.get(i);
            String[] splits = level0paths.get(i).split("/");
            temp.id = splits[splits.length - 1];
            level0.add(temp);
            Integer value = level_size.get(0);
            value = value + 1;
            level_size.set(0, value);
        }

        for(int j = 1; j < getDir(fileBaseDir).size(); j++){
            level_size.add(j, 0);
            TreeSet<SSTable> level = new TreeSet<SSTable>(new Comparator<SSTable>() {
                @Override
                public int compare(SSTable o1, SSTable o2) {
                    return (o1.loc == o2.loc) ? 0 : 1;
                }
            });
            ArrayList<String> levelpaths = getFile(fileBaseDir + "/level" + j);
            for(int i = 0; i < levelpaths.size(); i++){
                SSTable temp = new SSTable();
                temp.loc = levelpaths.get(i);
                String[] splits = levelpaths.get(i).split("/");
                temp.id = splits[splits.length - 1];
                level.add(temp);
                Integer value = level_size.get(j);
                value = value + 1;
                level_size.set(j, value);
            }
            SStables.add(level);
        }

//TODO: level 1 - more initialization
    }

    public ArrayList<String> getFile(String path){
        ArrayList<String> result = new ArrayList<String>();
        File file = new File(path);
        File[] array = file.listFiles();
        if(array == null) return null;
        for(int i = 0; i < array.length; i++){
            if(array[i].isFile()){
                result.add(array[i].getPath());
            }
        }
        return result;
    }

    public ArrayList<String> getDir(String path){
        ArrayList<String> result = new ArrayList<String>();
        File file = new File(path);
        File[] array = file.listFiles();
        for(int i = 0; i < array.length; i++){
            if(array[i].isDirectory()){
                result.add(array[i].getName());
            }
        }
        return result;
    }

    public static String getDate(){
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        String currentTime = dateFormat.format(now);
        return currentTime;
    }

    public static void logWriter(String logContext, String logPath) throws IOException {
        FileWriter fw = new FileWriter(logPath, true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(logContext);
        bw.newLine();
        bw.close();
        fw.close();
    }



    class SSTable implements Comparable<SSTable>{
        String loc;
        int begin;
        int end;
        int size;
        String id;
        //Since SSTable is immutable, it writes the file when constructing it.
        public SSTable(String loc, ArrayList<Record> tuples, String uuid){
            this.loc=loc;
            begin=tuples.get(0).ID;
            end=tuples.get(tuples.size()-1).ID;
            size=tuples.size();
            id=uuid;
            try {
                BufferedWriter w=new BufferedWriter(new FileWriter(loc));
                w.write(uuid);w.write('\n');
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
        public void delete(){
            File f=new File(loc);
            if (!f.delete()){
                System.out.println("delete "+loc+" failed");
            }
        }
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
