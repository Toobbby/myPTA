package LSM;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import Scheduler.TransactionManager;

public class LSMmyPTA {
    // read arguments to assign these parameters
     int sstableSize;  //how many records each SSTable can hold (tunable)
     int bufferSize;   //how many SSTables could stay in buffer concurrently, do NOT include Memtable (tunable)
     HashMap<String, LSMTree> tables;
     LSMBuffer buffer;
     static String logPath;
    public LSMmyPTA(int _sstableSize, int _bufferSize) {
        //run with 2 args 1.sstableSize 2.bufferSize
        sstableSize=_sstableSize;
        bufferSize=_bufferSize;
        // A hashMap to store tables
        tables = new HashMap<>();
        String tableLoc = "./tables";
        deleteDir(tableLoc);  //phase 1, every time running a new script, clear all tables

        //logging
        String currentTime = getDate();
        logPath = "./Logging/" + "log" + currentTime +".txt";
        String pathname = "./script.txt";
        // A global Buffer
        buffer = new LSMBuffer(bufferSize);

        // read script
        //readScript(pathname, logPath);
    }


    private static String[] operationAnalyzer(String line){
        return line.split(" ", 3);
    }

    public static Record recordGenerator(String tableName, String recordString) throws Exception {
        return new Record(recordString);
    }

    // Retrieve the record with ID=val in table. If table does not exist, the read is aborted.
    public Record read(String tableName, int val) throws Exception {

        if(!tables.containsKey(tableName)){
            System.out.println("The table does not exist, the read is aborted.");
            logWriter("MemoryManager: The table does not exist.");
            return null;
        }
        else{
            LSMTree t = tables.get(tableName);
            Record r = t.readID(val);
            if(r == null){
                System.out.println("The table " + tableName + " doesn't have record with ID = " + val);
                logWriter("The table " + tableName + " doesn't have record with ID = " + val);
            }
            else{
                System.out.println("Read: " + r.toString());
                //logWriter("Read: " + r.toString());
            }
            return r;
        }

    }
    public boolean erase(String tableName, int val) throws Exception{
        if(!tables.containsKey(tableName)){
            System.out.println("The table does not exist, the erase is aborted.");
            logWriter("The table does not exist, the erase is aborted.");
            return false;
        }
        else{
            LSMTree t = tables.get(tableName);
            t.deleteRecord(val);
            logWriter("Erased: " + tableName + " " + val);
            return true;
        }
    }
    public boolean write(String tableName, String recordValue) throws Exception {
        if(!tables.containsKey(tableName)){
            File thisTable = new File("./tables/"+tableName);
            thisTable.mkdirs();
            tables.put(tableName, new LSMTree(tableName,sstableSize,"./tables/"+tableName,buffer, logPath));  //TODO: pass parameters to LSMTree to create new table

            //not add second memtable for now
            System.out.println("The table " + tableName + " does not exist, it is created.");
        }
        LSMTree t = tables.get(tableName);
        Record r=new Record(recordValue);
        t.write(r);
        System.out.println("Write: " + r.toString() +" to " + tableName + " successfully!");
        return true;
    }

    public ArrayList<Record> showUserWithAreaCode(String tableName, String areaCode) throws IOException {
        if (!tables.containsKey(tableName)) {
            System.out.println("The table does not exist, show user with area code is aborted.");
            //logWriter("The table "+tableName+" does not exist, show user with area code is aborted.");
            return new ArrayList<>();
        } else {
            LSMTree t = tables.get(tableName);
            ArrayList<Record> matchedRecords = t.readAreaCode(areaCode);
            if (matchedRecords.size() == 0) {
                System.out.println("The table " + tableName + " doesn't have record with area code = " + areaCode);
              //  logWriter("The table " + tableName + " doesn't have record with area code = " + areaCode);
            } else {
                for (Record matchedRecord : matchedRecords) {
                    System.out.println("MRead: " + matchedRecord.toString());
                }
            }
            return matchedRecords;
        }
    }

    private void delete(String tableName) throws IOException {
        LSMTree t=tables.get(tableName);
        t.deleteTable();
        tables.remove(tableName);
        System.out.println("Table " + tableName + " has been dropped!");
        logWriter("Deleted: " + tableName);
    }

    public  static String getDate(){
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        String currentTime = dateFormat.format(now);
        return currentTime;
    }

    public  static void logWriter(String logContext) throws IOException {
        FileWriter fw = new FileWriter(logPath, true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(logContext);
        bw.newLine();
        bw.close();
        fw.close();
    }

    public static boolean deleteDir(String path){
        File file = new File(path);
        if(!file.exists()){
            System.err.println("The dir are not exists!");
            return false;
        }

        String[] content = file.list();
        for(String name : content){
            File temp = new File(path, name);
            if(temp.isDirectory()){
                deleteDir(temp.getAbsolutePath());
                temp.delete();
            }else{
                if(!temp.delete()){
                    System.err.println("Failed to delete " + name);
                }
            }
        }
        return true;
    }

    public  boolean deleteTable(String tablename) {
        try {
            delete(tablename);
            return true;
        }catch (IOException e){
            return false;
        }
    }

    public boolean commitToTransaction(ArrayList<TransactionManager.Operation> opBuffer) {
        //if commit, then we write insert things into disk
        for(TransactionManager.Operation op : opBuffer)
        {
            switch(op.getCommand())
            {

                case DELETE_TABLE:
                    deleteTable(op.getTableName());
                    break;
                case WRITE:
                    if(!op.getTransactionType()){  //read committed: write to disk at commit time
                        try {
                            if(write(op.getTableName(), op.getValue())){
                                break;
                            } else {
                                return false;
                            }
                        } catch (Exception e) {
                            System.out.println(e);
                            return false;
                        }

                    }
                    break;
                case ERASE:
                    if(!op.getTransactionType()) {  //read committed: erase at commit time
                        try {
                            if (erase(op.getTableName(), Integer.parseInt(op.getValue()))) {
                                break;
                            } else {
                                return false;
                            }
                        } catch (Exception e) {
                            System.out.println(e);
                            return false;
                        }
                    }
                    break;

                case ABORT:
                case BEGIN:
                case COMMIT:
                case READ_AREA_CODE:
                case READ_ID:
                    //all not used
                    break;
                default:
                    throw new UnsupportedOperationException("Command not supported");
            }
        }


        return true;
    }
    public boolean flush(){
        for (LSMTree tree:tables.values()){
            try {
                tree.flushMemtable();
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }
}
