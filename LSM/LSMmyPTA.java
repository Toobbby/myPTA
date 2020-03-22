package LSM;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class LSMmyPTA {
    // read arguments to assign these parameters
    static int sstableSize;  //how many records each SSTable can hold (tunable)
    static int bufferSize;   //how many SSTables could stay in buffer concurrently, do NOT include Memtable (tunable)
    static HashMap<String, LSMTree> tables;
    static LSMBuffer buffer;
    public static void main(String[] args) throws Exception {
        //run with 2 args 1.sstableSize 2.bufferSize
        sstableSize=Integer.parseInt(args[0]);
        bufferSize=Integer.parseInt(args[1]);
        // A hashMap to store tables
        tables = new HashMap<>();
//        tables.put("X", new Table("X"));
//        tables.put("Y", new Table("Y"));
        // A global Buffer
        buffer = new LSMBuffer(bufferSize);

        // read script
        readScript();
        for (LSMTree lsmtree:tables.values()){
            //lsmtree.deleteTable();
        }
    }

    public static void readScript() throws Exception {
        String pathname = "./src/script.txt";
        try(FileReader reader = new FileReader(pathname);
            BufferedReader br = new BufferedReader(reader)){
            String line;
            while((line = br.readLine()) != null){
                System.out.println(buffer.keySet());
                String[] keywords = operationAnalyzer(line);
                switch (keywords[0]) {
                    case "R":
                        read(keywords[1], Integer.parseInt(keywords[2]));
                        break;
                    case "W":
                        write(keywords[1], keywords[2]);
                        break;
                    case "M":
                        showUserWithAreaCode(keywords[1], keywords[2]);
                        break;
                    case "E":
                        erase(keywords[1], Integer.parseInt(keywords[2]));
                        break;
                    default:
                        delete(keywords[1]);
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String[] operationAnalyzer(String line){
        return line.split(" ", 3);
    }

    public static Record recordGenerator(String tableName, String recordString) throws Exception {
        String[] attributes = recordString.substring(1,recordString.length() - 1).split(", ",3);
        return new Record(Integer.parseInt(attributes[0]), attributes[1], attributes[2], tableName);
    }

    // Retrieve the record with ID=val in table. If table does not exist, the read is aborted.
    private static void read(String tableName, int val) throws Exception {

        if(!tables.containsKey(tableName)){
            System.out.println("The table does not exist, the read is aborted.");
        }
        else{
            LSMTree t = tables.get(tableName);
            Record r = t.readID(val);
            if(r == null){
                System.out.println("The table " + tableName + " doesn't have record with ID = " + val);
            }
            else{
                System.out.println("Read: " + r.toString());
            }
        }

    }
    public static void erase(String tableName, int val) throws Exception{
        if(!tables.containsKey(tableName)){
            System.out.println("The table does not exist, the erase is aborted.");
        }
        else{
            LSMTree t = tables.get(tableName);
            t.deleteRecord(val);
        }
    }
    private static void write(String tableName, String recordValue) throws Exception {
        if(!tables.containsKey(tableName)){
            File dir = new File("./" + tableName+"/");
            dir.mkdirs();
            tables.put(tableName, new LSMTree(tableName,sstableSize,"./test/"+tableName,buffer));  //TODO: pass parameters to LSMTree to create new table
            //not add second memtable for now
            System.out.println("The table " + tableName + " does not exist, it is created.");
        }
        LSMTree t = tables.get(tableName);
        Record r=new Record(recordValue);
        t.write(r);
        System.out.println("Write: " + r.toString() +" to " + tableName + " successfully!");
    }

    private static void showUserWithAreaCode(String tableName, String areaCode) {
        if (!tables.containsKey(tableName)) {
            System.out.println("The table does not exist, show user with area code is aborted.");
        } else {
            LSMTree t = tables.get(tableName);
            ArrayList<Record> matchedRecords = t.readAreaCode(areaCode);
            if (matchedRecords == null) {
                System.out.println("The table " + tableName + " doesn't have record with area code = " + areaCode);
            } else {
                for (Record matchedRecord : matchedRecords) {
                    System.out.println("MRead: " + matchedRecord.toString());
                }
            }
        }
    }

    private static void delete(String tableName){
        LSMTree t=tables.get(tableName);
        t.deleteTable();
        tables.remove(tableName);
        System.out.println("Table " + tableName + " has been dropped!");
    }




}
