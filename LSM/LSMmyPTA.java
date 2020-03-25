package LSM;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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
        File root = new File("./tables");
        File[] tableList = root.listFiles();
        //logging
        String currentTime = getDate();
        String logPath = "./Logging/" + "log" + currentTime +".txt";
        String pathname = "./script.txt";
        // A global Buffer
        buffer = new LSMBuffer(bufferSize);
        for (int i = 0; i < tableList.length; i++){  //initialize all existing tables
            String[] temp = tableList[i].toString().split("/");
            if(temp[temp.length - 1].length() == 1){
                tables.put(temp[temp.length - 1], new LSMTree(temp[temp.length - 1], sstableSize,"./tables/" + temp[temp.length - 1], buffer, logPath));
            }
        }
//        tables.put("X", new Table("X"));
//        tables.put("Y", new Table("Y"));

        // read script
        readScript(pathname, logPath);
        for (LSMTree lsmtree:tables.values()){
            //lsmtree.deleteTable();
        }


    }

    public static void readScript(String pathname, String logPath) throws Exception {
        try(FileReader reader = new FileReader(pathname);
            BufferedReader br = new BufferedReader(reader)){
            String line;
            while((line = br.readLine()) != null){
                logWriter("", logPath);  //empty line to separate each command in log
                logWriter(line, logPath);
                //System.out.println(buffer.keySet());
                String[] keywords = operationAnalyzer(line);
                switch (keywords[0]) {
                    case "R":
                        read(keywords[1], Integer.parseInt(keywords[2]), logPath);
                        break;
                    case "W":
                        write(keywords[1], keywords[2], logPath);
                        break;
                    case "M":
                        showUserWithAreaCode(keywords[1], keywords[2], logPath);
                        break;
                    case "E":
                        erase(keywords[1], Integer.parseInt(keywords[2]), logPath);
                        break;
                    default:
                        delete(keywords[1], logPath);
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
    private static void read(String tableName, int val, String logPath) throws Exception {

        if(!tables.containsKey(tableName)){
            System.out.println("The table does not exist, the read is aborted.");
            logWriter("The table does not exist, the read is aborted.", logPath);
        }
        else{
            LSMTree t = tables.get(tableName);
            Record r = t.readID(val);
            if(r == null){
                System.out.println("The table " + tableName + " doesn't have record with ID = " + val);
                logWriter("The table " + tableName + " doesn't have record with ID = " + val, logPath);
            }
            else{
                System.out.println("Read: " + r.toString());
                logWriter("Read: " + r.toString() + val, logPath);
            }
        }

    }
    public static void erase(String tableName, int val, String logPath) throws Exception{
        if(!tables.containsKey(tableName)){
            System.out.println("The table does not exist, the erase is aborted.");
            logWriter("The table does not exist, the erase is aborted.", logPath);
        }
        else{
            LSMTree t = tables.get(tableName);
            t.deleteRecord(val);
            logWriter("Erased: " + tableName + " " + val, logPath);
        }
    }
    private static void write(String tableName, String recordValue, String logPath) throws Exception {
        if(!tables.containsKey(tableName)){
            tables.put(tableName, new LSMTree(tableName,sstableSize,"./tables/"+tableName,buffer, logPath));  //TODO: pass parameters to LSMTree to create new table
            File thisTable = new File("./tables/"+tableName);
            thisTable.mkdirs();
            //not add second memtable for now
            System.out.println("The table " + tableName + " does not exist, it is created.");
        }
        LSMTree t = tables.get(tableName);
        Record r=new Record(recordValue);
        t.write(r);
        System.out.println("Write: " + r.toString() +" to " + tableName + " successfully!");
    }

    private static void showUserWithAreaCode(String tableName, String areaCode, String logPath) throws IOException {
        if (!tables.containsKey(tableName)) {
            System.out.println("The table does not exist, show user with area code is aborted.");
        } else {
            LSMTree t = tables.get(tableName);
            ArrayList<Record> matchedRecords = t.readAreaCode(areaCode);
            System.out.println(matchedRecords);
            if (matchedRecords.size() == 0) {
                System.out.println("The table " + tableName + " doesn't have record with area code = " + areaCode);
                logWriter("The table " + tableName + " doesn't have record with area code = " + areaCode, logPath);
            } else {
                for (Record matchedRecord : matchedRecords) {
                    System.out.println("MRead: " + matchedRecord.toString());

                }
            }
        }
    }

    private static void delete(String tableName, String logPath) throws IOException {
        LSMTree t=tables.get(tableName);
        t.deleteTable();
        tables.remove(tableName);
        System.out.println("Table " + tableName + " has been dropped!");
        logWriter("Deleted: " + tableName, logPath);
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


}
