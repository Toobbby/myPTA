import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class LSMmyPTA {
    // read arguments to assign these parameters
    static int sstableSize = 2;  //how many records each SSTable can hold (tunable)
    static int bufferSize = 3;   //how many SSTables could stay in buffer concurrently, do NOT include Memtable (tunable)
    static HashMap<String, LSMTree> tables;
    static LSMBuffer buffer;
    public static void main(String[] args) throws Exception {
        // A hashMap to store tables
        tables = new HashMap<>();
//        tables.put("X", new Table("X"));
//        tables.put("Y", new Table("Y"));
        // A global Buffer
        buffer = new LSMBuffer(bufferSize);

        // read script
        readScript();

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
                        showUserWithAreaCode(keywords[1], Integer.parseInt(keywords[2]));
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
    private static void read(String tableName, int val) {

        if(!tables.containsKey(tableName)){
            System.out.println("The table does not exist, the read is aborted.");
        }
        else{
            LSMTree t = tables.get(tableName);
            Record r = buffer.readRecord(t, tableName, val);
            if(r == null){
                System.out.println("The table " + tableName + " doesn't have record with ID = " + val);
            }
            else{
                System.out.println("Read: " + r.toString());
            }
        }

    }

    private static void write(String tableName, String recordValue) throws Exception {
        if(!tables.containsKey(tableName)){
            tables.put(tableName, new LSMTree(tableName));  //TODO: pass parameters to LSMTree to create new table
            File dir = new File("./" + tableName);
            dir.mkdirs();
            buffer.put(tableName + 1, new MemTable(tableName));
            buffer.put(tableName + 2, new MemTable(tableName));  //2 memtables, in order to support concurrent flash and write
            System.out.println("The table " + tableName + " does not exist, it is created.");
        }
        LSMTree t = tables.get(tableName);
        Record r = recordGenerator(tableName, recordValue);
        buffer.writeRecordInMemtable(t, r);
        System.out.println("Write: " + r.toString() +" to " + tableName + " successfully!");
    }

    private static void showUserWithAreaCode(String tableName, int areaCode) {
        if (!tables.containsKey(tableName)) {
            System.out.println("The table does not exist, show user with area code is aborted.");
        } else {
            LSMTree t = tables.get(tableName);
            ArrayList<Record> matchedRecords = buffer.readRecordWithAreaCode(t, tableName, areaCode);
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
        tables.remove(tableName);
        deleteDir(new File("./" + tableName));
        System.out.println("Table " + tableName + " has been dropped!");
    }

    public static void deleteDir(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles())
                deleteDir(f);
        }
        file.delete();
    }


}
