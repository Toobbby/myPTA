import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class myPTA {
    // TODO: read arguments to assign these parameters
    static int page_size = 2048;
    static int bufferSize = 3;
    static HashMap<String, Table> tables;
    static Buffer buffer;
    public static void main(String[] args) throws Exception {
        // A hashMap to store tables
        tables = new HashMap<>();
        tables.put("X", new Table());
        tables.put("Y", new Table());
        // A global Buffer
        buffer = new Buffer(bufferSize);

        // read script
        readScript();

    }

    public static void readScript() throws Exception {
        String pathname = "script.txt";
        try(FileReader reader = new FileReader(pathname);
            BufferedReader br = new BufferedReader(reader)){
            String line;
            while((line = br.readLine()) != null){
                String[] keywords = operationAnalyzer(line);
                if(keywords[0] == "R") read(keywords[1], Integer.parseInt(keywords[2]));
                else if(keywords[0] == "W") write(keywords[1], keywords[2]);
                else if(keywords[0] == "M") showUserWithAreaCode(keywords[1], Integer.parseInt(keywords[2]));
                else delete(keywords[1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String[] operationAnalyzer(String line){
        String[] keywords = line.split(" ", 3);
        return keywords;
    }

    public static Record recordGenerator(String tableName, String recordString) throws Exception {
        String[] attributes = recordString.substring(1,recordString.length() - 1).split(", ",3);
        Record r = new Record(Integer.parseInt(attributes[0]), attributes[1], attributes[2], tableName);
        return r;
    }

    // Retrieve the record with ID=val in table. If table does not exist, the read is aborted.
    private static void read(String tableName, int val) {

        if(!tables.containsKey(tableName)){
            System.out.println("The table does not exist, the read is aborted.");
        }
        else{
            Table t = tables.get(tableName);
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
            System.out.println("The table does not exist, the write is aborted.");
        }
        else{
            Table t = tables.get(tableName);
            Record r = recordGenerator(tableName, recordValue);
            buffer.writeRecordInTable(t, r);
            System.out.println("Write: " + r.toString() + "successfully!");
        }

    }

    private static void showUserWithAreaCode(String tableName, int areaCode) {
        if (!tables.containsKey(tableName)) {
            System.out.println("The table does not exist, show user with area code is aborted.");
        } else {
            Table t = tables.get(tableName);
            ArrayList<Record> matchedRecords = buffer.readRecordWithAreaCode(t, tableName, areaCode);
            if (matchedRecords == null) {
                System.out.println("The table " + tableName + " doesn't have record with area code = " + areaCode);
            } else {
                for (int i = 0; i < matchedRecords.size(); i++) {
                    System.out.println("Matched: " + matchedRecords.get(i).toString());
                }
            }
        }
    }

    private static void delete(String tableName){
        tables.remove(tableName);
        System.out.println("Table" + tableName + "has been dropped!");
    }

}
