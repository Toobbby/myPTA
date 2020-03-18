import java.io.*;
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
        tables.put("X", new Table("X"));
        tables.put("Y", new Table("Y"));
        // A global Buffer
        buffer = new Buffer(bufferSize);

        // read script
        readScript();

    }

    public static void readScript() throws Exception {
        String pathname = "/Users/fguo/Documents/Github/myPTA/src/script.txt";
        try(FileReader reader = new FileReader(pathname);
            BufferedReader br = new BufferedReader(reader)){
            String line;
            while((line = br.readLine()) != null){
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
            tables.put(tableName, new Table(tableName));
            File dir = new File("/Users/fguo/Documents/Github/myPTA/" + tableName);
            dir.mkdirs();
            System.out.println("The table " + tableName + " does not exist, it is created.");
        }
        Table t = tables.get(tableName);
        Record r = recordGenerator(tableName, recordValue);
        buffer.writeRecordInPage(t, r);
        System.out.println("Write: " + r.toString() +" to " + tableName + " successfully!");
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
                for (Record matchedRecord : matchedRecords) {
                    System.out.println("MRead: " + matchedRecord.toString());
                }
            }
        }
    }

    private static void delete(String tableName){
        tables.remove(tableName);
        deleteDir(new File("/Users/fguo/Documents/Github/myPTA/" + tableName));
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
