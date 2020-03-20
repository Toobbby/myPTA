import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class myPTA {
    // TODO: read arguments to assign these parameters
    static int page_size = 2;
    static int bufferSize = 3;
    static HashMap<String, Table> tables;
    static Buffer buffer;
    public static void main(String[] args) throws Exception {
        //logging
        String currentTime = getDate();
        String logPath = "./Logging/" + "log" + currentTime;
        // A hashMap to store tables
        tables = new HashMap<>();
//        tables.put("X", new Table("X"));
//        tables.put("Y", new Table("Y"));
        // A global Buffer
        buffer = new Buffer(bufferSize, logPath);
        // read script
        String pathname = "./src/script.txt";
        readScript(pathname, logPath);

    }

    public static void readScript(String pathname, String logPath) throws Exception {
        try(FileReader reader = new FileReader(pathname);
            BufferedReader br = new BufferedReader(reader)){
            String line;
            while((line = br.readLine()) != null){
                logWriter("", logPath);  //empty line to separate each command in log
                logWriter(line, logPath);
               // System.out.println(buffer.keySet());
                String[] keywords = operationAnalyzer(line);
                switch (keywords[0]) {
                    case "R":
                        read(keywords[1], Integer.parseInt(keywords[2]), logPath);
                        break;
                    case "W":
                        write(keywords[1], keywords[2], logPath);
                        break;
                    case "M":
                        showUserWithAreaCode(keywords[1], Integer.parseInt(keywords[2]), logPath);
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
    private static void read(String tableName, int val, String logPath) throws IOException {

        if(!tables.containsKey(tableName)){
            System.out.println("The table does not exist, the read is aborted.");
            logWriter("The table does not exist, the read is aborted.", logPath);
        }
        else{
            Table t = tables.get(tableName);
            Record r = buffer.readRecord(t, tableName, val, logPath);
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

    private static void write(String tableName, String recordValue, String logPath) throws Exception {
        if(!tables.containsKey(tableName)){
            tables.put(tableName, new Table(tableName));
            File dir = new File("./" + tableName);
            dir.mkdirs();
            Page p = new Page(new ArrayList<Record>());
            logWriter("Create " + "T-" + tableName + " P-0", logPath);
            buffer.put(tableName + "0", p);
            logWriter("Swap in  " + "T-" + tableName + " P-0", logPath);
            System.out.println("The table " + tableName + " does not exist, it is created.");
        }
        Table t = tables.get(tableName);
        Record r = recordGenerator(tableName, recordValue);
        buffer.writeRecordInPage(t, r, logPath);
        System.out.println("Write: " + r.toString() +" to " + tableName + " successfully!");
    }

    private static void showUserWithAreaCode(String tableName, int areaCode, String logPath) throws IOException {
        if (!tables.containsKey(tableName)) {
            System.out.println("The table does not exist, show user with area code is aborted.");
            logWriter("The table does not exist, show user with area code is aborted.", logPath);
        } else {
            Table t = tables.get(tableName);
            ArrayList<Record> matchedRecords = buffer.readRecordWithAreaCode(t, tableName, areaCode, logPath);
            if (matchedRecords == null) {
                System.out.println("The table " + tableName + " doesn't have record with area code = " + areaCode);
                logWriter("The table " + tableName + " doesn't have record with area code = " + areaCode, logPath);
            } else {
                for (Record matchedRecord : matchedRecords) {
                    System.out.println("MRead: " + matchedRecord.toString());
                    //logWriter("MRead: " + matchedRecord.toString(), logPath);
                }
            }
        }
    }

    private static void delete(String tableName, String logPath) throws IOException {
        tables.remove(tableName);
        deleteDir(new File("./" + tableName));
        System.out.println("Table " + tableName + " has been dropped!");
        logWriter("Deleted: " + tableName, logPath);
    }

    public static void deleteDir(File file) {
        if (file.isDirectory()) {
            for (File f : file.listFiles())
                deleteDir(f);
        }
        file.delete();
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
