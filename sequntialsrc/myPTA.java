import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class myPTA {
    // TODO: read arguments to assign these parameters
    static int page_size;
    static int bufferSize;
    static HashMap<String, Table> tables;
    static Buffer buffer;
    public static void main(String[] args) throws Exception {
        page_size = Integer.parseInt(args[0]);
        bufferSize = Integer.parseInt(args[1]);
        //logging
        String currentTime = getDate();
        String logPath = "./Logging/" + "log" + currentTime + ".txt";
        // A hashMap to store tables
        tables = new HashMap<>();
        File root = new File("./");
        File[] tableList = root.listFiles();
        for (int i = 0; i < tableList.length; i++){  //initialize all existing tables
            String[] temp = tableList[i].toString().split("/");
            if(temp[temp.length - 1].length() == 1){
                tables.put(temp[temp.length - 1], new Table(temp[temp.length - 1], page_size));
            }
        }
//        tables.put("X", new Table("X"));
//        tables.put("Y", new Table("Y"));

        // A global Buffer
        buffer = new Buffer(bufferSize, logPath, page_size);
        // read script
//        String pathname = "./src/script.txt";
        String pathname = "./script.txt";
        readScript(pathname, logPath);

    }

    public static void readScript(String pathname, String logPath) throws Exception {
        try(FileReader reader = new FileReader(pathname);
            BufferedReader br = new BufferedReader(reader)){
            String line;
            long startTime = System.currentTimeMillis();
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
                    case "E":
                        erase(keywords[1], Integer.parseInt(keywords[2]), logPath);
                        break;
                    case "D":
                        delete(keywords[1], logPath);
                        break;
                    default:
                        System.out.println("invalid operation");
                        break;
                }
            }
            long endTime = System.currentTimeMillis();
            System.out.println(endTime - startTime);
            logWriter("Runtime: " + (endTime - startTime), logPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void erase(String tableName, int val, String logPath) throws IOException {
        if(!tables.containsKey(tableName)){
            System.out.println("The table does not exist, the erase is aborted.");
            logWriter("The table does not exist, the erase is aborted.", logPath);
        }
        else{
            Table t = tables.get(tableName);
            boolean flag = buffer.eraseRecord(t, tableName, val);
            if(flag){
                System.out.println("Erased: " + tableName + " " + val);
                logWriter("Erased: " + tableName + " " + val, logPath);
            }
            else{
                System.out.println("There is no record with ID = " + val + " in Table " + tableName);
                logWriter("There is no record with ID = " + val + " in Table " + tableName, logPath);
            }
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
            Record r = buffer.readRecord(t, tableName, val);
            if(r == null){
                System.out.println("The table " + tableName + " doesn't have record with ID = " + val);
                logWriter("The table " + tableName + " doesn't have record with ID = " + val, logPath);
            }
            else{
                System.out.println("Read: " + r.toString());
                logWriter("Read: " + r.toString(), logPath);
            }
        }

    }

    private static void write(String tableName, String recordValue, String logPath) throws Exception {
        if(!tables.containsKey(tableName)){
            tables.put(tableName, new Table(tableName, page_size));
            File dir = new File("./" + tableName);
            dir.mkdirs();
            Page p = new Page(page_size, new ArrayList<Record>());
            logWriter("Create " + "T-" + tableName + " P-0", logPath);
            buffer.put(tableName + "0", p);
            logWriter("Swap in  " + "T-" + tableName + " P-0", logPath);
            System.out.println("The table " + tableName + " does not exist, it is created.");
        }
        Table t = tables.get(tableName);
        Record r = recordGenerator(tableName, recordValue);
        buffer.writeRecordInPage(t, r);
        System.out.println("Write: " + r.toString() +" to " + tableName + " successfully!");
    }

    private static void showUserWithAreaCode(String tableName, int areaCode, String logPath) throws IOException {
        if (!tables.containsKey(tableName)) {
            System.out.println("The table does not exist, show user with area code is aborted.");
            logWriter("The table does not exist, show user with area code is aborted.", logPath);
        } else {
            Table t = tables.get(tableName);
            ArrayList<Record> matchedRecords = buffer.readRecordWithAreaCode(t, tableName, areaCode);
            if (matchedRecords.size() == 0) {
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
