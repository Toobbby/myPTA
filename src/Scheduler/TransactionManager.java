package Scheduler;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import MyPTA.Command;
import LSM.Record;

public class TransactionManager {
    private final File file;
    private final BufferedReader fileReader;    //The file reader
    private boolean blockbit;                    // check if it is blocked
    private Command command;                    //The command of the last read line
    private String tableName;                    //The name of the table specified by the last read line
    private String value;
    private String fullString;                   // The full String of the last read line.  The type depends on what command is being used

    private int lineNumber;                        //The current line number in the script
    private boolean streamIsClosed;                //Whether or not the stream has been closed due to reaching the end of file or not

    private boolean error;
    private boolean TransactionType;
    public int TID;
    private ArrayList<Operation> OPBuffer;
    private ArrayList<HashMap<Integer, Record>> tempData; // store the updates
    private ArrayList<String> tempTableIndex; //works to find the index of the table in the tempdata
    public long startTimestamp;
    public HashMap<String,ArrayList<Integer>> beforeImageIds = new HashMap<>();

    public String beforeImageLocation = "./beforeImage/";
    public HashSet<String> deleted=new HashSet<>();
    public int RCount = 0;
    public int WCount = 0;
    public int MCount = 0;
    public int ECount = 0;
    public int DCount = 0;

    /**
     * The constructor.
     * Takes in a file name representing the location to find the script.
     *
     * @param filename The String representing the file name.
     * @throws FileNotFoundException if the file does not exist.
     */
    public TransactionManager(String filename) throws FileNotFoundException {
        this(new File(filename));
    }

    /**
     * The constructor.
     * Takes in a File representing the script.
     *
     * @param file The File to read from.
     * @throws FileNotFoundException if the file does not exist.
     */
    public TransactionManager(File file) throws FileNotFoundException {
        this.file = file;
        this.fileReader = new BufferedReader(new FileReader(file));
        this.lineNumber = 0;
        this.streamIsClosed = false;
        this.error = false;
        this.TransactionType = false;
        this.OPBuffer = new ArrayList<Operation>();
        this.tempData = new ArrayList<HashMap<Integer, Record>>();
        this.tempTableIndex = new ArrayList<String>();
        this.blockbit = false;
    }

    /**
     * Loads the next line of the script.
     *
     * @throws IOException if the end of the file has been reached or if the script has a false format line.
     */
    public void loadNextLine() throws IOException {
        if (this.blockbit) {
            return;
        }
        this.error = false;
        if (this.streamIsClosed) {
            this.error = true;
            throw new EOFException("The end of the file " + this.file + " has been reached.");
        }

        String line = this.fileReader.readLine();
        if (line == null) {
            this.fullString = "";
            this.command = null;
            this.tableName = null;
            this.value = null;
            this.streamIsClosed = true;
            this.fileReader.close();
            return;
        }

        this.lineNumber++;

        String[] split = line.split(" ", 3);
        if (split.length == 1) {
            if (split[0].equals("C")) {
                this.command = Command.COMMIT;
            } else if (split[0].equals("A")) {
                this.command = Command.ABORT;
            } else {
                this.error = true;
                throw new IOException("File is not in the correct format at line " + this.lineNumber + ".");
            }
        } else if (split.length == 2) {
            if (!split[0].equals("D") && !split[0].equals("B")) {
                this.error = true;
                throw new IOException("File is not in the correct format at line " + this.lineNumber + ".");
            } else if (split[0].equals("D")) {
                this.command = Command.DELETE_TABLE;
                this.tableName = split[1];
                this.value = null;
                DCount++;
            } else if (split[0].equals("B")) {
                this.command = Command.BEGIN;
                this.value = split[1];
                TransactionType = this.value.equals("1");
                startTimestamp = System.currentTimeMillis();
            }
        } else {
            switch (split[0]) {
                case "R":
                    this.command = Command.READ_ID;
                    this.value = split[2];
                    RCount++;
                    break;
                case "M":
                    this.command = Command.READ_AREA_CODE;
                    this.value = split[2];
                    MCount++;
                    break;
                case "W":
                    this.command = Command.WRITE;
                    this.value = split[2];
                    WCount++;
                    break;
                case "E":
                    this.command = Command.ERASE;
                    this.value = split[2];
                    ECount++;
                    break;
                default:
                    System.out.println(split[0]);
                    this.error = true;
                    throw new IOException("File is not in the correct format " + this.lineNumber + ".");
            }
            this.tableName = split[1];
        }
        this.fullString = line;
    }

    /**
     * Get a copy of OPBuffer
     *
     * @return
     */
    public ArrayList<Operation> getOPBuffer() {
        @SuppressWarnings("unchecked")
        ArrayList<Operation> temp = (ArrayList<Operation>) OPBuffer.clone();
        return temp;

    }

    /**
     * Store the W/E operation to tempData
     *
     * @param t         the type of the operation
     * @param id        the ID of the operation
     * @param r         the record to be inserted/updated
     * @param tableName the name of the table referenced
     */
    public void writeToTempData(Type t, int id, Record r, String tableName) {
        for (int i = 0; i < tempData.size(); i++) {
            if (tempTableIndex.get(i).equals(tableName)) {
                if (t == Type.E) {
                    tempData.get(i).put(id, null);
                    return;
                } else if (t == Type.W) {
                    tempData.get(i).put(id, r);
                    return;
                }
            }
        }
        if (t == Type.E) return;
        tempTableIndex.add(tableName);
        HashMap<Integer, Record> tempMap = new HashMap<Integer, Record>();
        tempMap.put(id, r);
        tempData.add(tempMap);
    }

    /**
     * Check if tempData has the operation associated with a given ID
     *
     * @param id
     * @param tableName the name of the table referenced
     * @return
     */
    public boolean ifIdInTempData(int id, String tableName) {
        for (int i = 0; i < tempData.size(); i++) {
            if (tempTableIndex.get(i).equals(tableName)) {
                return tempData.get(i).containsKey(id);
            }
        }
        return false;
    }

    /**
     * Read from tempData with a given ID and get the corresponding record
     *
     * @param id        the ID to search in tempData
     * @param tableName the name of the table referenced
     * @return
     */
    public Record ReadIdFromTempData(int id, String tableName) {
        if (deleted.contains(tableName))
            return null;
        for (int i = 0; i < tempData.size(); i++) {
            if (tempTableIndex.get(i).equals(tableName)) {
                return tempData.get(i).get(id);
            }
        }
        return null;
    }

    /** Read from tempData with a given areacode and get the corresponding records
     * @param area the areacode to search in tempData
     * @param tableName the name of the table referenced
     * @return
     */
    public ArrayList<Record> ReadAreaFromTempData(String area, String tableName) {
        if (deleted.contains(tableName))
            return null;
        ArrayList<Record> tempReturnResult = new ArrayList<Record>();
        for (int i = 0; i < tempData.size(); i++) {
            if (tempTableIndex.get(i).equals(tableName)) {
                //if(tempData.get(i).entrySet() == null) return tempReturnResult;
                for (Map.Entry<Integer, Record> entry : tempData.get(i).entrySet()) {
                    if (entry.getValue() != null && entry.getValue().getAreaCode().equals(area)) {
                        tempReturnResult.add(entry.getValue());
                    }
                }
            }
        }
        return tempReturnResult;
    }

    /**
     * Check from OPBuffer with a given tableName if the table has been deleted
     * @param tableName the name of the table referenced
     * @return
     */
    public boolean ifDeletetable(String tableName) {
        return deleted.contains(tableName);
    }

    /**
     * When the transaction is aborted, clears the states
     */
    public void Abort() {
        TransactionType = false;
        OPBuffer.clear();
        tempData.clear();
        tempTableIndex.clear();
    }

    /**
     * When a transaction is aborted by DeadLock, skip the following operations in this transaction until Commit/Abort
     * @throws IOException
     */
    public void DeadLockAbort() throws IOException {
        this.blockbit = false;
        while (!(this.getCommand().equals(Command.ABORT) || this.getCommand().equals(Command.COMMIT)))
            loadNextLine();
        Abort();
    }

    public void commit() {
        TransactionType = false;
        OPBuffer.clear();
        tempData.clear();
        tempTableIndex.clear();
    }

    public String getFullString() {
        return this.fullString;
    }

    /**
     * Retrieves the Command at the loaded line.
     *
     * @return Returns the Command that was issued by the script at the loaded line.
     */
    public Command getCommand() {
        return this.command;
    }

    /**
     * Retrieves the table name at the loaded line.
     *
     * @return Returns the String representing the table name that was issued by the script at the loaded line.
     */
    public String getTableName() {
        return this.tableName;
    }

    /**
     * Retrieves the record at the loaded line.
     *
     * @return Returns the Record representing the value that was issued by the script at the loaded line.
     */
    public String getValue() {
        return this.value;
    }

    /**
     * Retrieves the line number that will be read next if loadNextLine() is called.
     *
     * @return Returns the integer representing the current position of the cursor in the file.
     */
    public int getLineNumber() {
        return this.lineNumber;
    }

    //Added by Jeongmin
    public boolean streamIsClosed() {
        return this.streamIsClosed;
    }

    public boolean isError() {
        return this.error;
    }

    public String getFileName() {
        return this.file.getName();
    }

    public Operation getOperation() {

        Operation temp = new Operation(command, tableName, value, fullString, lineNumber, TransactionType, TID);

        return temp;
    }

    /**
     * If it is Serializable isolation level, add the operation to OPBuffer
     * @return
     */
    public void addOP() {
        OPBuffer.add(new Operation(command, tableName, value, fullString, lineNumber, TransactionType, TID));
        if (command==Command.DELETE_TABLE){
            deleted.add(tableName);
        }
        if (command==Command.WRITE){
            deleted.remove(tableName);
        }
    }

    public void beforeImageWriter(String logContext) throws IOException {
        FileWriter fw = new FileWriter(beforeImageLocation + TID + ".txt", true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(logContext);
        bw.newLine();
        bw.close();
        fw.close();
    }


    public class Operation {
        private Command command;                    //The command of the last read line
        private String tableName;                    //The name of the table referenced by the last read line
        private String value;                        //The value of the last read line.  The type depends on what command is being used
        private String fullString;

        private int lineNumber;
        private boolean TransactionType;            //0: read committed 1: serializable:
        private final int TID;

        public Operation(Command command, String tableName, String value, String fullString, int lineNumber, boolean TransactionType, int TID) {
            this.command = command;
            this.tableName = tableName;
            this.value = value;
            this.fullString = fullString;
            this.lineNumber = lineNumber;
            this.TransactionType = TransactionType;
            this.TID = TID;
        }

        public Record getTinRecordFormat() {
            return new Record(this.value);
        }

        public String getFullString() {
            return this.fullString;
        }

        /**
         * Retrieves the Command at the loaded line.
         *
         * @return Returns the Command that was issued by the script at the loaded line.
         */
        public Command getCommand() {
            return this.command;
        }

        /**
         * Retrieves the table name at the loaded line.
         *
         * @return Returns the String representing the table name that was issued by the script at the loaded line.
         */
        public String getTableName() {
            return this.tableName;
        }

        /**
         * Retrieves the record at the loaded line.
         *
         * @return Returns the Record representing the value that was issued by the script at the loaded line.
         */
        public String getValue() {
            return this.value;
        }

        /**
         * Retrieves the line number that will be read next if loadNextLine() is called.
         *
         * @return Returns the integer representing the current position of the cursor in the file.
         */
        public int getLineNumber() {
            return this.lineNumber;
        }

        public boolean getTransactionType() {
            return this.TransactionType;
        }

        public int getTID() {
            return this.TID;
        }
    }

    public void block() {
        blockbit = true;
    }

    public void unblock() {
        blockbit = false;

    }
}
