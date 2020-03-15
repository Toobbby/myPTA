import java.util.ArrayList;
import java.util.HashMap;

public class myPTA {
    // TODO: read arguments to assign these parameters
    static int page_size = 2048;
    static int bufferSize = 3;
    static HashMap<String, Table> tables;
    static Buffer buffer;
    public static void main(String[] args) {
        // A hashmap to store tables
        tables = new HashMap<>();
        tables.put("X", new Table());
        tables.put("Y", new Table());
        // A global Buffer
        buffer = new Buffer(bufferSize);

        // TODO: read the script
        // R table val    read
        String tableName = "X";
        int val = 1;
        if(!tables.containsKey(tableName)){
            System.out.println("The table does not exist, the read is aborted.");
        }
        else{
            Table t = tables.get(tableName);
            Record r = read(t, tableName, val);
            if(r == null){
                System.out.println("The table " + tableName + " doesn't have record with ID = " + val);
            }
            else{
                System.out.println("Read: " + r.toString());
            }
        }

        // TODO: other operations

    }

    // Retrieve the record with ID=val in table. If table does not exist, the read is aborted.
    private static Record read(Table t, String tableName, int val) {
        return buffer.readRecord(t, tableName, val);

    }

}
