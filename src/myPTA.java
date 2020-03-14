import java.util.ArrayList;
import java.util.HashMap;

public class myPTA {
    // TODO: read arguments to assign these parameters
    static int page_size = 2048;
    static int buffersize = 3;
    static HashMap<String, Table> tables;
    static Buffer buffer;
    public static void main(String[] args) {
        // A hashmap to store tables
        tables = new HashMap<>();
        tables.put("X", new Table());
        tables.put("Y", new Table());
        // A global Buffer
        buffer = new Buffer(buffersize);

        // TODO: read the script
        // R table val    read
        String tablename = "X";
        int val = 1;
        if(!tables.containsKey(tablename)){
            System.out.println("The table does not exist, the read is aborted.");
        }
        else{
            Table t = tables.get(tablename);
            Record r = read(t, tablename, val);
            if(r == null){
                System.out.println("The table " + tablename + " doesn't have record with ID = " + val);
            }
            else{
                System.out.println("Read: " + r.toString());
            }
        }

        // TODO: other operations

    }

    // Retrieve the record with ID=val in table. If table does not exist, the read is aborted.
    private static Record read(Table t, String tablename, int val) {
        ArrayList<Page> pages = t.pages;
        for (int i = 0; i < pages.size(); i++) {
            Page p = buffer.readPage(tablename + i, val);
        }

        return null;
    }

}
