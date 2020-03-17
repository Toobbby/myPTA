import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.*;

// Key: X1 (tableName + page_No)
// Value: Page
public class Buffer<K, V> extends LinkedHashMap<String, Page> {
    private int buffer_size;

    Buffer(int _buffer_size) {
        super(16, 0.75f, true);
        this.buffer_size = _buffer_size;
    }

//    Page readPage(String table_pageNo, int val) {
//        // first check if this page has already been in the buffer
//        // if it is, just return this page
//        if (this.containsKey(table_pageNo)) {
//            return this.get(table_pageNo);
//        }
//
//        // if not exist, then need to load this page from disk to buffer
//        String tableName = table_pageNo.substring(0, 1);
//        int page_No = Integer.parseInt(table_pageNo.substring(1, 2));
//        // TODO: use Page.readFile(String tableName, int page) to read from file.
//        Page p = Page.readFile(tableName, page_No);
//        return p;
//    }


    void writePage(String table_pageNo) { // write page in buffer back to the disk
        String tableName = table_pageNo.substring(0, 1);
        int pageNo = Integer.parseInt(table_pageNo.substring(1));  //need to consider page number has more than one digit
        Page p = this.get(table_pageNo);
        myPTA.tables.get(tableName).refreshPage(pageNo, p);
    }

    // LRU algorithm
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Page> eldest) {
        if (size() >= buffer_size) {
            // write eldest page in buffer back to the disk
            writePage(eldest.getKey());
            return true;
        }
        return false;
    }

    public Record readRecord(Table t, String tableName, int val) {
        ArrayList<Page> pages = t.pages;
        for (int i = 0; i < pages.size(); i++) {
            Record r = checkRecordInPage(tableName + i, val);
            if(r != null) return r;
        }
        return null;
    }

    public ArrayList<Record> readRecordWithAreaCode(Table t, String tableName, int areaCode) {
        ArrayList<Record> results = new ArrayList<Record>();
        ArrayList<Page> pages = t.pages;
        for (int i = 0; i < pages.size(); i++) {
            results.addAll(checkRecordInPageWithAreaCode(tableName + i, areaCode));
        }
        return results;
    }

    public Record checkRecordInPage(String table_pageNo, int val){
        // first check if this page has already been in the buffer
        // if it is in buffer, find the record directly
        if (this.containsKey(table_pageNo)) {
            return findRecord(this.get(table_pageNo), val);
        }
        else{   // load page, then find the record
            loadPage(table_pageNo);
            return findRecord(this.get(table_pageNo), val);
        }
    }

    public ArrayList<Record> checkRecordInPageWithAreaCode(String table_pageNo, int areaCode){
        // load page, then find the record ONE BY ONE
        loadPage(table_pageNo);
        return findRecordWithAreaCode(this.get(table_pageNo), areaCode);
    }


    public Record findRecord(Page p, int val){     // find record with ID=val in a particular page
        Record[] records = p.getRecords();
        if(records == null){
            return null;
        }

        for(Record r: records){
            if(r.getID() == val){
                return r;
            }
        }
        return null;
    }

    public ArrayList<Record> findRecordWithAreaCode(Page p, int areaCode){     // find record with areacode in a particular page
        ArrayList<Record> results = new ArrayList<Record>();
        Record[] records = p.getRecords();
        if(records == null){
            return null;
        }

        for(Record r: records){
            if(Integer.parseInt(r.getPhone().substring(0,3))== areaCode){
                results.add(r);
            }
        }
        return results;
    }

    public void writeRecordInTable(Table t, Record r){
        ArrayList<Page> pages = t.pages;
        int page_nums = pages.size();
        int ID = r.getID();
        String filename = "src/" + t.tableName + ".txt";
        for (int i = 0; i < page_nums; i++) {
            Page p = Page.readFile(t.tableName, i);
            Record[] records = p.getRecords();
            for(Record record: records){    // check if it is an update
                if(record.getID() == ID){
                    update(filename, ID, r.toString());
                    System.out.println("Update record: " + record.toString() + " to be: " + r.toString());
                    return;
                }
            }
        }
        // when it goes here, it means the operation will be an insert
        addLast(filename, r);
    }

    private void loadPage(String table_pageNo){
        String tableName = table_pageNo.substring(0, 1);
        int pageNo = Integer.parseInt(table_pageNo.substring(1));
        Page p = Page.readFile(tableName, pageNo);
        this.put(table_pageNo, p);
    }

    private static void update(String fileName, int id, String record) {
        File f = new File(fileName);
        BufferedReader br = null;

        PrintWriter pw = null;

        StringBuffer buff = new StringBuffer();

        String line = System.getProperty("line.separator");

        try {

            br = new BufferedReader(new FileReader(f));

            for (String str = br.readLine(); str != null; str = br.readLine()) {

                if (str.charAt(0) == id)
                    str = record;
                buff.append(str).append(line);
            }

            pw = new PrintWriter(new FileWriter(fileName), true);

            pw.println(buff);

        } catch (IOException e) {

            e.printStackTrace();

        } finally {

            if (br != null)

                try {

                    br.close();

                } catch (IOException e) {

                    e.printStackTrace();

                }

            if (pw != null)

                pw.close();

        }

    }

    static void addLast(String fileName, Record r){
        FileWriter output = null;
        try {
            output = new FileWriter(fileName, true);
            output.write("\n" + r.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            if(output == null){
                try {
                    output.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
