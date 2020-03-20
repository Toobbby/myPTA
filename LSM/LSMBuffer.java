import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.*;

// Key: X1 (tableName + page_No)
// Value: Page
public class LSMBuffer<K, V> extends LinkedHashMap<String, Page> {
    private int buffer_size;

    LSMBuffer(int _buffer_size) {
        super(16, 0.75f, true);
        this.buffer_size = _buffer_size;
    }


    void writePageBackToDisk(String table_pageNo) { // write page in buffer back to the disk
        String tableName = table_pageNo.substring(0, 1);
        int pageNo = Integer.parseInt(table_pageNo.substring(1));  //need to consider page number has more than one digit
        Page p = this.get(table_pageNo);
        System.out.println("Records " + p.getRecords().toString());
        System.out.println("Flush page " + table_pageNo + "back to disk"); //print nothing,which means buffer never overflow.
        p.writeFile(tableName, pageNo);
        myPTA.tables.get(tableName).refreshPage(pageNo, p);
    }

    // LRU algorithm
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Page> eldest) {
        if (size() > buffer_size) {
            // write eldest page in buffer back to the disk
            System.out.println("overflow: " + eldest.getKey());
            writePageBackToDisk(eldest.getKey());
            return true;
        }
        return false;
    }

    public Record readRecord(LSMTree t, String tableName, int val) {  //TODO: LSM read record, val is primary key value
        ArrayList<Page> pages = t.pages;
        for (int i = 0; i < pages.size(); i++) {
            Record r = checkRecordInTree(tableName + i, val);
            if(r != null) return r;
        }
        return null;
    }

    public ArrayList<Record> readRecordWithAreaCode(LSMTree t, String tableName, int areaCode) {  //TODO: loop through all records and add matched ones to results
        ArrayList<Record> results = new ArrayList<Record>();
        ArrayList<Page> pages = t.pages;
        for (int i = 0; i < pages.size(); i++) {
            results.addAll(checkRecordInTreeWithAreaCode(tableName + i, areaCode));
        }
        return results;
    }

    public Record checkRecordInTree(String table_pageNo, int val){  //TODO
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

    public ArrayList<Record> checkRecordInTreeWithAreaCode(String table_pageNo, int areaCode){  //TODO
        // load page, then find the record ONE BY ONE
        if(!this.containsKey(table_pageNo)){
            loadPage(table_pageNo);
        }
        return findRecordWithAreaCode(this.get(table_pageNo), areaCode);
    }


    public Record findRecord(Page p, int val){     // find record with ID=val in a particular page //TODO: find record in each sstable
        ArrayList<Record> records = p.getRecords();
        if(records == null){
            return null;
        }
//        System.out.println(records.length);
//        System.out.println(records[0].toString());
//        System.out.println(records[0].ID);
//        System.out.println(val);
        for(Record r: records){
            if(r == null) break;
            if(r.getID() == val){
                return r;
            }
        }
        return null;
    }

    public ArrayList<Record> findRecordWithAreaCode(Page p, int areaCode){     // find record with areacode in a particular page  //TODO: change page to SSTable
        ArrayList<Record> results = new ArrayList<Record>();
        ArrayList<Record> records = p.getRecords();
        if(records == null){
            return null;
        }

        for(Record r: records){
            if(r == null) break;
            if(Integer.parseInt(r.getPhone().substring(0,3))== areaCode){
                results.add(r);
            }
        }
        return results;
    }

    public void writeRecordInMemtable(LSMTree t, Record r){  //write into t's corresponding Memtable  //TODO: write includes modify or insert, but always to memtable

    }

    public void loadSSTable(String table_pageNo){  //TODO: find a SSTable with a unique key
        String tableName = table_pageNo.substring(0, 1);
        int pageNo = Integer.parseInt(table_pageNo.substring(1));
        Page p = Page.readFile(tableName, pageNo);
        this.put(table_pageNo, p);
    }

    private static void update(String fileName, int id, String record) {  //ignore
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

    static void addLast(String fileName, Record r){  //ignore
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
