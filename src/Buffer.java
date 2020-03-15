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

    public Record checkRecordInPage(String table_pageNo, int val){
        // first check if this page has already been in the buffer
        // if it is in buffer, find the record directly
        if (this.containsKey(table_pageNo)) {
            return this.get(table_pageNo).findRecord(val); //TODO: Page.findRecord(val)
        }
        else{
            loadPage(table_pageNo);
            return this.get(table_pageNo).findRecord(val);
        }
    }

    public void loadPage(String table_pageNo){
        String tableName = table_pageNo.substring(0, 1);
        int pageNo = Integer.parseInt(table_pageNo.substring(1));
        this.put(table_pageNo, myPTA.tables.get(tableName).getPage(pageNo));
    }
}
