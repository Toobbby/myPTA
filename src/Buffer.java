import java.util.LinkedHashMap;
import java.util.Map;
import java.util.*;

// Key: X1 (tablename + page_No)
// Value: Page
public class Buffer<K, V> extends LinkedHashMap<String, Page> {
    private int buffer_size;

    Buffer(int _buffer_size){
        super(16,0.75f,true);
        this.buffer_size = _buffer_size;
    }

    Page readPage(String table_pageNo, int val){
        // first check if this page has already been in the buffer
        // if it is, just return this page
        if(this.containsKey(table_pageNo)){
            return this.get(table_pageNo);
        }

        // if not exist, then need to load this page from disk to buffer
        String tablename = table_pageNo.substring(0, 1);
        int page_No = Integer.parseInt(table_pageNo.substring(1, 2));
        // TODO: use Page.read(String tablename, int page) to read from file.
        Page p = Page.readFile(tablename, page_No);
        return p;
    }

    // TODO
    void writePage(String table_pageNo){
        String tablename = table_pageNo.substring(0, 1);
        int page_No = Integer.parseInt(table_pageNo.substring(1, 2));
        Page p = this.get(table_pageNo);
    }

    // LRU algorithm
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Page> eldest){
        if(size() > buffer_size){
            // write eldest page in buffer back to the disk
            writePage(eldest.getKey());
            return true;
        }
        return false;
    }
}
