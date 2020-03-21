package LSM;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.*;

// Key: uuid
// Value: Memtable
public class LSMBuffer<K, V> extends LinkedHashMap<String, MemTable> {
    private int buffer_size;
    LSMBuffer(int _buffer_size) {
        super(16, 0.75f, true);
        this.buffer_size = _buffer_size;
    }

    // LRU algorithm
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, MemTable> eldest) {
        if (size() > buffer_size) {
            // write eldest page in buffer back to the disk
            System.out.println("overflow: " + eldest.getKey());
            return true;
        }
        return false;
    }
}
