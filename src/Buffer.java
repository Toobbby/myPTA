import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.*;

// Key: X1 (tableName + page_No)
// Value: Page
public class Buffer<K, V> extends LinkedHashMap<String, Page> {
    private int buffer_size;
    private int page_Size;
    private String logPath;

    Buffer(int _buffer_size, String logPath, int page_Size) {
        super(16, 0.75f, true);
        this.buffer_size = _buffer_size;
        this.logPath = logPath;
        this.page_Size = page_Size;
    }


   public void writePageBackToDisk(String table_pageNo) throws IOException { // write page in buffer back to the disk
        String tableName = table_pageNo.substring(0, 1);
        int pageNo = Integer.parseInt(table_pageNo.substring(1));  //need to consider page number has more than one digit
        Page p = this.get(table_pageNo);
        System.out.println("Records " + p.getRecords().toString());
        System.out.println("Flush page " + table_pageNo + "back to disk"); //print nothing,which means buffer never overflow.
        p.writeFile(tableName, pageNo);
        myPTA.tables.get(tableName).refreshPage(pageNo, p);
        logWriter("Swap out  " + "T-" + tableName + " P-" + pageNo, this.logPath);
    }

    // LRU algorithm
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Page> eldest) {
        if (size() > buffer_size) {
            // write eldest page in buffer back to the disk
            String table_pageNo = eldest.getKey();
            String tableName = table_pageNo.substring(0, 1);
            int pageNo = Integer.parseInt(table_pageNo.substring(1));
            System.out.println("overflow: " + table_pageNo);
            try {
                writePageBackToDisk(eldest.getKey());
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        }
        return false;
    }

    public Record readRecord(Table t, String tableName, int val) throws IOException {
        ArrayList<Page> pages = t.pages;
        for (int i = 0; i < pages.size(); i++) {
            Record r = checkRecordInPage(tableName + i, val);
            if (r != null) return r;
        }
        return null;
    }

    public ArrayList<Record> readRecordWithAreaCode(Table t, String tableName, int areaCode) throws IOException {
        ArrayList<Record> results = new ArrayList<Record>();
        ArrayList<Page> pages = t.pages;
        for (int i = 0; i < pages.size(); i++) {
            results.addAll(checkRecordInPageWithAreaCode(tableName + i, areaCode));
        }
        return results;
    }

    public Record checkRecordInPage(String table_pageNo, int val) throws IOException {
        // first check if this page has already been in the buffer
        // if it is in buffer, find the record directly
        if (this.containsKey(table_pageNo)) {
            return findRecord(this.get(table_pageNo), val);
        } else {   // load page, then find the record
            loadPage(table_pageNo);
            return findRecord(this.get(table_pageNo), val);
        }
    }

    public ArrayList<Record> checkRecordInPageWithAreaCode(String table_pageNo, int areaCode) throws IOException {
        // load page, then find the record ONE BY ONE
        if (!this.containsKey(table_pageNo)) {
            loadPage(table_pageNo);
        }
        return findRecordWithAreaCode(this.get(table_pageNo), areaCode);
    }


    public Record findRecord(Page p, int val) {     // find record with ID=val in a particular page
        ArrayList<Record> records = p.getRecords();
        if (records == null) {
            return null;
        }
//        System.out.println(records.length);
//        System.out.println(records[0].toString());
//        System.out.println(records[0].ID);
//        System.out.println(val);
        for (Record r : records) {
            if (r == null) break;
            if (r.getID() == val) {
                return r;
            }
        }
        return null;
    }

    public ArrayList<Record> findRecordWithAreaCode(Page p, int areaCode) throws IOException {     // find record with areacode in a particular page
        ArrayList<Record> results = new ArrayList<Record>();
        ArrayList<Record> records = p.getRecords();
        if (records == null) {
            return null;
        }

        for (Record r : records) {
            if (r == null) break;
            if (Integer.parseInt(r.getPhone().substring(0, 3)) == areaCode) {
                results.add(r);
                logWriter("MRead: " + r.toString(), this.logPath);
            }
        }
        return results;
    }

    public void writeRecordInPage(Table t, Record r) throws IOException {  //all operations in buffer is focusing on Page
        ArrayList<Page> pages = t.pages;
        int page_nums = pages.size();
//        System.out.println("page_nums: " + page_nums);
        int ID = r.getID();
        //String filename = "src/" + t.tableName + ".txt";
        for (int j = 0; j < page_nums; j++) {
            if (!this.containsKey(t.tableName + j)) {    // the page is in the buffer
                loadPage(t.tableName + j);
            }
            ArrayList<Record> records = this.get(t.tableName + j).getRecords();
            for (int i = 0; i < records.size(); i++) {    // find the record which needs to be modified
                if (records.get(i) == null) break;
                if (records.get(i).getID() == ID) {
                    records.set(i, r);
                    this.put(t.tableName + j, new Page(this.page_Size, records));  //refresh the page in buffer
                    //System.out.println("Update record: " + record.toString() + " to be: " + r.toString());
                    logWriter("Written: " + r.toString(), this.logPath);
                    return;
                }
            }
            if (j == page_nums - 1) {  //no record--> check freeSpace --> if no freeSpace --> insert to the end
                //                            --> has freeSpace --> insert to the pop page
                if (t.freeSpace.isEmpty()) {
                    if (records.size() == this.page_Size) {  //if the last page is full, create a new page to store the record
                        ArrayList<Record> newRecord = new ArrayList<>();
                        newRecord.add(r);
                        this.put(t.tableName + (j + 1), new Page(this.page_Size, newRecord));
                        logWriter("Create " + "T-" + t.tableName + " P-" + (j + 1), this.logPath);
                        logWriter("Swap in  " + "T-" + t.tableName + " P-" + (j + 1), this.logPath);
                        t.pages.add(new Page(this.page_Size, newRecord));
                        System.out.println("Creating");
                        logWriter("Written: " + r.toString(), this.logPath);
                        return;
                    }
                    records.add(r);
                    System.out.println("add record");
                    this.put(t.tableName + j, new Page(this.page_Size, records));
                } else {
                    int pageNo_tobeinsert = t.freeSpace.pop();
                    System.out.println(t.tableName + " Page" + pageNo_tobeinsert + " has free space. Insert to this page.");
                    logWriter(t.tableName + " Page" + pageNo_tobeinsert + " has free space. Insert to this page.", this.logPath);
                    if (!this.containsKey(t.tableName + pageNo_tobeinsert)) {
                        loadPage(t.tableName + pageNo_tobeinsert);
                    }
                    Page p = this.get(t.tableName + pageNo_tobeinsert);
                    ArrayList<Record> records1 = p.getRecords();
                    records1.add(r);
                    p.records = records1;
                    this.put(t.tableName + pageNo_tobeinsert, p);
                }
                logWriter("Written: " + r.toString(), this.logPath);
                return;
            }
        }
    }
//            else {   // load page, then find the record
//                loadPage(t.tableName + j, logPath);
//                ArrayList<Record> records = this.get(t.tableName + j).getRecords();
////                Record[] recordArray = this.get(t.tableName + j).getRecords();
////                ArrayList<Record> records = new ArrayList<Record>(Arrays.asList(recordArray));
//                for (int i = 0; i < records.size(); i++) {    // find the record which needs to be modified
//                    if (records.get(i) == null) break;
//                    if (records.get(i).getID() == ID) {
//                        records.set(i, r);
//                        this.put(t.tableName + j, new Page(records));
//                        logWriter("Written: " + r.toString(), logPath);
//                        return;
//                    }
//                    if (j == page_nums - 1) {  //no record-->insert to the end
//                        if (records.size() == Page.size) {  //if the last page is full, create a new page to store the record
//                            ArrayList<Record> newRecord = new ArrayList<>();
//                            newRecord.add(r);
//                            this.put(t.tableName + (j + 1), new Page(newRecord));
//                            t.pages.add(new Page(newRecord));
//                            logWriter("Create " + "T-" + t.tableName + " P-" + (j + 1), logPath);
//                            logWriter("Swap in  " + "T-" + t.tableName + " P-" + (j + 1), logPath);
//                            System.out.println("Creating");
//                            logWriter("Written: " + r.toString(), logPath);
//                            return;
//                        }
//                        records.add(r);
//                        this.put(t.tableName + j, new Page(records));
//                        logWriter("Written: " + r.toString(), logPath);
//                        return;
//                    }
//                }
//
//            }



    public void loadPage(String table_pageNo) throws IOException {
        String tableName = table_pageNo.substring(0, 1);
        int pageNo = Integer.parseInt(table_pageNo.substring(1));
        Page p = Page.readFile(tableName, pageNo, this.page_Size);
        this.put(table_pageNo, p);
        logWriter("Swap in  " + "T-" + tableName + " P-" + pageNo, this.logPath);
    }

    public boolean eraseRecord(Table t, String tableName, int val) throws IOException {
        ArrayList<Page> pages = t.pages;
        for (int i = 0; i < pages.size(); i++) {
            // find the index of page that contains the record with val
            Record r = checkRecordInPage(tableName + i, val);
            if (r != null){
                erasePageWithVal(t, tableName, i, val);
                return true;
            }
        }
        return false;
    }

    public void erasePageWithVal(Table t, String tableName, int page_no, int val){
        Page oldPage = this.get(tableName + page_no);
        ArrayList<Record> records = oldPage.getRecords();
        for (int i = 0; i < records.size(); i++) {
            if(records.get(i).getID() == val){
                records.remove(i);
                t.freeSpace.push(page_no);
                break;
            }
        }
        oldPage.records = records;
        this.put(tableName + page_no, oldPage);
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

    static void addLast(String fileName, Record r) {
        FileWriter output = null;
        try {
            output = new FileWriter(fileName, true);
            output.write("\n" + r.toString());

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (output == null) {
                try {
                    output.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void logWriter(String logContext, String logPath) throws IOException {
        FileWriter fw = new FileWriter(logPath, true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(logContext);
        bw.newLine();
        bw.close();
        fw.close();
    }

}
