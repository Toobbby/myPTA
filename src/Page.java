import java.io.*;

public class Page{
	static int size;
	Record[] records;
	Page(){}

	Page(Record[] _records){
		records = _records;
	}

	public Record[] getRecords() {
		return records;
	}

	public static Page readFile(String tablename, int page_No){    // read a particular page from file(table)
		Record[] results = new Record[size];
		try {
			FileInputStream inputStream = new FileInputStream("./" + tablename + ".txt");
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
			String line;
			int line_No = 0;
			int idx = 0;

			while ((line = bufferedReader.readLine())!= null){
				line_No++;
				if(line_No <= page_No * size){      // before the page
					// do nothing
				}
				else if(line_No > (page_No + 1)* size){     // out of the page
					break;
				}
				else {      // the accurate page to be loaded
					String[] str = line.split(" ");
					results[idx++] = new Record(Integer.valueOf(str[0]), str[1], str[2], tablename);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new Page(results);
	}
}

/*
public class Page {
	int page_No;
	int page_Size;
//    HashMap<Integer,Integer> hashMap = new HashMap<>();
    HashMap<Integer,Integer> pointer = new HashMap<>();

	public Page(int page_No){
        this.page_No = page_No;
        this.page_Size = page_Size;
    }

//	map pointer (int primaryKey, int offset)

	public size(){
		return page_Size;
	}
}
*/
