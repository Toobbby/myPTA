import java.io.*;
import java.util.ArrayList;

public class Page{
	int size;
	ArrayList<Record> records;
	Page(int size, ArrayList<Record> _records){
		this.size = size;
		this.records = _records;
	}

	public ArrayList<Record> getRecords() {
		return this.records;
	}

	public static Page readFile(String tablename, int page_No, int page_Size){    // read a particular page from file
		ArrayList<Record> results = new ArrayList<>();
		try {
			FileInputStream inputStream = new FileInputStream("./" + tablename + "/" + "page" + page_No + ".txt");
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
			String line;
			int line_No = 0;
			int idx = 0;

			while ((line = bufferedReader.readLine())!= null){
				line_No++;
				// the accurate page to be loaded
				String[] str = line.split(", ");
//				System.out.println(str[0]);
//                System.out.println(str[1]);
//                System.out.println(str[2]);
				results.add(new Record(Integer.valueOf(str[0]), str[1], str[2], tablename));
//				results[idx] = new Record(Integer.valueOf(str[0]), str[1], str[2], tablename);
                idx++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new Page(page_Size, results);
	}

	public void writeFile(String tablename, int page_No){
		String fileName = "./" + tablename + "/" + "page" + page_No + ".txt";
        BufferedWriter out = null;
		try
		{
			out = new BufferedWriter(new FileWriter(fileName));
			for(int i = 0; i < this.records.size(); i++){
				out.write(this.getRecords().get(i).toString());
				out.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
	}

	public int getNextOffset(){
		return records.size() * 32;  // sequentially next location to insert new record
	}
}


