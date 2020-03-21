import java.util.*;

public class Table {
    String tableName;
    ArrayList<Page> pages = new ArrayList<Page>();
    Queue<Tuple> freeSpace;
    Tuple nextInsert;
    HashSet<Integer> spaces;
    public Table(String tableName){
        this.tableName = tableName;
        Page emptyPage = new Page();
        pages.add(emptyPage);
        Comparator<Tuple> comparator = new Comparator<Tuple>(){
            @Override
            public int compare(Tuple t1, Tuple t2){
                return (t1.first - t2.first)*2048 + t1.second - t2.second;
            }
        };
        freeSpace = new PriorityQueue<Tuple>(comparator);
        spaces = new HashSet<>();
    }

    public void refreshNextInsert(Tuple currentDelete){ //refresh freeSpace after each delete
       freeSpace.add(currentDelete);
    }

    public Tuple getInsertSpace(){  //assign location for next insert value, needs to call Page.nextSpace() to get next available offset
        if(!freeSpace.isEmpty()) nextInsert = freeSpace.remove();
        else{
            //nextInsert = new Tuple(pages.size() - 1, pages.get(pages.size() - 1).getNextOffset());  //generally speaking, pages cannot be called
            Page finalPage = Page.readFile(tableName, pages.size() - 1);
            nextInsert = new Tuple(pages.size() - 1, finalPage.getNextOffset());
        }
       return nextInsert;
    }

    public void refreshPage(int PageNo, Page p){

    }

}
