import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

public class Table {
    ArrayList<Page> pages;
    Queue<Tuple> freeSpace;
    Tuple nextInsert;
    public Table(){
        Page emptyPage = new Page();
        pages.add(emptyPage);
        Comparator<Tuple> comparator = new Comparator<Tuple>(){
            @Override
            public int compare(Tuple t1, Tuple t2){
                return (t1.first - t2.first)*2048 + t1.second - t2.second;
            }
        };
        freeSpace = new PriorityQueue<Tuple>(comparator);
    }

    public void refreshNextInsert(Tuple currentDelete){ //refresh freeSpace after each delete
       freeSpace.add(currentDelete);
    }

    public Tuple getInsertSpace(){  //assign location for next insert value, needs to call Page.nextSpace() to get next available offset
        if(!freeSpace.isEmpty()) nextInsert = freeSpace.remove();
        else{
            nextInsert = new Tuple(pages.size() - 1, pages.get(pages.size() - 1).getNextOffset());
        }
       return nextInsert;
    }

}
