package Scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;


public class Scheduler {
    //TODO: modify locktable to hashmap<tablename, LockTree>
    HashMap<String, ArrayList<Lock>> LockTable = new HashMap<>();
    long timestamp;

    public Scheduler() {
        timestamp = 0;
    }


    public boolean addTupleLock(Type type, int TID, String id, String TableName, String area_code) {
        Lock l = new Lock(type, 0, TID, id, TableName, area_code);
        //check if lock is held by others
        l.WaitforT = getLatestWaitfor(l);
        if (l.WaitforT.isEmpty()) {
            //if no one have lock, successfully add lock
            l.getOrNot = true;
            if(LockTable.containsKey(TableName)) LockTable.get(TableName).add(l);
            else{
                ArrayList<Lock> temp = new ArrayList<>();
                temp.add(l);
                LockTable.put(TableName, temp);
            }
            return true;
        } else {
            //add lock to list and return false
            if (!LockTable.getOrDefault(TableName, new ArrayList<Lock>()).contains(l)) {
                if(LockTable.keySet().contains(TableName)) LockTable.get(TableName).add(l);
                else{
                    ArrayList<Lock> temp = new ArrayList<>();
                    temp.add(l);
                    LockTable.put(TableName, temp);
                }
            }
            return false;
        }
    }

    //release lock and remove from queue
    public void releaseLock(int TID) {
        for(ArrayList<Lock> values: LockTable.values()){
            int i = values.size() - 1;
            while (i >= 0) {
                if (values.get(i).TID == TID) {
                    values.remove(i);
                } else if (values.get(i).WaitforT.contains(TID))
                    values.get(i).WaitforT.remove(values.get(i).WaitforT.indexOf(TID));
                --i;
            }
        }
        return;
    }

    //count what other locks are held before
    private LinkedList<Integer> getLatestWaitfor(Lock L) {
        ArrayList<Lock> locksOfTable = LockTable.getOrDefault(L.TableName, new ArrayList<>());
        int i = locksOfTable.size() - 1;
        LinkedList<Integer> ret = new LinkedList<Integer>();
        while (i >= 0) {
            if(!locksOfTable.get(i).getOrNot){
                --i;
                continue;
            }
            if (CompatTable(L.Ttype, locksOfTable.get(i).Ttype)) { //if true, consider next(type level)
                --i;
                continue;
            } else {
                if(L.TID == locksOfTable.get(i).TID){
                    --i;
                    continue;
                }
            	// all operations should wait for D/ D should wait for all operations
				if ((L.Ttype == Type.D || locksOfTable.get(i).Ttype == Type.D) &&
						!ret.contains(locksOfTable.get(i).TID))
					ret.add(locksOfTable.get(i).TID);  //Table lock


            	//			M/D id is null							R D delete areacode is null
            	if((L.Ttype == Type.M && locksOfTable.get(i).Ttype == Type.E ||
								L.Ttype == Type.E && locksOfTable.get(i).Ttype == Type.M) &&
						!ret.contains(locksOfTable.get(i).TID))
						ret.add(locksOfTable.get(i).TID);  //Lock on areaCode
            	//M E/E M



                if ((((L.ID != null) && L.ID.equals(locksOfTable.get(i).ID)) ||
								(L.AreaCode != null) && L.AreaCode.equals(locksOfTable.get(i).AreaCode)) &&
						!ret.contains(locksOfTable.get(i).TID))
                //     R W/ W R	/E W/ W E/ R E/ E R	/ E W / W E	/M W/ W M
                {
                    ret.add(locksOfTable.get(i).TID);  //tuple lock
                }
                --i;



            }
        }
        return ret;
    }


    //check deadlock
    private Set<Integer> cycleDectect() {
        Graph WaitforGraph = new Graph();
        for(ArrayList<Lock> locksOfTable: LockTable.values()){
            int i = locksOfTable.size() - 1;
            while (i >= 0) {
                for (int j = 0; j < locksOfTable.get(i).WaitforT.size(); ++j) {
                    WaitforGraph.addEdge(locksOfTable.get(i).TID, locksOfTable.get(i).WaitforT.get(j));
                }
                --i;
            }
        }
        return WaitforGraph.checkCycle();
    }

   //check dead lock and pick one to abort
    public int DeadLockDetectFree() {
        Set<Integer> DeadVertex = cycleDectect();
        int DeadVertexTID = -1;
        if (!DeadVertex.isEmpty()) {
            DeadVertexTID = (int) DeadVertex.toArray()[0];
            //get dead TID, then clear the table entry whose wait for field is this TID
            releaseLock(DeadVertexTID);
        }
        return DeadVertexTID;
    }

    //only read and M are compatible
    private boolean CompatTable(Type T1, Type T2) {  //read read return true
        if (T1 == Type.M || T1 == Type.R) {
            if (T2 == Type.M || T2 == Type.R) {
                return true;
            }
        }
        return false;
    }

    public class Lock{
        public Type Ttype;
        public long TimeStamp;
        public int TID;
        public String ID;
        public String TableName;
        public String AreaCode;
        public LinkedList<Integer> WaitforT; //restore the tuple lock (read-committed)
        public boolean getOrNot = false;

        public Lock(Type type, int Stamp, int TID, String id2, String TableName, String area_code) {
            this.Ttype = type;
            this.TimeStamp = Stamp;
            this.TID = TID;
            this.ID = id2;
            this.TableName = TableName;
            this.AreaCode = area_code;
            this.WaitforT = null;

        }

        @Override
        public boolean equals(Object o){
            Lock lock = (Lock) o;
            String ID = lock.ID == null? "":lock.ID;
            String AreaCode = lock.AreaCode == null? "":lock.AreaCode;
            String myID = this.ID == null? "":this.ID;
            String myAreaCode = this.AreaCode == null? "":this.AreaCode;
            return this.TID == lock.TID && myID.equals(ID) && this.TableName.equals(lock.TableName) && myAreaCode.equals(AreaCode);


        }


    }
}
