package MyPTA;

import LSM.LSMmyPTA;
import LSM.Record;
import Scheduler.Scheduler;
import Scheduler.TransactionManager;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import Scheduler.Type;

public class OperationManager {
    static int executingSequencyType = 1;
    static long randomSeed = 0;
    static int nextScriptIndex = 0;
    static int numberOfScript = 0;
    static int[] fileEndCheck;
    static int endFileCount = 0;
    static long debugClock = 0;
    static final long timeOutSheld = 10;
    static int TransactionCommitedCounter = 0;

    static long totalRespondTime = 0;
    static int processCounter = 0;
    static int globalTID = 0;

    static  int RCount=0;
    static int WCount=0;
    static int MCount=0;
    static int ECount=0;
    static int DCount=0;

    public static void runScript(int bufferSizeInBytes, String[] scriptFiles) throws Exception {
        LSMmyPTA memoryManager = new LSMmyPTA(3, bufferSizeInBytes);  //Initialize data manager
        //run all scripts
        TransactionManager[] scriptTransactionManager = new TransactionManager[scriptFiles.length];
        for (int i = 0; i < scriptFiles.length; i++) {
            //System.out.println("Running Script " + scriptFiles[i]);
            numberOfScript++;
            try {
                scriptTransactionManager[i] = new TransactionManager(scriptFiles[i]);
                scriptTransactionManager[i].TID = globalTID;
            } catch (FileNotFoundException e) {
                System.out.println("File " + scriptFiles[i] + " not found, skipping file.");
            }
        }
        fileEndCheck = new int[numberOfScript];
        Scheduler scheduler = new Scheduler();
        Random rand = new Random(randomSeed);
        while (true) {
            //scheduler
            if (executingSequencyType == 0) {//round robin
                nextScriptIndex++;
                nextScriptIndex = nextScriptIndex % numberOfScript;
            } else {  //random access

                nextScriptIndex = rand.nextInt(numberOfScript);
            }

            Object value = "  ";
            TransactionManager chosenTM = scriptTransactionManager[nextScriptIndex];
            do {
                try {
                    ++debugClock;
                    chosenTM.loadNextLine();
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                }
            } while (chosenTM.isError() && !chosenTM.streamIsClosed());

            if (!chosenTM.streamIsClosed()) {
                TransactionManager.Operation t = chosenTM.getTransaction();
                String areaCode = null;
                if (debugClock % timeOutSheld == 0) {
                    //check for deadlock
                    int deadLockedTID = scheduler.DeadLockDetectFree();
                    if (deadLockedTID != -1)  //there is a deadlock, abort the selected dead Operation
                    {
                        boolean assertionCheck = false;
                        for (TransactionManager manager : scriptTransactionManager) {
                            if (manager.getTransaction().getTID() == deadLockedTID) {
                                assertionCheck = true;  //found the selected dead vertex abort it
                                LSMmyPTA.logWriter("[T_" + t.getTID() + ", DEADLOCK ABORT]");
                                manager.DeadLockAbort();
                                processCounter++;
                                totalRespondTime += System.currentTimeMillis()-chosenTM.startTimestamp;
                                LSMmyPTA.logWriter("[T_" + t.getTID() + ", start undo]");
                                undo(manager, memoryManager);
                                LSMmyPTA.logWriter("[T_" + t.getTID() + ", end undo]");
                                //write out to after image
                                break;
                            }
                        }
                        if (!assertionCheck)  //did not found the selected dead vertex
                        {
                            throw new IllegalStateException("Could not find Operation to abort; deadlock still detected.");
                        }else {
                            continue;
                        }
                    }
                }
                switch (chosenTM.getCommand()) {
                    case READ_ID:
                        if (t.getTransactionType()) {  //serializable
                            if (!scheduler.addTupleLock(Type.R, t.getTID(), chosenTM.getValue(), chosenTM.getTableName(), null))
                            //cannot get a tuple lock (currently occupied)
                            {
                                chosenTM.block(); //block the Operation
                                continue;
                            }
                            chosenTM.unblock();
                            chosenTM.addOP(); //add the Operation to buffer
                            LSMmyPTA.logWriter("T" + t.getTID() + ": " + chosenTM.getFullString());
                            Record temp = chosenTM.ReadIdFromTempData(Integer.parseInt(chosenTM.getValue()), chosenTM.getTableName());
                            if (temp != null) LSMmyPTA.logWriter("T" + t.getTID() + ": " +"Read: " + temp.toString());
                            //
                            if (temp == null && !chosenTM.ifDeletetable(chosenTM.getTableName()))
                                temp = memoryManager.read(chosenTM.getTableName(), Integer.parseInt(chosenTM.getValue()));
                            if (temp != null) LSMmyPTA.logWriter("T" + t.getTID() + ": " +"Read: " + temp.toString());
                            else LSMmyPTA.logWriter("T" + t.getTID() + ": " +chosenTM.getFullString() + " Failed!");

                        } else { //read committed
                            if (!scheduler.addTupleLock(Type.R, t.getTID(), chosenTM.getValue(), chosenTM.getTableName(), null))
                            //cannot get a tuple lock (currently occupied)
                            {
                                chosenTM.block(); //block the Operation
                                continue;
                            }
                            chosenTM.unblock();
                            chosenTM.addOP(); //add the Operation to buffer
                            //read committed isolation read tempData
                            LSMmyPTA.logWriter("T" + t.getTID() + ": " + chosenTM.getFullString());
                            Record temp = chosenTM.ReadIdFromTempData(Integer.parseInt(chosenTM.getValue()), chosenTM.getTableName());
                            if (temp != null) LSMmyPTA.logWriter("T" + t.getTID() + ": " + "Read: " + temp.toString());

                            //
                            if (temp == null && !chosenTM.ifDeletetable(chosenTM.getTableName()))
                                temp = memoryManager.read(chosenTM.getTableName(), Integer.parseInt(chosenTM.getValue()));
                            if (temp != null) LSMmyPTA.logWriter("T" + t.getTID() + ": " + "Read: " + temp.toString());
                            else LSMmyPTA.logWriter("T" + t.getTID() + ": " + chosenTM.getFullString() + " Failed!");
                            scheduler.releaseLock(t.getTID());
                        }
                        break;
                    case READ_AREA_CODE:
                        if (t.getTransactionType()) { // serializable
                            areaCode = chosenTM.getValue();
                            if (!scheduler.addTupleLock(Type.M, t.getTID(), null, chosenTM.getTableName(), areaCode)) {
                                chosenTM.block();
                                continue;
                            }
                            chosenTM.unblock();
                            chosenTM.addOP();
                            LSMmyPTA.logWriter("T" + t.getTID() + ": " + chosenTM.getFullString());
                            ArrayList<Record> rList = null;
                            ArrayList<Record> RecordList = null;
                            // check if previous Operation has already deleted corresponding table in disk
                            if (!chosenTM.ifDeletetable(chosenTM.getTableName())) {
                                rList = memoryManager.showUserWithAreaCode(chosenTM.getTableName(), areaCode);
                                RecordList = new ArrayList<>();
                                for (Record r : rList) {
                                    // check if previous Operation has erased the record in disk
                                    if ((!chosenTM.ifIdInTempData(r.getID(), chosenTM.getTableName()))) {
                                        RecordList.add(r);
                                    }
                                }
                            }
                            ArrayList<Record> transactionRecords = chosenTM.ReadAreaFromTempData(areaCode, chosenTM.getTableName());
                            if (RecordList != null) {
                                transactionRecords.addAll(RecordList);
                            }
                            value = transactionRecords.isEmpty() ? null : transactionRecords.toArray(new Record[0]);
                            if (!transactionRecords.isEmpty()) {
                                for (Record r : transactionRecords) {
                                    LSMmyPTA.logWriter("T" + t.getTID() + ": " + "Mread: " + r.toString());
                                }
                            } else {
                                LSMmyPTA.logWriter("T" + t.getTID() + ": " + "Mread: no records found with " + areaCode);
                            }
                        } else {  // read committed
                            areaCode = chosenTM.getValue();
                            if (!scheduler.addTupleLock(Type.M, t.getTID(), null, chosenTM.getTableName(), areaCode)) {
                                chosenTM.block();
                                continue;
                            }
                            chosenTM.unblock();
                            chosenTM.addOP();
                            LSMmyPTA.logWriter("T" + t.getTID() + ": " + chosenTM.getFullString());
                            ArrayList<Record> rList = null;
                            ArrayList<Record> RecordList = null;
                            // check if previous Operation has already deleted corresponding table in disk
                            if (!chosenTM.ifDeletetable(chosenTM.getTableName())) {
                                rList = memoryManager.showUserWithAreaCode(chosenTM.getTableName(), areaCode);
                                RecordList = new ArrayList<>();
                                for (Record r : rList) {
                                    // check if previous Operation has erased the record in disk
                                    if ((!chosenTM.ifIdInTempData(r.getID(), chosenTM.getTableName()))) {
                                        RecordList.add(r);
                                    }
                                }
                            }
                            ArrayList<Record> transactionRecords = chosenTM.ReadAreaFromTempData(areaCode, chosenTM.getTableName());
                            if (RecordList != null) {
                                transactionRecords.addAll(RecordList);
                            }
                            value = transactionRecords.isEmpty() ? null : transactionRecords.toArray(new Record[0]);
                            scheduler.releaseLock(t.getTID());
                        }
                        break;
                    case WRITE:
                        Record r = new Record(t.getValue());
                        if (!scheduler.addTupleLock(Type.W, t.getTID(), String.valueOf(r.getID()), chosenTM.getTableName(), r.getAreaCode())) {
                            chosenTM.block();
                            continue;
                        }
                        chosenTM.unblock();
                        chosenTM.addOP();
                        LSMmyPTA.logWriter("T" + t.getTID() + ": " + chosenTM.getFullString());
                        chosenTM.writeToTempData(Type.W, r.getID(), r, chosenTM.getTransaction().getTableName());
                        if (t.getTransactionType()) { // serializable
                            if (!chosenTM.beforeImageIds.contains(r.getID())) {
                                Record beforeImage = memoryManager.read(chosenTM.getTableName(), r.getID());
                                if (beforeImage == null)
                                    chosenTM.beforeImageWriter("E " + chosenTM.getTableName() + " " + r.getID());
                                else
                                    chosenTM.beforeImageWriter("W " + chosenTM.getTableName() + " " + beforeImage.toString());
                                chosenTM.beforeImageIds.add(r.getID());
                            }
                            memoryManager.write(chosenTM.getTableName(), r.toString());


                        } else { // read committed
                            scheduler.releaseLock(t.getTID());
                        }
                        break;
                    case ERASE:
                        int recordID = Integer.parseInt(t.getValue());
                        if (!scheduler.addTupleLock(Type.E, t.getTID(), t.getValue(), chosenTM.getTableName(), null)) {
                            chosenTM.block();
                            continue;
                        }
                        chosenTM.unblock();
                        chosenTM.addOP();
                        LSMmyPTA.logWriter("T" + t.getTID() + ": " + chosenTM.getFullString());
                        chosenTM.writeToTempData(Type.E, Integer.parseInt(t.getValue()), null, chosenTM.getTransaction().getTableName());
                        if (t.getTransactionType()) { // serializable
                            if (!chosenTM.beforeImageIds.contains(recordID)) {
                                Record beforeImage = memoryManager.read(chosenTM.getTableName(), recordID);
                                if (beforeImage != null)
                                    chosenTM.beforeImageWriter("W " + chosenTM.getTableName() + " " + beforeImage.toString());
                                chosenTM.beforeImageIds.add(recordID);
                            }
                            memoryManager.erase(chosenTM.getTableName(), recordID);
                        } else { // read committed
                            scheduler.releaseLock(t.getTID());
                        }
                        break;
                    case DELETE_TABLE:
                        if (!scheduler.addTupleLock(Type.D, t.getTID(), null, chosenTM.getTableName(), null)) {
                            chosenTM.block();
                            continue;
                        }
                        chosenTM.unblock();
                        chosenTM.addOP();
                        LSMmyPTA.logWriter("T" + t.getTID() + ": " + chosenTM.getFullString());
                        if (t.getTransactionType()) { // serializable
                            //put into op buffer
                        } else { // read committed
//							result=memoryManager.deleteTable(chosenTM.getTableName());
                            scheduler.releaseLock(t.getTID());
                        }
                        break;
                    case COMMIT:    // need to check commit function in MM
                        if (t.getTransactionType()) { // serializable
                            LSMmyPTA.logWriter("T" + t.getTID() + ": " + chosenTM.getFullString());
                            memoryManager.commitToTransaction(chosenTM.getOPBuffer());
                            chosenTM.beforeImageIds.clear();
                            scheduler.releaseLock(t.getTID());
                        } else { // read committed
                            LSMmyPTA.logWriter("T" + t.getTID() + ": " + chosenTM.getFullString());
                            memoryManager.commitToTransaction(chosenTM.getOPBuffer());
                        }
                        chosenTM.commit();
                        TransactionCommitedCounter++;
                        totalRespondTime += System.currentTimeMillis()-chosenTM.startTimestamp;
                         RCount+=chosenTM.RCount;
                         WCount+=chosenTM.WCount;
                          MCount+=chosenTM.MCount;
                          ECount+=chosenTM.ECount;
                          DCount+=chosenTM.DCount;
                        break;
                    case ABORT:
                        LSMmyPTA.logWriter("T" + t.getTID() + ": " + chosenTM.getFullString());
                        if (t.getTransactionType()) { // serializable
                            chosenTM.Abort();
                            //recovery
                            undo(chosenTM, memoryManager);
                            chosenTM.beforeImageIds.clear();
                            scheduler.releaseLock(t.getTID());
                        } else { // read committed
                            chosenTM.Abort();
                        }
                        processCounter++;
                        totalRespondTime += System.currentTimeMillis()-chosenTM.startTimestamp;
                        break;
                    case BEGIN:
                        //Nothing
                        ++globalTID;
                        chosenTM.TID = globalTID;
                        LSMmyPTA.logWriter("T" + chosenTM.TID + ": " + chosenTM.getFullString());
                        chosenTM.startTimestamp=System.currentTimeMillis();
                        break;
                    default:
                        throw new UnsupportedOperationException("Command not supported");
                }
            }else{

                if (fileEndCheck[nextScriptIndex] == 1) {
                    continue;
                }
                fileEndCheck[nextScriptIndex] = 1;
                endFileCount++;
                System.out.println("Execution on " + scriptFiles[nextScriptIndex] + " is finished!");
                if (endFileCount == numberOfScript) {
                    int totalCounter=RCount+WCount+DCount+ECount+MCount;
                    memoryManager.flush();
                    System.out.println(TransactionCommitedCounter + " transactions commited");
                    System.out.println((double) RCount / ((double) totalCounter ) * 100 + " % of operation is read");
                    System.out.println((double) WCount / ((double) totalCounter ) * 100 + " % of operation is write");
                    System.out.println((double) MCount / ((double) totalCounter ) * 100 + " % of operation is readAreaCode");
                    System.out.println((double) ECount / ((double) totalCounter ) * 100 + " % of operation is Erase");
                    System.out.println((double) DCount / ((double) totalCounter ) * 100 + " % of operation is Delete table");

                    System.out.println("Total respond time is " + totalRespondTime);
                    System.out.println("Total number of process is " + (processCounter+TransactionCommitedCounter));
                    System.out.println("Average respond time is " + (double)totalRespondTime / (processCounter + TransactionCommitedCounter));
                    System.exit(0);
                }
            }
        }
    }


    public static void undo(TransactionManager chosenTM, LSMmyPTA memoryManager){
        try {

            BufferedReader beforeImageReader = new BufferedReader(new FileReader(chosenTM.beforeImageLocation + chosenTM.TID + ".txt"));
            String line = null;
            while ((line = beforeImageReader.readLine()) != null) {
                String[] split = line.split(" ", 3);
                if (split[0].equals("E"))
                    memoryManager.erase(split[1], Integer.parseInt(split[2]));
                else memoryManager.write(split[1], split[2]);


            }
        }catch(Exception e){
            System.out.print(e);
        }
    }








    }

