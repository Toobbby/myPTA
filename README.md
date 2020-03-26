# myPTA (Phase 1)
myPTA (my Pitt Transaction Agent) is a transactional row store database that efficiently supports concurrent execution of OLTP workloads.

## How to run

### Sequential Files Strategy
Download ***myPTA.jar*** from repository. Create your own script or use our ***script.txt*** to test the correctness.
#### Requirements
> Java 1.8   
> ***myPTA.jar***, ***script.txt***, an empty folder called ***Logging*** in the same folder ***F*** (for example).
#### Running
Access folder ***F*** in command line, then enter
'''
java -jar myPTA.jar arg1 arg2

'''
in which arg1 represents the size of each page (how many records each page could hold) and arg2 represents the size of global buffer (how many pages the buffer could hold).
#### Checking the results
After running, check the ***logging*** folder. A text file called ***log+currentTime.txt*** has been created. There are also some lines printed out in command line which can help you verify the correctness.

### LSM Tree Strategy
Download ***LSMmyPTA.jar*** from repository. Create your own script or use our ***script.txt*** to test the correctness.
#### Requirements
> Java 1.8   
> ***LSMmyPTA.jar***, ***script.txt***, an empty folder called ***Logging***  and an empty folder called ***tables*** in the same folder ***F*** (for example).
#### Running
Access folder ***F*** in command line, then enter
'''
java -jar LSMmyPTA.jar arg1 arg2

'''
in which arg1 represents the size of SSTable (how many records each SSTable could hold) and arg2 represents the size of global buffer (how many SSTables the buffer could hold). We assigned each table a Memtable in buffer, so global buffer size does not include Memtable parts.
#### Checking the results
After running, check the ***logging*** folder. A text file called ***log+currentTime.txt*** has been created. There are also some lines printed out in command line which can help you verify the correctness.

### Helper
If you want to test our system's correctness, you can download script.txt in this repository. It includes test cases for all operation, and anticipate log result for it is in testcase.txt in this repository.
