# myPTA (Phase 2)
myPTA (my Pitt Transaction Agent) is a transactional row store database that efficiently supports concurrent execution of OLTP workloads.

## How to run

Download ***myPTA.jar*** from repository. Create your own script or use provided scripts to test the correctness.
#### Requirements
> Java 1.8   
> ***myPTA.jar***, an folder with scripts (test_folder)      
>an empty folder called ***Logging***     
>an empty folder called ***tables***      
>an empty folder called ***beforeImage*** in the same folder ***F*** (for example).     
#### Running
Access folder ***F*** in command line, then enter
···
java -jar myPTA.jar arg1 arg2 arg3 arg4     
···
in which       
* arg1: the size of SSTable (how many records each SSTable could hold)      
* arg2: the size of global buffer (how many SSTables the buffer could hold)        
* arg3: the script reading method. 0 represents round robin; 1 represents random      
* arg4: test_folder's name       

for example, 
···
java -jar myPTA.jar 5 10 0 benchmark_sl
···

#### Checking the results
After running, check the ***logging*** folder. A text file called ***log+currentTime.txt*** has been created. The logging will record all operations of all transactions, also including the interaction with disk. The beforeImage is designed for ***UNDO*** of ***serializable*** isolation. It will record the opposite operation in a individual file for each transaction's ***W*** and ***E*** operations. If the transaction aborts eventually, the before image will be the criteria for system ***UNDO***.      

The metrics will be print out after all transaction finished.

### Helper
#### testing the system's correctness
You can use ***CCTesting*** folder in ***testing***, to test the correctness of locking mechanism. The result is analyzed in the report.
#### scriptGenerator.java
This is a script design for generating large scale of data. Simply run it with an empty folder called ***benchmark*** with     
* arg0: random seed
* arg1: isolation level: 0->read-committed; 1->serializable
will help you generate 100 scripts with 1000 operations each.

#### issues
If you have any issues running the code, please contact [Fangzheng Guo](fag24@pitt.edu).
