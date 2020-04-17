package MyPTA;

import java.io.File;
import java.util.Arrays;

import LSM.LSMmyPTA;

public class MyPTA {
    static int concurrentReadMethod = 1;  //concurrent read script method: 0: round robin ; 1: Random(seed)
    static long randomSeed = 0;
    static int lsmPageSize;
    static int bufferSize;

	/**
	 * check the args and pass them to operation manager
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
        LSMmyPTA.deleteDir("./beforeImage");
        //argument check
        if (args.length != 4) {
            System.out.println("Please check arguments \n<" + "<LSM_page_size> <buffer_size_bytes> <seed> <scriptFolder>");
            return;
        }
        try {
            lsmPageSize = Integer.parseInt(args[0]);
            bufferSize = Integer.parseInt(args[1]);
            randomSeed = Long.parseLong(args[2]);
        } catch (NumberFormatException e) {
            System.out.println("Error: invalid numbers");
            System.out.println("Please check arguments \n<" + "<LSM_page_size> <buffer_size_bytes> <seed> <scriptFolder>");
            return;
        }

        if (randomSeed == 0) {
            concurrentReadMethod = 0;
        }

        File scriptFolder = new File(args[3]);
        OperationManager.concurrentReadMethod = concurrentReadMethod;
        String[] content = scriptFolder.list();
        for (int i = 0; i < content.length; i++) {
            content[i] = args[3] + "/" + content[i];  //get the locations of scripts
        }
//		MemoryManager memoryManager = new MemoryManager(bufferSizeInBytes, new Disk(), new RowColumnStorage());
        OperationManager.runScript(lsmPageSize, bufferSize, content);

    }
}
