package MyPTA;

import java.io.File;
import java.util.Arrays;

import LSM.LSMmyPTA;

public class MyPTA {
	static int executingSequencyType = 1;
	static long randomSeed = 0;

	public static void main(String[] args) throws Exception {

		String operationLogLoc = "./OperationLogging/";
		String currentTime = LSMmyPTA.getDate();
		LSMmyPTA.deleteDir("./beforeImage");

		//argument check
		if (args.length != 3) {
			System.out.println("CPU Usage:\n"
					+ "java -jar CPU.jar <buffer_size_bytes> <seed> <scriptFolder>");
			return;
		}
		int bufferSizeInBytes = -1;
		try {
			bufferSizeInBytes = Integer.parseInt(args[0]);
			randomSeed = Long.parseLong(args[1]);
		} catch (NumberFormatException e) {
			System.out.println("Error: Invalid buffer size or seed provided.");
			System.out.println("CPU Usage:\n"
					+ "java -jar CPU.jar <buffer_size_bytes> <seed> <scriptFolder>");
			return;
		}

		if (randomSeed == 0) {
			executingSequencyType = 0;
		}

		File scriptFolder=new File(args[2]);
		OperationManager.executingSequencyType=executingSequencyType;
		String[] content = scriptFolder.list();
		for (int i = 0; i < content.length; i++) {
			content[i]=args[2]+"/"+content[i];
		}
//		MemoryManager memoryManager = new MemoryManager(bufferSizeInBytes, new Disk(), new RowColumnStorage());
		OperationManager.runScript(bufferSizeInBytes, content);

	}
}
