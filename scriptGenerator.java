import java.io.*;
import java.util.Random;

public class scriptGenerator {
    static String folderLoc = "./benchmark/";
    static int randomSeed = 0;
    public static void main(String args[]) throws IOException {
        BufferedWriter bw = null;
        randomSeed = Integer.parseInt(args[0]);
        String isolation = args[1]; //isolation level "0": read committed; "1": serializable
        for(int i = 0; i < 100; i++){
            File script = new File(folderLoc + "script" + i + ".txt");
            if(!script.exists()) script.createNewFile();
            FileWriter fw = new FileWriter(script);
            bw = new BufferedWriter(fw);
            Random r = new Random(randomSeed + i);
            writeScript(bw, randomSeed, isolation, r);
        }
    }

    static void writeScript(BufferedWriter bw ,int seed, String isolation, Random r) throws IOException {
        bw.write("B " + isolation);
        bw.newLine();
        bw.flush();
        for(int j = 0; j < 1000; j++){
            int commandType = r.nextInt(5); // 0: R; 1: M; 2: D; 3: W; 4:E
            writeLine(commandType, r, bw);
            bw.newLine();
            bw.flush();
        }
        bw.write("C");
        bw.flush();
        if(bw != null) bw.close();

    }

    static void writeLine(int commandType, Random r, BufferedWriter bw) throws IOException {
        String[] tables = new String[3];
        tables[0] = "X";
        tables[1] = "Y";
        tables[2] = "Z";
        switch(commandType){
            case 0:
                bw.write("R " + tables[r.nextInt(3)] + " " + r.nextInt(100));
                break;
            case 1:
                bw.write("M " + tables[r.nextInt(3)] + " " + (r.nextInt(900) + 100));
                break;
            case 2:
                bw.write("R " + tables[r.nextInt(3)] + " "  + r.nextInt(100));
                break;
            case 3:
                bw.write("W " + tables[r.nextInt(3)] + " " + "(" + r.nextInt(100) + ", " + "Felix, " + (r.nextInt(900) + 100) + "-" + "111-1111" + ")");
                break;
            case 4:
                bw.write("E " + tables[r.nextInt(3)] + " " + r.nextInt(100));
                break;
        }


    }
}
