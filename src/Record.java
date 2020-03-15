public class Record {
    //ID: 4-byte integer (Primary Key)
    //ClientName: 16-byte long string
    //Phone: 12-byte long string
    int ID;
    String ClientName;
    String Phone;
    String tableName;

    public Record(){}
    public Record(String stringRecord){
        String[] parts= stringRecord.split(",");
        ID=Integer.parseInt(parts[0].substring(1));
        Phone=parts[2].substring(0,parts[2].length()-1);
        ClientName=parts[1];
    }
    @Override
    public String toString(){
        return "(" + ID + "," + ClientName + "," + Phone + ")";
    }
}
