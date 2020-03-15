public class Record {
    //ID: 4-byte integer (Primary Key)
    //ClientName: 16-byte long string
    //Phone: 12-byte long string
    int ID;
    String ClientName;
    String Phone;
    String tableName;

    public Record(int ID, String clientName, String phone, String tableName) {
        this.ID = ID;
        ClientName = clientName;
        Phone = phone;
        this.tableName = tableName;
    }


    //need discuss
    public Record(String stringRecord){
        String[] parts=stringRecord.split(",");
        ID=Integer.parseInt(parts[0].substring(1));
        ClientName=parts[1];
        Phone=parts[2].substring(0,parts[2].length()-1);
    }

    @Override
    public String toString(){
        return "(" + ID + "," + ClientName + "," + Phone + ")";
    }
}
