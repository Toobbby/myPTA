public class Record {
    //ID: 4-byte integer (Primary Key)
    //ClientName: 16-byte long string
    //Phone: 12-byte long string
    int ID;
    String ClientName;
    String Phone;
    String tableName;

    public Record(int ID, String clientName, String phone, String tableName) throws Exception {
        if(ClientName.length() > 16 || Phone.length() > 12) throw new Exception("String Length Exceeded!");
        this.ID = ID;
        this.ClientName = clientName;
        this.Phone = phone;
        this.tableName = tableName;
    }

    @Override
    public String toString(){
        return "(" + ID + "," + ClientName + "," + Phone + ")";
    }
}
