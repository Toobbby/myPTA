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

    public int getID() {
        return ID;
    }

    public void setID(int ID) {
        this.ID = ID;
    }

    public String getClientName() {
        return ClientName;
    }

    public void setClientName(String clientName) {
        ClientName = clientName;
    }

    public String getPhone() {
        return Phone;
    }

    public void setPhone(String phone) {
        Phone = phone;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
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
