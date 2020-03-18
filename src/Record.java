public class Record {
    //ID: 4-byte integer (Primary Key)
    //ClientName: 16-byte long string
    //Phone: 12-byte long string
    public int ID;
    public String ClientName;
    public String Phone;
    public String tableName;

    public Record(int id, String clientName, String phone, String tableName) throws Exception {
        //if(clientName.length() > 16 || phone.length() > 12) throw new Exception("String Length Exceeded!");
        ID = id;
        ClientName = clientName;
        Phone = phone;
        tableName = tableName;
    }

    public int getID() {
        return ID;
    }

    public void setID(int id) {
        ID = id;
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

    public void setTableName(String tablename) {
        tableName = tablename;
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
