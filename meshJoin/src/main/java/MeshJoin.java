import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.checkerframework.checker.units.qual.A;

import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

public class MeshJoin {
    private Connection sourceConnect=null;
    private Connection destConnect=null;
    private final Integer tuple=100;
    private Integer TransactionsTotal=0;
    private final Integer partitions=10;
    private final Integer numTransactions=50;
    private String user;
    private String password;
    private String source;
    private String dest;
    private int rowsAdded=0;
    ListMultimap<String, Transaction> hashMap = ArrayListMultimap.create();
    ArrayList<MasterData> diskBuffer= new ArrayList<>();
    Queue<ArrayList<String>> pointersQueue = new LinkedList<ArrayList<String>>();

    MeshJoin() throws ClassNotFoundException, SQLException {
    	//building connection with databases
    	user="root";
    	password="1234";
    	source="projectsource";
    	dest="dwh";
        Class.forName("com.mysql.cj.jdbc.Driver");
        this.sourceConnect=DriverManager.getConnection("jdbc:mysql://localhost/"+source+"?"+"user="+user+"&password="+password+"&autoReconnect=true&useSSL=false");
        this.destConnect=DriverManager.getConnection("jdbc:mysql://localhost/"+dest+"?"+"user="+user+"&password="+password+"&autoReconnect=true&useSSL=false");
        
        //counting number of total rows in transactions table
        String sql=("Select count(*) from transactions");
        PreparedStatement pstmt = sourceConnect.prepareStatement(sql);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next())
        	TransactionsTotal=rs.getInt(1);
        
        System.out.println(TransactionsTotal);
        	
    }

    public int getTransactions(Integer id) throws SQLException {
    	//Fetching 50 rows from transaction table
        int latest=0;
        ArrayList<String> productIDs= new ArrayList<>();
        Statement st = sourceConnect.createStatement();
        String sql = ("SELECT * FROM transactions where TRANSACTION_ID > ? ORDER BY TRANSACTION_ID ASC LIMIT ?;");
        PreparedStatement pstmt = sourceConnect.prepareStatement(sql);
        pstmt.setInt(1, id);
        pstmt.setInt(2, numTransactions);
        ResultSet rs = pstmt.executeQuery();
        while(rs.next()) {
            latest=rs.getInt("TRANSACTION_ID");
            Transaction temp = new Transaction();
            temp.Transaction_ID=latest;
            temp.Product_ID=rs.getString("PRODUCT_ID");
            temp.Customer_ID=rs.getString("CUSTOMER_ID");
            temp.Customer_Name=rs.getString("CUSTOMER_NAME");
            temp.Store_ID=rs.getString("STORE_ID");
            temp.Store_Name=rs.getString("STORE_NAME");
            temp.T_Date=rs.getDate("T_DATE");
            temp.Quantity=rs.getInt("QUANTITY");

            productIDs.add(temp.Product_ID);
            this.hashMap.put(temp.Product_ID, temp);
        }
        pointersQueue.add(productIDs);
        System.out.println(latest);
        return latest;
    }

    public void getMasterData(Integer id) throws SQLException {
    	//Fetching one partition from masterdata
        this.diskBuffer.clear();
        Statement st = sourceConnect.createStatement();
        String sql = ("SELECT a.PRODUCT_ID, a.PRODUCT_NAME, a.SUPPLIER_ID, a.SUPPLIER_NAME, a.PRICE FROM\n" +
                "    (SELECT *, ROW_NUMBER() OVER ( ORDER BY PRODUCT_ID ) row_num FROM  masterdata) as a\n" +
                "WHERE a.row_num>?\n" +
                "LIMIT 10;");
        PreparedStatement pstmt = sourceConnect.prepareStatement(sql);
        pstmt.setInt(1, id);
        ResultSet rs = pstmt.executeQuery();
        while(rs.next()) {
            MasterData temp = new MasterData();
            temp.PRODUCT_ID=rs.getString(1);
            temp.PRODUCT_NAME=rs.getString(2);
            temp.SUPPLIER_ID=rs.getString(3);
            temp.SUPPLIER_NAME=rs.getString(4);
            temp.PRICE=rs.getDouble(5);
            this.diskBuffer.add(temp);
        }

    }

    public void addSupplier(String id, String name) throws SQLException{
        String sql1=("INSERT INTO supplier(supplier_id,supplier_name)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as supplier_id, ? as supplier_name) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT supplier_id FROM supplier WHERE supplier_id = ?\n" +
                "    );");
        PreparedStatement pstmt = this.destConnect.prepareStatement(sql1);
        pstmt.setString(1, id);
        pstmt.setString(2, name);
        pstmt.setString(3, id);
        pstmt.executeUpdate();
    }

    public void addCustomer(String id, String name) throws SQLException{
        String sql2=("INSERT INTO customer(customer_id,customer_name)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as customer_id, ? as customer_name) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT customer_id FROM customer WHERE customer_id = ?\n" +
                "    );");
        PreparedStatement pstmt=this.destConnect.prepareStatement(sql2);
        pstmt.setString(1, id);
        pstmt.setString(2, name);
        pstmt.setString(3, id);
        pstmt.executeUpdate();
    }

    public void addStore(String id, String name) throws SQLException{
        String sql3=("INSERT INTO store(store_id,store_name)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as store_id, ? as store_name) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT store_id FROM store WHERE store_id = ?\n" +
                "    );");
        PreparedStatement pstmt=this.destConnect.prepareStatement(sql3);
        pstmt.setString(1, id);
        pstmt.setString(2, name);
        pstmt.setString(3, id);
        pstmt.executeUpdate();
    }

    public void addProduct(String id, String name) throws SQLException{
        String sql4=("INSERT INTO product(product_id,product_name)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as product_id, ? as product_name) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT product_id FROM product WHERE product_id = ?\n" +
                "    );");
        PreparedStatement pstmt=this.destConnect.prepareStatement(sql4);
        pstmt.setString(1, id);
        pstmt.setString(2, name);
        pstmt.setString(3, id);
        pstmt.executeUpdate();

    }

    public void addDate(Date date) throws SQLException {
        String sql5=("INSERT INTO date(date,day, month, quarter, year)\n" +
                "SELECT * FROM\n" +
                "    (SELECT ? as date, ? as day, ? as month, ? as quarter, ? as year) as a\n" +
                "WHERE NOT exists(\n" +
                "        SELECT date FROM date WHERE date = ?\n" +
                "    );");
        PreparedStatement pstmt=this.destConnect.prepareStatement(sql5);
        pstmt.setDate(1,  date);
        String day= new SimpleDateFormat("EEEE", Locale.ENGLISH).format(date);
        pstmt.setString(2, day);
        pstmt.setInt(3, date.getMonth()+1);
        int q;
        if (date.getMonth()+1 <=3)
            q=1;
        else if (date.getMonth()+1 <=6)
            q=2;
        else if (date.getMonth()+1 <=9)
            q=3;
        else
            q=4;
        pstmt.setInt(4, q);
        int year=date.getYear()+1900;
        pstmt.setInt(5, year);
        pstmt.setDate(6, date);
        pstmt.executeUpdate();
    }

    public void addFact(int tid, String pid, String cid, String sid, Date date, String suid, int q, double sale) throws SQLException{
        String sql6=("INSERT INTO transaction_fact value (?, ?, ?, ?, ?, ?, ?, ?);");
        PreparedStatement pstmt=this.destConnect.prepareStatement(sql6);
        pstmt.setInt(1, tid);
        pstmt.setString(2, pid);
        pstmt.setString(3, cid);
        pstmt.setString(4, sid);
        pstmt.setDate(5, date);
        pstmt.setString(6,suid);
        pstmt.setInt(7,q);
        pstmt.setDouble(8,sale);
        pstmt.executeUpdate();
        rowsAdded++;
        System.out.println("Rows added: "+rowsAdded+"*******************");
    }

    public void loadTuple(Transaction tuple) throws SQLException {
        //checking supplier dimension
        this.addSupplier(tuple.Supplier_ID, tuple.Supplier_Name);

        //checking customer dimension
        this.addCustomer(tuple.Customer_ID, tuple.Customer_Name);

        //checking store dimension
        this.addStore(tuple.Store_ID, tuple.Store_Name);

        //checking product dimension
        this.addProduct(tuple.Product_ID,tuple.Product_Name);

        //checking date dimension
        this.addDate((Date) tuple.T_Date);

        //inserting into fact table
        this.addFact(tuple.Transaction_ID,tuple.Product_ID,tuple.Customer_ID, tuple.Store_ID,(Date) tuple.T_Date, tuple.Supplier_ID, tuple.Quantity,tuple.Total_Sale) ;

    }

    public void removeQueueHead() throws SQLException{
        ArrayList<String> toRemove = this.pointersQueue.remove();

        for (String id : toRemove) {
            ArrayList<Transaction> toDelete = new ArrayList<>();
            Collection<Transaction> matched = this.hashMap.get(id);
            for (Transaction t : matched) {
                if (t.Product_Name != null) {
                    toDelete.add(t);
                }
            }
            this.hashMap.get(id).removeAll(toDelete);
        }

    }

    public void applyMeshJoin() throws SQLException {
        int j=0;
        // 1. Reading 50 tuples into hash table and their join attributes into queue
        for (int i=0; i<this.TransactionsTotal; j+=10 ){
            i=this.getTransactions(i);

            // 2. Loading next partition from Master Data in cyclic form
            if (j== this.tuple){
                j=0;
            }
            this.getMasterData(j);

            // 3. Look up all values of the disk buffer against the hash map
            for (int x=0; x<10; x++){
                MasterData current = this.diskBuffer.get(x);
                List<Transaction> match= this.hashMap.get(current.PRODUCT_ID);
                if (!match.isEmpty()){
                    //4. If the tuple matches, add the required attributes
                    for (int y=0 ; y<match.size(); y++){
                        match.get(y).Product_Name= current.PRODUCT_NAME;
                        match.get(y).Supplier_ID= current.SUPPLIER_ID;
                        match.get(y).Supplier_Name= current.SUPPLIER_NAME;
                        match.get(y).Total_Sale=match.get(y).Quantity*current.PRICE;

                        // 5. Load tuple in DWH
                        this.loadTuple(match.get(y));
                    }
                }
            }

            //6. Once all MasterData tuples checked, remove last element in queue
            if (this.pointersQueue.size()==this.partitions) {
                this.removeQueueHead();
            }
        }
        //empty the remaining queue
        for (int i=0; i<this.partitions-1; i++, j+=10)
        {
            if (j== this.tuple){
                j=0;
            }
            this.getMasterData(j);

            for (int x=0; x<10; x++){
                MasterData current = this.diskBuffer.get(x);
                List<Transaction> match= this.hashMap.get(current.PRODUCT_ID);
                if (!match.isEmpty()){
                    //4. If the tuple matches, add the required attributes
                    for (int y=0 ; y<match.size(); y++){
                        match.get(y).Product_Name= current.PRODUCT_NAME;
                        match.get(y).Supplier_ID= current.SUPPLIER_ID;
                        match.get(y).Supplier_Name= current.SUPPLIER_NAME;
                        match.get(y).Total_Sale=match.get(y).Quantity*current.PRICE;

                        // 5. Load tuple in DWH
                        this.loadTuple(match.get(y));
                    }
                }
            }
            //System.out.println("Queue Remaining size is : "+this.pointersQueue.size());
            this.removeQueueHead();

        }
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        MeshJoin MJ = new MeshJoin();
        MJ.applyMeshJoin();
    }
}

