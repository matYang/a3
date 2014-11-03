import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class A3 {
    // The JDBC Connector Class.
	
    private static final String dbClassName = "com.mysql.jdbc.Driver";
    // Connection string. cs348 is the database the program is trying to connection
    // is connecting to,127.0.0.1 is the local loopback IP address for this machine, user name for the connection 
    // is root, password is cs348
    // private static final String CONNECTION_STRING = "jdbc:mysql://127.0.0.1/tpch?user=root&password=cs348&Database=tpch;";
    public static final String GETV = "GETV";
    public static final String SETV = "SETV";
    public static final String SETM = "SETM";
    public static final String DELETE = "DELETE";
    public static final String ADD = "ADD";
    public static final String SUB = "SUB";
    public static final String MULT = "MULT";
    public static final String TRANSPOSE = "TRANSPOSE";
    public static final String SQL = "SQL";
    
    public static final String DONE = "DONE";
    public static final String ERROR = "ERROR";
    
    private static class DBException extends RuntimeException{
		private static final long serialVersionUID = 7105074796770678818L;
		
		private String message;
    	
//    	public DBException(String message){
//    		super();
//    		this.message = message;
//    	}
    	
    	public DBException(){
    		super();
    		this.message = ERROR;
    	}
    	
    	public String message(){
    		return this.message;
    	}
    }
    
    
    private static class Matrix{
    	
    	public int ID;
    	public int ROW_DIM;
    	public int COL_DIM;
    	public double[][] data;
    	private Connection conn;
    	
    	public Matrix(int id, Connection conn){
    		this.ID = id;
    		this.ROW_DIM = 0;
    		this.COL_DIM = 0;
    		this.data = null;
    		this.conn = conn;
    	}
    	
    	public Matrix(int id, Connection conn, int row_dim, int col_dim){
    		this.ID = id;
    		this.ROW_DIM = row_dim;
    		this.COL_DIM = col_dim;
    		this.data = new double[row_dim][col_dim];
    		this.conn = conn;
    		for (int i = 0; i < this.ROW_DIM; i++){
    			for (int j = 0; j < this.COL_DIM; j++){
    				this.data[i][j] = 0;
    			}
    		}
    	}
    	
    	public void exeFetchDimension() throws SQLException{
    		String query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + this.ID;
            Statement stmt = this.conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            //if empty, throw exp
            if (!rs.next()){
            	throw new DBException();
            }
            this.ROW_DIM = rs.getInt("ROW_DIM");
            this.COL_DIM = rs.getInt("COL_DIM");   
    	}
    	
    	public double exeFetchValue(int row, int col) throws SQLException{
    		this.exeFetchDimension();
    		
    		//out of range
    		if (row <= 0 || row > this.ROW_DIM || col <= 0 || col > this.COL_DIM){
				throw new DBException();
			}
    		
    		String query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + this.ID + " AND ROW_NUM = " + row + " AND COL_NUM = " + col;
    		Statement stmt = this.conn.createStatement();
    		ResultSet rs = stmt.executeQuery(query);
    		
    		//not exist, but in range, then it means it is 0
    		if (!rs.next()){
    			return 0.0;
    		}
    		else{
    			return rs.getDouble("VALUE");
    		}
    	}
    	
    	public void exeSetValue(int row, int col, double val) throws SQLException{
    		this.exeFetchDimension();
    		
    		//out of range
    		if (row <= 0 || row > this.ROW_DIM || col <= 0 || col > this.COL_DIM){
				throw new DBException();
			}
    		
    		String query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + this.ID + " AND ROW_NUM = " + row + " AND COL_NUM = " + col;
    		Statement stmt = this.conn.createStatement();
    		ResultSet rs = stmt.executeQuery(query);
    		
    		//not exist, but in range, then use insert
    		if (!rs.next()){
    			PreparedStatement p = this.conn.prepareStatement("INSERT INTO MATRIX_DATA VALUES(?,?,?,?)");
				p.setInt(1, this.ID);
				p.setInt(2, row);
				p.setInt(3, col);
				p.setDouble(4, val);
				p.executeUpdate();
				p.close();
    		}
    		//exist, use update
    		else{
    			PreparedStatement p = this.conn.prepareStatement("UPDATE MATRIX_DATA set VALUE = ? where MATRIX_ID = ? and ROW_NUM = ? and COL_NUM = ?");
    			p.setDouble(1, val);
    			p.setInt(2, this.ID);
    			p.setInt(3, row);
    			p.setInt(4, col);
    			p.executeUpdate();
    			p.close();
    		}
    		
    	}
    	
    	public void exeDelete() throws SQLException{
    		PreparedStatement p = this.conn.prepareStatement("DELETE FROM MATRIX where MATRIX_ID = ?");
    		p.setInt(1, this.ID);
			p.executeUpdate();
			
			p = this.conn.prepareStatement( "DELETE FROM MATRIX_DATA where MATRIX_ID = ?" );
    		p.setInt(1, this.ID);
			p.executeUpdate();
			p.close();
    	}
    	
    	public void exeSetDimension(int row_dim, int col_dim) throws SQLException{
    		String query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + this.ID;
            Statement stmt = this.conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            //if empty, throw exp
            if (!rs.next()){
            	PreparedStatement p = this.conn.prepareStatement("INSERT INTO MATRIX VALUES(?,?,?)");
				p.setInt(1, this.ID);
				p.setInt(2, row_dim);
				p.setInt(3, col_dim);
				p.executeUpdate();
				p.close();
				this.ROW_DIM = row_dim;
				this.COL_DIM = col_dim;
            	return;
            }
            this.ROW_DIM = rs.getInt("ROW_DIM");
            this.COL_DIM = rs.getInt("COL_DIM");
            
            //contraction
            if (row_dim < this.ROW_DIM){
            	query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + this.ID + " AND ROW_NUM > " + row_dim + " AND ROW_NUM < " + (this.ROW_DIM + 1);
            	stmt = this.conn.createStatement();
            	rs = stmt.executeQuery(query);
            	if (rs.next()){
            		throw new DBException();
            	}
            }
            if (col_dim < this.COL_DIM){
            	query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + this.ID + " AND COL_NUM > " + col_dim + " AND COL_NUM < " + (this.COL_DIM + 1);
            	stmt = this.conn.createStatement();
            	rs = stmt.executeQuery(query);
            	if (rs.next()){
            		throw new DBException();
            	}
            }
            PreparedStatement p = this.conn.prepareStatement("UPDATE MATRIX set ROW_DIM = ?, COL_DIM = ? where MATRIX_ID = ?");
            p.setInt(1, row_dim);
			p.setInt(2, col_dim);
			p.setInt(3, this.ID);
			p.executeUpdate();
			p.close();
            
    	}
    	
    	public Matrix add(Matrix m){
			if (this.ROW_DIM != m.ROW_DIM || this.COL_DIM != m.COL_DIM){
				throw new DBException();
			}
			
			Matrix result = new Matrix(-1, this.conn, this.ROW_DIM, this.COL_DIM);
			
    		for (int i = 0; i < this.ROW_DIM; i++){
    			for (int j = 0; j < this.COL_DIM; j++){
    				result.data[i][j] = this.data[i][j] + m.data[i][j];
    			}
    		}
    		
    		return result;
    	}
    	
    	public Matrix subtract(Matrix m){
			if (this.ROW_DIM != m.ROW_DIM || this.COL_DIM != m.COL_DIM){
				throw new DBException();
			}
			
			Matrix result = new Matrix(-1, this.conn, this.ROW_DIM, this.COL_DIM);
			
    		for (int i = 0; i < this.ROW_DIM; i++){
    			for (int j = 0; j < this.COL_DIM; j++){
    				result.data[i][j] = this.data[i][j] - m.data[i][j];
    			}
    		}
    		
    		return result;
    	}
    	
    	public Matrix multiply(Matrix m){
    		if (this.COL_DIM != m.ROW_DIM){
				throw new DBException();
			}
    		
    		Matrix result = new Matrix(-1, this.conn, this.ROW_DIM, m.COL_DIM);

            for (int i = 0; i < this.ROW_DIM; i++) {
                for (int j = 0; j < m.COL_DIM; j++) {
                    for (int k = 0; k < this.COL_DIM; k++) {
                        result.data[i][j] += this.data[i][k] * m.data[k][j];
                    }
                }
            }
            
            return result;
    	}
    	
    	public Matrix transpose(){	
    		Matrix result = new Matrix(-1, this.conn, this.COL_DIM, this.ROW_DIM);

    		for (int i = 0; i < this.ROW_DIM; i++){
                for (int j = 0; j < this.COL_DIM; j++){
                    result.data[j][i] = this.data[i][j];
                }
    		}
            
            return result;
    	}
    	
    	
    	public void exeFetchData() throws SQLException{
    		this.exeFetchDimension();
    		
    		this.data = new double[this.ROW_DIM][this.COL_DIM];
    		for (int i = 0; i < this.ROW_DIM; i++){
    			for (int j = 0; j < this.COL_DIM; j++){
    				this.data[i][j] = 0.0;
    			}
    		}
    		String query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + this.ID;
    		Statement stmt = this.conn.createStatement();
    		ResultSet rs = stmt.executeQuery(query);
    		while (rs.next()){
    			int row = rs.getInt("ROW_NUM");
    			int col = rs.getInt("COL_NUM");
    			double val = rs.getDouble("VALUE");
    			if (row <= 0 || row > this.ROW_DIM || col <= 0 || col > this.COL_DIM){
    				throw new DBException();
    			}
    			this.data[row-1][col-1] = val;
    		}
    	}
    	
    	public void exeCreateData() throws SQLException{
    		PreparedStatement p = this.conn.prepareStatement("INSERT INTO MATRIX VALUES(?,?,?)");
			p.setInt(1, this.ID);
			p.setInt(2, this.ROW_DIM);
			p.setInt(3, this.COL_DIM);
			p.executeUpdate();
			p.close();
			
			for (int i = 0; i < this.ROW_DIM; i++){
				for (int j = 0; j < this.COL_DIM; j++) {
					if (data[i][j] != 0){
						p = this.conn.prepareStatement("INSERT INTO MATRIX_DATA VALUES(?,?,?,?)");
						p.setInt(1, this.ID);
						p.setInt(2, i+1);
						p.setInt(3, j+1);
						p.setDouble(4, data[i][j]);
						p.executeUpdate();
						p.close();
					}
				}
			}
    	}
    	
    }
    
    
    private static void processCmd(String cmd, Connection conn) throws SQLException{
    	if (cmd == null){
    		throw new DBException();
    	}
    	//empty command, do nothing
    	if (cmd.length() == 0){
    		return;
    	}
    	
    	String[] cmdArr = cmd.split("\\s+");
    	try{
    		if (cmdArr[0].equals(GETV)){
        		int id = Integer.valueOf(cmdArr[1]);
        		int row = Integer.valueOf(cmdArr[2]);
        		int col = Integer.valueOf(cmdArr[3]);

        		Matrix m = new Matrix(id, conn);
        		double val = m.exeFetchValue(row, col);
        		System.out.println(val);
        	}
    		else if (cmdArr[0].equals(SETV)){
    			int id = Integer.valueOf(cmdArr[1]);
        		int row = Integer.valueOf(cmdArr[2]);
        		int col = Integer.valueOf(cmdArr[3]);
        		double val = Double.valueOf(cmdArr[4]);
        		
        		Matrix m = new Matrix(id, conn);
        		m.exeSetValue(row, col, val);
        		System.out.println(DONE);
        		
    		}
    		else if (cmdArr[0].equals(SETM)){
    			int id = Integer.valueOf(cmdArr[1]);
        		int row_dim = Integer.valueOf(cmdArr[2]);
        		int col_dim = Integer.valueOf(cmdArr[3]);
        		
        		Matrix m = new Matrix(id, conn);
        		m.exeSetDimension(row_dim, col_dim);
        		System.out.println(DONE);
    		}
    		else if (cmdArr[0].equals(DELETE)){
    			if (cmdArr[1].equals("ALL")){
    				PreparedStatement p = conn.prepareStatement("DELETE FROM MATRIX");
    				p.executeUpdate();
    				p.close();
    				
    				p = conn.prepareStatement("DELETE FROM MATRIX_DATA");
    				p.executeUpdate();
    				p.close();
    			}
    			else{
    				int id = Integer.valueOf(cmdArr[1]);
    				Matrix m = new Matrix(id, conn);
    				m.exeDelete();
    			}
    			System.out.println(DONE);
    		}
    		else if (cmdArr[0].equals(ADD)){
    			int id_1 = Integer.valueOf(cmdArr[1]);
    			int id_2 = Integer.valueOf(cmdArr[2]);
    			int id_3 = Integer.valueOf(cmdArr[3]);
    			
    			Matrix m2 = new Matrix(id_2, conn);
    			Matrix m3 = new Matrix(id_3, conn);
    			
    			m2.exeFetchDimension();
    			m3.exeFetchDimension();
    			
    			if (m2.ROW_DIM != m3.ROW_DIM || m2.COL_DIM != m3.COL_DIM){
    				throw new DBException();
    			}
    			m2.exeFetchData();
    			m3.exeFetchData();
    			
    			Matrix m1 = m2.add(m3);
    			m1.ID = id_1;
    			m1.exeDelete();
    			m1.exeCreateData();
    			
    			System.out.println(DONE);
    		}
    		else if (cmdArr[0].equals(SUB)){
    			int id_1 = Integer.valueOf(cmdArr[1]);
    			int id_2 = Integer.valueOf(cmdArr[2]);
    			int id_3 = Integer.valueOf(cmdArr[3]);
    			
    			Matrix m2 = new Matrix(id_2, conn);
    			Matrix m3 = new Matrix(id_3, conn);
    			
    			m2.exeFetchDimension();
    			m3.exeFetchDimension();
    			
    			if (m2.ROW_DIM != m3.ROW_DIM || m2.COL_DIM != m3.COL_DIM){
    				throw new DBException();
    			}
    			
    			m2.exeFetchData();
    			m3.exeFetchData();
    			
    			Matrix m1 = m2.subtract(m3);
    			m1.ID = id_1;
    			m1.exeDelete();
    			m1.exeCreateData();
    			
    			System.out.println(DONE);
    		}
    		else if (cmdArr[0].equals(MULT)){
    			int id_1 = Integer.valueOf(cmdArr[1]);
    			int id_2 = Integer.valueOf(cmdArr[2]);
    			int id_3 = Integer.valueOf(cmdArr[3]);

    			Matrix m2 = new Matrix(id_2, conn);
    			Matrix m3 = new Matrix(id_3, conn);
    			
    			m2.exeFetchDimension();
    			m3.exeFetchDimension();
    			
    			if (m2.COL_DIM != m3.ROW_DIM){
    				throw new DBException();
    			}
    			
    			m2.exeFetchData();
    			m3.exeFetchData();
    			
    			Matrix m1 = m2.multiply(m3);
    			m1.ID = id_1;
    			m1.exeDelete();
    			m1.exeCreateData();
    			
    			System.out.println(DONE);
    		}
    		else if (cmdArr[0].equals(TRANSPOSE)){
    			int id_1 = Integer.valueOf(cmdArr[1]);
    			int id_2 = Integer.valueOf(cmdArr[2]);
    			
    			Matrix m2 = new Matrix(id_2, conn);
    			
    			m2.exeFetchData();
    			
    			Matrix m1 = m2.transpose();
    			m1.ID = id_1;
    			m1.exeDelete();
    			m1.exeCreateData();
    			
    			System.out.println(DONE);
    		}
    		else if (cmdArr[0].equals(SQL)){
    			String query = cmd.split("\\s+", 2)[1];

                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                if (!rs.next()){
                	throw new DBException();
                }
                else{
                	System.out.println(rs.getString(1));
                }
    		}
    		else{
    			throw new DBException();
    		}
    	} catch (DBException e){
    		//e.printStackTrace();
			//System.out.println(e.message());
    		System.out.println(ERROR);
		} catch (Throwable t){
			//e.printStackTrace();
			//System.out.println("Unexpect error : " + e.getMessage());
			System.out.println(ERROR);
		}
    	
    }
    
    public static void main(String[] args) throws ClassNotFoundException,SQLException{
    	String CONNECTION_STRING = args[0];
    	String inputFile = args[1];
    	
    	// Try to connect
        Connection conn = null;
        try{
        	Class.forName(dbClassName).newInstance();
        	conn = DriverManager.getConnection(CONNECTION_STRING);
        	
        	BufferedReader br = new BufferedReader(new FileReader(inputFile));
        	String line;
        	while ((line = br.readLine()) != null) {
        		processCmd(line, conn);
        	}
        	br.close(); 	
        	
        } catch (Throwable e){
        	//e.printStackTrace();
        	System.out.println(ERROR);
        } finally{
        	conn.close();
        }
        
    }
    
    
}
