package main.resources;

import java.io.*;
import java.sql.*;
import java.util.Properties;

public class A3{
    
    public enum Actions {
        GETV,
        SETV,
        SETM,
        DELETE,
        ADD,
        SUB,
        MULT,
        TRANSPOSE,
        SQL
		  }
    
    public static void main(String[] args) throws SQLException{
        String CONNECTION_STRING = "";
        Connection con = null;
        try{
            CONNECTION_STRING = args[2].replace("\"","");
            String file = args[3];
            con = DriverManager.getConnection(CONNECTION_STRING);
            
            BufferedReader br = new BufferedReader(new FileReader(file));
            String strLine;
            while ((strLine = br.readLine()) != null){
                try{
                    String[] split = strLine.split("\\s+");
                    String command = split[0];
                    Actions cmd = Actions.valueOf(command.toUpperCase());
                    
                    switch(cmd){
                        case GETV:{
                            int matrix_id = Integer.parseInt(split[1]);
                            int row_num = Integer.parseInt(split[2]);
                            int col_num = Integer.parseInt(split[3]);
                            String query = "SELECT * FROM MATRIX WHERE MARIX_ID = " + matrix_id;
                            Statement stmt = con.createStatement();
                            ResultSet rs = stmt.executeQuery(query);
                            int row_dim = rs.getInt("ROW_DIM");
                            int col_dim = rs.getInt("COL_DIM");
                            
                            if(row_num > row_dim || col_num >col_dim || row_num == 0 || col_num == 0){
                                System.out.println("ERROR");
                            }else{
                                query = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix_id;
                                rs = stmt.executeQuery(query);
                                boolean found = false;
                                while(rs.next()){
                                    if(rs.getInt("ROW_NUM")==row_num &&rs.getInt("COL_NUM") == col_num ){
                                        System.out.println(rs.getInt("VALUE"));
                                        found = true;
                                        break;
                                    }
                                }
                                if(!found){
                                    System.out.println("ERROR");
                                }
                            }
                            break;
                        }
                        case SETV:{
                            int matrix_id = Integer.parseInt(split[1]);
                            int row_num = Integer.parseInt(split[2]);
                            int col_num = Integer.parseInt(split[3]);
                            int value = Integer.parseInt(split[4]);
                            String query = "SELECT * FROM MATRIX WHERE MARIX_ID = " + matrix_id;
                            Statement stmt = con.createStatement();
                            ResultSet rs = stmt.executeQuery(query);
                            int row_dim = rs.getInt("ROW_DIM");
                            int col_dim = rs.getInt("COL_DIM");
                            
                            if(row_num > row_dim || col_num >col_dim || row_num == 0 || col_num == 0){
                                System.out.println("ERROR");
                            }else{
                                query = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix_id;
                                rs = stmt.executeQuery(query);
                                boolean updated = false;
                                while(rs.next()){
                                    if(rs.getInt("ROW_NUM")==row_num &&rs.getInt("COL_NUM") == col_num ){
                                        rs.updateInt("VALUE",value);
                                    }
                                }
                                if(!updated){
                                    PreparedStatement p = con.prepareStatement(
                                                                               query = "INSERT INTO MAXTRIX_DATA VALUES(?,?,?,?)");
                                    p.setInt(1,matrix_id);
                                    p.setInt(2,row_num);
                                    p.setInt(3,col_num);
                                    p.setInt(4, value);
                                    p.executeUpdate();
                                }
                            }
                            System.out.println("DONE");
                            break;
                        }
                        case SETM:{
                            int matrix_id = Integer.parseInt(split[1]);
                            int row_num = Integer.parseInt(split[2]);
                            int col_num = Integer.parseInt(split[3]);
                            String query = "SELECT * FROM MATRIX WHERE MARIX_ID = " + matrix_id;
                            Statement stmt = con.createStatement();
                            ResultSet rs = stmt.executeQuery(query);
                            
                            if(rs.wasNull()){
                                PreparedStatement p = con.prepareStatement( "INSERT INTO MAXTRIX VALUES(?,?,?)");
                                p.setInt(1,matrix_id);
                                p.setInt(2,row_num);
                                p.setInt(3,col_num);
                                p.executeUpdate();
                                System.out.println("DONE");
                                break;
                            }else{
                                PreparedStatement p = con.prepareStatement(
                                                                           "UPDATE MAXTRIX "+"set ROW_DIM = ?, set COL_DIM = ? where MATRIX_ID = ?");
                                int row_dim = rs.getInt("ROW_DIM");
                                int col_dim = rs.getInt("COL_DIM");
                                if( row_num >= row_dim && col_num >= col_dim ){
                                    p.setInt(1,row_num);
                                    p.setInt(2,col_num);
                                    p.setInt(3,matrix_id);
                                    p.executeUpdate();
                                }else if( row_num >= row_dim && col_num < col_dim){
                                    query = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix_id;
                                    rs = stmt.executeQuery(query);
                                    boolean found = false;
                                    while(rs.next()){
                                        if(rs.getInt("COL_NUM")<= col_dim && rs.getInt("COL_NUM")> col_num){
                                            found = true;
                                            break;
                                        }
                                    }
                                    if(found){
                                        System.out.println("ERROR");
                                        break;
                                    }else{
                                        p.setInt(1,matrix_id);
                                        p.setInt(2,row_num);
                                        p.setInt(3,col_num);
                                        p.executeUpdate();
                                        System.out.println("DONE");
                                        break;
                                    }
                                }else if (row_num < row_dim && col_num >= col_dim){
                                    query = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix_id;
                                    rs = stmt.executeQuery(query);
                                    boolean found = false;
                                    while(rs.next()){
                                        if(rs.getInt("ROW_NUM")<= row_dim && rs.getInt("ROW_NUM")> row_num){
                                            found = true;
                                            break;
                                        }
                                    }
                                    if(found){
                                        System.out.println("ERROR");
                                        break;
                                    }else{
                                        p.setInt(1,matrix_id);
                                        p.setInt(2,row_num);
                                        p.setInt(3,col_num);
                                        p.executeUpdate();
                                        System.out.println("DONE");
                                        break;
                                    }
                                }else if (row_num < row_dim && col_num < col_dim){
                                    query = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix_id;
                                    rs = stmt.executeQuery(query);
                                    boolean found = false;
                                    while(rs.next()){
                                        if(rs.getInt("ROW_NUM")<= row_dim && rs.getInt("ROW_NUM")> row_num||
                                           rs.getInt("COL_NUM")<= col_dim && rs.getInt("COL_NUM")> col_num){
                                            found = true;
                                            break;
                                        }
                                    }
                                    if(found){
                                        System.out.println("ERROR");
                                        break;
                                    }else{
                                        p.setInt(1,matrix_id);
                                        p.setInt(2,row_num);
                                        p.setInt(3,col_num);
                                        p.executeUpdate();
                                        System.out.println("DONE");
                                        break;
                                    }
                                }
                            }
                        
                            if(split[1] == "ALL"){
                                query = "DELETE * FROM MATRIX ";
                                rs = stmt.executeQuery(query);
                                query = "DELETE * FROM MATRIX_DATA ";
                                rs = stmt.executeQuery(query);
                                System.out.println("DONE");
                                break;
                            }else{
                                matrix_id = Integer.parseInt(split[1]);
                                PreparedStatement p = con.prepareStatement(
                                                                           "DELETE FROM ? WHERE MAXTRIX_ID = ?");
                                p.setString(1,"MATRIX");
                                p.setInt(2,matrix_id);
                                p.executeUpdate();
                                
                                p.setString(1,"MATRIX_DATA");
                                p.setInt(2,matrix_id);
                                p.executeUpdate();
                            }
                           
                            
                            System.out.println("DONE");
                            break;
                        }    
                        case ADD:{
                            int matrix1 = Integer.parseInt(split[1]);
                            int matrix2 = Integer.parseInt(split[2]);
                            int matrix3 = Integer.parseInt(split[3]);
                            
                            String query = "DELETE * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix1;

                            String query2 = "SELECT * FROM MATRIX WHERE MARIX_ID = " + matrix2;
                            String query3 = "SELECT * FROM MATRIX WHERE MARIX_ID = " + matrix3;
                            Statement stmt = con.createStatement();
                            ResultSet rs2 = stmt.executeQuery(query2);
                            ResultSet rs3 = stmt.executeQuery(query3);
                            
                            if(rs2.getInt("ROW_DIM")!=rs3.getInt("ROW_DIM")||rs2.getInt("COL_DIM")!=rs3.getInt("COL_DIM")){
                                System.out.println("ERROR");
                                break;
                            }else{
                                ResultSet rs = stmt.executeQuery(query);
                                query2 = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix2;
                                query3 = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix3;
                                rs2 = stmt.executeQuery(query2);
                                rs3 = stmt.executeQuery(query3);
                                PreparedStatement p = con.prepareStatement(
                                                                           "INSERT INTO MAXTRIX_DATA VALUES(?,?,?,?)");
                                while(rs2.next()){
                                    rs3 = stmt.executeQuery(query3);
                                    while(rs3.next()){
                                        if(rs2.getInt("ROW_NUM") == rs3.getInt("ROW_NUM") && rs2.getInt("COL_NUM") == rs3.getInt("COL_NUM") ){
                                            p.setInt(1,matrix1);
                                            p.setInt(2,rs2.getInt("ROW_NUM"));
                                            p.setInt(3,rs2.getInt("COL_NUM"));
                                            p.setInt(4,rs2.getInt("VALUE")+rs3.getInt("VALUE"));
                                            p.executeUpdate();
                                        }else{
                                            p.setInt(1,matrix1);
                                            p.setInt(2,rs2.getInt("ROW_NUM"));
                                            p.setInt(3,rs2.getInt("COL_NUM"));
                                            p.setInt(4,rs2.getInt("VALUE"));
                                            p.executeUpdate();
                                        }
                                    }
                                }
                                
                                rs2 = stmt.executeQuery(query2);
                                rs3 = stmt.executeQuery(query3);

                                while(rs3.next()){
                                    rs2 = stmt.executeQuery(query2);
                                    while(rs2.next()){
                                        if(rs2.getInt("ROW_NUM") == rs3.getInt("ROW_NUM") && rs2.getInt("COL_NUM") == rs3.getInt("COL_NUM") ){}else{
                                            p.setInt(1,matrix1);
                                            p.setInt(2,rs3.getInt("ROW_NUM"));
                                            p.setInt(3,rs3.getInt("COL_NUM"));
                                            p.setInt(4,rs3.getInt("VALUE"));
                                            p.executeUpdate();
                                        }
                                    }
                                }
                                
                                System.out.println("DONE");
                                break;
                            }
                        }
                        case SUB:{
                            int matrix1 = Integer.parseInt(split[1]);
                            int matrix2 = Integer.parseInt(split[2]);
                            int matrix3 = Integer.parseInt(split[3]);
                            
                            String query = "DELETE * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix1;
                            
                            String query2 = "SELECT * FROM MATRIX WHERE MARIX_ID = " + matrix2;
                            String query3 = "SELECT * FROM MATRIX WHERE MARIX_ID = " + matrix3;
                            Statement stmt = con.createStatement();
                            ResultSet rs2 = stmt.executeQuery(query2);
                            ResultSet rs3 = stmt.executeQuery(query3);
                            
                            if(rs2.getInt("ROW_DIM")!=rs3.getInt("ROW_DIM")||rs2.getInt("COL_DIM")!=rs3.getInt("COL_DIM")){
                                System.out.println("ERROR");
                                break;
                            }else{
                                ResultSet rs = stmt.executeQuery(query);
                                query2 = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix2;
                                query3 = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix3;
                                rs2 = stmt.executeQuery(query2);
                                rs3 = stmt.executeQuery(query3);
                                PreparedStatement p = con.prepareStatement(
                                                                           "INSERT INTO MAXTRIX_DATA VALUES(?,?,?,?)");
                                while(rs2.next()){
                                    rs3 = stmt.executeQuery(query3);
                                    while(rs3.next()){
                                        if(rs2.getInt("ROW_NUM") == rs3.getInt("ROW_NUM") && rs2.getInt("COL_NUM") == rs3.getInt("COL_NUM") ){
                                            p.setInt(1,matrix1);
                                            p.setInt(2,rs2.getInt("ROW_NUM"));
                                            p.setInt(3,rs2.getInt("COL_NUM"));
                                            p.setInt(4,rs2.getInt("VALUE")-rs3.getInt("VALUE"));
                                            p.executeUpdate();
                                        }else{
                                            p.setInt(1,matrix1);
                                            p.setInt(2,rs2.getInt("ROW_NUM"));
                                            p.setInt(3,rs2.getInt("COL_NUM"));
                                            p.setInt(4,rs2.getInt("VALUE"));
                                            p.executeUpdate();
                                        }
                                    }
                                }
                                
                                rs2 = stmt.executeQuery(query2);
                                rs3 = stmt.executeQuery(query3);
                                
                                while(rs3.next()){
                                    rs2 = stmt.executeQuery(query2);
                                    while(rs2.next()){
                                        if(rs2.getInt("ROW_NUM") == rs3.getInt("ROW_NUM") && rs2.getInt("COL_NUM") == rs3.getInt("COL_NUM") ){}else{
                                            p.setInt(1,matrix1);
                                            p.setInt(2,rs3.getInt("ROW_NUM"));
                                            p.setInt(3,rs3.getInt("COL_NUM"));
                                            p.setInt(4,rs3.getInt("VALUE"));
                                            p.executeUpdate();
                                        }
                                    }
                                }
                                
                                System.out.println("DONE");
                                break;
                            }
                        }
                        case MULT:{
                            int matrix1 = Integer.parseInt(split[1]);
                            int matrix2 = Integer.parseInt(split[2]);
                            int matrix3 = Integer.parseInt(split[3]);
                            
                            String query = "DELETE * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix1;
                            
                            String query2 = "SELECT * FROM MATRIX WHERE MARIX_ID = " + matrix2;
                            String query3 = "SELECT * FROM MATRIX WHERE MARIX_ID = " + matrix3;
                            Statement stmt = con.createStatement();
                            ResultSet rs2 = stmt.executeQuery(query2);
                            ResultSet rs3 = stmt.executeQuery(query3);
                            int m2rowdim = rs2.getInt("ROW_DIM");
                            int m3rowdim = rs3.getInt("ROW_DIM");
                            
                            if(rs2.getInt("COL_DIM")!=rs3.getInt("ROW_DIM")||rs2.getInt("ROW_DIM")!=rs3.getInt("COL_DIM")){
                                System.out.println("ERROR");
                                break;
                            }else{
                                query = "DELETE * FROM MATRIX WHERE MARIX_ID = " + matrix1;
                                ResultSet rs = stmt.executeQuery(query);
                                query = "DELETE * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix1;
                                rs = stmt.executeQuery(query);
                                query = "DELETE * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix1;
                                PreparedStatement p_all = con.prepareStatement(
                                                                           "INSERT INTO MAXTRIX VALUES(?,?,?)");
                                p_all.setInt(1,matrix1);
                                p_all.setInt(2,rs2.getInt("ROW_NUM"));
                                p_all.setInt(3,rs3.getInt("COL_NUM"));
                                
                                query2 = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix2;
                                query3 = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix3;
                                rs2 = stmt.executeQuery(query2);
                                rs3 = stmt.executeQuery(query3);
                                
                                
                                PreparedStatement p = con.prepareStatement(
                                                                           "INSERT INTO MAXTRIX_DATA VALUES(?,?,?,?)");
                                query2 = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix2;
                                rs2 = stmt.executeQuery(query2);
                                query3 = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix3;
                                rs3 = stmt.executeQuery(query3);
                                
                                for (int i = 1; i <= m2rowdim; i++){
                                    for (int j = 1; j<= m3rowdim;j++){
                                        int sum = 0;
                                         String query_tmp2 = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix3 + "AND COL_NUM = "+ j;
                                        ResultSet rs_tmp2 = stmt.executeQuery(query_tmp2);

                                        String query_tmp3 = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix3 + "AND COL_NUM = "+ i;
                                        ResultSet rs_tmp3 = stmt.executeQuery(query_tmp3);
                                        while(rs_tmp3.next()){
                                            sum = sum + rs_tmp2.getInt("VALUE")*rs_tmp3.getInt("VALUE");
                                        }
                                        
                                        p.setInt(1,matrix1);
                                        p.setInt(2,i);
                                        p.setInt(3,j);
                                        p.setInt(4,sum);
                                        p.executeUpdate();
                                    }
                                }
                                
                            }
                            System.out.println("DONE");
                            break;
                        }
                        case TRANSPOSE:{
                            int matrix1 = Integer.parseInt(split[1]);
                            int matrix2 = Integer.parseInt(split[2]);
                            
                            String query = "DELETE * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix1;
                            Statement stmt = con.createStatement();
                            ResultSet rs = stmt.executeQuery(query);
                            
                            query = "SELECT * FROM MATRIX WHERE MARIX_ID = " + matrix2;
                            rs = stmt.executeQuery(query);
                            if (!rs.next()){
                                System.out.println("ERROR");
                                break;
                            }else{
                                int col = rs.getInt("COL_NUM");
                                int row = rs.getInt("ROW_NUM");
                                rs.updateInt("COL_NUM",row);
                                rs.updateInt("ROW_NUM",col);
                                //rs.executeUpdate();
                                
                                query = "SELECT * FROM MATRIX_DATA WHERE MARIX_ID = " + matrix2;
                                rs = stmt.executeQuery(query);
                                while (rs.next()){
                                    col = rs.getInt("COL_NUM");
                                    row = rs.getInt("ROW_NUM");
                                    rs.updateInt("COL_NUM",row);
                                    rs.updateInt("ROW_NUM",col);
                                    //rs.executeUpdate();
                                }
                                System.out.println("DONE");
                                break;
                            }
                        }
                        case SQL:{
                            String query = split[1];
                            Statement stmt = con.createStatement();
                            ResultSet rs = stmt.executeQuery(query);
                            String ret = rs.getString(1);
                            System.out.println(ret);
                            break;
                        }
                    }
                }catch(Exception e){
                    System.out.println("ERROR: Invalid Command");
                }
            }
        }catch(Exception e){
            System.out.println("ERROR: Invalid Database Connection String");
        } finally {
        	if (con != null){
        		con.close();
        	}
        }
        
    }
}