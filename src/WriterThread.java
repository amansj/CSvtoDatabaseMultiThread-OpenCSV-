
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
public class WriterThread implements Runnable {
	String fileName;
	
	int start,end;
	
	public void run()
	{  
		Connection con=null;
		CSVReader csvReader=null;
 		try {
 			Reader reader = Files.newBufferedReader(Paths.get(fileName));		 		    
 			csvReader =new CSVReaderBuilder(reader).withSkipLines(start).build();  		 		     
 			con=C3P0DataSource.getInstance().getConnection();
 			PreparedStatement ps=null;
 			for(long i=start;i<=end;i++)
 			{  
 				String[] data=csvReader.readNext();
			    try 
			    {
			    	ps=con.prepareStatement("insert into pg values(?,?,?,?,?)");
				    ps.setInt(1,Integer.parseInt(data[0]));
				    ps.setString(2,(data[1]));
				    ps.setString(3,(data[2]));
				    ps.setString(4,(data[3]));
				    ps.setInt(5,Integer.parseInt(data[4]));
				    int r=ps.executeUpdate();
				    if(r==1) {
				    	
				    	System.out.println("/****************************************Processed  " +i+ "th Record********************************************************************************/");
			    	}
			    }
			    catch (SQLException e) {
					// TODO Auto-generated catch block
			    	e.printStackTrace();
				}
			    finally {
			    	try {
					   if(ps!=null)
					   {
						   ps.close();
						   
					   }
					} 
			    	catch (SQLException e) {
						// TODO Auto-generated catch block
				   		e.printStackTrace();
					}
			    	
   				} 
			}
	   	}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
   		}
 		finally 
 		{
 			try {
 				if(con!=null)
 				{
 					con.close();
 				}
 				if(csvReader!=null)
 				{
 					csvReader.close();
 				}
 			} catch (SQLException e) {
 				// TODO Auto-generated catch block
 				e.printStackTrace();
 			} catch (IOException e) {
 				// TODO Auto-generated catch block
 				e.printStackTrace();
 			}
	   }			
	}  
	public void setIndex(int s,int e)
	{
		
		start=s;
		end=e;
	}
	WriterThread(String file)
	{
		fileName=file;
	}
}
