
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
public class WriterThread implements Runnable {
	String fileName;	
	int start,end;
	private final String TABLE_NAME="PG";
	final Logger logger = Logger.getLogger("Global Logger");
	HashMap<String,Integer> header;
	HashMap<String,String[]> tableMappingDesc;
	private synchronized String sqlBuilder(String[] data)
	{
		String sql="insert into "+TABLE_NAME;
		String col="(";
		String coldata="values(";
		int i=0;
		String[] desc;
		for(Map.Entry<String, String[]> entry:tableMappingDesc.entrySet())
		{
			if(i>0)
			{
				col+=",";
				coldata+=",";
			}
			desc=entry.getValue();
			col+=desc[0];
			coldata+=colTypeModifier(desc[1],data[header.get(entry.getKey().toUpperCase())]);
			i++;
		}
		col=col+")";
		coldata=coldata+")";
		sql=sql+col+coldata;
		return sql;
	}
	private synchronized String colTypeModifier(String colDatabaseDataType,String csvColData)
	{
		String result="";
		switch(colDatabaseDataType)
		{
			case "VARCHAR2":
			case "CHAR":
			case "NVARCHAR2":
			case "CLOB":
			case "NLOB":
			case "NCHAR":
			case "DATE":
				result="'"+csvColData+"'";
				break;
			default:
				result=csvColData;
		}
		return result;
	}
	private synchronized void errorinfo(Connection con,Exception e,int rownum)
	{
		PreparedStatement ps=null;
		try {
			ps=con.prepareStatement("insert into errortable values(?,?)");
			ps.setInt(1, rownum);
			ps.setString(2, e.getMessage());
			if(ps.executeUpdate()==1)
			{
				logger.info("Error Msg Inserted ");
			}
		} catch (SQLException e1) {
			logger.error(e.toString());
		}
		
	}
	public void run()
	{  
		Connection con=null;
		CSVReader csvReader=null;
 		try {
 			FileInputStream input=new FileInputStream(fileName);
 			CharsetDecoder decoder=Charset.forName("UTF-8").newDecoder();
 			decoder.onMalformedInput(CodingErrorAction.IGNORE);
 			Reader reader=new InputStreamReader(input,decoder);
 			//Reader reader = Files.newBufferedReader(Paths.get(fileName));		 		    
 			csvReader =new CSVReaderBuilder(reader).withSkipLines(start).build();  		 		     
 			con=C3P0DataSource.getInstance().getConnection();
 			PreparedStatement ps=null;
 			for(long i=start;i<=end;i++)
 			{  
 				String[] data=csvReader.readNext();
			    try 
			    {
			    	String sql=sqlBuilder(data);
			    	ps=con.prepareStatement(sql);
				    int r=ps.executeUpdate();
				    if(r==1) {
				    logger.info("/****************************************Processed  " +i+ "th Record********************************************************************************/");
			    	}
			    }
			    catch (SQLException e) {
			    	errorinfo(con,e,(int) i);
			    	logger.error(e.toString());
				}
			    finally {
			    	try {
					   if(ps!=null)
					   {
						   ps.close();
						   
					   }
					} 
			    	catch (SQLException e) {
			    		errorinfo(con,e,(int) i);
			    		logger.error(e.toString());
					}
			    	
   				} 
			}
	   	}catch (Exception e) {
	   		errorinfo(con,e,(int) start);
	   		logger.error(e.toString());
	   		//e.printStackTrace();
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
 				logger.error(e.toString());
 				errorinfo(con,e,(int) start);
 				
 			} catch (IOException e) {
 				errorinfo(con,e,(int) start);
 				logger.error(e.toString());
 				//e.printStackTrace();
 			}
	   }			
	}  
	public void setIndex(int s,int e)
	{
		
		start=s;
		end=e;
	}
	WriterThread(String file,HashMap<String,Integer> header,HashMap<String,String[]> tableMappingDesc)
	{
		this.tableMappingDesc=tableMappingDesc;
		this.header=header;
		fileName=file;
	}
}
