import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

public class Main {
	private static HashMap<String,String[]> tableDescription()
	{
		HashMap<String,String[]> tableMappingDesc=new HashMap<String,String[]>();
		Connection con=C3P0DataSource.getInstance().getConnection();
		try {
			PreparedStatement pStatement=con.prepareStatement("select * from descPG ");
			ResultSet rs=pStatement.executeQuery();
			
			while(rs.next())
			{
				tableMappingDesc.put(rs.getString(1),new String[]{rs.getString(2),rs.getString(3)});
			}
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.toString());
			
		}
		finally{
			try {
				if(con!=null)
				{
					con.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(e.toString());
				
			}	
		}
		return tableMappingDesc;
	}
	private static HashMap<String,Integer> csvHeader(String fileName)
	{
		HashMap<String,Integer> header=new HashMap<String,Integer>();
		CSVReader csvReader=null;
		Reader reader;
		try {
			reader = Files.newBufferedReader(Paths.get(fileName));
			csvReader =new CSVReaderBuilder(reader).build();
			String[] data;
			data=csvReader.readNext();
			for(int j=0;j<data.length;j++)
			{
				header.put(data[j], j);
			}
			csvReader.close();
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
		}		 		    
			
		return header;
	}
	private static void pageCalculations(MutableInt numOfRecord,MutableInt numPages,MutableInt remainingRecord,int numOfThread,File file) {
		numOfRecord.setValue(countLineNumber(file));
		numPages.setValue((numOfRecord.intValue())/numOfThread);
		remainingRecord.setValue((numOfRecord.intValue())-numOfThread*(numPages.intValue()));	
	}
	private static int countLineNumber(File file)
	{
		int lines = 0;
		try {
			LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(file));
			lineNumberReader.skip(Long.MAX_VALUE);
	        lines = lineNumberReader.getLineNumber();
	      	lineNumberReader.close();
		}catch (IOException e) {
			logger.error(e.toString());
	    }
		return lines-1;
	}
	private static final int NO_OF_CORES=4;
	final static Logger logger = Logger.getLogger("Global Logger");
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.print("Enter Number of Thread:");
		Scanner sc=new Scanner(System.in);
		int numOfThread=sc.nextInt();
		Instant starttime = Instant.now();
		String fileName="doc/pg5.csv";
		File file=new File(fileName);
		MutableInt numRecords = new MutableInt();
		MutableInt numPage=new MutableInt();
		MutableInt remRecords=new MutableInt();
		pageCalculations(numRecords,numPage,remRecords,numOfThread,file);
		int numOfRecord=numRecords.intValue();
		int numPages=numPage.intValue();
		HashMap<String,Integer> header=csvHeader(fileName);
		HashMap<String,String[]> tableMappingDesc=tableDescription();
		int remainingRecord=remRecords.intValue();
		ExecutorService execService=Executors.newFixedThreadPool(NO_OF_CORES);
		int start,end;
		int i;
		for(i=0;i<numOfThread;i++)
		{
			start=i*numPages+1;
			end=start+numPages-1;
			WriterThread thread=new WriterThread(fileName,header,tableMappingDesc);
			thread.setIndex(start, end);
			execService.execute(thread);
		}
		while(remainingRecord!=0)
		{
			start=numOfRecord-remainingRecord+1;
			end=numOfRecord-remainingRecord+1;
			WriterThread thread=new WriterThread(fileName,header,tableMappingDesc);
			thread.setIndex(start, end);
			execService.execute(thread);
			remainingRecord--;
		}
		execService.shutdown();  
		while (!execService.isTerminated()) {   }  		
		sc.close();
		Instant endtime = Instant.now();
		logger.info(Duration.between(starttime, endtime));
		System.out.println(Duration.between(starttime, endtime));
	}
}