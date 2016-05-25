/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.origin;

import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.indra.sofia2.streamsets.Errors;
import com.indra.sofia2.streamsets.GroupsHDFS;
import com.indra.sofia2.streamsets.format.DataFormat;
import com.indra.sofia2.streamsets.format.DelimitedFormat;
import com.indra.sofia2.streamsets.format.HeaderFormat;
import com.indra.sofia2.streamsets.format.TransformToRegrex;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

/**
 * This target is an example and does not actually write to any destination.
 */
public class HDFSOrigin extends BaseSource {

	public String hdfsUri;
	public String hdfsUser;
	public String hdfsInputPath;
	public DataFormat dataFormat;
	public DelimitedFormat delimiterFormat;
	public HeaderFormat headerFormat;
	public Integer sizeDelimiter;
	private List<String> cabeceras;
	private FileSystem fs;
	private Scanner sc;
	private boolean isNew;
	private Integer numberThreads;
	ExecutorService executor;
	
	public HDFSOrigin(String hdfsUri, String hdfsUser, String hdfsInputPath,
			DataFormat dataFormat,DelimitedFormat delimitedFormat,HeaderFormat headerFormat,Integer numberThreads) {
		super();
		this.hdfsUri = hdfsUri;
		this.hdfsUser = hdfsUser;
		this.hdfsInputPath = hdfsInputPath;
		this.dataFormat = dataFormat;
		this.delimiterFormat=delimitedFormat;
		this.headerFormat=headerFormat;
		this.numberThreads=numberThreads;
	}

	/** {@inheritDoc} */
	@Override
	protected List<ConfigIssue> init() {
		// Validate configuration values and open any required resources.
		List<ConfigIssue> issues = super.init();
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////
		////////////////////////////// Comprobación variables globales ////////////////////////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		
		if (hdfsUri.equals("invalidValue") || hdfsUri.isEmpty()) {
			issues.add(getContext().createConfigIssue(GroupsHDFS.HDFS.name(), "config", Errors.ERROR_00,
					"Hadoop FS URI is required"));
		}
		if (hdfsUser.equals("invalidValue") || hdfsUser.isEmpty()) {
			issues.add(getContext().createConfigIssue(GroupsHDFS.HDFS.name(), "config", Errors.ERROR_00,
					"HDFSUser is required"));
		}
		if (hdfsInputPath.equals("invalidValue") || hdfsInputPath.isEmpty()) {
			issues.add(getContext().createConfigIssue(GroupsHDFS.HDFS.name(), "config", Errors.ERROR_00,
					"InputPath is required"));
		}
		if (dataFormat.equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsHDFS.HDFS.name(), "config", Errors.ERROR_00,
					"DataFormat is required"));
		}
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////
		////////////////////////////// Comprobación casos particulares ////////////////////////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		if (dataFormat.compareTo(DataFormat.DELIMITED)==0 && (delimiterFormat.equals("invalidValue") || delimiterFormat.equals(""))) {
			issues.add(getContext().createConfigIssue(GroupsHDFS.HDFS.name(), "config", Errors.ERROR_00,
					"delimiterFormat is required if dataFormat is DELIMITED"));
		}
		
		if (dataFormat.compareTo(DataFormat.DELIMITED)==0 && (headerFormat.equals("invalidValue") || headerFormat.equals(""))) {
			issues.add(getContext().createConfigIssue(GroupsHDFS.HDFS.name(), "config", Errors.ERROR_00,
					"Delimiter Format Type is required if dataFormat is DELIMITED"));
		}
		
		try {
			getConfigFs();
			fs.open(new Path(hdfsInputPath));
		} catch (Exception e) {
			issues.add(getContext().createConfigIssue(GroupsHDFS.HDFS.name(), "config", Errors.ERROR_00,
					"Error starting connection with HDFS:" +e.getCause()));
		}
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////
		////////////////////////////// Comprobación fichero existe ////////////////////////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////////////
		
		return issues;
	}

	/** {@inheritDoc} */
	@Override
	public void destroy() {
		super.destroy();
	}

	/** {@inheritDoc} */
	@Override
	public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
		
		isNew=false;
		int nextSourceOffset=setNextSourceOffset(lastSourceOffset);
	
		try {
			
			getConfigFs();//fs.getWorkingDirectory()
			getScanner();
			//FileStatus[] status = fs.listStatus(new Path("sofia"));status[i].getPath());
			switch (dataFormat) {
				case TEXT:
					if (isNew){
						jumpLines(nextSourceOffset);
					}
					nextSourceOffset= analyzeText(nextSourceOffset,maxBatchSize,batchMaker);
					break;
					
				case DELIMITED:
					nextSourceOffset= analyzeDelimited(nextSourceOffset,maxBatchSize,batchMaker);
					break;
			}	  
		}catch (Exception e){
	        throw new StageException(Errors.ERROR_04, e);
		} finally {
			try {
				executor.shutdown();
			}catch (Exception e){
				e.printStackTrace();
			}
			if (!sc.hasNextLine()){				//si ya ha leido todo el fichero, cierro todo
				try {
					sc.close();
					fs.close();
				}catch (Exception e){
					e.printStackTrace();
				}
			}
		}
		return String.valueOf(nextSourceOffset);
	}
	
	private void jumpLines(int nextSourceOffset){
		
		for (int contInfo=0;sc.hasNextLine();contInfo++){
			
			if (contInfo<nextSourceOffset && isNew){
				sc.nextLine();
			} else {
				break;
			}
		}
	}
	
	private int setNextSourceOffset(String lastSourceOffset){
		if (lastSourceOffset==null){
			return 0;
		}
		return Integer.parseInt(lastSourceOffset);
	}
	
	private void getScanner() throws Exception{
		if (sc==null){
			sc = new Scanner(fs.open(new Path(hdfsInputPath)), "UTF-8");
			isNew=true;
		}
	}
	
	private int analyzeDelimited(int nextSourceOffset,int maxBatchSize,BatchMaker batchMaker) throws StageException {
		
		if (delimiterFormat==null || delimiterFormat.equals("")){
	        throw new StageException(Errors.ERROR_00, "delimiterFormat is null or empty");
		}
		
		List<Future<Record>> set = new LinkedList<Future<Record>>();
		executor = Executors.newFixedThreadPool(numberThreads);
		
		switch (headerFormat) {
		
			case WITHHEADERLINE:	
				
				if (sc.hasNextLine() && isNew ){ //Si es nuevo, el Scanner apunta al primer elemento

					cabeceras=new LinkedList<String>();
					String[] listadoSplit=sc.nextLine().split(Pattern.quote(TransformToRegrex.getDelimitedValue(delimiterFormat)));
					for (String temp:listadoSplit){
					 	cabeceras.add(temp);
					}
					jumpLines(nextSourceOffset);
					if (nextSourceOffset<1){
						nextSourceOffset++;
					}
				}
				
				for (int i=0;sc.hasNextLine() && i<maxBatchSize;i++,nextSourceOffset++) {
					Callable<Record> worker = new HDFSWorker(getContext(), hdfsInputPath, nextSourceOffset, sc.nextLine(),false,cabeceras,delimiterFormat);
					set.add(executor.submit(worker));	
				}
				
				for (Future<Record> future : set) {
					try{
						batchMaker.addRecord(future.get());
					}catch (Throwable e){
						try{
							getContext().toError(getContext().createRecord(hdfsInputPath+nextSourceOffset), Errors.ERROR_01, e.toString());
						} catch(Exception e1){
						}
					}
				}
				
				break;
				
			case IGNOREHEADERLINE:
				
				if (sc.hasNextLine() && isNew && nextSourceOffset<1){ //Si es nuevo, el Scanner apunta al primer elemento. En este caso se ignora
					sc.nextLine();	
				}			
			case NOHEADERLINE:

				jumpLines(nextSourceOffset);
				if (headerFormat.name().equals(HeaderFormat.IGNOREHEADERLINE) && isNew && nextSourceOffset<1){
					nextSourceOffset++;
				}
				for (int i=0;sc.hasNextLine() && i<maxBatchSize;i++,nextSourceOffset++) {
					Callable<Record> worker = new HDFSWorker(getContext(), hdfsInputPath, nextSourceOffset, sc.nextLine(),false,null,delimiterFormat);
					set.add(executor.submit(worker));
				}					

				for (Future<Record> future : set) {
					try{
						batchMaker.addRecord(future.get());
					}catch (Throwable e){
						try{
							getContext().toError(getContext().createRecord(hdfsInputPath+nextSourceOffset), Errors.ERROR_01, e.toString());
						} catch(Exception e1){
						}
					}
				}
				break;
			default:
				break;
		} 
	
	    return nextSourceOffset;
	}
	
	private int analyzeText(int nextSourceOffset,int maxBatchSize,BatchMaker batchMaker) throws Exception {

			List<Future<Record>> set = new LinkedList<Future<Record>>();
			executor = Executors.newFixedThreadPool(numberThreads);
			for (int i=0;sc.hasNextLine() && i<maxBatchSize;i++,nextSourceOffset++) {
				Callable<Record> worker = new HDFSWorker(getContext(), hdfsInputPath, nextSourceOffset, sc.nextLine(),true,null,null);
				set.add(executor.submit(worker));
			}
			
			for (Future<Record> future : set) {
				try{
					batchMaker.addRecord(future.get());
				}catch (Throwable e){
					try{
						getContext().toError(getContext().createRecord(hdfsInputPath+nextSourceOffset), Errors.ERROR_01, e.toString());
					} catch(Exception e1){
					}
				}
			}
	    // note that Scanner suppresses exceptions
	    if (sc.ioException() != null) {
	        throw new StageException(Errors.ERROR_04, sc.ioException());
	    }
	    return nextSourceOffset;
	}
	
	private void getConfigFs() throws Exception {
		
		Configuration configFs;

		if (fs==null){
			configFs = new Configuration();
			configFs.set("fs.defaultFS", hdfsUri);
			configFs.set("hadoop.job.ugi", hdfsUser);
			System.setProperty("HADOOP_USER_NAME", hdfsUser);
			fs=FileSystem.get(configFs);
		}
		 
	}
	
}
