package com.indra.sofia2.streamsets.destination;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;

public class ErrorManager {

	private OnRecordErrorException error;
	private Record record;
	
	public ErrorManager(OnRecordErrorException error, Record record){
		this.error=error;
		this.record=record;
	}
	
	public void getException() throws OnRecordErrorException{
		if (error!=null){
			OnRecordErrorException tmp =error;
			error=null;
			throw tmp;
		}
	}
	
	public Record getRecord(){
		return this.record;
	}
	
	
}
