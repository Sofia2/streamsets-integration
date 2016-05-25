package com.indra.sofia2.streamsets.destination;

import java.util.concurrent.Callable;

import com.indra.sofia2.ssap.ssap.body.SSAPBodyReturnMessage;
import com.indra.sofia2.streamsets.Errors;
import com.indra.sofia2.streamsets.connection.KpOperations;
import com.indra.sofia2.streamsets.destination.ontology.Sofia2Ontology;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;

public class Sofia2Worker implements Callable<ErrorManager> {

	private KpOperations kpOperations;
	private Record record;
	private Sofia2Ontology sofia2Ontology;
	private Boolean ontologyRequired;
	private String defaultSeparator;
	private String defaultFieldName;
	private String ontology;
	private Integer bulk;
	
	public Sofia2Worker(KpOperations kpOperations, Sofia2Ontology sofia2Ontology, Record record, Boolean ontologyRequired, String defaultSeparator, String defaultFieldName, String ontology, Integer bulk){
		this.kpOperations=kpOperations;
		this.record=record;
		this.sofia2Ontology=sofia2Ontology;
		this.ontologyRequired=ontologyRequired;
		this.defaultSeparator=defaultSeparator;
		this.defaultFieldName=defaultFieldName;
		this.ontology=ontology;
		this.bulk=bulk;
	}
	
	 @Override
	 public ErrorManager call() {
		 try{
			 SSAPBodyReturnMessage retorno = this.kpOperations.insert(sofia2Ontology.constructOntologyInstance(record, ontologyRequired, ontology, defaultSeparator, defaultFieldName), ontology, bulk);
			 if (!retorno.isOk()) {
				 return new ErrorManager(new OnRecordErrorException(Errors.ERROR_01, record, retorno.getError()), record);
			 }
			 return new ErrorManager(null, null);
		 }catch (Throwable e){
			 return new ErrorManager(new OnRecordErrorException(Errors.ERROR_01, record, e.getMessage()), record);	
		 }
	 }
}
