/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.destination;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.indra.sofia2.ssap.kp.exceptions.ConnectionToSibException;
import com.indra.sofia2.streamsets.Errors;
import com.indra.sofia2.streamsets.GroupsSofia;
import com.indra.sofia2.streamsets.connection.KpOperations;
import com.indra.sofia2.streamsets.connection.KpOperationsMQTT;
import com.indra.sofia2.streamsets.connection.KpOperationsREST;
import com.indra.sofia2.streamsets.destination.ontology.Sofia2Ontology;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;

/**
 * This target is an example and does not actually write to any destination.
 */
public abstract class Sofia2Target extends BaseTarget {

	/**
	 * Gives access to the UI configuration of the stage provided by the
	 * {@link Sofia2DTarget} class.
	 */
	public abstract String getHost();
	public abstract Integer getPort();
	public abstract String getToken();
	public abstract String getKp();
	public abstract String getOntology();
	public abstract String getDefaultaSeparator();
	public abstract String getDefaultFieldName();
	public abstract Boolean getMqttConnection();
	public abstract Boolean getOntologyRequired();
	public abstract Integer getBulk();
	//public abstract Boolean getCreateOntology();
	public abstract Integer getThread();

	private List<KpOperations> kpOperations;
	private Sofia2Ontology sofia2Ontology;
	private ExecutorService executor;

	/** {@inheritDoc} */
	@Override
	protected List<ConfigIssue> init() {
		// Validate configuration values and open any required resources.
		List<ConfigIssue> issues = super.init();

		if (getHost().equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsSofia.SOFIA2D.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad Host"));
		}
		if (getPort().equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsSofia.SOFIA2D.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad Port"));
		}
		if (getToken().equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsSofia.SOFIA2D.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad Token"));
		}
		if (getKp().equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsSofia.SOFIA2D.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad Kp"));
		}
		if (getOntology().equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsSofia.SOFIA2D.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad Ontology"));
		}
		if (getMqttConnection().equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsSofia.SOFIA2D.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad MqttConnection"));
		}
		this.kpOperations=new ArrayList<KpOperations>();
		for (int i=0 ; i< getThread(); i++){
			if (getMqttConnection()){
				this.kpOperations.add(new KpOperationsMQTT(getHost(), getPort(), getToken(), getKp()));
			}else{
				this.kpOperations.add(new KpOperationsREST(getHost(), getPort(), getToken(), getKp()));
			}
		}
		
		// If issues is not empty, the UI will inform the user of each
		// configuration issue in the list.
		
		sofia2Ontology=new Sofia2Ontology(getContext().getResourcesDirectory());
		return issues;
	}

	/** {@inheritDoc} */
	@Override
	public void destroy() {
		// Clean up any open resources.
		super.destroy();
	}

	/** {@inheritDoc} */
	@Override
	public void write(Batch batch) throws StageException {
		Iterator<Record> batchIterator = batch.getRecords();
		executor = Executors.newFixedThreadPool(getThread());
		// Establecemos la configuración física	
		try {
			batchIterator = batch.getRecords();
			Set<Future<ErrorManager>> set = new HashSet<Future<ErrorManager>>();
			int conexion=0;
			while (batchIterator.hasNext()) {
				Record record = batchIterator.next();
				Callable<ErrorManager> worker = new Sofia2Worker(kpOperations.get(conexion), sofia2Ontology, record, getOntologyRequired(), getDefaultaSeparator(), getDefaultFieldName(), getOntology(), getBulk());
				set.add(executor.submit(worker));
				conexion=conexion+1;
				if (conexion==kpOperations.size()){
					conexion=0;
				}
			}
			for (Future<ErrorManager> future : set) {
				try{
					ErrorManager error= future.get();
					try{
						error.getException();
					} catch (Exception e) {
						switch (getContext().getOnErrorRecord()) {
						case DISCARD:
							break;
						case TO_ERROR:
							getContext().toError(error.getRecord(), Errors.ERROR_01, e.toString());
							break;
						case STOP_PIPELINE:
							throw new StageException(Errors.ERROR_01, e.toString());
						default:
							throw new IllegalStateException(Utils.format("Error desconocido al insertar el registro '{}'",
									getContext().getOnErrorRecord(), e));
						}
					}
				}catch (Exception e){
					
				}
			 }
		} finally {
			try {
				executor.shutdown();
			}catch (Exception e){
				e.printStackTrace();
			}
			try {
				for (int i = 0 ; i < kpOperations.size(); i++){
					this.kpOperations.get(0).leave();
				}
			} catch (ConnectionToSibException e) {
				e.printStackTrace();
			}
		}
	}
	
}