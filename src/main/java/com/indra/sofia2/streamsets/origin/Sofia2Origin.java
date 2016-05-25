/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.origin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.indra.sofia2.ssap.kp.exceptions.ConnectionToSibException;
import com.indra.sofia2.ssap.ssap.body.SSAPBodyReturnMessage;
import com.indra.sofia2.streamsets.Errors;
import com.indra.sofia2.streamsets.GroupsSofia;
import com.indra.sofia2.streamsets.connection.KpOperations;
import com.indra.sofia2.streamsets.connection.KpOperationsMQTT;
import com.indra.sofia2.streamsets.connection.KpOperationsREST;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

/**
 * This target is an example and does not actually write to any destination.
 */
public class Sofia2Origin extends BaseSource {

	public boolean noFinish=true;
	
	public String host;
	public Integer port;
	public String token;
	public String kp;
	public String ontology;
	public Boolean mqttConnection;
	public String query;
	public String queryType;

	public Sofia2Origin(String host,
		      Integer port,
		      String token,
		      String kp,
		      String ontology,
		      Boolean mqttConnection,
		      String query,
		      String queryType
		  ){
		this.host=host;
		this.port=port;
		this.token=token;
		this.kp=kp;
		this.ontology=ontology;
		this.mqttConnection=mqttConnection;
		this.query=query;
		this.queryType=queryType;
		
	}
	
	private KpOperations kpOperations;

	/** {@inheritDoc} */
	@Override
	protected List<ConfigIssue> init() {
		// Validate configuration values and open any required resources.
		List<ConfigIssue> issues = super.init();

		if (host.equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsSofia.SOFIA2O.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad Host"));
		}
		if (port.equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsSofia.SOFIA2O.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad Port"));
		}
		if (token.equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsSofia.SOFIA2O.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad Token"));
		}
		if (kp.equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsSofia.SOFIA2O.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad Kp"));
		}
		if (ontology.equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsSofia.SOFIA2O.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad Ontology"));
		}
		if (mqttConnection.equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsSofia.SOFIA2D.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad MqttConnection"));
		}
		// If issues is not empty, the UI will inform the user of each
		// configuration issue in the list.
		if (mqttConnection){
			this.kpOperations=new KpOperationsMQTT(host, port, token, kp);
		}else{
			this.kpOperations=new KpOperationsREST(host, port, token, kp);
		}
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
	public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
		long nextSourceOffset = 0;
		if (noFinish){
			try{
				// Offsets can vary depending on the data source. Here we use an integer
				// as an example only.
				if (lastSourceOffset != null) {
					nextSourceOffset = Long.parseLong(lastSourceOffset);
				}
				
				SSAPBodyReturnMessage message=kpOperations.query(ontology, query, queryType);
				if (message.isOk()){
					JSONParser parser = new JSONParser();
					try {
						JSONArray jsnarray = (JSONArray)parser.parse(message.getData());
						int numRecords = 0;
						for (Object  jsnobject : jsnarray){
							Record record = getContext().createRecord(ontology + nextSourceOffset);
							Map<String, Field> rootmap = new HashMap<>();
							for (Object key :((JSONObject)jsnobject).keySet()){
								JSONObject jsnonElement = (JSONObject)((JSONObject)jsnobject).get(key);
								Map<String, Field> map = EvaluateMap(jsnonElement);
								rootmap.put(key.toString(), Field.create(map));
							}
							record.set(Field.create(rootmap));
							batchMaker.addRecord(record);
							++nextSourceOffset;
							++numRecords;
						}
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
			}finally{
				try {
					this.kpOperations.leave();
				} catch (ConnectionToSibException e) {
					e.printStackTrace();
				}
			}
		}
		noFinish=false;
		return String.valueOf(nextSourceOffset);
	}
	
	private Map<String, Field> EvaluateMap(Object jsnonElement){
		Map<String, Field> map= new HashMap<String, Field>();
		for (Object key : ((JSONObject)jsnonElement).keySet()){
			if (((JSONObject)jsnonElement).get(key) instanceof Map){
				map.put(key.toString(), Field.create(EvaluateMap (((JSONObject)jsnonElement).get(key))));
			}else{
				Field campo = EvaluateSingle(((JSONObject)jsnonElement).get(key));
				map.put(key.toString(), campo);
			}
		}
		return map;
	}
	
	private Field EvaluateSingle(Object objeto){
		if (objeto instanceof Integer){
			return Field.create((int)objeto);
		}else if (objeto instanceof Float){
			return Field.create((Float)objeto);
		}else if (objeto instanceof Long){
			return Field.create((Long)objeto);
		}else if (objeto instanceof Boolean){
			return Field.create((boolean)objeto);
		}else {
			return Field.create((String)objeto);
		}
	}
}
