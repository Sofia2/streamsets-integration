/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.connection;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.indra.sofia2.ssap.kp.implementations.rest.SSAPResourceAPI;
import com.indra.sofia2.ssap.kp.implementations.rest.exception.ResponseMapperException;
import com.indra.sofia2.ssap.kp.implementations.rest.resource.SSAPResource;
import com.indra.sofia2.ssap.ssap.body.SSAPBodyReturnMessage;

public class KpOperationsREST implements KpOperations {

	private SSAPResourceAPI kpClient;
	private String sessionKey;
		
	public String getSessionKey() {
		return sessionKey;
	}
	
	private String host;
	private String token;
	private String kp;
	private int port;
	
	private List<String> bulkMessage;
	private int bulkCounter=0;
	
	public KpOperationsREST(String host, Integer port, String token, String kp){
		this.host=host;
		this.port=port;
		this.kp=kp;
		this.token=token;
	}
	
	/**
	 * Realiza la conexión física con la configuración definida
	 */
	private void fisicalConnection() {
		try{
			if (kpClient == null) {
				this.kpClient=new SSAPResourceAPI("http://"+host+":"+port+"/sib/services/api_ssap");
			}
		}catch (Exception e ){
			e.printStackTrace();
		}
	}

	public void resetConnection() {
		this.kpClient = null;
		this.sessionKey = null;
	}

	public SSAPBodyReturnMessage login() {
		if (kpClient == null) {
			fisicalConnection();
		}
		SSAPResource ssapJoin=new SSAPResource();
		ssapJoin.setJoin(true);
		ssapJoin.setInstanceKP(this.kp + ":" + UUID.randomUUID().toString());
		ssapJoin.setToken(this.token);
		Response respJoin=this.kpClient.insert(ssapJoin);
		try {
			SSAPResource resource = this.kpClient.responseAsSsap(respJoin);
			this.sessionKey=resource.getSessionKey();
			SSAPBodyReturnMessage retorno = new SSAPBodyReturnMessage();
			retorno.setOk(true);
			retorno.setData(resource.getData());
			return retorno;
		} catch (Exception e) {
			SSAPBodyReturnMessage retorno = new SSAPBodyReturnMessage();
			retorno.setOk(false);
			retorno.setError(e.getMessage());
			return retorno;
		}
	}

	/* (non-Javadoc)
	 * @see com.indra.sofia2.streamsets.connection.Kp#leave()
	 */
	@Override
	public SSAPBodyReturnMessage leave() {
		if (sessionKey != null) {
			SSAPResource ssapLeave=new SSAPResource();
			ssapLeave.setLeave(true);
			ssapLeave.setSessionKey(sessionKey);
			Response respLeave=this.kpClient.insert(ssapLeave);
			try {
				SSAPResource resource = this.kpClient.responseAsSsap(respLeave);
				SSAPBodyReturnMessage retorno = new SSAPBodyReturnMessage();
				retorno.setOk(true);
				retorno.setData(resource.getData());
				return retorno;
			} catch (ResponseMapperException e) {
				
			}
		}
		return null;
	}
	
	/* (non-Javadoc)
	 * @see com.indra.sofia2.streamsets.connection.Kp#query(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public SSAPBodyReturnMessage query(String ontology, String query, String queryType) {
		try{
			if (sessionKey==null){
				login();
			}
			return queryWork(ontology, query, queryType);
		}catch (Exception e) {
			resetConnection();
			fisicalConnection();
			login();
			return queryWork(ontology, query, queryType);
		}
	}
	
	public SSAPBodyReturnMessage queryWork(String ontology, String query, String queryType) {
		Response respQuery=this.kpClient.query(sessionKey, ontology, query, null, queryType);
		try {
			SSAPResource resource = this.kpClient.responseAsSsap(respQuery);
			SSAPBodyReturnMessage retorno = new SSAPBodyReturnMessage();
			retorno.setOk(true);
			retorno.setData(resource.getData());
			return retorno;
		} catch (ResponseMapperException e) {
			SSAPBodyReturnMessage retorno = new SSAPBodyReturnMessage();
			retorno.setOk(false);
			retorno.setError(e.getMessage());
			return retorno;
		}
	}
	
	/* (non-Javadoc)
	 * @see com.indra.sofia2.streamsets.connection.Kp#insert(java.lang.String, java.lang.String)
	 */
	@Override
	public SSAPBodyReturnMessage insert(String message, String ontology, Integer bulk) {
		try {
			if (sessionKey==null){
				login();
			}
			return insertWork(message, ontology, bulk, false);
		} catch (Exception e) {
			resetConnection();
			fisicalConnection();
			login();
			return insertWork(message, ontology, bulk, true);
		}
	}

	private SSAPBodyReturnMessage insertWork(String message, String ontology, Integer bulk, Boolean retry) {
		SSAPResource ssapInsert=new SSAPResource();
		ssapInsert.setSessionKey(sessionKey);
		ssapInsert.setOntology(ontology);
		SSAPBodyReturnMessage retorno=null;
		if (bulk.intValue()!=0){
			if (bulkMessage==null){
				bulkMessage=new ArrayList<String>();
			}
			if (!retry){
				bulkMessage.add(message);
				bulkCounter++;
			}
			if (bulkCounter>=bulk){
				retorno = insertWorkExecution(ssapInsert);
				bulkCounter=0;
				bulkMessage=null;
			}else{
				retorno = new SSAPBodyReturnMessage();
				retorno.setOk(true);
			}
		}else{
			ssapInsert.setData(message);
			retorno = insertWorkExecution(ssapInsert);
		}
		return retorno;
	}

	private SSAPBodyReturnMessage insertWorkExecution(SSAPResource ssapInsert) {
		SSAPBodyReturnMessage retorno;
		Response responseInsert=null;
		try {
			responseInsert = this.kpClient.insert(ssapInsert);
			SSAPResource resource = this.kpClient.responseAsSsap(responseInsert);
			retorno = new SSAPBodyReturnMessage();
			retorno.setOk(true);
			retorno.setData(resource.getData());
		} catch (ResponseMapperException e) {
			((InputStream)responseInsert.getEntity()).toString();
			retorno = new SSAPBodyReturnMessage();
			retorno.setOk(false);
			retorno.setError(e.getMessage());
		}
		return retorno;
	}
}
