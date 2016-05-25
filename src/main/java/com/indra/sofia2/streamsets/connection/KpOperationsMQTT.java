/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.connection;

import java.util.UUID;

import org.fusesource.mqtt.client.QoS;

import com.indra.sofia2.ssap.kp.Kp;
import com.indra.sofia2.ssap.kp.SSAPMessageGenerator;
import com.indra.sofia2.ssap.kp.config.MQTTConnectionConfig;
import com.indra.sofia2.ssap.kp.exceptions.ConnectionToSibException;
import com.indra.sofia2.ssap.kp.exceptions.NotSupportedMessageTypeException;
import com.indra.sofia2.ssap.kp.implementations.KpMQTTClient;
import com.indra.sofia2.ssap.ssap.SSAPBulkMessage;
import com.indra.sofia2.ssap.ssap.SSAPMessage;
import com.indra.sofia2.ssap.ssap.SSAPQueryType;
import com.indra.sofia2.ssap.ssap.body.SSAPBodyReturnMessage;

public class KpOperationsMQTT implements KpOperations {

	private Kp kpClient;
	private String sessionKey;
		
	private String host;
	private String token;
	private String kp;
	private int port;
	
	private SSAPBulkMessage bulkMessage;
	private int bulkCounter=0;
	
	public KpOperationsMQTT(String host, Integer port, String token, String kp){
		this.host=host;
		this.port=port;
		this.kp=kp;
		this.token=token;
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
		fisicalConnection();
		SSAPMessage msgInsert = SSAPMessageGenerator.getInstance().generateInsertMessage(sessionKey, ontology, message);
		SSAPMessage response=null;
		if (bulk.intValue()!=0){
			try {
				if (bulkMessage==null){
					bulkMessage = SSAPMessageGenerator.getInstance().generateBulkMessage(sessionKey, ontology);
				}
				if (!retry){
					bulkMessage.addMessage(msgInsert);
					bulkCounter++;
				}
				if (bulkCounter>=bulk){
					response = this.kpClient.send(bulkMessage);
					bulkCounter=0;
					bulkMessage=null;
				}else{
					response=new SSAPMessage();
					SSAPBodyReturnMessage bodyReturn= new SSAPBodyReturnMessage();
					bodyReturn.setOk(true);
					response.setBody(bodyReturn.toJson());
				}
			} catch (NotSupportedMessageTypeException e) {
				response=new SSAPMessage();
				SSAPBodyReturnMessage bodyReturn= new SSAPBodyReturnMessage();
				bodyReturn.setOk(false);
				bodyReturn.setError(e.getMessage());
				response.setBody(bodyReturn.toJson());
			}
		}else{
			response = this.kpClient.send(msgInsert);
		}
		return SSAPBodyReturnMessage.fromJsonToSSAPBodyReturnMessage(response.getBody());
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
		fisicalConnection();
		SSAPMessage msgQuery = SSAPMessageGenerator.getInstance().generateQueryMessage(sessionKey, ontology, query, SSAPQueryType.valueOf(queryType));
		SSAPMessage response=this.kpClient.send(msgQuery);
		return SSAPBodyReturnMessage.fromJsonToSSAPBodyReturnMessage(response.getBody());
	}
	
	
	/**
	 * Establece la configuración de la conexión física
	 */
	private void fisicalConnectionConfiguration(String host, Integer port) {
		MQTTConnectionConfig config = new MQTTConnectionConfig();
		config.setHostSIB(host);
		config.setPortSIB(port);
		config.setKeepAliveInSeconds(5);
		config.setQualityOfService(QoS.AT_LEAST_ONCE);
		config.setTimeOutConnectionSIB(5000);
		this.kpClient = new KpMQTTClient(config);
	}
	
	/**
	 * Realiza la conexión física con el SIB
	 */
	private void fisicalConnection() {
		try{
			if (kpClient == null || !kpClient.isConnectionEstablished()) {
				fisicalConnectionConfiguration(host, port);
				try{
					//Intentamos conectar hasta en tres ocasiones
					this.kpClient.connect();
				}catch (ConnectionToSibException e){
					try{
						this.kpClient.connect();
					}catch (ConnectionToSibException e2){
						this.kpClient.connect();
					}
				}
			}	
		}catch (Exception e ){
			e.printStackTrace();
		}
	}

	/**
	 * Cerramos la conexión fisica con el SIB
	 */
	public void resetConnection() {
		if (this.kpClient != null) {
			try {
				this.kpClient.disconnect();
			} catch (Exception e) {

			}
		}
		this.kpClient = null;
		this.sessionKey = null;
	}

	public SSAPBodyReturnMessage login() {
		fisicalConnection();
		SSAPMessage msgJoin = SSAPMessageGenerator.getInstance().generateJoinByTokenMessage(this.token,
				this.kp + ":" + UUID.randomUUID().toString());
		SSAPMessage response = this.kpClient.send(msgJoin);
		this.sessionKey = response.getSessionKey();
		return SSAPBodyReturnMessage.fromJsonToSSAPBodyReturnMessage(response.getBody());
	}

	/* (non-Javadoc)
	 * @see com.indra.sofia2.streamsets.connection.Kp#leave()
	 */
	@Override
	public SSAPBodyReturnMessage leave() {
		if (sessionKey != null) {
			fisicalConnection();
			SSAPMessage msgLeave = SSAPMessageGenerator.getInstance().generateLeaveMessage(sessionKey);
			SSAPMessage response = this.kpClient.send(msgLeave);
			resetConnection();
			return SSAPBodyReturnMessage.fromJsonToSSAPBodyReturnMessage(response.getBody());
		}
		return null;
	}

}
