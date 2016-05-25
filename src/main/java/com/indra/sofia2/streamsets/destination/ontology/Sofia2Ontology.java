/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.destination.ontology;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.indra.sofia2.streamsets.destination.ontology.dto.Ontology;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;

public class Sofia2Ontology {

	private String resourcesDirectory;
	private Properties properties;
	
	public Sofia2Ontology(String resourcesDirectory){
		this.resourcesDirectory=resourcesDirectory;
	}
	
	public void ConstrucOntologyShema (String ontology, Record record, String user, String basicUrl){
		URL url;
		try {
			url = new URL(basicUrl+"/console/api/rest/ontologias/"+ontology);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Authorization", "Basic "+user);
			conn.setRequestProperty("Accept", "application/json");
			if (conn.getResponseCode()==200){
				BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
				String respuesta = br.readLine();
				try{
					Ontology ontologia = Ontology.toObject(respuesta);
					if (!ontologia.isActiva()){
						ontologia.setActiva(true);
					}
				}catch(Exception e){
					//Error al buscar la ontología, seguramente de seguridad, no creamos la ontología
				}
			}else if (conn.getResponseCode()==400){
				//No deberia de ser este error sino el 404 comprobamos con contenido del mensaje
				BufferedReader br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
				String respuesta = br.readLine();
				if (respuesta.contains("No results were found")){
					//ejecutamos creacion de la ontologia
				}
			}else if (conn.getResponseCode()==404){
				//ejecutamos creacion de la ontologia
			}
		} catch (MalformedURLException e) {
			
		} catch (IOException e) {
			
		}
	}
	
	private String getProperty(String key) throws IOException{
		if (properties==null){
			properties=new Properties();
			properties.load(new FileInputStream(resourcesDirectory+"/sofia2.properties"));
		}
		return properties.getProperty(key);
	}
	
	public String constructOntologyInstance(Record record, Boolean isOntologyRequired, String ontology, String defaultSeparator, String defaultFieldName) {
		StringBuffer buffer = new StringBuffer("{");
		if (isOntologyRequired){
			buffer.append("\"");
			buffer.append(ontology);
			buffer.append("\":{");
		}
		buffer.append(constructBody(record.get(), defaultSeparator, defaultFieldName));
		buffer.append("}");
		if (isOntologyRequired){
			buffer.append("}");
		}
		return buffer.toString();
	}
	
	private String constructBody(Field campo, String defaultSeparator, String defaultFieldName){
		Boolean primero=true;
		StringBuffer buffer = new StringBuffer();
		if (campo.getType()==Field.Type.MAP){
			Map<String, Field>subcampos = (Map<String, Field>)campo.getValue();
			for (String subcampoName : subcampos.keySet()){
				Field subcampo = subcampos.get(subcampoName);
				if (subcampo.getType()==Field.Type.MAP || campo.getType()==Field.Type.LIST_MAP){
					if (!primero){
						buffer.append(",");
					}
					buffer.append("\"");
					buffer.append(subcampoName);
					buffer.append("\":{");
					buffer.append(constructBody(subcampo, defaultSeparator, defaultFieldName));
					buffer.append("}");
				}else{
					buffer.append(constructBodyField(subcampo, subcampoName, primero));
				}
				primero=false;
			}
		}else if (campo.getType()==Field.Type.LIST_MAP){
			Map<String, Field>subcampos = (Map<String, Field>)campo.getValue();
			for (String subcampoName : subcampos.keySet()){
				primero = constructListStructure(primero, buffer, subcampoName, subcampos.get(subcampoName).getValue(), defaultFieldName);	
			}
		}else if (campo.getType()==Field.Type.LIST){
				List<Field> subcampos = (List)campo.getValue();
				for (Field field : subcampos){
					Map<String, Field> datos = (Map<String,Field>)field.getValue();
					primero = constructListStructure(primero, buffer, String.valueOf(datos.get("header").getValue()), datos.get("value").getValue(), defaultFieldName);
				}
		}	
		return buffer.toString();
	}

	private Boolean constructListStructure(Boolean primero, StringBuffer buffer, String cabecera, Object value, String defaultFieldName) {
		//Controlamos si es necesario coma para separar de campos anteriores
		if (!primero) {
			buffer.append(",");
		}
		buffer.append("\"");
		if (cabecera!=null){
			buffer.append(cabecera);
		}else{
			buffer.append(defaultFieldName);
		}
		buffer.append("\":");
		
		if (value instanceof String){
			buffer.append("\"");
			buffer.append(value);
			buffer.append("\"");
		}else if (value instanceof Date || value instanceof DateTime){
			buffer.append("{\"$date\":\"");
			buffer.append(formatToIsoDate(new DateTime(value)));
			buffer.append("\"}");
		}else if (value instanceof Map){
			buffer=null;
		}else{
			buffer.append(String.valueOf(value));
		}
		primero=false;
		return primero;
	}
	
	private String constructBodyField(Field field, String fieldName, Boolean primero){
		StringBuffer buffer = new StringBuffer();
		try{
			//Controlamos si es necesario coma para separar de campos anteriores
			if (!primero) {
				buffer.append(",");
			}
			buffer.append("\"");
			buffer.append(fieldName);
			buffer.append("\":");
			switch (field.getType()) {
			case INTEGER:
				buffer.append(String.valueOf(field.getValueAsInteger()));
				break;
			case LONG:
				buffer.append(String.valueOf(field.getValueAsLong()));
				break;
			case FLOAT:
				buffer.append(String.valueOf(field.getValueAsFloat()));
				break;
			case DOUBLE:
				buffer.append(String.valueOf(field.getValueAsDouble()));
				break;
			case BOOLEAN:
				buffer.append(String.valueOf(field.getValueAsBoolean()));
				break;
			case DATE:
				buffer.append("{\"$date\":\"");
				buffer.append(formatToIsoDate(new DateTime(field.getValueAsDate())));
				buffer.append("\"}");
				break;
			case DATETIME:
				buffer.append("{\"$date\":\"");
				buffer.append(formatToIsoDate(new DateTime(field.getValueAsDatetime())));
				buffer.append("\"}");
				break;
			case MAP:
				buffer=null;
				break;
			default:
				buffer.append("\"");
				buffer.append(field.getValueAsString());
				buffer.append("\"");
				break;
			}
		}catch (Exception e){
			
		}
		return buffer.toString();
	}
	
	private static DateTimeFormatter isoDateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

	private static String formatToIsoDate(DateTime date) {
		return isoDateFormatter.print(date);
	}
	
}
