/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.destination.ontology.dto;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class Ontology {

	private String identificacion;
	private String identificacionUsuario;
	private boolean activa;
	private boolean publico;
	private String descripcion;
	private String identificacionPlantilla;
	private String versionPlantilla;
	private String esquemajson;
	private boolean bdtrclean;
	private String bdtrbdh;
	private boolean padre;
	private boolean reduce;
	private int shardBy;
	
	public String toJSON(){
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonGenerationException e) {
			return null;
		} catch (JsonMappingException e) {
			return null;
		} catch (IOException e) {
			return null;
		}
	}
	
	public static Ontology toObject(String json){
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(json, Ontology.class);
		} catch (IOException e) {
			return null;
		}
	}
	
	public String getIdentificacion() {
		return identificacion;
	}
	public String getIdentificacionUsuario() {
		return identificacionUsuario;
	}
	public boolean isActiva() {
		return activa;
	}
	public boolean isPublico() {
		return publico;
	}
	public String getDescripcion() {
		return descripcion;
	}
	public String getIdentificacionPlantilla() {
		return identificacionPlantilla;
	}
	public String getVersionPlantilla() {
		return versionPlantilla;
	}
	public String getEsquemajson() {
		return esquemajson;
	}
	public boolean isBdtrclean() {
		return bdtrclean;
	}
	public String getBdtrbdh() {
		return bdtrbdh;
	}
	public boolean isPadre() {
		return padre;
	}
	public boolean isReduce() {
		return reduce;
	}
	public int getShardBy() {
		return shardBy;
	}
	public void setIdentificacion(String identificacion) {
		this.identificacion = identificacion;
	}
	public void setIdentificacionUsuario(String identificacionUsuario) {
		this.identificacionUsuario = identificacionUsuario;
	}
	public void setActiva(boolean activa) {
		this.activa = activa;
	}
	public void setPublico(boolean publico) {
		this.publico = publico;
	}
	public void setDescripcion(String descripcion) {
		this.descripcion = descripcion;
	}
	public void setIdentificacionPlantilla(String identificacionPlantilla) {
		this.identificacionPlantilla = identificacionPlantilla;
	}
	public void setVersionPlantilla(String versionPlantilla) {
		this.versionPlantilla = versionPlantilla;
	}
	public void setEsquemajson(String esquemajson) {
		this.esquemajson = esquemajson;
	}
	public void setBdtrclean(boolean bdtrclean) {
		this.bdtrclean = bdtrclean;
	}
	public void setBdtrbdh(String bdtrbdh) {
		this.bdtrbdh = bdtrbdh;
	}
	public void setPadre(boolean padre) {
		this.padre = padre;
	}
	public void setReduce(boolean reduce) {
		this.reduce = reduce;
	}
	public void setShardBy(int shardBy) {
		this.shardBy = shardBy;
	}
}
