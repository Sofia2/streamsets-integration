/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.destination;

import com.indra.sofia2.streamsets.GroupsSofia;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

@StageDef(version = 1, label = "Sofia2", description = "Insert data as a Sofia2 Ontology Instance", icon = "sofia2.png", recordsByRef = true, onlineHelpRefUrl = "www.sofia2.com")
@ConfigGroups(value = GroupsSofia.class)
@GenerateResourceBundle
public class Sofia2DTarget extends Sofia2Target {

	@ConfigDef(required = true, type = ConfigDef.Type.STRING, defaultValue = "localhost", label = "Host", displayPosition = 10, group = "SOFIA2")
	public String host;

	/** {@inheritDoc} */
	@Override
	public String getHost() {
		return host;
	}

	@ConfigDef(required = true, type = ConfigDef.Type.NUMBER, defaultValue = "8080", label = "Port", displayPosition = 12, group = "SOFIA2")
	public Integer port;

	/** {@inheritDoc} */
	@Override
	public Integer getPort() {
		return port;
	}
	
	@ConfigDef(required = true, type = ConfigDef.Type.STRING, defaultValue = "token", label = "Token", displayPosition = 14, group = "SOFIA2")
	public String token;

	/** {@inheritDoc} */
	@Override
	public String getToken() {
		return token;
	}
	
	@ConfigDef(required = true, type = ConfigDef.Type.STRING, defaultValue = "kp", label = "Kp", displayPosition = 16, group = "SOFIA2")
	public String kp;

	/** {@inheritDoc} */
	@Override
	public String getKp() {
		return kp;
	}
	
	@ConfigDef(required = true, type = ConfigDef.Type.STRING, defaultValue = "ontology", label = "Ontology", displayPosition = 18, group = "SOFIA2")
	public String ontology;

	/** {@inheritDoc} */
	@Override
	public String getOntology() {
		return ontology;
	}
	
	@ConfigDef(required = true, type = ConfigDef.Type.BOOLEAN, defaultValue = "TRUE", label = "MQTT", displayPosition = 20, group = "SOFIA2")
	public Boolean mqttConnection;

	/** {@inheritDoc} */
	@Override
	public Boolean getMqttConnection() {
		return mqttConnection;
	}
	
	@ConfigDef(required = true, type = ConfigDef.Type.STRING, defaultValue = ";", label = "Separator default charactero", displayPosition = 10, group = "SOFIA2D")
	public String defaultaSeparator;

	/** {@inheritDoc} */
	@Override
	public String getDefaultaSeparator() {
		return defaultaSeparator;
	}
	
	@ConfigDef(required = true, type = ConfigDef.Type.STRING, defaultValue = "FIELD", label = "Default name field", displayPosition = 12, group = "SOFIA2D")
	public String defaultFieldName;

	/** {@inheritDoc} */
	@Override
	public String getDefaultFieldName() {
		return defaultFieldName;
	}
	
	@ConfigDef(required = true, type = ConfigDef.Type.BOOLEAN, defaultValue = "FALSE", label = "Add ontology name", displayPosition = 12, group = "SOFIA2D")
	public Boolean ontologyRequired;

	/** {@inheritDoc} */
	@Override
	public Boolean getOntologyRequired() {
		return ontologyRequired;
	}
	
	@ConfigDef(required = true, type = ConfigDef.Type.NUMBER, defaultValue = "0", label = "Bulk", displayPosition = 12, group = "SOFIA2D")
	public Integer bulk;

	/** {@inheritDoc} */
	@Override
	public Integer getBulk() {
		return bulk;
	}
	
	//@ConfigDef(required = true, type = ConfigDef.Type.BOOLEAN, defaultValue = "FALSE", label = "Create Ontology", displayPosition = 14, group = "SOFIA2D")
	//public Boolean createOntology;

	/** {@inheritDoc} */
	//@Override
	//public Boolean getCreateOntology() {
	//	return createOntology;
	//}
	
	@ConfigDef(required = true, type = ConfigDef.Type.NUMBER, defaultValue = "2", label = "ThreadPool", displayPosition = 14, group = "SOFIA2D")
	public Integer thread;

	/** {@inheritDoc} */
	@Override
	public Integer getThread() {
		return thread;
	}
}
