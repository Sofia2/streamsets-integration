/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.origin;

import com.indra.sofia2.streamsets.GroupsSofia;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;

@StageDef(
	version = 1, 
	label = "Sofia2", 
	description = "Obtain data from Sofia2 Platform",
	execution= ExecutionMode.STANDALONE,
	icon = "sofia2.png", 
	recordsByRef = true, 
	resetOffset = true, 
	onlineHelpRefUrl = "www.sofia2.com")
@ConfigGroups(value = GroupsSofia.class)
@GenerateResourceBundle
public class Sofia2DOrigin extends DSource {

	@ConfigDef(
		required = true, 
		type = ConfigDef.Type.STRING, 
		label = "Host", 
		defaultValue = "localhost", 
		description = "Host to listen on",
		group = "SOFIA2",
		displayPosition = 10
	)
	public String host;

	@ConfigDef(
		required = true, 
		type = ConfigDef.Type.NUMBER,
		label = "Port",
		defaultValue = "8080", 
		description = "Port to listen on",
		group = "SOFIA2",
		displayPosition = 12
	)
	public Integer port;

	@ConfigDef(
		required = true, 
		type = ConfigDef.Type.STRING, 
		label = "Token",
		defaultValue = "token", 
		description = "Token used by Kp",
		group = "SOFIA2",
		displayPosition = 14
	)
	public String token;

	@ConfigDef(
		required = true, 
		type = ConfigDef.Type.STRING,
		label = "Kp",
		defaultValue = "kp", 
		description = "Kp used to connect",
		group = "SOFIA2",
		displayPosition = 16 
	)
	public String kp;

	@ConfigDef(
		required = true, 
		type = ConfigDef.Type.STRING,
		label = "Ontologia",
		defaultValue = "ontology", 
		description = "Ontology from obtain data",
		group = "SOFIA2",
		displayPosition = 18
	)
	public String ontology;

	@ConfigDef(
		required = true, 
		type = ConfigDef.Type.BOOLEAN, 
		label = "MQTT", 
		defaultValue = "TRUE", 
		description = "Use MQTT to connect with Sofia or REST API",
		group = "SOFIA2",
		displayPosition = 20
	)
	public Boolean mqttConnection;

	@ConfigDef(
		required = true, 
		type = ConfigDef.Type.STRING,
		label = "Query",
		defaultValue = "select * from ontology", 
		description = "Query use to obtain data",
		group = "SOFIA2O",
		displayPosition = 10	
	)
	public String query;

	@ConfigDef(
		required = true, 
		type = ConfigDef.Type.STRING,
		label = "Default name field", 
		defaultValue = "SQLLIKE", 
		description = "If field have not name used this Field Name",
		group = "SOFIA2O",
		displayPosition = 12
	)
	public String queryType;

	@Override
	protected Source createSource() {
		return new Sofia2Origin(host, port, token, kp, ontology, mqttConnection, query, queryType);
	}

}
