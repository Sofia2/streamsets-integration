/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.origin;

import java.util.List;

import com.indra.sofia2.streamsets.GroupsExcel;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;


@StageDef(
	version = 1, 
	label = "Excel", 
	description = "Obtain data from Excel file",
	execution = ExecutionMode.STANDALONE, 
	icon = "excel.png", 
	recordsByRef = true, 
	resetOffset = true, 
	onlineHelpRefUrl = "www.sofia2.com")
@ConfigGroups(value = GroupsExcel.class)
@GenerateResourceBundle
public class ExcelDOrigin extends DSource {

	@ConfigDef(
		required = true, 
		type = ConfigDef.Type.STRING,
		label = "File", 
		defaultValue = "file",
		description = "Excel file to process",
		group = "EXCEL",
		displayPosition = 10
	)
	public String file;

	@ConfigDef(
		required = false, 
		type = ConfigDef.Type.MODEL,
		label = "Header", 
		defaultValue = "", 
		description = "Header name of all element, if void the first file is de Field names",
		group = "EXCEL",
		displayPosition = 12
	)
	@ListBeanModel
	public List<Cabecera> cabecera;

	@ConfigDef(
		required = true, 
		type = ConfigDef.Type.STRING,
		label = "Default Field", 
		defaultValue = "FIELD",
		description = "Default Field Name when no exist Field Name",
		group = "EXCEL",
		displayPosition = 12
	)
	public String defaultFieldName;

	public static class Cabecera {

		@ConfigDef(
			required = true, 
			type = ConfigDef.Type.STRING, 
			label = "Field Name"
		)
		public String field;
		
		public Cabecera(){
			
		}
		
		public Cabecera (String field){
			this.field=field;
		}
	}

	@Override
	protected Source createSource() {
		return new ExcelOrigin(file, cabecera, defaultFieldName);
	}

}
