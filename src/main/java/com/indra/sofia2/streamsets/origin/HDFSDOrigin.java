/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.origin;

import com.indra.sofia2.streamsets.GroupsHDFS;
import com.indra.sofia2.streamsets.format.DataFormat;
import com.indra.sofia2.streamsets.format.DataFormatChooserValues;
import com.indra.sofia2.streamsets.format.DelimitedFormat;
import com.indra.sofia2.streamsets.format.DelimitedFormatChooserValues;
import com.indra.sofia2.streamsets.format.HeaderFormat;
import com.indra.sofia2.streamsets.format.HeaderFormatChooserValues;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;

@StageDef(version = 1, label = "HDFS", description = "Reads data from Hadoop File System", execution = ExecutionMode.STANDALONE, icon = "hdfs.png", recordsByRef = true, resetOffset = true, onlineHelpRefUrl = "www.sofia2.com")
@ConfigGroups(value = GroupsHDFS.class)
@GenerateResourceBundle
public class HDFSDOrigin extends DSource {

	@ConfigDef(required = true, type = ConfigDef.Type.STRING, label = "HDFS URI", defaultValue = "hdfs://localhost:8020", description = "Hadoop URL and port. For example: hdfs://localhost:8020", group = "HDFS", displayPosition = 10)
	public String hdfsUri;

	@ConfigDef(required = true, type = ConfigDef.Type.STRING, label = "HDFSUser", defaultValue = "cloudera-scm", description = "Reads from HDFS as this user", group = "HDFS", displayPosition = 12)
	public String hdfsUser;

	@ConfigDef(required = true, type = ConfigDef.Type.STRING, label = "InputPath", defaultValue = "sofia/test.txt", description = "HDFS Input Path", group = "HDFS", displayPosition = 14)
	public String hdfsInputPath;

	@ConfigDef(required = true, type = ConfigDef.Type.MODEL, label = "Data Format", description = "Format of data in the files", displayPosition = 16, group = "HDFS")
	@ValueChooserModel(DataFormatChooserValues.class)
	public DataFormat dataFormat;
	
	@ConfigDef(required = true, type = ConfigDef.Type.NUMBER, label = "Number of threads",defaultValue = "1",description = "Number of threads created for data processing. 1 means sequential processing", group = "HDFS", displayPosition = 18)
	public Integer numberThreads;
	
	// DELIMITADO//
	@ConfigDef(required = false, type = ConfigDef.Type.MODEL, label = "CSV Delimiter",description = "CSV Delimiter. By default: COMMA", group = "DELIMITED", displayPosition = 10)
	@ValueChooserModel(DelimitedFormatChooserValues.class)
	public DelimitedFormat CSVDelimiter;
	
	@ConfigDef(required = false, type = ConfigDef.Type.MODEL, label = "Delimiter Format Type",description = "Delimiter Format Type. By default: WITHHEADERLINE ", group = "DELIMITED", displayPosition = 12)
	@ValueChooserModel(HeaderFormatChooserValues.class)
	public HeaderFormat headerFormat;
	
	
	//////////
	
	@Override
	protected Source createSource() {
		return new HDFSOrigin(hdfsUri, hdfsUser, hdfsInputPath,dataFormat,CSVDelimiter,headerFormat,numberThreads);
	}
}
