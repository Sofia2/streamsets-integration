package com.indra.sofia2.streamsets.format;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;


	@GenerateResourceBundle
	public enum DataFormat implements Label {
	  TEXT("Text"),
	  DELIMITED("Delimited");
	  ;

	private final String label;

	DataFormat(String label) {
	   this.label = label;
	}
	
	@Override
	public String getLabel() {
	  return label;
	}
	  
}
