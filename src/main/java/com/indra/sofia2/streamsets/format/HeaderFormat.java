package com.indra.sofia2.streamsets.format;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;


	@GenerateResourceBundle
	public enum HeaderFormat implements Label {
	  WITHHEADERLINE("With Header Line"),
	  IGNOREHEADERLINE("Ignore Header Line"),
	  NOHEADERLINE("No Header Line");
	  ;

	private final String label;

	HeaderFormat(String label) {
	   this.label = label;
	}
	
	@Override
	public String getLabel() {
	  return label;
	}
	  
}
