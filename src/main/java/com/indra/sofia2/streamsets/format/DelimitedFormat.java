package com.indra.sofia2.streamsets.format;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;


	@GenerateResourceBundle
	public enum DelimitedFormat implements Label {
	  COMMA("COMMA (,)"),
	  SEMICOLONS("SEMI-COLONS (;)"),
	  COLON("COLON (:)"),
	  PIPES("PIPES (|)")
	  ;

	private final String label;

	DelimitedFormat(String label) {
	   this.label = label;
	}
	
	@Override
	public String getLabel() {
	  return label;
	}
	  
}
