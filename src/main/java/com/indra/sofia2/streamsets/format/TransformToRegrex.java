package com.indra.sofia2.streamsets.format;

public class TransformToRegrex {

	
	public static String getDelimitedValue(DelimitedFormat delimitedFormat){
		
		switch (delimitedFormat) {
			case COLON:
				return ":";
			case SEMICOLONS:
				return ";";
			case COMMA:
				return ",";
			case PIPES:
				return "|";
			default:
				return null;
		}
	}
}
