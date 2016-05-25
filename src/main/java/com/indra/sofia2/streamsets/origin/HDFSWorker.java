package com.indra.sofia2.streamsets.origin;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import com.indra.sofia2.streamsets.format.DelimitedFormat;
import com.indra.sofia2.streamsets.format.TransformToRegrex;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source.Context;

public class HDFSWorker implements Callable<Record>{

	private Context context;
	private String hdfsInputPath;
	private int nextSourceOffset;
	private String line;
	private List<String> cabecera;
	private boolean isText;
	private DelimitedFormat delimiterFormat;
	
	public HDFSWorker(Context context, String hdfsInputPath, int nextSourceOffset,String line,boolean isText,List<String> cabecera,DelimitedFormat delimiterFormat) {
		this.context = context;
		this.hdfsInputPath = hdfsInputPath;
		this.nextSourceOffset = nextSourceOffset;
		this.line = line;
		this.isText=isText;
		this.cabecera=cabecera;
		this.delimiterFormat=delimiterFormat;
	}

	@Override
	public Record call() throws Exception {

		if (isText){
			return analyzeText();
		} else {
			return analyzeDelimited();
		}
	}
	
	private Record analyzeText() throws Exception {
		
		Map<String, Field> map= new HashMap<String, Field>();
		Record record = context.createRecord(hdfsInputPath+nextSourceOffset);

		map.put("text",Field.create(line));
		record.set(Field.create(map));
		return record;
	}
	
	private Record analyzeDelimited() throws Exception {
		
		Map<String, Field> map= new HashMap<String, Field>();

		String[] listadoSplit=line.split(Pattern.quote(TransformToRegrex.getDelimitedValue(delimiterFormat)));
		Record record =context.createRecord(hdfsInputPath+nextSourceOffset);
		int contador=0;
		map= new LinkedHashMap<String, Field>();
		
		if (this.cabecera!=null){
			for (String temp:listadoSplit){    	
			    map.put(cabecera.get(contador),Field.create(temp));
				contador++;
			}
			if (cabecera.size()>contador+1){
			    map.put(cabecera.get(contador+1),Field.create(""));
			}
		} else {
			for (String temp:listadoSplit){    	
			    map.put(String.valueOf(contador),Field.create(temp));
				contador++;
			}		
		}
		record.set(Field.createListMap((LinkedHashMap<String, Field>) map));

	    return record;
	}
}
