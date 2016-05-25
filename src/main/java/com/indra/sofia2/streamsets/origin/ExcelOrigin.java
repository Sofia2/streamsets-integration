/*******************************************************************************
 * © Indra Sistemas, S.A.
 * 2013 - 2014  SPAIN
 * 
 * All rights reserved
 ******************************************************************************/
package com.indra.sofia2.streamsets.origin;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import com.indra.sofia2.streamsets.Errors;
import com.indra.sofia2.streamsets.GroupsExcel;
import com.indra.sofia2.streamsets.origin.ExcelDOrigin.Cabecera;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;

/**
 * This target is an example and does not actually write to any destination.
 */
public class ExcelOrigin extends BaseSource {

	public boolean noFinish=true;
	
	public String file;
	public List<Cabecera> cabecera;
	public String defaultFieldName;

	public ExcelOrigin(String file,
			  List<Cabecera> cabecera,
			  String defaultFieldName
		  ){
		this.file=file;
		this.cabecera=cabecera;
		this.defaultFieldName=defaultFieldName;
	}

	/** {@inheritDoc} */
	@Override
	protected List<ConfigIssue> init() {
		// Validate configuration values and open any required resources.
		List<ConfigIssue> issues = super.init();

		if (file.equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsExcel.EXCEL.name(), "config", Errors.ERROR_00,
					"Se requiere la propiedad File"));
		}
		if (cabecera.equals("invalidValue")) {
			issues.add(getContext().createConfigIssue(GroupsExcel.EXCEL.name(), "config", Errors.ERROR_00,
					"Las cabeceras no son válidas"));
		}
		return issues;
	}

	/** {@inheritDoc} */
	@Override
	public void destroy() {
		// Clean up any open resources.
		super.destroy();
	}

	/** {@inheritDoc} */
	@Override
	public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
		long nextSourceOffset = 0;
		if (noFinish){
			// Offsets can vary depending on the data source. Here we use an integer
			// as an example only.
			if (lastSourceOffset != null) {
				nextSourceOffset = Long.parseLong(lastSourceOffset);
			}
			FileInputStream file=null;
			HSSFWorkbook workbook=null;
			try{
				try {
					file = new FileInputStream(new File(this.file));
				} catch (FileNotFoundException e) {
					throw new StageException(Errors.ERROR_02, this.file);
				}
				//Get the workbook instance for XLS file 
				try {
					workbook = new HSSFWorkbook(file);
				} catch (IOException e) {
					throw new StageException(Errors.ERROR_03, e.getMessage(), this.file);
				}
				int numRecords = 0;
				Record record = getContext().createRecord(this.file + nextSourceOffset);
				for (int i=0; i<workbook.getNumberOfSheets(); i++){
					//Get the sheet from the workbook
					HSSFSheet sheet = workbook.getSheetAt(i);
					//Get iterator to all the rows in current sheet
					Iterator<Row> rowIterator = sheet.iterator();
					Boolean isCabecera=true;
					List<Cabecera> cabecera=new ArrayList<Cabecera>();
					if (this.cabecera.size()>0){
						cabecera=this.cabecera;
						isCabecera=false;
					}
					while (rowIterator.hasNext()){
						Row row =rowIterator.next();
						//Get iterator to all cells of current row
						Iterator<Cell> cellIterator = row.cellIterator();
						Map<String, Field> map= new HashMap<String, Field>();
						int cabeceraPosition=0;
						while (cellIterator.hasNext()){
							Cell cell =cellIterator.next();
							if (isCabecera){
								EvaluateCabecera(cell, cabecera);
							}else{
								try{
									map.put(cabecera.get(cabeceraPosition).field, EvaluateCell(cell));
								}catch(NullPointerException e){
									map.put(this.defaultFieldName, EvaluateCell(cell));
								}catch (IndexOutOfBoundsException e) {
									map.put(this.defaultFieldName, EvaluateCell(cell));
								}
								cabeceraPosition++;
							}
						}
						if (!isCabecera){
							record.set(Field.create(map));
							batchMaker.addRecord(record);
						}
						isCabecera=false;
						++nextSourceOffset;
						++numRecords;
					}
				}
				return String.valueOf(nextSourceOffset);
			}finally{
				try {
					if (workbook!=null){
						workbook.close();
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					if (file!=null){
						file.close();
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		noFinish=false;
		return String.valueOf(nextSourceOffset);
	}
	
	private Field EvaluateCell(Cell cell){
		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_BLANK:
			return Field.create("");
		case Cell.CELL_TYPE_BOOLEAN:
			return Field.create(cell.getBooleanCellValue());
		case Cell.CELL_TYPE_ERROR:
			return Field.create(String.valueOf(cell.getErrorCellValue()));
		case Cell.CELL_TYPE_FORMULA:
			int formulaResultType = cell.getCachedFormulaResultType();
			switch (formulaResultType) {
				case Cell.CELL_TYPE_BOOLEAN:
					return Field.create(cell.getBooleanCellValue());
				case Cell.CELL_TYPE_ERROR:
					return Field.create(String.valueOf(cell.getErrorCellValue()));
				case Cell.CELL_TYPE_NUMERIC:
					if (HSSFDateUtil.isCellDateFormatted(cell)){
						return Field.createDate(cell.getDateCellValue());
					}
					return Field.create(cell.getNumericCellValue());
				case Cell.CELL_TYPE_STRING:
					return Field.create(cell.getStringCellValue());
				default:
					return Field.create(this.defaultFieldName);
			}
		case Cell.CELL_TYPE_NUMERIC:
			if (HSSFDateUtil.isCellDateFormatted(cell)){
				return Field.createDate(cell.getDateCellValue());
			}
			return Field.create(cell.getNumericCellValue());
		case Cell.CELL_TYPE_STRING:
			return Field.create(cell.getStringCellValue());
		default:
			return Field.create(this.defaultFieldName);
		}
	}
	
	private void EvaluateCabecera (Cell cell, List<Cabecera> cabecera){
		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_BLANK:
			cabecera.add(new Cabecera(this.defaultFieldName));
			break;
		case Cell.CELL_TYPE_BOOLEAN:
			cabecera.add(new Cabecera(String.valueOf(cell.getBooleanCellValue())));
			break;
		case Cell.CELL_TYPE_ERROR:
			cabecera.add(new Cabecera(String.valueOf(cell.getErrorCellValue())));
			break;
		case Cell.CELL_TYPE_FORMULA:
			int formulaResultType = cell.getCachedFormulaResultType();
			switch (formulaResultType) {
				case Cell.CELL_TYPE_BOOLEAN:
					cabecera.add(new Cabecera(String.valueOf(cell.getBooleanCellValue())));
					break;
				case Cell.CELL_TYPE_ERROR:
					cabecera.add(new Cabecera(String.valueOf(cell.getErrorCellValue())));
					break;
				case Cell.CELL_TYPE_NUMERIC:
					cabecera.add(new Cabecera(String.valueOf(cell.getNumericCellValue())));
					break;
				case Cell.CELL_TYPE_STRING:
					cabecera.add(new Cabecera(String.valueOf(cell.getStringCellValue())));
					break;
				default:
					cabecera.add(new Cabecera(this.defaultFieldName));
					break;
			}
			break;
		case Cell.CELL_TYPE_NUMERIC:
			cabecera.add(new Cabecera(String.valueOf(cell.getNumericCellValue())));
			break;
		case Cell.CELL_TYPE_STRING:
			cabecera.add(new Cabecera(String.valueOf(cell.getStringCellValue())));
			break;
		default:
			cabecera.add(new Cabecera(this.defaultFieldName));
			break;
		}
	}
}
