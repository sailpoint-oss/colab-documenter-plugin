package sailpoint.mcdocumenter.task;

import java.io.File;
import java.io.FileInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import sailpoint.api.SailPointContext;
import sailpoint.api.IncrementalObjectIterator;
import sailpoint.api.PersistenceManager;
import sailpoint.api.ObjectUtil;
import sailpoint.task.BasePluginTaskExecutor;
import sailpoint.object.Attributes;
import sailpoint.object.Bundle;
import sailpoint.object.Custom;
import sailpoint.object.EmailFileAttachment;
import sailpoint.object.EmailOptions;
import sailpoint.object.EmailTemplate;
import sailpoint.object.Filter;
import sailpoint.object.QueryOptions;
import sailpoint.object.TaskResult;
import sailpoint.object.TaskResult.CompletionStatus;
import sailpoint.object.TaskSchedule;
import sailpoint.tools.GeneralException;
import sailpoint.tools.Util;
import sailpoint.tools.Message;
import sailpoint.tools.Message.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
//import org.apache.poi.ss.usermodel.Cell;
//import org.apache.poi.ss.usermodel.CellType;
//import org.apache.poi.ss.usermodel.Row;
//import org.apache.poi.ss.usermodel.Workbook;
//import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory;
//import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.util.*;

/**
 * Role Analysis
 *
 * @author Keith Smith
 */
public class CustomUploader extends BasePluginTaskExecutor {
  private static final String PLUGIN_NAME = "MCDocumenterPlugin";
  private static Log alog = LogFactory.getLog(CustomUploader.class);
  private boolean terminate=false;
  private String sailpointVersion="";
  private String sailpointPatch="";
  private StringBuffer sboutput=new StringBuffer();
  private String inputBasePath=null;
  private String inputFilename=null;
  private String mappingObjName=null;
  private Boolean deleteOnRead=false;
  private Boolean fullOverwrite=false;
  private boolean unixOrigin=false;
  private TaskResult taskResult=null;
  private String headerSize="0";
  private int linesToSkip=0;
  public boolean success=false;
  @SuppressWarnings({"rawtypes","unchecked"})
  @Override
  public void execute(SailPointContext context, TaskSchedule schedule,
    TaskResult result, Attributes args) throws Exception {
    taskResult=result;
    DateFormat sdfout=new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    DateFormat sdfnotime=new SimpleDateFormat("MM/dd/yyyy");
    Date now=new Date();
    Date nowNoTime=Util.getBeginningOfDay(now);
    String dayTimeStr=sdfnotime.format(nowNoTime);
    String runTimeStr=sdfout.format(now);
    sboutput.append("Processing data at "+runTimeStr);
    alog.debug("CUT-010 Processing data at "+runTimeStr);
    String folderSep=File.separator;
    if(folderSep.equals("/")) {
      unixOrigin=true;
    }
    sailpointVersion=sailpoint.Version.getVersion();
    sailpointPatch=sailpoint.Version.getPatchLevel();
    /*
     * basePath processing
     */
    if(args.containsKey("basePath")) {
      inputBasePath=args.getString("basePath");
      alog.debug("CUT-002 read basePath of "+inputBasePath);
      inputBasePath = inputBasePath.replaceAll("\\\\", "/");
      if (!inputBasePath.endsWith("/")) {
        inputBasePath = inputBasePath + "/";
      }
      // Check for existence of basePath
      File basePathObj=new File(inputBasePath);
      if(basePathObj.exists()) {
        alog.debug("CUT-018 The basePath "+inputBasePath+" exists");
        sboutput.append("\nFound input folder: "+inputBasePath);
      }
      else {
        sboutput.append("\nCould not find input folder: "+inputBasePath);
        alog.error("CUT-030 Could not find folder "+inputBasePath);
        taskResult.setAttribute("resultString", sboutput.toString());
        taskResult.setCompletionStatus(TaskResult.CompletionStatus.Error);
        taskResult.addMessage(new Message(Message.Type.Error,"Could not find input folder"));
        return;
      }
    }
    /*
     * fileName processing
     */
    int filetype=0;
    if(args.containsKey("fileName")) {
      inputFilename=args.getString("fileName");
      alog.debug("CUT-003 read fileName of "+inputFilename);
      sboutput.append("\nFilename specified: "+inputFilename);
    }
    else {
      sboutput.append("\nFilename not specified, searching");
      try {
        File basePathFolder=new File(inputBasePath);
        File[] files=basePathFolder.listFiles();
        if(files!=null) {
          for(File file: files) {
            if(!(file.isDirectory())) {
              String filename=file.getAbsolutePath();
              if(filename.endsWith(".xlsx")) {
                inputFilename=filename;
                filetype=1;
              }
              else if(filename.endsWith(".xls")) {
                inputFilename=filename;
                filetype=2;
              }
              else if(filename.endsWith(".csv")) {
                inputFilename=filename;
                filetype=3;
              }
            }
          }
        }
        sboutput.append("\nUsing filename "+inputFilename);
        alog.debug("CUT-004 filename="+inputFilename+" type="+filetype);
      }
      catch (Exception exf) {
        alog.error("CUT-005 "+exf.getClass().getName()+":"+exf.getMessage());
      }
    }
    File inputFile=new File(inputBasePath+inputFilename);
    if(inputFilename==null || !inputFile.exists()) {
      sboutput.append("\nCould not find input file");
      alog.error("CUT-031 Could not find input file");
      taskResult.setAttribute("resultString", sboutput.toString());
      taskResult.setCompletionStatus(TaskResult.CompletionStatus.Error);
      taskResult.addMessage(new Message(Message.Type.Error,"Could not find input folder"));
      return;
    }
    deleteOnRead=args.getBoolean("postDelete");
    alog.debug("CUT-045 deleteOnRead="+deleteOnRead);
    sboutput.append("\nDelete spreadsheet after reading: "+deleteOnRead);
    fullOverwrite=args.getBoolean("fullOverwrite");
    alog.debug("CUT-046 fullOverwite="+fullOverwrite);
    sboutput.append("\nFull overwrite of the Custom object: "+fullOverwrite);
    /*
     * headerCustomObj is the mapping object specified to map the sheet name to
     * the number of lines to skip for each sheet
     */
    Custom headerCustomObj=null;
    Attributes headerCustomAttr=null;
    Map headerCustomMap=null;
    if(args.containsKey("headerSize")) {
      headerSize=args.getString("headerSize");
      alog.debug("CUT-047 Header size input="+headerSize);
      sboutput.append("\nHeader size input="+headerSize);
      if(Util.isNumeric(headerSize)) {
        linesToSkip=Integer.parseInt(headerSize);
        alog.debug("CUT-048 Numeric, read lines to skip for all sheets = "+linesToSkip);
        sboutput.append("\nNumeric, read lines to skip for all sheets = "+linesToSkip);
      }
      else {
        headerCustomObj=context.getObjectByName(Custom.class,headerSize);
        if(headerCustomObj==null) {
          alog.debug("CUT-049 Did not find Custom object named "+headerSize);
          sboutput.append("\nDid not find Custom object named "+headerSize);
        }
        else {
          alog.debug("CUT-050 Found Custom object named "+headerSize);
          sboutput.append("\nFound Custom object named "+headerSize);
          headerCustomAttr=headerCustomObj.getAttributes();
          headerCustomMap=headerCustomAttr.getMap();
        }
      }
    }
    alog.debug("CUT-067 Grab the custom mapping object");
    /*
     * mappingCustomObj is the mapping object specified to map the sheet name to
     * the custom object being modified.
     */
    Custom mappingCustomObj=null;
    Attributes mappingCustomAttr=null;
    Map mappingCustomMap=null;
    if(args.containsKey("mappingObj")) {
      mappingObjName=args.getString("mappingObj");
      alog.debug("CUT-068 a custom mapping object was specified: "+mappingObjName);
      sboutput.append("\nCustom Mapping object specified: "+mappingObjName);
      mappingCustomObj=context.getObject(Custom.class,mappingObjName);
      if(mappingCustomObj!=null) {
        mappingCustomAttr=mappingCustomObj.getAttributes();
      }
      if(mappingCustomAttr!=null) {
        mappingCustomMap=mappingCustomAttr.getMap();
      }
    }
    else {
      alog.debug("CUT-069 no custom mapping object was specified");
    }
    /*
     * Open the spreadsheet
     */
    alog.debug("CUT-070 opening the spreadsheet now");
    FileInputStream fileStream = null;
    Workbook workBook = null;
    XSSFWorkbookFactory wbFactory=null;
    String modifiedCustomName=null;
    Custom modifiedCustomObj=null;
    Attributes modifiedCustomAttr=null;
    Map modifiedCustomMap=null;
    Custom objToUpdate=null;
    QueryOptions qo=new QueryOptions();
    Filter nameFilter=null;
    int mcCount=0;
    boolean customObjectFound=false;
    // keyColumn = 1 means first column
    int keyColumn=1;
    // valueColumn = 2 means second column, but -2 means start at 2 and then anything right of that
    int valueColumn=-2;
    int absValueColumn=2;
    // likewise keyColumnStr means keyColumn=1
    String keyColumnStr="A";
    // And valueColumnStr=B means valueColumn=2 and B+ means -2
    String valueColumnStr="B+";
    // Lines to skip
    String linesToSkipStr="0";
    boolean addLastUpdated=false;
    String lastUpdatedStr="";
    boolean addCustomNote=false;
    String customNote="";
    boolean addCustomDesc=false;
    String customDescription="";
    try {
      alog.debug("CUT-071 Opening "+inputFile.getAbsolutePath());
      sboutput.append("\nOpening "+inputFile.getAbsolutePath());
      fileStream = new FileInputStream(inputFile);
      alog.debug("CUT-072 Opening workbook");
      wbFactory=new XSSFWorkbookFactory();
      workBook = wbFactory.create(fileStream);
      int sheetNo = 0;
      int numSheets=workBook.getNumberOfSheets();
      alog.debug("CUT-073 Found the following number of sheets in the file:"+numSheets);
      sboutput.append("\nFound the following number of sheets in the file:"+numSheets);
      for (sheetNo=0; sheetNo < numSheets; sheetNo++) {
        int skipLines=linesToSkip;
        keyColumnStr="A";
        valueColumnStr="B+";
        linesToSkipStr="0";
        addLastUpdated=false;
        lastUpdatedStr=runTimeStr;
        addCustomNote=false;
        customNote="";
        addCustomDesc=false;
        customDescription="";
        alog.debug("CUT-014 getting sheet "+sheetNo);
        Sheet sheet = workBook.getSheetAt(sheetNo);
        String sheetName=sheet.getSheetName();
        alog.debug("CUT-033 sheet name:"+sheetName+", looking for mapping");
        sboutput.append("\nSheet name:"+sheetName+", looking for mapping");
        customObjectFound=false;
        /*
         * This section: the mappingCustom object is not found
         * If the fullOverwrite is true AND the sheet name starts with Custom-
         * then we can accept that the Custom object should be created new
         * with the name of the sheet MINUS the Custom-
         * and same if there is no mapping and it is found.
         */
        if(mappingCustomMap==null) {
          modifiedCustomName=sheetName;
          qo=new QueryOptions();
          nameFilter=Filter.eq("name",modifiedCustomName);
          qo.addFilter(nameFilter);
          mcCount=context.countObjects(Custom.class,qo);
          if(mcCount==0) {
            alog.debug("CUT-040 no mapping object, and could not find Custom object named "+modifiedCustomName);
            if(modifiedCustomName.startsWith("Custom-")) {
              alog.debug("CUT-051 the sheet name starts with Custom-");
              modifiedCustomName=sheetName.substring(7);
              alog.debug("CUT-052 trying search on "+modifiedCustomName);
              qo=new QueryOptions();
              nameFilter=Filter.eq("name",modifiedCustomName);
              qo.addFilter(nameFilter);
              mcCount=context.countObjects(Custom.class,qo);
              if(mcCount==0) {
                alog.debug("CUT-053 did not find Custom object named "+modifiedCustomName);
                if(fullOverwrite) {
                  sboutput.append("\nNo mapping object, will create Custom object named "+modifiedCustomName);
                  sboutput.append("\nColumn A will be the key entries");
                  sboutput.append("\nThe last column in each row will be the value entry");
                  sboutput.append("\nRows do not have to be fully populated");
                  alog.debug("CUT-054 Allowing creation of new Custom object named "+modifiedCustomName);
                }
                else {
                  sboutput.append("\nNo mapping object, and could not find Custom object named "+modifiedCustomName);
                  continue;
                }
              }
              else {
                alog.debug("CUT-074 Custom object found:"+modifiedCustomName);
                customObjectFound=true;
              }
            }
            else {
              sboutput.append("\nNo mapping object, and could not find Custom object named "+modifiedCustomName);
              sboutput.append("\nTo create new, preface sheet name with Custom- and set overwrite to true");
              continue;
            }
          }
          else {
            customObjectFound=true;
            alog.debug("CUT-041 no mapping object, but found Custom object named "+modifiedCustomName);
            sboutput.append("\nNo mapping object, but found Custom object named "+modifiedCustomName);
            sboutput.append("\nColumn A will be the key entries");
            sboutput.append("\nThe last column in each row will be the value entry");
            sboutput.append("\nRows do not have to be fully populated");
          }
        }
        else {
          alog.debug("CUT-090 mappingCustomMap found, looking for entry with key="+sheetName);
          if(mappingCustomMap.containsKey(sheetName)) {
            alog.debug("CUT-091 found entry with key="+sheetName+", reading the value");
            Object sheetNameValue=mappingCustomMap.get(sheetName);
            if(sheetNameValue instanceof String) {
              // <entry key="SHEET1" value="XYZ O365 License Map"/>
              modifiedCustomName=(String)(sheetNameValue);
              alog.debug("CUT-092 value is a string, output to "+modifiedCustomName+" with default columns");
              sboutput.append("\nMapping object contains single value: ["+sheetName+","+modifiedCustomName+"]");
              sboutput.append("\nColumn A will be the key entries");
              sboutput.append("\nThe last column in each row will be the value entry");
              sboutput.append("\nRows do not have to be fully populated");
            }
            else if(sheetNameValue instanceof Map) {
              alog.debug("CUT-093 value is a Map, getting the entries");
              Map sheetNameValueMap=(Map)sheetNameValue;
              // <entry key="SHEET1">
              //   <value>
              //     <Map>
              //       <entry key="CUSTOM_NAME" value="XYZ O365 License Map"/>
              //       <entry key="KEY_COLUMN" value="A"/>
              //       <entry key="VALUE_COLUMN" value="B"/>
              //     </Map>
              //   </value>
              // </entry>
              if(sheetNameValueMap.containsKey("CUSTOM_NAME")) {
                modifiedCustomName=(String)(sheetNameValueMap.get("CUSTOM_NAME"));
                alog.debug("CUT-094 will write "+sheetName+" to "+modifiedCustomName);
                sboutput.append("\nMapping object contains Map: ["+sheetName+","+modifiedCustomName+"]");
              }
              else {
                alog.error("CUT-095 Mapping object contains Map but needs CUSTOM_NAME entry");
                sboutput.append("\nMapping object contains Map but needs CUSTOM_NAME entry");
                continue;
              }
              if(sheetNameValueMap.containsKey("KEY_COLUMN")) {
                keyColumnStr=(String)(sheetNameValueMap.get("KEY_COLUMN"));
                alog.error("CUT-096 read in keyColumnStr = "+keyColumnStr);
              }
              if(sheetNameValueMap.containsKey("VALUE_COLUMN")) {
                valueColumnStr=(String)(sheetNameValueMap.get("VALUE_COLUMN"));
                alog.error("CUT-097 read in valueColumnStr = "+valueColumnStr);
              }
              if(sheetNameValueMap.containsKey("SKIP_LINES")) {
                linesToSkipStr=(String)(sheetNameValueMap.get("SKIP_LINES"));
                alog.error("CUT-097 read in linesToSkipStr = "+linesToSkipStr);
                if(Util.isNumeric(linesToSkipStr)) {
                  skipLines=Integer.parseInt(linesToSkipStr);
                  alog.debug("CUT-060 for sheet named "+sheetName+" skipping "+skipLines+" lines");
                }
              }
              if(sheetNameValueMap.containsKey("ADD_UPDATE_TIME")) {
                String addUpdatedValue=(String)sheetNameValueMap.get("ADD_UPDATE_TIME");
                if("true".equalsIgnoreCase(addUpdatedValue)) {
                  addLastUpdated=true;
                  alog.debug("CUT-100 Adding LAST_UPDATED to map data");
                }
              }
              if(sheetNameValueMap.containsKey("CUSTOM_NOTE")) {
                addCustomNote=true;
                customNote=(String)sheetNameValueMap.get("CUSTOM_NOTE");
              }
              if(sheetNameValueMap.containsKey("CUSTOM_DESCRIPTION")) {
                addCustomDesc=true;
                customDescription=(String)sheetNameValueMap.get("CUSTOM_DESCRIPTION");
              }
              sboutput.append("\nColumn "+keyColumnStr+" will be the key entries");
              sboutput.append("\nColumn "+valueColumnStr+" will be the value entries");
              sboutput.append("\nIf ends in + rows past will be read");
            }
            else {
              alog.error("CUT-059 value needs to be a string or Map");
              sboutput.append("\nMapping object contains misconfigured entry for "+sheetName);
              continue;
            }
            alog.debug("CUT-055 trying search on "+modifiedCustomName);
            qo=new QueryOptions();
            nameFilter=Filter.eq("name",modifiedCustomName);
            qo.addFilter(nameFilter);
            mcCount=context.countObjects(Custom.class,qo);
            if(mcCount==0) {
              alog.debug("CUT-042 mapping object found but, could not find Custom object named "+modifiedCustomName);
              if(fullOverwrite) {
                alog.debug("CUT-056 Allowing creation of new Custom object named "+modifiedCustomName);
              }
              else {
                sboutput.append("\nMapping object found but, could not find Custom object named "+modifiedCustomName);
                continue;
              }
            }
            else {
              customObjectFound=true;
              alog.debug("CUT-043  mapping object found, found Custom object named "+modifiedCustomName);
              sboutput.append("\nMapping object found, found Custom object named "+modifiedCustomName);
            }
          }
          else {
            alog.debug("CUT-044 mapping object "+mappingObjName+" is missing entry "+sheetName);
            sboutput.append("\nMapping object "+mappingObjName+" is missing entry "+sheetName);
            continue;
          }
        }
        if(headerCustomMap!=null) {
          alog.debug("CUT-061 looking in "+headerSize+" for "+sheetName);
          if(headerCustomMap.containsKey(sheetName)) {
            linesToSkipStr=(String)(headerCustomMap.get(sheetName));
            if(Util.isNumeric(linesToSkipStr)) {
              skipLines=Integer.parseInt(linesToSkipStr);
              alog.debug("CUT-062 for sheet named "+sheetName+" skipping "+skipLines+" lines");
            }
          }
        }
        alog.debug("CUT-015 getting sheet "+sheetName);
        alog.debug("CUT-063 for sheet named "+sheetName+" skipping "+skipLines+" lines");
        sboutput.append("\nGetting sheet "+sheetName);
        int mergeNum = sheet.getNumMergedRegions();
        alog.debug("CUT-016 there are "+mergeNum+" merged regions");
        List regionsList = new ArrayList();
        for(int i = 0; i < mergeNum; i++) {
          alog.debug("CUT-017 getting merged region "+i);
          regionsList.add(sheet.getMergedRegion(i));
        }
        /*
         * Create new map and read in the key value table
         */
        modifiedCustomMap=new HashMap();
        int lastRowNum = sheet.getLastRowNum();
        alog.debug("CUT-018 lastRowNum = "+lastRowNum);
        sboutput.append("\nSheet contains "+(lastRowNum+1)+" rows");
        boolean printedNumberOfColumns=false;
        int startRow=skipLines;
        /*
         * Get the key column and value column
         */
        keyColumn=translateCellNameToColumn(keyColumnStr);
        valueColumn=translateCellNameToColumn(valueColumnStr);
        absValueColumn=(valueColumn>0) ? valueColumn : -valueColumn;
        /*
         * Iterate through the rows
         */
        for (int rowIndex = startRow; rowIndex <= lastRowNum ; rowIndex++) {
          alog.debug("CUT-019 getting row "+rowIndex);
          Row row = sheet.getRow(rowIndex);
          if ( row == null ) {
            alog.debug("CUT-032 error: row is null");
            continue;
          }
          boolean firstCell = true;
          String key=null;
          int lastCellNum = row.getLastCellNum();
          alog.debug("CUT-021 for row "+rowIndex+", lastCellNum = "+lastCellNum);
          if(!printedNumberOfColumns) {
            sboutput.append("\nFor row "+rowIndex+", number of columns = "+lastCellNum);
            printedNumberOfColumns=true;
          }
          for (int cellIndex = 0; cellIndex < lastCellNum ; cellIndex++) {
            alog.debug("CUT-022 getting cell "+cellIndex);
            Cell cell = row.getCell(cellIndex);
            if(!firstCell) {
              alog.debug("CUT-023 not first cell");
            }
            if(cell!=null) {
              String value = "";
              CellType cellType=cell.getCellType();
              alog.debug("CUT-024 cellType="+cellType);
              if(cellType == CellType.STRING) {
                value = cell.getStringCellValue();
                alog.debug("CUT-025 STRING value of "+cell.getAddress()+" = "+value);
              }
              else if(cellType == CellType.NUMERIC) {
                double cellNumValue=cell.getNumericCellValue();
                alog.debug("CUT-026 NUMERIC(Date) value of "+cell.getAddress()+" = "+cellNumValue);
                java.util.Date cellDateValue=null;
                boolean canBeADate=false;
                try {
                  alog.debug("CUT-027 trying to get the cell date value");
                  cellDateValue=cell.getDateCellValue();
                  long diffInMillies = cellDateValue.getTime()-now.getTime();
                  alog.debug("CUT-028 diff in millseconds = "+diffInMillies);
                  if(diffInMillies < 0L)diffInMillies = -diffInMillies;
                  if(diffInMillies < (1000 * 60 * 60 * 24 * 365 * 10)) {
                    canBeADate=true;
                    alog.debug("CUT-029 this could be a date");
                  }
                  else {
                    alog.debug("CUT-034 this is not a date");
                  }
                }
                catch (Exception exd) {
                  alog.debug("CUT-037 "+exd.getClass().getName()+":"+exd.getMessage());
                  canBeADate=false;
                }
                if(canBeADate) {
                  alog.debug("CUT-036 formatting date");
                  DateFormat ymdFormat=new SimpleDateFormat("yyyy-MM-dd");
                  value=ymdFormat.format(cellDateValue);
                  alog.debug("CUT-038 NUMERIC(Date) value of "+cell.getAddress()+" = "+value);
                }
                else {
                  alog.debug("CUT-035 formatting number");
                  value=String.format("%.0f",cellNumValue);
                  alog.debug("CUT-039 NUMERIC value of "+cell.getAddress()+" = "+value);
                }
              }
              else if(cellType == CellType.BOOLEAN) {
                boolean cellBoolValue=cell.getBooleanCellValue();
                if(cellBoolValue)value="true";
                else value="false";
                alog.debug("CUT-026 BOOLEAN value of "+cell.getAddress()+" = "+value);
              }
              else if(cellType == CellType.BLANK) {
                value="";
              }
              else if(cellType == CellType.FORMULA) {
                value=cell.getCellFormula();
                if("TRUE()".equals(value)) {
                  value="true";
                }
                else if("FALSE()".equals(value)) {
                  value="false";
                }
              }
              else {
                value="Unknown cell type "+cellType;
              }
              //if ( Util.isNotNullOrEmpty(value) ) {
              //  if ( value.indexOf(',') != -1 ) {
              //    value = "\"" + value + "\"";
              //  }
              //  alog.debug("CUT-036 adding to the file:"+value);
              //}
              if(cellIndex == (keyColumn-1)) {
                alog.debug("CUT-064 cellIndex = "+cellIndex+", keyColumn="+keyColumn+", saving as key");
                key=value;
              }
              else if((valueColumn > 0) && (cellIndex == (valueColumn-1))) {
                alog.debug("CUT-065 cellIndex = "+cellIndex+", valueColumn="+valueColumn+", saving as value");
                modifiedCustomMap.put(key,value);
              }
              else if((valueColumn < 0) && (cellIndex >= (absValueColumn-1))) {
                alog.debug("CUT-066 cellIndex = "+cellIndex+", valueColumn="+valueColumn+", saving as value");
                modifiedCustomMap.put(key,value);
              }
            }
            firstCell=false;
          }
        }
        if(customObjectFound) {
          alog.debug("CUT-075 Spreadsheet read into map, writing Custom object");
          sboutput.append("\nSpreadsheet read into map, writing Custom object");
          objToUpdate = ObjectUtil.lockObject(context, Custom.class, null, modifiedCustomName,
            PersistenceManager.LOCK_TYPE_TRANSACTION);
          alog.debug("CUT-076 got the object and locked it");
          Map existingData = objToUpdate.getAttributes().getMap();
          alog.debug("CUT-077 got the map");
          String description = objToUpdate.getDescription();
          if(description!=null) {
            if(description.startsWith("\n")) {
              description=description.substring(1);
              description=description.trim();
            }
            if(description.startsWith("Created")) {
              String[] desclines=description.split("\n");
              if (desclines.length==1) {
                description = "\n" + "    " + description.trim() + "\n" + "    " + "Modified by MC Custom Uploader at "+runTimeStr+"\n"+"  ";
              }
              else {
                description = "\n" + "    " + desclines[0].trim() + "\n" + "    " + "Modified by MC Custom Uploader at "+runTimeStr+"\n"+"  ";
              }
            }
            else {
              description = "Modified by MC Custom Uploader at "+runTimeStr;
            }
          }
          else {
            description = "Modified by MC Custom Uploader at "+runTimeStr;
          }
          // Optional if CUSTOM_DESCRIPTION is set
          if(addCustomDesc) {
            description=customDescription;
          }
          objToUpdate.setDescription(description);
          if(fullOverwrite) {
            alog.debug("CUT-078 clearing old data");
            sboutput.append("\nClearing old data");
            existingData.clear();
          }
          alog.debug("CUT-079 updating the data");
          existingData.putAll(modifiedCustomMap);
          // Optional if ADD_UPDATE_TIME is set to true
          if(addLastUpdated) {
            existingData.put("LAST_UPDATED",runTimeStr);
          }
          // Optional if CUSTOM_NOTE is set
          if(addCustomNote) {
            existingData.put("CUSTOM_NOTE",customNote);
          }
          alog.debug("CUT-080 setting map with the new data");
          objToUpdate.getAttributes().setMap(existingData);
          alog.debug("CUT-081 saving the object");
          context.startTransaction();
          context.saveObject(objToUpdate);
          context.commitTransaction();
          alog.debug("CUT-082 unlocking");
          ObjectUtil.unlockObject(context, objToUpdate, PersistenceManager.LOCK_TYPE_TRANSACTION);
          objToUpdate=null;
        }
        else {
          alog.debug("CUT-090 Spreadsheet read into map, creating new Custom object");
          sboutput.append("\nSpreadsheet read into map, creating new Custom object");
          alog.debug("CUT-091 creating new custom object");
          objToUpdate = new Custom();
          alog.debug("CUT-092 setting name to:"+modifiedCustomName);
          objToUpdate.setName(modifiedCustomName);
          alog.debug("CUT-093 setting the map");
          objToUpdate.getAttributes().setMap(modifiedCustomMap);
          alog.debug("CUT-094 setting description");
          objToUpdate.setDescription("Created by MC Custom Uploader at "+runTimeStr);
          alog.debug("CUT-095 saving object");
          context.startTransaction();
          context.saveObject(objToUpdate);
          context.commitTransaction();
          alog.debug("CUT-096 completed");
          objToUpdate=null;
        }
      }
      success=true;
    }
    catch (Exception exw) {
      alog.error("CUT-910 "+exw.getClass().getName()+":"+exw.getMessage());
      exw.printStackTrace();
    }
    finally {
      try {
        if(objToUpdate!=null)ObjectUtil.unlockObject(context, objToUpdate, PersistenceManager.LOCK_TYPE_TRANSACTION);
        //if(fileStream!=null)fileStream.close();
        if(workBook!=null)workBook.close();
      }
      catch (Exception fex) {
        alog.error("CUT-920 "+fex.getClass().getName()+":"+fex.getMessage());
      }
    }
    /*
     * Wrap it up
     */
    taskResult.setAttribute("resultString", sboutput.toString());
    if(success) {
      taskResult.setCompletionStatus(TaskResult.CompletionStatus.Success);
      taskResult.addMessage(new Message(Message.Type.Info,"Processed"));
    }
    else {
      taskResult.setCompletionStatus(TaskResult.CompletionStatus.Warning);
      taskResult.addMessage(new Message(Message.Type.Warn,"Failed"));
    }
    alog.debug("CUT-012 exiting");
    return;
  }
  private int translateCellNameToColumn(String cn) {
    int rval=0;
    int col1=0;
    int col2=0;
    char A='A';
    alog.debug("CUT-301 Entered translateCellNameToColumn("+cn+")");
    String cellName=cn.toUpperCase();
    int nameLen=cellName.length();
    alog.debug("CUT-302 cellName length="+nameLen);
    int multiplier=1;
    int cellNumber=0;
    if(cellName.endsWith("+")) {
      multiplier=-1;
      nameLen=nameLen-1;
      cellName=cellName.substring(0,nameLen);
      alog.debug("CUT-303 found + suffix, new cellName="+cellName);
    }
    if(nameLen > 1) {
      char ch=cellName.charAt(0);
      col1=(ch-A);
      ch=cellName.charAt(1);
      col2=(ch-A);
    }
    else {
      char ch=cellName.charAt(0);
      col2=(ch-A);
    }
    alog.debug("CUT-304 col1="+col1+" col2="+col2);
    rval=(26*col1)+col2+1;
    rval=multiplier*rval;
    alog.debug("CUT-305 returning "+rval);
    return rval;
  }
  public boolean terminate() {
    terminate=true;
    taskResult.setTerminated(true);
    if (alog.isDebugEnabled())
      alog.debug("Task was terminated."); 
    return true;
  }
  
  public String getPluginName() {
    return PLUGIN_NAME;
  }
}