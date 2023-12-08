package sailpoint.mcdocumenter.task;

import java.io.*;
import java.text.*;
import java.util.*;
import java.nio.charset.StandardCharsets;
import sailpoint.api.SailPointFactory;
import sailpoint.api.SailPointContext;
import sailpoint.object.Resolver;
import sailpoint.object.SailPointObject;
import sailpoint.api.IncrementalObjectIterator;
import sailpoint.task.BasePluginTaskExecutor;
import sailpoint.tools.GeneralException;
import sailpoint.tools.Util;
import sailpoint.object.Application;
import sailpoint.object.Attributes;
import sailpoint.object.AttributeDefinition;
import sailpoint.object.AttributeSource;
import sailpoint.object.AttributeTarget;
import sailpoint.object.Bundle;
import sailpoint.object.Configuration;
import sailpoint.object.EmailFileAttachment;
import sailpoint.object.EmailOptions;
import sailpoint.object.EmailTemplate;
import sailpoint.object.Filter;
import sailpoint.object.Identity;
import sailpoint.object.IdentityTypeDefinition;
import sailpoint.object.ObjectAttribute;
import sailpoint.object.ObjectConfig;
import sailpoint.object.QueryOptions;
import sailpoint.object.Rule;
import sailpoint.object.Schema;
import sailpoint.object.TaskResult;
import sailpoint.object.TaskResult.CompletionStatus;
import sailpoint.object.TaskSchedule;
import sailpoint.tools.Message;
import sailpoint.tools.Message.Type;
// Mail imports
import javax.mail.internet.MimeMessage;
import javax.mail.internet.InternetAddress;
import javax.mail.BodyPart;
import javax.mail.internet.MimeBodyPart;
import javax.mail.Session;
import javax.mail.internet.MimeMultipart;
import javax.mail.Multipart;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.activation.DataHandler;
import javax.mail.internet.MimePartDataSource;
import javax.mail.Transport;

//Excel imports
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Role Analysis
 *
 * @author Keith Smith
 */
public class Documenter extends BasePluginTaskExecutor {
  private static final String PLUGIN_NAME = "MCDocumenterPlugin";
  private static Log log = LogFactory.getLog(Documenter.class);
  private boolean terminate=false;
  private String sailpointVersion="";
  private String sailpointPatch="";
  private StringBuffer sbfile=new StringBuffer();
  private StringBuffer sboutput=new StringBuffer();
  private TaskResult taskResult;
  private String _basePath=null;
  private String _identityFileName=null;
  private String _mailTo=null;
  private String _mailTemplate="Default Report Template";
  private boolean unixOrigin=false;
  private boolean showHierarchy=false;
  private Boolean documentIdentityAttributes=false;
  private boolean successfulFileWrite=false;
  private String outputFilenameStr="";
  private String outputFolderpath="";
  private String outputFilename="";
  private String _ignoreApps=null;
  private Map<String,Map<String,Integer>> appOrdinals=new HashMap<String,Map<String,Integer>>();
  @SuppressWarnings({"rawtypes","unchecked"})
  @Override
  public void execute(SailPointContext context, TaskSchedule schedule,
    TaskResult result, Attributes args) throws Exception {
    DateFormat sdfout=new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    DateFormat sdfnotime=new SimpleDateFormat("MM/dd/yyyy");
    Date now=new Date();
    Date nowNoTime=Util.getBeginningOfDay(now);
    String dayTimeStr=sdfnotime.format(nowNoTime);
    String runTimeStr=sdfout.format(now);
    String folderSep=File.separator;
    if(folderSep.equals("/")) {
      unixOrigin=true;
    }
    sailpointVersion=sailpoint.Version.getVersion();
    sailpointPatch=sailpoint.Version.getPatchLevel();
    List<String> ignoredApps=new ArrayList<String>();
    if(args.containsKey("ignoreApps")) {
      _ignoreApps=args.getString("ignoreApps");
      log.debug("DOC-121 ignoreApps = "+_ignoreApps);
      if(_ignoreApps.contains(",")) {
        String[] ignoreAppsArr=_ignoreApps.split(",");
        for(int iig=0; iig<ignoreAppsArr.length; iig++) {
          String val=ignoreAppsArr[iig].trim();
          ignoredApps.add(val);
        }
      }
      else {
        ignoredApps.add(_ignoreApps.trim());
      }
      log.debug("DOC-122 ignoredApps="+ignoredApps.toString());
    }
    /*
     * basePath processing
     */
    if(args.containsKey("basePath")) {
      _basePath=args.getString("basePath");
      log.debug("DOC-002 read basePath of "+_basePath);
      _basePath = _basePath.replaceAll("\\\\", "/");
      if (!_basePath.endsWith("/")) {
        _basePath = _basePath + "/";
      }
      if(_basePath.contains("$date$")) {
        DateFormat bpdf=new SimpleDateFormat("yyyyMMdd");
        String nowStr=bpdf.format(now);
        _basePath=_basePath.replace("$date$", nowStr);
      }
      if(_basePath.contains("$datetime$")) {
        DateFormat bpdf=new SimpleDateFormat("yyyyMMdd-HHmm");
        String nowStr=bpdf.format(now);
        _basePath=_basePath.replace("$datetime$", nowStr);
      }
      // Check for existence of basePath
      File basePathObj=new File(_basePath);
      if(basePathObj.exists()) {
        log.debug("DOC-018 The basePath "+_basePath+" exists");
      }
      else {
        if(basePathObj.mkdirs()) {
          log.debug("DOC-019 Successfully created "+_basePath);
        }
        else {
          log.error("DOC-020 Could not create folder "+_basePath);
          taskResult.setCompletionStatus(TaskResult.CompletionStatus.Error);
          taskResult.addMessage(new Message(Message.Type.Error,"Could not create output folder"));
          return;
        }
      }
    }
    /*
     * fileName processing
     */
    documentIdentityAttributes=args.getBoolean("documentIdentityAttrs");
    if(documentIdentityAttributes.booleanValue()) {
      if(args.containsKey("identityFileName")) {
        _identityFileName=args.getString("identityFileName");
        log.debug("DOC-003 read fileName of "+_identityFileName);
        if(_identityFileName.contains("$date$")) {
          DateFormat bpdf=new SimpleDateFormat("yyyyMMdd");
          String nowStr=bpdf.format(now);
          _identityFileName=_identityFileName.replace("$date$", nowStr);
        }
        if(_identityFileName.contains("$datetime$")) {
          DateFormat bpdf=new SimpleDateFormat("yyyyMMdd-HHmm");
          String nowStr=bpdf.format(now);
          _identityFileName=_identityFileName.replace("$datetime$", nowStr);
        }
      }
    }
    outputFolderpath=_basePath;
    outputFilename=_identityFileName;
    if(outputFilename.endsWith(".xlsx")) {
      log.debug("DOC-024 outputFilename already ends with excel file extension");
    }
    else {
      outputFilename=outputFilename+".xlsx";
      log.debug("DOC-024 outputFilename added excel file extension");
    }
    if(outputFolderpath.endsWith(folderSep)) {
      outputFilenameStr=outputFolderpath+outputFilename;
    }
    else {
      outputFilenameStr=outputFolderpath+folderSep+outputFilename;
    }
    /*
     * Process email settings
     */
    if(args.containsKey("mailTo")) {
      _mailTo=args.getString("mailTo");
      log.debug("DOC-004 read mailTo of "+_mailTo);
    }
    if(args.containsKey("mailTemplate")) {
      _mailTemplate=args.getString("mailTemplate");
      log.debug("DOC-005 read mailTemplate of "+_mailTemplate);
    }
    /*
     * System Configuration
     */
    Configuration systemConfiguration=Configuration.getSystemConfig(); // context.getObjectByName(Configurate.class,"SystemConfiguration");
    String serverRootPath=systemConfiguration.getString(systemConfiguration.SERVER_ROOT_PATH);
    log.debug("DOC-021 serverRootPath = "+serverRootPath);
    log.debug("DOC-022 version = "+sailpointVersion);
    log.debug("DOC-023 patch level = "+sailpointPatch);
    
    SailPointContext neuCtx = SailPointFactory.createPrivateContext();
    SailPointObject clonedObj=null;
    
    if(documentIdentityAttributes.booleanValue()) {
      /*
       * Start to create the Excel file
       */
      Workbook wb = null;
      OutputStream fileOut = null;
      Map<Integer,Row> rowMap=new HashMap<Integer,Row>();
      Integer rowOrdinal=null;
      int rowNumber=0;
      int columnNumber=0;
      int maxColumnNumber=0;
      boolean advanceRow=true;
      try {
        log.debug("DOC-070 creating a new XSSFWorkbook");
        wb = new XSSFWorkbook();
        fileOut = new FileOutputStream(outputFilenameStr);
        CreationHelper createHelper = wb.getCreationHelper();
        log.debug("DOC-071 Creating a new sheet with the name Types");
        CellStyle baseCellStyle=wb.createCellStyle();
        CellStyle highlightStyle=wb.createCellStyle();
        highlightStyle.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
        highlightStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        Sheet sheet = wb.createSheet("Types");
        /*
         * Write the serverRootPath and version data
         */
        log.debug("DOC-072 Creating the first row");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,new ArrayList<Object>(Arrays.asList("ServerRootPath",serverRootPath)));
        rowNumber++;
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,new ArrayList<Object>(Arrays.asList("Version",sailpointVersion)));
        rowNumber++;
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,new ArrayList<Object>(Arrays.asList("Patch Level",sailpointPatch)));
        rowNumber+=3;
        /*
         * Identity Attributes
         */
        // Gather identity attributes
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,new ArrayList<Object>(Arrays.asList("Identity Types")));
        rowNumber+=2;
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,new ArrayList<Object>(Arrays.asList("Name","Display Name","Description","Disallowed","Manager Cert Attr")));
        /*
         * Found that if you remove from the sources or targets objects, it actually modifies the ObjectConfig-Identity object.
         * Hence in order to remove applications I have to use a clone.  This is the proper cloning method.
         */
        ObjectConfig fromObj=null;
        ObjectConfig identityConfig=null;
        fromObj=neuCtx.getObjectByName(ObjectConfig.class,"Identity");
        identityConfig=(ObjectConfig)fromObj.derive((Resolver)neuCtx);
        identityConfig.setName("ClonedIdentity");
        log.debug("DOC-041 obtained identityConfig");
        /*
         * First analyze the identity types
         */
        Map<String,IdentityTypeDefinition> identityTypes=identityConfig.getIdentityTypesMap();
        for(String idtype:identityTypes.keySet()) {
          log.debug("DOC-042 looking at type="+idtype);
          IdentityTypeDefinition tdef=identityTypes.get(idtype);
          List<Object> values=new ArrayList<Object>();
          log.debug("DOC-043 type name = "+tdef.getName()+" displayName="+tdef.getDisplayName());
          values.add(tdef.getName());
          values.add(tdef.getDisplayName());
          log.debug("DOC-044 type description = "+tdef.getDescription());
          values.add(tdef.getDescription());
          log.debug("DOC-045 disallowed attributes = "+(tdef.getDisallowedAttributes()).toString());
          values.add((tdef.getDisallowedAttributes()).toString());
          log.debug("DOC-046 manager cert attribute = "+tdef.getManagerCertifierAttribute());
          values.add(tdef.getManagerCertifierAttribute());
          rowNumber++;
          writeRow(sheet,rowNumber,rowMap,baseCellStyle,values);
        }
        maxColumnNumber=5;
        for (int columnIndex=0; columnIndex < maxColumnNumber; ++columnIndex) {
          sheet.autoSizeColumn(columnIndex);
          int colWidth=sheet.getColumnWidth(columnIndex);
          float colWidthPix=sheet.getColumnWidthInPixels(columnIndex);
          log.debug("DOC-084 Column number: "+columnIndex+" has width: "+colWidth+" / "+colWidthPix);
          // Adding two characters in case the user wants to hit the Filter button.
          colWidth=colWidth+512;
          sheet.setColumnWidth(columnIndex,colWidth);
        }
        rowNumber+=3;
        List<Object> kv=new ArrayList<Object>();
        kv.add("Key to Attributes sheet");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Column");
        kv.add("Description");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Num");
        kv.add("This is the row number");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Src");
        kv.add("Source application ordinal");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Source System");
        kv.add("Source Application");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Source Attribute");
        kv.add("The attribute or rule");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Usg");
        kv.add("Usage: U=No interaction I=has a source T=has a target F=has a source and target");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Idn");
        kv.add("Identity ordinal in ObjectConfig");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Ido");
        kv.add("Ordinal of this identity row");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Identity Attr");
        kv.add("Formal name of attribute");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Display Name");
        kv.add("Display name of the attribute");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Props");
        kv.add("Properties: S-NC = Named Column  S-E# = Extended attribute  S-O = Searchable (OOTB)");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("");
        kv.add("            GF = Group Factory  I = Identity class");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("");
        kv.add("            E-P = Editable Perm   E-U = Editable (Temp)");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Tgn");
        kv.add("Ordinal on the Target application");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Target App");
        kv.add("The target application");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Attr Name");
        kv.add("Target attribute");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Transform Rule");
        kv.add("Transformation Rule");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        sheet.autoSizeColumn(0);
        int xcolWidth=sheet.getColumnWidth(0);
        xcolWidth=xcolWidth+512;
        sheet.setColumnWidth(0,xcolWidth);
        rowNumber+=3;
        kv.clear();
        kv.add("Helpful hints");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("To restore the sheet, sort on Column A (Num) with headers on");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("To see only the identity attributes, auto-filter and choose 1 from the Ido column (G)");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Auto-filter on source or target apps to see how they interact");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("The application ordinals are where they reside in their Schema");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("The identity ordinals are where they reside in the ObjectConfig-Identity");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("Some identity attributes have been ignored:");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("lastLogin,capabilities,assignedRoles,lastRefresh,bundleSummary,workgroups,exceptions,");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber++;
        kv.clear();
        kv.add("assignedRoleSummary,administrator,managerStatus,bundles,rights,scorecard.compositeScore,softwareVersion");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        rowNumber+=2;
        kv.clear();
        kv.add("Author: Keith Smith");
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,kv);
        sboutput.append("\nSuccessfully populated Types sheet");
        /*
         * Create a new sheet with the identity attributes on it.
         */
        sheet = wb.createSheet("Attributes");
        log.debug("DOC-050 Created Attributes sheet");
        Map<String,ObjectAttribute> objMap=identityConfig.getObjectAttributeMap();
        rowNumber=0;
        rowMap.clear();
        writeRow(sheet,rowNumber,rowMap,baseCellStyle,new ArrayList<Object>(Arrays.asList(
          "Num","Src","Source System","Source Attribute","Usg","Idn","Ido","Identity Attr","Display Name","Props",
          "Tgn","Target App","Attr Name","Transform Rule"
        )));
        rowNumber++;
        int numOrdinal=0;
        Map<String,Integer> srcOrdinalMap=new HashMap<String,Integer>();
        Map<String,Integer> tgtOrdinalMap=new HashMap<String,Integer>();
        Object srcOrdinalObj=new String("");
        Integer srcOrdinalInt=Integer.valueOf(1);
        Object tgtOrdinalObj=new String("");
        Integer tgtOrdinalInt=Integer.valueOf(1);
        int identOrdinal=0;
        List<String> ignoreAttrs=new ArrayList<String>(Arrays.asList("lastLogin","capabilities",
          "assignedRoles","lastRefresh","bundleSummary","workgroups","exceptions",
          "assignedRoleSummary","administrator","managerStatus","bundles","rights",
          "scorecard.compositeScore","softwareVersion"));
        String primarySourceAppName="";
        String primaryTargetAppName="";
        for(String objAttrName: objMap.keySet()) {
          log.debug("DOC-051 analyzing objAttrName:"+objAttrName);
          if(ignoreAttrs.contains(objAttrName)) {
            log.debug("DOC-052 ignoring "+objAttrName);
            continue;
          }
          List<Object> lineList=new ArrayList<Object>();
          ObjectAttribute objAttr=objMap.get(objAttrName);
          // numOrdinal++;  this does not belong here any more
          srcOrdinalObj=new String("");
          tgtOrdinalObj=new String("");
          /*
           * The 7th and 8th attributes, respectively, are:
           * objAttrName     - the raw attribute name
           * attrDisplayName - the attribute's display name
           */
          String attrDisplayName=objAttr.getDisplayName();
          /*
           * The 2nd, 3rd, 4th attributes include the source system
           * application, the source system attribute, and its ordinal
           * I originally thought I could use clone but it is restricted.
           */
          List<AttributeSource> sources=objAttr.getSources();
          /*List<AttributeSource> sources=new ArrayList<AttributeSource>();
          if(orig_sources!=null && !orig_sources.isEmpty()) {
            for(AttributeSource attrSrc: orig_sources) {
              sources.add((AttributeSource)(attrSrc.clone()));
            }
          }
          */
          List<AttributeTarget> targets=objAttr.getTargets();
          /*List<AttributeTarget> targets=new ArrayList<AttributeTarget>();
          if(orig_targets!=null && !orig_targets.isEmpty()) {
            for(AttributeTarget attrTgt: orig_targets) {
              targets.add((AttributeTarget)(attrTgt.clone()));
            }
          }
          */
          String attrSourceAppName="";
          List<String> attrSourceAppNameList=new ArrayList<String>();
          String attrSourceAttrName="";
          List<String> attrSourceAttrNameList=new ArrayList<String>();
          String attrSourceRuleName="";
          List<String> attrSourceRuleNameList=new ArrayList<String>();
          boolean hasSources=false;
          boolean hasTargets=false;
          int numSources=0;
          int numTargets=0;
          /*
           * if some apps are to be ignored, remove from any sources
           */
          if(!ignoredApps.isEmpty()) {
            log.debug("DOC-101 ignoredApps is not empty, checking sources");
            /*
             * How do you remove a list of items from a list?
             * Determine the ordinals to be removed.
             * put them into a sorted set, sorted in reverse order.
             * Remove them in the "reverse" order.
             */
            SortedSet<Integer> ignoredSourceAppsOrd=new TreeSet<Integer>(Collections.reverseOrder());
            if(sources!=null && !sources.isEmpty()) {
              int appsOrd=0;
              for(AttributeSource attrSourceObj: sources) {
                Application attrSourceApp=attrSourceObj.getApplication();
                if(attrSourceApp!=null) {
                  String attrSourceAppStr=attrSourceApp.getName();
                  log.debug("DOC-102 checking "+appsOrd+" attrSourceAppStr="+attrSourceAppStr);
                  if(ignoredApps.contains(attrSourceAppStr)) {
                    log.debug("DOC-103 adding ordinal "+appsOrd+" to the Set");
                    ignoredSourceAppsOrd.add(Integer.valueOf(appsOrd));
                  }
                }
                appsOrd++;
              }
            }
            if(!ignoredSourceAppsOrd.isEmpty()) {
              log.debug("DOC-104 ignoredSourceAppsOrd is not empty, iterating");
              for(Integer appsOrdInt: ignoredSourceAppsOrd) {
                int appsOrd=appsOrdInt.intValue();
                log.debug("DOC-105 removing sources["+appsOrd+"]");
                sources.remove(appsOrd);
              }
            }
            log.debug("DOC-111 ignoredApps is not empty, checking targets");
            SortedSet<Integer> ignoredTargetAppsOrd=new TreeSet<Integer>(Collections.reverseOrder());
            if(targets!=null && !targets.isEmpty()) {
              int appsOrd=0;
              for(AttributeSource attrTargetObj: targets) {
                Application attrTargetApp=attrTargetObj.getApplication();
                if(attrTargetApp!=null) {
                  String attrTargetAppStr=attrTargetApp.getName();
                  log.debug("DOC-112 checking "+appsOrd+" attrTargetAppStr="+attrTargetAppStr);
                  if(ignoredApps.contains(attrTargetAppStr)) {
                    log.debug("DOC-113 adding ordinal "+appsOrd+" to the Set");
                    ignoredTargetAppsOrd.add(Integer.valueOf(appsOrd));
                  }
                }
                appsOrd++;
              }
            }
            if(!ignoredTargetAppsOrd.isEmpty()) {
              log.debug("DOC-114 ignoredTargetAppsOrd is not empty, iterating");
              for(Integer appsOrdInt: ignoredTargetAppsOrd) {
                int appsOrd=appsOrdInt.intValue();
                log.debug("DOC-115 removing targets["+appsOrd+"]");
                targets.remove(appsOrd);
              }
            }
          }
          /*
           * Determine a list of applications and names to be used
           * on the Source section
           */
          if(sources!=null && !sources.isEmpty()) {
            hasSources=true;
            numSources=sources.size();
            log.debug("DOC-054 Identified attribute has sources numbering "+sources.size());
            AttributeSource primarySource=sources.get(0);
            log.debug("DOC-055 Identified primary source as "+primarySource.getName());
            for(AttributeSource attrSourceObj: sources) {
              attrSourceAppName="";
              attrSourceAttrName="";
              attrSourceRuleName="";
              Application attrSourceApp=attrSourceObj.getApplication();
              if(attrSourceApp!=null) log.debug("DOC-056 attrSourceObj application="+attrSourceApp.getName()); 
              String attrSourceAttr=attrSourceObj.getName();
              log.debug("DOC-057 attrSourceObj Name="+attrSourceAttr);
              Rule attrSourceRule=attrSourceObj.getRule();
              if(attrSourceRule!=null) log.debug("DOC-058 attrSourceObj Rule="+attrSourceRule.getName());
              if(attrSourceApp!=null) {
                attrSourceAppName=attrSourceApp.getName();
                if(primarySourceAppName.isEmpty()) {
                  primarySourceAppName=attrSourceAppName;
                }
                gatherAppOrdinals(context,attrSourceAppName,appOrdinals);
              }
              else {
                attrSourceAppName="Global Rule:";
              }
              if(attrSourceRule==null) {
                attrSourceAttrName=attrSourceAttr;
              }
              else {
                if(Util.isNullOrEmpty(attrSourceAttr)) {
                  attrSourceAttrName=attrSourceAttr;
                }
                else {
                  attrSourceAttrName=attrSourceAttr;
                }
              }
              attrSourceAppNameList.add(attrSourceAppName);
              attrSourceAttrNameList.add(attrSourceAttrName);
              attrSourceRuleNameList.add(attrSourceRuleName);
              log.debug("DOC-059 saved to source lists, sizes now = "
               +attrSourceAppNameList.size()
               +"/"+attrSourceAttrNameList.size()
               +"/"+attrSourceRuleNameList.size());
            }
          }
          /*
           * Determine a list of applications and names to be used
           * on the Target section
           */
          String attrTargetAppName="";
          List<String> attrTargetAppNameList=new ArrayList<String>();
          String attrTargetAttrName="";
          List<String> attrTargetAttrNameList=new ArrayList<String>();
          String attrTargetRuleName="";
          List<String> attrTargetRuleNameList=new ArrayList<String>();
          if(targets!=null && !targets.isEmpty()) {
            hasTargets=true;
            log.debug("DOC-060 Identified attribute has targets numbering "+targets.size());
            int appNum=0;
            for(AttributeTarget tgtObj: targets) {
              numTargets++;
              appNum++;
              attrTargetAppName="";
              attrTargetAttrName="";
              attrTargetRuleName="";
              Application attrTargetApp=tgtObj.getApplication();
              Rule attrTargetRule=tgtObj.getRule();
              if(attrTargetApp!=null) {
                attrTargetAppName=attrTargetApp.getName();
                log.debug("DOC-061 target application "+appNum+" name = "+attrTargetApp.getName());
                String attrTargetAppType=attrTargetApp.getType();
                log.debug("DOC-062 target application "+appNum+" type = "+attrTargetAppType);
                attrTargetAttrName=tgtObj.getName();
                log.debug("DOC-063 attrTargetAttrName = "+attrTargetAttrName);
                gatherAppOrdinals(context,attrTargetAppName,appOrdinals);
                if(attrTargetAppName.isEmpty() && "Active Directory - Direct".equals("attrTargetAppType")) {
                  log.debug("DOC-064 setting primaryTargetAppName to "+attrTargetAppName);
                  primaryTargetAppName=attrTargetAppName;
                }
                else if(attrTargetAppName.isEmpty() && "LDAP".equals("attrTargetAppType")) {
                  log.debug("DOC-065 setting primaryTargetAppName to "+attrTargetAppName);
                  primaryTargetAppName=attrTargetAppName;
                }
                if(attrTargetRule!=null) {
                  attrTargetRuleName=attrTargetRule.getName();
                }
              }
              else {
                attrTargetAppName="FAILURE";
              }
              attrTargetAppNameList.add(attrTargetAppName);
              attrTargetAttrNameList.add(attrTargetAttrName);
              attrTargetRuleNameList.add(attrTargetRuleName);
              log.debug("DOC-069 saved to target lists, sizes now = "
               +attrTargetAppNameList.size()
               +"/"+attrTargetAttrNameList.size()
               +"/"+attrTargetRuleNameList.size());
            }
          }
          /*
           * Props includes I for Identity, S-NC for Searchable-Named Column,
           * S-E# for Searchable-ExtendedNumber #, E-P for Editable-permanent
           * E-U for Editable-Temporary, and GF for group factory
           */
          List<String> ootbSearchables=new ArrayList<String>(Arrays.asList(
            "firstname","manager","displayName","type","lastname","inactive","name","email"
          ));
          String propsStr="";
          List<String> propsList=new ArrayList<String>();
          if(objAttr.isIdentity()) {
            propsList.add("I");
          }
          if(ootbSearchables.contains(objAttrName)) {
            propsList.add("S-O");
          }
          else {
            if(objAttr.isSearchable()) {
              if(objAttr.isNamedColumn()) {
                propsList.add("S-NC");
              }
              else if(objAttr.isExtended()) {
                int extNum=objAttr.getExtendedNumber();
                propsList.add("S-E"+String.format("%d",extNum));
              }
            }
          }
          if(objAttr.isEditable()) {
            ObjectAttribute.EditMode editMode=objAttr.getEditMode();
            if(ObjectAttribute.EditMode.Permanent==editMode) {
              propsList.add("E-P");
            }
            else if(ObjectAttribute.EditMode.UntilFeedValueChanges==editMode) {
              propsList.add("E-U");
            }
            else {
              log.warn("DOC-075 Invalid edit mode found: "+editMode);
            }
          }
          if(objAttr.isGroupFactory()) propsList.add("GF"); 
          if(!propsList.isEmpty()) {
            propsStr=propsList.toString();
            propsStr=propsStr.substring(1);
            propsStr=propsStr.substring(0,propsStr.length()-1);
            log.debug("DOC-076 propsStr="+propsStr);
          }
          /*
           * Usage String is U for no sources or targets, I for sources only,
           * F for sources and targets
           */ 
          String usageStr="U";
          if(hasSources) {
            if(hasTargets) {
              usageStr="F";
            }
            else {
              usageStr="I";
            }
          }
          else {
            if(hasTargets) {
              usageStr="T";
            }
          }
          log.debug("DOC-077 usageStr="+usageStr);
          identOrdinal++;
          log.debug("DOC-079 identOrdinal="+identOrdinal);
          /*
           * The first column (1) is the numOrdinal which is an Integer
           */
          int numEntries=1;
          if(numSources>1)numEntries=numSources;
          if(numTargets>numEntries)numEntries=numTargets;
          log.debug("DOC-080 numEntries="+numEntries);
          for(int ientry=0; ientry<numEntries; ientry++) {
            lineList.clear();
            log.debug("DOC-081 ientry="+ientry);
            numOrdinal++;
            lineList.add(Integer.valueOf(numOrdinal));
            log.debug("DOC-053 Added numOrdinal of "+numOrdinal);
            /*
             * Initialize the values
             */
            srcOrdinalObj=new String("");
            attrSourceAppName="";
            attrSourceAttrName="";
            log.debug("DOC-086 initialized source fields for ientry="+ientry);
            /*
             * Now see if there is data in the source list at this location
             */
            if(attrSourceAppNameList.size() > ientry) {
              attrSourceAppName=attrSourceAppNameList.get(ientry);
              attrSourceAttrName=attrSourceAttrNameList.get(ientry);
              srcOrdinalObj=getAppOrdinal(attrSourceAppName,attrSourceAttrName,appOrdinals);
              log.debug("DOC-090 pulled list data for ientry="+ientry);
            }
            lineList.add(srcOrdinalObj);
            lineList.add(attrSourceAppName);
            lineList.add(attrSourceAttrName);
            log.debug("DOC-087 added source: "+srcOrdinalObj+" "+attrSourceAppName+" "+attrSourceAttrName);
            /*
             * Identity values
             */
            lineList.add(usageStr);
            lineList.add(Integer.valueOf(identOrdinal));
            lineList.add(Integer.valueOf(ientry+1));
            lineList.add(objAttrName);
            lineList.add(attrDisplayName);
            lineList.add(propsStr);
            log.debug("DOC-088 added idents: "+usageStr+" "+identOrdinal+" "+objAttrName+" "+attrDisplayName+" "+propsStr);
            /*
             * Initialize target values
             */
            tgtOrdinalObj=new String("");
            attrTargetAppName="";
            attrTargetAttrName="";
            attrTargetRuleName="";
            log.debug("DOC-089 initialized target fields for ientry="+ientry);
            /*
             * Now see if there is data in the target list at this location
             */
            if(attrTargetAppNameList.size() > ientry) {
              attrTargetAppName=attrTargetAppNameList.get(ientry);
              attrTargetAttrName=attrTargetAttrNameList.get(ientry);
              attrTargetRuleName=attrTargetRuleNameList.get(ientry);
              tgtOrdinalObj=getAppOrdinal(attrTargetAppName,attrTargetAttrName,appOrdinals);
              log.debug("DOC-091 pulled list data for ientry="+ientry);
            }
            lineList.add(tgtOrdinalObj);
            lineList.add(attrTargetAppName);
            lineList.add(attrTargetAttrName);
            lineList.add(attrTargetRuleName);
            log.debug("DOC-092 added target: "+tgtOrdinalObj+" "+attrTargetAppName
              +" "+attrTargetAttrName+" "+attrTargetRuleName);
            log.debug("DOC-093 lineList length="+lineList.size());
            writeRow(sheet,rowNumber,rowMap,baseCellStyle,lineList);
            rowNumber++;
          }
        }
        maxColumnNumber=14;
        for (int columnIndex=0; columnIndex < maxColumnNumber; ++columnIndex) {
          sheet.autoSizeColumn(columnIndex);
          int colWidth=sheet.getColumnWidth(columnIndex);
          float colWidthPix=sheet.getColumnWidthInPixels(columnIndex);
          log.debug("DOC-085 Column number: "+columnIndex+" has width: "+colWidth+" / "+colWidthPix);
          colWidth=colWidth+512;
          sheet.setColumnWidth(columnIndex,colWidth);
        }
        sboutput.append("\nSuccessfully populated Attributes sheet");
      }
      catch (Exception ex) {
        log.error("DOC-099 "+ex.getClass().getName()+":"+ex.getMessage());
        sboutput.append("\nError: "+ex.getClass().getName()+":"+ex.getMessage());
      }
      finally {
        if(fileOut!=null) {
          try {
            wb.write(fileOut);
            fileOut.flush();
            fileOut.close();
            successfulFileWrite=true;
            sboutput.append("\nSuccessfully wrote output file");
          }
          catch (Exception gex) {
            log.error("DOC-199 "+gex.getClass().getName()+":"+gex.getMessage());
            sboutput.append("\nError: "+gex.getClass().getName()+":"+gex.getMessage());
          }
        }
      }
    }
    /*
     * Process email
     */
    boolean proceed=true;
    while(proceed) {
      if(Util.isNullOrEmpty(_mailTo)) {
        log.debug("DOC-013 the mailTo field is blank, skipping");
        break;
      }
      if(Util.isNullOrEmpty(_mailTemplate)) {
        log.debug("DOC-014 the mailTemplate field is blank, skipping");
        break;
      }
      EmailTemplate mailTemplate=context.getObjectByName(EmailTemplate.class,_mailTemplate);
      if(mailTemplate==null) {
        log.debug("DOC-015 the mailTemplate "+_mailTemplate+" is not found, skipping");
        break;
      }
      try {
        EmailFileAttachment fileAttachment=new EmailFileAttachment("changelog.html",
          EmailFileAttachment.MimeType.MIME_HTML,sbfile.toString().getBytes(StandardCharsets.UTF_8));
        Map templateVariables = new HashMap();
        EmailOptions mailOptions=new EmailOptions(_mailTo,templateVariables);
        mailOptions.addAttachment(fileAttachment);
        context.sendEmailNotification(mailTemplate,mailOptions);
      }
      catch(Exception mex) {
        log.error("DOC-901 "+mex.getClass().getName()+":"+mex.getMessage());
      }
      proceed=false;
    }
    result.setAttribute("resultString", sboutput.toString());
    result.setCompletionStatus(TaskResult.CompletionStatus.Success);
    result.addMessage(new Message(Message.Type.Info,"Processed"));
    log.debug("DOC-012 exiting");
    return;
  }
  /**
   * write the data to the sheet
   */
  private void writeRow(Sheet sh, int rownum, Map<Integer,Row> rmap, CellStyle style, List<Object> values) throws Exception {
    Integer roword=Integer.valueOf(rownum);
    Row rw=null;
    int cn=0;
    if(rmap.containsKey(roword)) {
      rw = rmap.get(roword);
    }
    else {
      rw=sh.createRow(rownum);
      rmap.put(roword,rw);
    }
    for(Object val:values) {
      Cell cell = rw.createCell(cn);
      if(val == null) {
        cell.setCellValue("");
      }
      else {
        if(val instanceof Integer) {
          cell.setCellValue(((Integer)val).intValue());
          log.debug("DOC-074 wrote cell "+cell.getAddress()+" = "+cell.getNumericCellValue());
        }
        else {
          cell.setCellValue(val.toString());
          log.debug("DOC-073 wrote cell "+cell.getAddress()+" = "+cell.getStringCellValue());
        }
      }
      cell.setCellStyle(style);
      cn++;
    }
  }
  /**
   * Gather the ordinals for the application no matter source or target
   */
  private void gatherAppOrdinals(SailPointContext con, String appName, Map<String,Map<String,Integer>> omap) throws Exception {
    if(omap.containsKey(appName)) {
      log.debug("DOC-101 gatherAppOrdinals found "+appName+", returning");
      return;
    }
    Map<String,Integer> amap=new HashMap<String,Integer>();
    Application appObj=con.getObjectByName(Application.class,appName);
    if(appObj==null) {
      log.warn("DOC-102 did not find application named "+appName);
      return;
    }
    Schema acctSchema=appObj.getSchema("account");
    if(acctSchema==null) {
      log.warn("DOC-103 did not find account schema for "+appName);
      List<Schema> appSchemas=appObj.getSchemas();
      for (Schema sc:appSchemas) {
        log.warn("DOC-104 found one named "+sc.getName()+" type="+sc.getObjectType());
      }
      return;
    }
    List<AttributeDefinition> scattrs=acctSchema.getAttributes();
    int ord=0;
    for(AttributeDefinition scattr: scattrs) {
      String attrName=scattr.getName();
      ord++;
      Integer attrOrd=Integer.valueOf(ord);
      amap.put(attrName,attrOrd);
    }
    con.decache(appObj);
    omap.put(appName,amap);
  }
  /**
   * return the ordinal as Integer or String
   */
  private Object getAppOrdinal(String appName, String attrName, Map<String,Map<String,Integer>> omap) {
    Object rval=new String("");
    if(omap.containsKey(appName)) {
      Map<String,Integer> amap=omap.get(appName);
      if(amap.containsKey(attrName)) {
        rval=amap.get(attrName);
      }
      else {
        rval=new String("R");
      }
    }
    return rval;
  }
  public boolean terminate() {
    terminate=true;
    taskResult.setTerminated(true);
    if (log.isDebugEnabled())
      log.debug("Task was terminated."); 
    return true;
  }
  
  public String getPluginName() {
    return PLUGIN_NAME;
  }
}