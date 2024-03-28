package sailpoint.mcdocumenter.task;

import java.io.*;
import java.text.*;
import java.util.*;
import java.nio.charset.StandardCharsets;
import sailpoint.api.SailPointContext;
import sailpoint.api.IncrementalObjectIterator;
import sailpoint.task.BasePluginTaskExecutor;
import sailpoint.tools.GeneralException;
import sailpoint.tools.Util;
import sailpoint.object.Attributes;
import sailpoint.object.Bundle;
import sailpoint.object.EmailFileAttachment;
import sailpoint.object.EmailOptions;
import sailpoint.object.EmailTemplate;
import sailpoint.object.Filter;
import sailpoint.object.QueryOptions;
import sailpoint.object.TaskResult;
import sailpoint.object.TaskResult.CompletionStatus;
import sailpoint.object.TaskSchedule;
import sailpoint.tools.Message;
import sailpoint.tools.Message.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Role Analysis
 *
 * @author Keith Smith
 */
public class RoleAnalysis extends BasePluginTaskExecutor {
  private static final String PLUGIN_NAME = "MCDocumenterPlugin";
  private static Log log = LogFactory.getLog(RoleAnalysis.class);
  private boolean terminate=false;
  private String sailpointVersion="";
  private String sailpointPatch="";
  private StringBuffer sbfile=new StringBuffer();
  private StringBuffer sboutput=new StringBuffer();
  private String _basePath=null;
  private String _fileName=null;
  private String _mailTo=null;
  private String _mailTemplate="Default Report Template";
  private boolean unixOrigin=false;
  private boolean showHierarchy=false;
  private TaskResult taskResult=null;
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
      _basePath=args.getString("basePath");
      log.debug("RAT-002 read basePath of "+_basePath);
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
        log.debug("RAT-018 The basePath "+_basePath+" exists");
      }
      else {
        if(basePathObj.mkdirs()) {
          log.debug("RAT-019 Successfully created "+_basePath);
        }
        else {
          log.error("RAT-020 Count not create folder "+_basePath);
          taskResult.setCompletionStatus(TaskResult.CompletionStatus.Error);
          taskResult.addMessage(new Message(Message.Type.Error,"Could not create output folder"));
          return;
        }
      }
    }
    /*
     * fileName processing
     */
    if(args.containsKey("fileName")) {
      _fileName=args.getString("fileName");
      log.debug("RAT-003 read fileName of "+_fileName);
      if(_fileName.contains("$date$")) {
        DateFormat bpdf=new SimpleDateFormat("yyyyMMdd");
        String nowStr=bpdf.format(now);
        _fileName=_fileName.replace("$date$", nowStr);
      }
      if(_fileName.contains("$datetime$")) {
        DateFormat bpdf=new SimpleDateFormat("yyyyMMdd-HHmm");
        String nowStr=bpdf.format(now);
        _fileName=_fileName.replace("$datetime$", nowStr);
      }
    }
    /*
     * Process email settings
     */
    if(args.containsKey("mailTo")) {
      _mailTo=args.getString("mailTo");
      log.debug("RAT-004 read mailTo of "+_mailTo);
    }
    if(args.containsKey("mailTemplate")) {
      _mailTemplate=args.getString("mailTemplate");
      log.debug("RAT-005 read mailTemplate of "+_mailTemplate);
    }
    /*
     * Gather inputs regarding the role analysis
     * First, gather a list of bundles to analyze (default is to analyze all)
     */
    String topLevelBundleStr=null;
    if(args.containsKey("topLevelBundle")) {
      topLevelBundleStr=args.getString("topLevelBundle");
      log.debug("RAT-006 topLevelBundle="+topLevelBundleStr);
    }
    String analysisProcess="topdown";
    if(args.containsKey("analysisProcess")) {
      analysisProcess=args.getString("analysisProcess");
      log.debug("RAT-007 analysisProcess="+analysisProcess);
    }
    List<String> analysisCommands=new ArrayList<String>();
    if(analysisProcess.contains(" ")) {
      String[] ap=analysisProcess.split(" ");
      for(int iap=0; iap<ap.length; iap++) {
        analysisCommands.add(ap[iap]);
      }
    }
    else {
      analysisCommands.add(analysisProcess);
    }
    boolean topdownDelivery=true;
    for(String cmd: analysisCommands) {
      log.debug("RAT-008 command: "+cmd);
      String lcmd=cmd.toLowerCase();
      if(lcmd.equals("bottomup")) {
        topdownDelivery=false;
        log.debug("RAT-009 setting topdownDelivery=false");
      }
      if(lcmd.equals("showhierarchy")) {
        showHierarchy=true;
      }
    }
    List<String> topLevelBundleList=new ArrayList<String>();
    List<String> allBundlesList=new ArrayList<String>();
    List<String> allCatBundles=new ArrayList<String>();
    if(topLevelBundleStr!=null) {
      topLevelBundleList.add(topLevelBundleStr);
    }
    else {
      topLevelBundleList=findAllTopLevelBundles(context,
        analysisCommands,allBundlesList,allCatBundles);
    }
    List<String> printedBundleList=new ArrayList<String>();
    List<String> concatenatedBundleList=new ArrayList<String>();
    try {
      /*
       *  Desired printout for topdown:
       *  First, print out the topLevel bundle name
       *  Next, print out the first organizational bundle name under that
       *  Next, print out the first business role under that
       *  Finally, print out all IT roles under that.
       *  Next, the next business role/it roles
       *  When exhausted go to the next org role
       *  When org roles are exhausted go to any business/it roles in the top level role
       */
      for(String bunStr: topLevelBundleList) {
        Bundle bun=context.getObjectByName(Bundle.class,bunStr);
        log.debug("RAT-023 calling addOrganizationalRoleAndHierarchy bun="+bun.getName());
        int lev=0;
        addOrganizationalRoleAndHierarchy(context,bun,lev,
          printedBundleList,concatenatedBundleList);
        context.decache(bun);
      }
    }
    catch (Exception ex) {
      log.error("RAT-029 "+ex.getClass().getName()+":"+ex.getMessage());
    }
    //
    // Creating a table
    //
    Deque<String> outputDeque=new LinkedList<String>();
    sboutput.append("Processing data at "+runTimeStr);
    log.debug("RAT-010 Processing data at "+runTimeStr);
    sbfile.append("Role Analysis performed at "+runTimeStr);
    sbfile.append("\nCommands:");
    for(String cmd: analysisCommands) {
      sbfile.append("\n  "+cmd);
    }
    sbfile.append("\nResults:\n");
    sbfile.append("\n             Role Type                     Role Name");
    allCatBundles.removeAll(concatenatedBundleList);
    outputDeque.addAll(printedBundleList);
    //for(String x: printedBundleList) {
    Iterator<String> diter=null;
    if(topdownDelivery) {
      diter=outputDeque.iterator();
    }
    else {
      diter=outputDeque.descendingIterator();
    }
    while(diter.hasNext()) {
      String x=diter.next();
      sbfile.append("\n"+x);
    }
    if(!allCatBundles.isEmpty()) {
      sbfile.append("\n\n   Roles not found in top level hierarchy");
      sbfile.append("\n             Role Type                     Role Name");
      for(String x: allCatBundles) {
        String[] vals=x.split(":::");
        sbfile.append("\n"+String.format("%-40s",vals[0])+vals[1]);
      }
    }
    sboutput.append("\nSaving data to "+_basePath+_fileName);
    log.debug("RAT-011 Saving data to "+_basePath+_fileName);
    Util.writeFile(_basePath + _fileName, sbfile.toString());
    log.debug("RAT-012 Saved file, checking for emailing");
    /*
     * Process email
     */
    boolean proceed=true;
    while(proceed) {
      if(Util.isNullOrEmpty(_mailTo)) {
        log.debug("RAT-013 the mailTo field is blank, skipping");
        break;
      }
      if(Util.isNullOrEmpty(_mailTemplate)) {
        log.debug("RAT-014 the mailTemplate field is blank, skipping");
        break;
      }
      EmailTemplate mailTemplate=context.getObjectByName(EmailTemplate.class,_mailTemplate);
      if(mailTemplate==null) {
        log.debug("RAT-015 the mailTemplate "+_mailTemplate+" is not found, skipping");
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
        log.error("RAT-901 "+mex.getClass().getName()+":"+mex.getMessage());
      }
      proceed=false;
    }
    taskResult.setAttribute("taskResultString", sboutput.toString());
    taskResult.setCompletionStatus(TaskResult.CompletionStatus.Success);
    taskResult.addMessage(new Message(Message.Type.Info,"Processed"));
    log.debug("RAT-012 exiting");
    return;
  }
  @SuppressWarnings({"rawtypes","unchecked"})
  private List<String> findAllTopLevelBundles(SailPointContext ctx,
    List<String> acmd, List<String> abun, List<String> cbun) {
    log.debug("RAT-100 Entered findAllTopLevelBundles");
    List<String> rlist=new ArrayList<String>();
    boolean topdownDelivery=true;
    String orderByField="name";
    boolean reverseOrder=false;
    String topLevelType="organizational";
    boolean onlyActive=false;
    for(String cmd: acmd) {
      log.debug("RAT-101 command: "+cmd);
      String lcmd=cmd.toLowerCase();
      if(lcmd.equals("bottomup")) {
        topdownDelivery=false;
        log.debug("RAT-102 setting topdownDelivery=false");
      }
      else if(lcmd.equals("reverseorder")) {
        reverseOrder=true;
        log.debug("RAT-103 setting reverseOrder=true");
      }
      else if(lcmd.equals("onlyactive")) {
        onlyActive=true;
        log.debug("RAT-104 setting onlyActive=true");
      }
      else if(lcmd.startsWith("orderby")) {
        String[] oba=cmd.split("=");
        orderByField=oba[1];
        log.debug("RAT-105 setting orderByField="+orderByField);
      }
      else if(lcmd.startsWith("topleveltype")) {
        String[] oba=cmd.split("=");
        topLevelType=oba[1];
        log.debug("RAT-106 setting topLevelType="+topLevelType);
      }
    }
    QueryOptions qo=new QueryOptions();
    qo.setOrderBy(orderByField);
    if(reverseOrder) {
      qo.setOrderAscending(false);
    }
    List<Filter> filterList=new ArrayList<Filter>();
    int numFilters=0;
    Filter typeFilter=Filter.eq("type",topLevelType);
    filterList.add(typeFilter);
    numFilters++;
    if(onlyActive) {
      Filter activeFilter=Filter.eq("enabled",true);
      filterList.add(activeFilter);
      numFilters++;
    }
    if(numFilters==1) {
      qo.addFilter(typeFilter);
    }
    else {
      qo.setFilters(filterList);
    }
    try {
      IncrementalObjectIterator iter=new IncrementalObjectIterator(ctx,Bundle.class,qo);
      if(iter!=null) {
        while(iter.hasNext()) {
          Bundle bun=(Bundle)iter.next();
          log.debug("RAT-110 analyzing bundle named "+bun.getName());
          List<Bundle> inherits=bun.getInheritance();
          if(inherits==null || inherits.isEmpty()) {
            log.debug("RAT-111 this bundle does not inherit any other roles, it is a top level role");
            rlist.add(bun.getName());
          }
          else {
            for(Bundle ibun:inherits) {
              log.debug("RAT-112 inherits "+ibun.getName());
            }
          }
          ctx.decache(bun);
        }
      }
    }
    catch (Exception ex) {
      log.error("RAT-199 "+ex.getClass().getName()+":"+ex.getMessage());
    }
    QueryOptions qo2=new QueryOptions();
    qo2.setOrderBy(orderByField);
    if(reverseOrder) {
      qo2.setOrderAscending(false);
    }
    List<Filter> filterList2=new ArrayList<Filter>();
    int numFilters2=0;
    if(onlyActive) {
      Filter activeFilter=Filter.eq("enabled",true);
      filterList2.add(activeFilter);
      numFilters2++;
    }
    if(numFilters2==1) {
      qo2.addFilter(filterList2.get(0));
    }
    try {
      IncrementalObjectIterator iter2=new IncrementalObjectIterator(ctx,Bundle.class,qo2);
      if(iter2!=null) {
        while(iter2.hasNext()) {
          Bundle bun=(Bundle)iter2.next();
          abun.add(String.format("%-40s",bun.getType())+bun.getName());
          cbun.add(bun.getType()+":::"+bun.getName());
          ctx.decache(bun);
        }
      }
    }
    catch (Exception ex2) {
      log.error("RAT-199 "+ex2.getClass().getName()+":"+ex2.getMessage());
    }
    return rlist;
  }
  private void addOrganizationalRoleAndHierarchy(SailPointContext ctx,Bundle bun,int blev,
    List<String> pbl, List<String> cbl) throws Exception {
    log.debug("RAT-200 entered addOrganizationalRoleAndHierarchy bun="+bun.getName()+" blev="+blev);
    List<Bundle> lhier=bun.getHierarchy(ctx);
    List<Bundle> orgs=new ArrayList<Bundle>();
    List<Bundle> buss=new ArrayList<Bundle>();
    String bunStr=bun.getName();
    String btype=bun.getType();
    addRoleLine(btype, bunStr, blev, pbl, cbl);
    log.debug("RAT-202 examining hierarchy");
    for(Bundle hbun: lhier) {
      String htyp=hbun.getType();
      String hnam=hbun.getName();
      String hstr=String.format("%-25s",htyp)+hnam;
      log.debug("RAT-203 examining "+hstr);
      if(hnam.equals(bun.getName())) {
        log.debug("RAT-204 found self, skipping");
        continue;
      }
      if(htyp.equals("organizational")) {
        log.debug("RAT-205 found org role, adding to orgs array");
        orgs.add(hbun);
      }
      else if(htyp.equals("business")) {
        log.debug("RAT-206 found bus role, adding to buss array");
        buss.add(hbun);
        //log.debug("RAT-206 found business role, calling addBusinessRoleAndHierarchy");
        //addBusinessRoleAndHierarchy(ctx,hbun,pbl);
      }
      else {
        log.debug("RAT-207 found other, skipping");
        continue;
        //pbl.add(String.format("%-25s",htyp)+hnam);
      }
    }
    for(Bundle obun:orgs) {
      log.debug("RAT-208 calling addOrganizationalRoleAndHierarchy on "+obun.getName());
      addOrganizationalRoleAndHierarchy(ctx,obun,(blev+1),pbl,cbl);
    }
    for(Bundle bbun:buss) {
      log.debug("RAT-209 calling addBusinessRoleAndHierarchy on "+bbun.getName());
      addBusinessRoleAndHierarchy(ctx,bbun,(blev+1),pbl,cbl);
    }
  }
  private void addBusinessRoleAndHierarchy(SailPointContext ctx,Bundle bun,int blev,
    List<String> pbl, List<String> cbl) throws Exception {
    log.debug("RAT-300 entered addBusinessRoleAndHierarchy bun="+bun.getName());
    List<Bundle> lhier=bun.getHierarchy(ctx);
    List<Bundle> orgs=new ArrayList<Bundle>();
    List<Bundle> buss=new ArrayList<Bundle>();
    List<Bundle> reqs=bun.getRequirements();
    List<Bundle> pers=bun.getPermits();
    String bunStr=bun.getName();
    String btype=bun.getType();
    addRoleLine(btype, bunStr, blev, pbl, cbl);
    log.debug("RAT-311 examining requirements");
    if(reqs!=null && !reqs.isEmpty()) {
      for(Bundle rbun: reqs) {
        String rtyp=rbun.getType();
        String rnam=rbun.getName();
        addRoleLine(rtyp, rnam, blev+1, pbl, cbl);
      }
    }
    log.debug("RAT-313 examining permits");
    if(pers!=null && !pers.isEmpty()) {
      for(Bundle pbun: pers) {
        String ptyp=pbun.getType();
        String pnam=pbun.getName();
        addRoleLine(ptyp, pnam, blev+1, pbl, cbl);
      }
    }
    log.debug("RAT-302 examining hierarchy");
    for(Bundle hbun: lhier) {
      String htyp=hbun.getType();
      String hnam=hbun.getName();
      String hstr=String.format("%-25s",htyp)+hnam;
      log.debug("RAT-303 examining "+hstr);
      if(hnam.equals(bun.getName())) {
        log.debug("RAT-304 found self, skipping");
        continue;
      }
      if(htyp.equals("organizational")) {
        log.debug("RAT-305 found org role, adding to orgs array");
        orgs.add(hbun);
      }
      else if(htyp.equals("business")) {
        log.debug("RAT-306 found bus role, adding to buss array");
        buss.add(hbun);
      }
      else {
        log.debug("RAT-307 found other, writing "+hstr);
        addRoleLine(htyp, hnam, blev+1, pbl, cbl);
      }
    }
    for(Bundle bbun:buss) {
      log.debug("RAT-308 calling addBusinessRoleAndHierarchy on "+bbun.getName());
      addBusinessRoleAndHierarchy(ctx,bbun,blev+1,pbl,cbl);
    }
    for(Bundle obun:orgs) {
      log.debug("RAT-309 calling addOrganizationalRoleAndHierarchy on "+obun.getName());
      addOrganizationalRoleAndHierarchy(ctx,obun,blev+1,pbl,cbl);
    }
  }
  private void addRoleLine(String rtyp, String rnam, int rlev,
    List<String> pbl, List<String> cbl) {
    String rstr="";
    String cstr=rtyp+":::"+rnam;
    rstr=String.format("%-40s",rtyp)+rnam;
    if(showHierarchy && rlev > 0) {
      String xstr="";
      for(int ix=0; ix<rlev; ix++) {
        xstr=xstr+"|-";
      }
      xstr=xstr+rtyp;
      rstr=String.format("%-40s",xstr)+rnam;
    }
    if(!cbl.contains(cstr)) {
      log.debug("RAT-201 adding to list: "+rstr);
      pbl.add(rstr);
      cbl.add(cstr);
    }
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