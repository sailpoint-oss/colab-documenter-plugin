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
public class RoleSuggester extends BasePluginTaskExecutor {
  private static final String PLUGIN_NAME = "MCDocumenterPlugin";
  private static Log log = LogFactory.getLog(RoleSuggester.class);
  private boolean terminate=false;
  private String sailpointVersion="";
  private String sailpointPatch="";
  private StringBuffer sbfile=new StringBuffer();
  private StringBuffer sboutput=new StringBuffer();
  private TaskResult taskResult;
  private String _basePath=null;
  private String _fileName=null;
  private String _mailTo=null;
  private String _mailTemplate="Default Report Template";
  private boolean unixOrigin=false;
  private boolean showHierarchy=false;
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
    /*
     * basePath processing
     */
    if(args.containsKey("basePath")) {
      _basePath=args.getString("basePath");
      log.debug("MC-RSG-002 read basePath of "+_basePath);
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
        log.debug("MC-RSG-018 The basePath "+_basePath+" exists");
      }
      else {
        if(basePathObj.mkdirs()) {
          log.debug("MC-RSG-019 Successfully created "+_basePath);
        }
        else {
          log.error("MC-RSG-020 Could not create folder "+_basePath);
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
      log.debug("MC-RSG-003 read fileName of "+_fileName);
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
      log.debug("MC-RSG-004 read mailTo of "+_mailTo);
    }
    if(args.containsKey("mailTemplate")) {
      _mailTemplate=args.getString("mailTemplate");
      log.debug("MC-RSG-005 read mailTemplate of "+_mailTemplate);
    }
    /*
     * Gather inputs regarding the role analysis
     * First, gather a list of bundles to analyze (default is to analyze all)
     */
    String topLevelBundleStr=null;
    if(args.containsKey("topLevelBundle")) {
      topLevelBundleStr=args.getString("topLevelBundle");
      log.debug("MC-RSG-006 topLevelBundle="+topLevelBundleStr);
    }
    String analysisProcess="suggest";
    if(args.containsKey("analysisProcess")) {
      analysisProcess=args.getString("analysisProcess");
      log.debug("MC-RSG-007 analysisProcess="+analysisProcess);
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
    /*
     * Here is where new code will exist
     * The process looks like this:
     * Starting at this level
     * Find all business roles under this role (org or business, not IT)
     * Look at the match lists on all of them.
     * Look for duplicates of match list entries
     * Assemble all roles that have the same match list entry starting with the one with the most
     * Create a role that contains the one match list and use that as the top level business role
     * Remove that match list from all of the roles that contain it, and mark each of those roles
     * as inheriting the newly created role.
     * Do the analysis over and over until each role has only unique match list combinations.
     * Next analyze all of the IT Roles, both required and permitted, for all of the IT roles that
     * these business roles access.
     * Find the application and entitlement for each role
     * Split up similarly so that each entitlement is contained in only one IT role.
     * Replace the IT Roles and match to each business role
     */
    boolean suggest=false;
    boolean rebuild=false;
    for(String cmd: analysisCommands) {
      log.debug("MC-RSG-008 command: "+cmd);
      String lcmd=cmd.toLowerCase();
      if(lcmd.equals("suggest")) {
        suggest=true;
        log.debug("MC-RSG-009 setting suggest=true");
        sboutput.append("Configured to suggest fixes");
      }
      if(lcmd.equals("rebuild")) {
        rebuild=true;
        log.debug("MC-RSG-010 setting rebuild=true");
        sboutput.append("Configured to rebuild roles");
      }
    }
    // topLevelBundleList is a list of all bundles under the top level
    // or a list of all bundles at the top level
    List<String> topLevelBundleList=new ArrayList<String>();
    // allBundlesList is a list of bundles in the form type                             Name
    List<String> allBundlesList=new ArrayList<String>();
    // allCatBundles is a list of bundles in the form type:::name
    List<String> allCatBundles=new ArrayList<String>();
    if(topLevelBundleStr!=null) {
      topLevelBundleList.add(topLevelBundleStr);
    }
    else {
      topLevelBundleList=findAllTopLevelBundles(context,
        analysisCommands,allBundlesList,allCatBundles);
    }
    try {
      /*
       * 
       */
      sboutput.append("\nTop Level roles:");
      for(String bunStr: topLevelBundleList) {
        sboutput.append("\n"+bunStr);
      }
      sboutput.append("\n\nAll roles:");
      for(String bunStr: allBundlesList) {
        sboutput.append("\n"+bunStr);
      }
    }
    catch (Exception ex) {
      log.error("MC-RSG-029 "+ex.getClass().getName()+":"+ex.getMessage());
    }
    /*
     * Process email
     */
    result.setAttribute("resultString", sboutput.toString());
    result.setCompletionStatus(TaskResult.CompletionStatus.Success);
    result.addMessage(new Message(Message.Type.Info,"Processed"));
    log.debug("MC-RSG-012 exiting");
    return;
  }
  @SuppressWarnings({"rawtypes","unchecked"})
  private List<String> findAllTopLevelBundles(SailPointContext ctx,
    List<String> acmd, List<String> abun, List<String> cbun) {
    log.debug("MC-RSG-100 Entered findAllTopLevelBundles");
    List<String> rlist=new ArrayList<String>();
    String orderByField="name";
    String topLevelType="organizational";
    boolean onlyActive=false;
    for(String cmd: acmd) {
      log.debug("MC-RSG-101 command: "+cmd);
      String lcmd=cmd.toLowerCase();
      if(lcmd.equals("onlyactive")) {
        onlyActive=true;
        log.debug("MC-RSG-104 setting onlyActive=true");
      }
      if(lcmd.startsWith("orderby")) {
        String[] oba=cmd.split("=");
        orderByField=oba[1];
        log.debug("MC-RSG-105 setting orderByField="+orderByField);
      }
      if(lcmd.startsWith("topleveltype")) {
        String[] oba=cmd.split("=");
        topLevelType=oba[1];
        log.debug("MC-RSG-106 setting topLevelType="+topLevelType);
      }
    }
    QueryOptions qo=new QueryOptions();
    qo.setOrderBy(orderByField);
    qo.setOrderAscending(true);
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
          log.debug("MC-RSG-110 analyzing bundle named "+bun.getName());
          List<Bundle> inherits=bun.getInheritance();
          if(inherits==null || inherits.isEmpty()) {
            log.debug("MC-RSG-111 this bundle does not inherit any other roles, it is a top level role");
            rlist.add(bun.getName());
          }
          else {
            for(Bundle ibun:inherits) {
              log.debug("MC-RSG-112 inherits "+ibun.getName());
            }
          }
          ctx.decache(bun);
        }
      }
    }
    catch (Exception ex) {
      log.error("MC-RSG-199 "+ex.getClass().getName()+":"+ex.getMessage());
    }
    QueryOptions qo2=new QueryOptions();
    qo2.setOrderBy(orderByField);
    qo2.setOrderAscending(true);
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
      log.error("MC-RSG-199 "+ex2.getClass().getName()+":"+ex2.getMessage());
    }
    return rlist;
  }
  private void addOrganizationalRoleAndHierarchy(SailPointContext ctx,Bundle bun,int blev,
    List<String> pbl, List<String> cbl) throws Exception {
    log.debug("MC-RSG-200 entered addOrganizationalRoleAndHierarchy bun="+bun.getName()+" blev="+blev);
    List<Bundle> lhier=bun.getHierarchy(ctx);
    List<Bundle> orgs=new ArrayList<Bundle>();
    List<Bundle> buss=new ArrayList<Bundle>();
    String bunStr=bun.getName();
    String btype=bun.getType();
    addRoleLine(btype, bunStr, blev, pbl, cbl);
    log.debug("MC-RSG-202 examining hierarchy");
    for(Bundle hbun: lhier) {
      String htyp=hbun.getType();
      String hnam=hbun.getName();
      String hstr=String.format("%-25s",htyp)+hnam;
      log.debug("MC-RSG-203 examining "+hstr);
      if(hnam.equals(bun.getName())) {
        log.debug("MC-RSG-204 found self, skipping");
        continue;
      }
      if(htyp.equals("organizational")) {
        log.debug("MC-RSG-205 found org role, adding to orgs array");
        orgs.add(hbun);
      }
      else if(htyp.equals("business")) {
        log.debug("MC-RSG-206 found bus role, adding to buss array");
        buss.add(hbun);
        //log.debug("MC-RSG-206 found business role, calling addBusinessRoleAndHierarchy");
        //addBusinessRoleAndHierarchy(ctx,hbun,pbl);
      }
      else {
        log.debug("MC-RSG-207 found other, skipping");
        continue;
        //pbl.add(String.format("%-25s",htyp)+hnam);
      }
    }
    for(Bundle obun:orgs) {
      log.debug("MC-RSG-208 calling addOrganizationalRoleAndHierarchy on "+obun.getName());
      addOrganizationalRoleAndHierarchy(ctx,obun,(blev+1),pbl,cbl);
    }
    for(Bundle bbun:buss) {
      log.debug("MC-RSG-209 calling addBusinessRoleAndHierarchy on "+bbun.getName());
      addBusinessRoleAndHierarchy(ctx,bbun,(blev+1),pbl,cbl);
    }
  }
  private void addBusinessRoleAndHierarchy(SailPointContext ctx,Bundle bun,int blev,
    List<String> pbl, List<String> cbl) throws Exception {
    log.debug("MC-RSG-300 entered addBusinessRoleAndHierarchy bun="+bun.getName());
    List<Bundle> lhier=bun.getHierarchy(ctx);
    List<Bundle> orgs=new ArrayList<Bundle>();
    List<Bundle> buss=new ArrayList<Bundle>();
    List<Bundle> reqs=bun.getRequirements();
    List<Bundle> pers=bun.getPermits();
    String bunStr=bun.getName();
    String btype=bun.getType();
    addRoleLine(btype, bunStr, blev, pbl, cbl);
    log.debug("MC-RSG-311 examining requirements");
    if(reqs!=null && !reqs.isEmpty()) {
      for(Bundle rbun: reqs) {
        String rtyp=rbun.getType();
        String rnam=rbun.getName();
        addRoleLine(rtyp, rnam, blev+1, pbl, cbl);
      }
    }
    log.debug("MC-RSG-313 examining permits");
    if(pers!=null && !pers.isEmpty()) {
      for(Bundle pbun: pers) {
        String ptyp=pbun.getType();
        String pnam=pbun.getName();
        addRoleLine(ptyp, pnam, blev+1, pbl, cbl);
      }
    }
    log.debug("MC-RSG-302 examining hierarchy");
    for(Bundle hbun: lhier) {
      String htyp=hbun.getType();
      String hnam=hbun.getName();
      String hstr=String.format("%-25s",htyp)+hnam;
      log.debug("MC-RSG-303 examining "+hstr);
      if(hnam.equals(bun.getName())) {
        log.debug("MC-RSG-304 found self, skipping");
        continue;
      }
      if(htyp.equals("organizational")) {
        log.debug("MC-RSG-305 found org role, adding to orgs array");
        orgs.add(hbun);
      }
      else if(htyp.equals("business")) {
        log.debug("MC-RSG-306 found bus role, adding to buss array");
        buss.add(hbun);
      }
      else {
        log.debug("MC-RSG-307 found other, writing "+hstr);
        addRoleLine(htyp, hnam, blev+1, pbl, cbl);
      }
    }
    for(Bundle bbun:buss) {
      log.debug("MC-RSG-308 calling addBusinessRoleAndHierarchy on "+bbun.getName());
      addBusinessRoleAndHierarchy(ctx,bbun,blev+1,pbl,cbl);
    }
    for(Bundle obun:orgs) {
      log.debug("MC-RSG-309 calling addOrganizationalRoleAndHierarchy on "+obun.getName());
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
      log.debug("MC-RSG-201 adding to list: "+rstr);
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