<%@ page import="com.google.appengine.tools.pipeline.*" %>
<%@ page import="com.google.appengine.tools.pipeline.demo.*" %>
<%@ page import="com.google.appengine.tools.pipeline.demo.LetterCountExample.LetterCounter" %>
<%@ page import="java.util.SortedMap" %>
<%@taglib uri="http://github.com/GoogleCloudPlatform/appengine-pipelines/functions" prefix="f" %>

<%!
    private static final String TEXT_PARAM_NAME = "text";
    private static final String PIPELINE_ID_PARAM_NAME = "pipelineId";
    private static final String CLEANUP_PIPELINE_ID_PARAM_NAME = "cleanupId";

%>
<HTML>
<HEAD>
    <link rel="stylesheet" type="text/css" href="someStyle.css">
    <style type="text/css">
        .period {
            font-style: italic;
            margin-bottom: 1em;
            font-size: 0.8em;
        }

        h4.withperiod {
            margin-bottom: 0em;
        }
    </style>
</HEAD>
<BODY>

<H2>Compute letter counts by spanwing a sub-job for each word</H2>

<%
    String text = request.getParameter(TEXT_PARAM_NAME);
    String pipelineId = request.getParameter(PIPELINE_ID_PARAM_NAME);
    String cleanupId = request.getParameter(CLEANUP_PIPELINE_ID_PARAM_NAME);
    PipelineService service = PipelineServiceFactory.newPipelineService();
    if (null != cleanupId) {
        service.deletePipelineRecords(cleanupId);
    }
    if (null != text) {
%>
<H4>Computing letter counts...</H4>
<em><%=text%>
</em>

<p>

        <%
  if(null == pipelineId){
    pipelineId = service.startNewPipeline(new LetterCounter(), text);
  }
  JobInfo jobInfo = service.getJobInfo(pipelineId);
  switch(jobInfo.getJobState()){
        case COMPLETED_SUCCESSFULLY:
%>
    Computation completed.

<p>
        <%
  SortedMap<Character, Integer> map = (SortedMap<Character, Integer>) jobInfo.getOutput();
  for(char c : map.keySet()) {
    int count = map.get(c);
    out.print("<b>" + c + "</b> : " + count + "&nbsp;&nbsp;&nbsp;&nbsp;");
  }
%>

<form method="post">
    <input name="<%=TEXT_PARAM_NAME%>" value="" type="hidden">
    <input name="<%=PIPELINE_ID_PARAM_NAME%>" value="" type="hidden">
    <input name="<%=CLEANUP_PIPELINE_ID_PARAM_NAME%>" value="<%=pipelineId%>" type="hidden">
    <input type="submit" value="Do it again">
</form>
<%
        break;
    case RUNNING:
%>
Calculation not yet completed.
<p>

<form method="post">
    <input name="<%=TEXT_PARAM_NAME%>" value="<%=text%>" type="hidden">
    <input name="<%=PIPELINE_ID_PARAM_NAME%>" value="<%=pipelineId%>" type="hidden">
    <input type="submit" value="Check Again">
</form>
<%
        break;
    case STOPPED_BY_ERROR:
%>
Calculation stopped. An error occurred.
<p>

<form method="post">
    <input name="<%=TEXT_PARAM_NAME%>" value="" type="hidden">
    <input name="<%=PIPELINE_ID_PARAM_NAME%>" value="" type="hidden">
    <input type="submit" value="Do it again">
</form>
<p>
    error info:

<p>
        <%=jobInfo.getError()%>
        <%
          break;
    case CANCELED_BY_REQUEST:
%>
Calculation canceled.
<p>

<form method="post">
    <input name="<%=TEXT_PARAM_NAME%>" value="" type="hidden">
    <input name="<%=PIPELINE_ID_PARAM_NAME%>" value="" type="hidden">
    <input type="submit" value="Do it again">
</form>
        <%
          break;        
        case STOPPED_BY_REQUEST:
%>
    Calculation stopped by request;

<p>

<form method="post">
    <input name="<%=TEXT_PARAM_NAME%>" value="" type="hidden">
    <input name="<%=PIPELINE_ID_PARAM_NAME%>" value="" type="hidden">
    <input type="submit" value="Do it again">
</form>
<%
            break;
    }// end switch
}// end: if
else {
%>
Enter some text:
<form method="post">
    <textarea name="<%=TEXT_PARAM_NAME%>" cols=40 rows=6></textarea>
    <br>
    <input type="submit" value="Compute Letter Count">
</form>
<%
    }

    if (null != pipelineId) {
%>
<p>
  <a href="${f:baseUrl()}status.html?root=<%=pipelineId%>" target="Pipeline Status">view status page</a>
<%
}
%>


</BODY>
</HTML>
