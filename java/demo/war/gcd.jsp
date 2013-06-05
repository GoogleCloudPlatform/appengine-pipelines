<%@ page import="com.google.appengine.tools.pipeline.*" %>
<%@ page import="com.google.appengine.tools.pipeline.demo.*" %>
<%@ page import="com.google.appengine.tools.pipeline.demo.GCDExample.GCDJob" %>

<%!
    private static final String X_PARAM_NAME = "x";
    private static final String Y_PARAM_NAME = "y";
    private static final String PIPELINE_ID_PARAM_NAME = "pipelineId";
    private static final String CLEANUP_PIPELINE_ID_PARAM_NAME = "cleanupId";

    private Integer getInteger(String paramName, HttpServletRequest request) {
        try {
            return Integer.parseInt((String) request.getParameter(paramName));
        } catch (Exception e) {
            return null;
        }
    }
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

<H2>Calculate GCD of two integers using AppEngine Pipeline</H2>

<%
    Integer x = getInteger(X_PARAM_NAME, request);
    Integer y = getInteger(Y_PARAM_NAME, request);
    String pipelineId = request.getParameter(PIPELINE_ID_PARAM_NAME);
    String cleanupId = request.getParameter(CLEANUP_PIPELINE_ID_PARAM_NAME);
    PipelineService service = PipelineServiceFactory.newPipelineService();
    if (null != cleanupId) {
        service.deletePipelineRecords(cleanupId);
    }
    if (null != x && null != y && x > 0 && y > 0) {
%>
<H4>Calculating the GCD of <%=x%> and <%=y%>...</H4>
<%
    if (null == pipelineId) {
        pipelineId = service.startNewPipeline(new GCDJob(), x, y);
    }
    JobInfo jobInfo = service.getJobInfo(pipelineId);
    switch (jobInfo.getJobState()) {
        case COMPLETED_SUCCESSFULLY:
%>
Calculation completed.
<p>
    The GCD is <%=jobInfo.getOutput()%>

<form method="post">
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
    <input name="<%=X_PARAM_NAME%>" value="<%=x%>" type="hidden">
    <input name="<%=Y_PARAM_NAME%>" value="<%=y%>" type="hidden">
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
    <input name="<%=CLEANUP_PIPELINE_ID_PARAM_NAME%>" value="<%=pipelineId%>" type="hidden">
    <input type="submit" value="Do it again">
</form>
<p>
    error info:

<p>
        <%=jobInfo.getError()%>
        <%
          break;
    case CANCELED:
%>
Calculation canceled.
<p>

<form method="post">
    <input name="<%=CLEANUP_PIPELINE_ID_PARAM_NAME%>" value="<%=pipelineId%>" type="hidden">
    <input type="submit" value="Do it again">
</form>
        <%
          break;
        case STOPPED_BY_REQUEST:
%>
    Calculation stopped by request;

<p>

<form method="post">
    <input name="<%=CLEANUP_PIPELINE_ID_PARAM_NAME%>" value="<%=pipelineId%>" type="hidden">
    <input type="submit" value="Do it again">
</form>
<%
            break;
    }// end switch
}// end: if
else {
%>
Enter two positive integers:
<form method="post">
    x: <input name="<%=X_PARAM_NAME%>">
    y: <input name="<%=Y_PARAM_NAME%>">
    <input type="submit" value="Calculate GCD">
</form>
<%
    }

    if (null != pipelineId) {
%>
<p>
    <a href="/_ah/pipeline/status.html?root=<%=pipelineId%>" target="Pipeline Status">view status
        page</a>
        <%
}
%>


</BODY>
</HTML>