package com.zhangpengfei.flink.kafka;

public class ApiCallLog {

    /**
     *
     * dcoosApiId,apiType,backendResponseCode,businessResponseCode,callByte,callIp,callStartTime,callEndTime,dayId,errLevel,errCode,gatewayBusinessResponseCode,gatewayResponseCode,host,hourId,packageId,logCnt,logId,method,monthId,rawData,reqCnt,requestForwardTime,requestParam,requestReceivedTime,requestSize,responseForwardTime,responseParam,responseReceivedTime,responseSize,resultFlag,sId,seqId,subTime,traceId,uri,userAgent,userId;
     *
     *
     * {
     * "apiType": "3",
     * "sId": "ideal_lixiang_specialarea_test",
     * "userId": "fb769e569f334c9b9ceb090ca9620159",
     * "monthId": 202001,
     * "dayId": 2,
     * "hourId": 15,
     * "packageId": null,
     * "requestReceivedTime": 1577951764672,
     * "responseForwardTime": 1577951764682,
     * "responseSize": "606",
     * "callByte": "606",
     * "callIp": "10.142.101.110",
     * "userAgent": "Java/1.8.0_171",
     * "responseParam": "{\"code\":\"10000\",\"data\":{\"code\":0,\"message\":[{\"fullName\":\"杨会毅\",\"id\":1246,\"userName\":\"zb_yanghuiyi\",\"roleId\":631,\"roleName\":\"案例配置开发组\"},{\"fullName\":\"田金涛\",\"id\":469,\"userName\":\"tianjintao_tydic\",\"roleId\":631,\"roleName\":\"案例配置开发组\"},{\"fullName\":\"任佳宝\",\"id\":1244,\"userName\":\"zb_renjiabao\",\"roleId\":631,\"roleName\":\"案例配置开发组\"},{\"fullName\":\"许哲\",\"id\":1245,\"userName\":\"zb_xuzhe\",\"roleId\":631,\"roleName\":\"案例配置开发组\"}],\"pageIndex\":1,\"pageSize\":4,\"count\":4,\"pageNums\":1,\"success\":0},\"message\":\"成功\",\"seqid\":\"ae9dc2bf-ce2a-4d25-9592-69dd0f6aa35b\"}",
     * "seqId": "ae9dc2bf-ce2a-4d25-9592-69dd0f6aa35b",
     * "requestSize": "20",
     * "requestParam": "{\"pageEnable\":[\"1\"]}",
     * "requestForwardTime": 1577951764672,
     * "responseReceivedTime": 1577951764682,
     * "resultFlag": 1,
     * "errLevel": null,
     * "errCode": null,
     * "method": "GET",
     * "host": "10.142.101.156:80",
     * "uri": "/ideal_lixiang_specialarea_test/project/573/account",
     * "gatewayBusinessResponseCode": null,
     * "gatewayResponseCode": "200",
     * "backendResponseCode": "200",
     * "businessResponseCode": null
     * }
     */

    private String dcoosApiId;
    private String apiType;
    private String backendResponseCode;
    private String businessResponseCode;
    private Integer callByte;
    private String callIp;
    private Long callStartTime;
    private Long callEndTime;
    private Integer dayId;
    private String errLevel;
    private String errCode;
    private String gatewayBusinessResponseCode;
    private String gatewayResponseCode;
    private String host;
    private Integer hourId;
    private String packageId;
    private String logCnt;
    private Integer logId;
    private String method;
    private Integer monthId;
    private String rawData;
    private String reqCnt;
    private Long requestForwardTime;
    private String requestParam;
    private Long requestReceivedTime;
    private String requestSize;
    private Long responseForwardTime;
    private String responseParam;
    private Long responseReceivedTime;
    private String responseSize;
    private Integer resultFlag;
    private String sId;
    private String seqId;
    private Integer subTime;
    private String traceId;
    private String uri;
    private String userAgent;
    private String userId;

    @Override
    public String toString() {
        return
                dcoosApiId + "\t" +
                        apiType + "\t" +
                        backendResponseCode + "\t" +
                        businessResponseCode + "\t" +
                        callByte + "\t" +
                        callIp + "\t" +
                        callStartTime + "\t" +
                        callEndTime + "\t" +
                        dayId + "\t" +
                        errLevel + "\t" +
                        errCode + "\t" +
                        gatewayBusinessResponseCode + "\t" +
                        gatewayResponseCode + "\t" +
                        host + "\t" +
                        hourId + "\t" +
                        packageId + "\t" +
                        logCnt + "\t" +
                        logId + "\t" +
                        method + "\t" +
                        monthId + "\t" +
                        rawData + "\t" +
                        reqCnt + "\t" +
                        requestForwardTime + "\t" +
                        requestParam + "\t" +
                        requestReceivedTime + "\t" +
                        requestSize + "\t" +
                        responseForwardTime + "\t" +
                        responseParam + "\t" +
                        responseReceivedTime + "\t" +
                        responseSize + "\t" +
                        resultFlag + "\t" +
                        sId + "\t" +
                        seqId + "\t" +
                        subTime + "\t" +
                        traceId + "\t" +
                        uri + "\t" +
                        userAgent + "\t" +
                        userId + "\t"
                ;
    }

    public String getDcoosApiId() {
        return dcoosApiId;
    }

    public void setDcoosApiId(String dcoosApiId) {
        this.dcoosApiId = dcoosApiId;
    }

    public String getApiType() {
        return apiType;
    }

    public void setApiType(String apiType) {
        this.apiType = apiType;
    }

    public String getBackendResponseCode() {
        return backendResponseCode;
    }

    public void setBackendResponseCode(String backendResponseCode) {
        this.backendResponseCode = backendResponseCode;
    }

    public String getBusinessResponseCode() {
        return businessResponseCode;
    }

    public void setBusinessResponseCode(String businessResponseCode) {
        this.businessResponseCode = businessResponseCode;
    }

    public Integer getCallByte() {
        return callByte;
    }

    public void setCallByte(Integer callByte) {
        this.callByte = callByte;
    }

    public String getCallIp() {
        return callIp;
    }

    public void setCallIp(String callIp) {
        this.callIp = callIp;
    }

    public Long getCallStartTime() {
        return callStartTime;
    }

    public void setCallStartTime(Long callStartTime) {
        this.callStartTime = callStartTime;
    }

    public Long getCallEndTime() {
        return callEndTime;
    }

    public void setCallEndTime(Long callEndTime) {
        this.callEndTime = callEndTime;
    }

    public Integer getDayId() {
        return dayId;
    }

    public void setDayId(Integer dayId) {
        this.dayId = dayId;
    }

    public String getErrLevel() {
        return errLevel;
    }

    public void setErrLevel(String errLevel) {
        this.errLevel = errLevel;
    }

    public String getErrCode() {
        return errCode;
    }

    public void setErrCode(String errCode) {
        this.errCode = errCode;
    }

    public String getGatewayBusinessResponseCode() {
        return gatewayBusinessResponseCode;
    }

    public void setGatewayBusinessResponseCode(String gatewayBusinessResponseCode) {
        this.gatewayBusinessResponseCode = gatewayBusinessResponseCode;
    }

    public String getGatewayResponseCode() {
        return gatewayResponseCode;
    }

    public void setGatewayResponseCode(String gatewayResponseCode) {
        this.gatewayResponseCode = gatewayResponseCode;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getHourId() {
        return hourId;
    }

    public void setHourId(Integer hourId) {
        this.hourId = hourId;
    }

    public String getPackageId() {
        return packageId;
    }

    public void setPackageId(String packageId) {
        this.packageId = packageId;
    }

    public String getLogCnt() {
        return logCnt;
    }

    public void setLogCnt(String logCnt) {
        this.logCnt = logCnt;
    }

    public Integer getLogId() {
        return logId;
    }

    public void setLogId(Integer logId) {
        this.logId = logId;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Integer getMonthId() {
        return monthId;
    }

    public void setMonthId(Integer monthId) {
        this.monthId = monthId;
    }

    public String getRawData() {
        return rawData;
    }

    public void setRawData(String rawData) {
        this.rawData = rawData;
    }

    public String getReqCnt() {
        return reqCnt;
    }

    public void setReqCnt(String reqCnt) {
        this.reqCnt = reqCnt;
    }

    public Long getRequestForwardTime() {
        return requestForwardTime;
    }

    public void setRequestForwardTime(Long requestForwardTime) {
        this.requestForwardTime = requestForwardTime;
    }

    public String getRequestParam() {
        return requestParam;
    }

    public void setRequestParam(String requestParam) {
        this.requestParam = requestParam;
    }

    public Long getRequestReceivedTime() {
        return requestReceivedTime;
    }

    public void setRequestReceivedTime(Long requestReceivedTime) {
        this.requestReceivedTime = requestReceivedTime;
    }

    public String getRequestSize() {
        return requestSize;
    }

    public void setRequestSize(String requestSize) {
        this.requestSize = requestSize;
    }

    public Long getResponseForwardTime() {
        return responseForwardTime;
    }

    public void setResponseForwardTime(Long responseForwardTime) {
        this.responseForwardTime = responseForwardTime;
    }

    public String getResponseParam() {
        return responseParam;
    }

    public void setResponseParam(String responseParam) {
        this.responseParam = responseParam;
    }

    public Long getResponseReceivedTime() {
        return responseReceivedTime;
    }

    public void setResponseReceivedTime(Long responseReceivedTime) {
        this.responseReceivedTime = responseReceivedTime;
    }

    public String getResponseSize() {
        return responseSize;
    }

    public void setResponseSize(String responseSize) {
        this.responseSize = responseSize;
    }

    public Integer getResultFlag() {
        return resultFlag;
    }

    public void setResultFlag(Integer resultFlag) {
        this.resultFlag = resultFlag;
    }

    public String getsId() {
        return sId;
    }

    public void setsId(String sId) {
        this.sId = sId;
    }

    public String getSeqId() {
        return seqId;
    }

    public void setSeqId(String seqId) {
        this.seqId = seqId;
    }

    public Integer getSubTime() {
        return subTime;
    }

    public void setSubTime(Integer subTime) {
        this.subTime = subTime;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
