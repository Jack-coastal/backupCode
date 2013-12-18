#!/usr/bin/python
# Filename: Python_API_1.py
import TCLIService

from ttypes import TOpenSessionReq, TGetTablesReq, TFetchResultsReq,\
  TStatusCode, TGetResultSetMetadataReq, TGetColumnsReq, TType,\
  TExecuteStatementReq, TGetOperationStatusReq, TFetchOrientation,\
  TCloseSessionReq, TGetSchemasReq, TGetCatalogsReq, TCancelOperationReq,TRowSet

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import string

import DB 
#import QuantTechnologies.Client
from  datetime  import  * 
import time as Time

Maxrow=4294967295

#fp = open("temp.csv",'w')

def timeconvert(st):
    result=string.split(st,":")
    a=(string.atoi(result[0])*3600+string.atoi(result[1])*60+string.atoi(result[2]))*1000
    return a

def timeconvert2(tm):
    tm1=(tm.hour*3600+tm.minute*60+tm.second)*1000
    return tm1

def timeFormat(a):
    HH=a//3600000
    MM=(a-HH*3600000)//60000
    SS=(a-HH*3600000-MM*60000)//1000
    ms= "{:02.3f}".format((a-HH*3600000-MM*60000)/1000.0-SS)
    newTime="{:02d}".format(HH)+':'+"{:02d}".format(MM)+':'+"{:02d}".format(SS)+ms[1:]
    return newTime

def doFetchExgprints(client,sessHandle,symbols,date):
    execReq=TExecuteStatementReq()
    #swBegin=Time.time()
    execReq.sessionHandle=sessHandle
    q="select * from marketdata.exgprints where day = "+date.strftime('%Y%m%d')
    p=''
    if len(symbols)!=0 and len(symbols) < 400:
        for s in symbols:
          p=p+"','"+s
        p="("+p[2:]+"')"
        q=q+ ' and symbol in '+p
    print "run the query, date: "+ date.strftime('%Y-%m-%d') + ". Please wait!"
    #print q
    execReq.statement=q
    execResp = client.ExecuteStatement(execReq)
    stmtHandle = execResp.operationHandle
    fetchReq=TFetchResultsReq()
    fetchReq.operationHandle=stmtHandle
    fetchReq.orietntion=TFetchOrientation.FETCH_FIRST
    fetchReq.maxRows=Maxrow
    count=0
    bmore=True
    #swEnd=Time.time()
    while bmore:
      resultsResp = client.FetchResults(fetchReq)
      resultSet = resultsResp.results
      count=len(resultSet.rows)
      bmore=resultsResp.hasMoreRows;
      for row in resultSet.rows:
        symExi = False
        strSym = row.colVals[0].stringVal.value
        for sym in symbols:
          if (strSym==sym) or len(symbols) < 400:
            symExi = True
            break;
        if(symExi):
          exgP=DB.Exgprints()
          exgP.Date(row.colVals[7].i32Val.value)
          exgP.Time(timeFormat(row.colVals[1].i32Val.value))
          exgP.Symbol(row.colVals[0].stringVal.value)
          exgP.Price(row.colVals[5].doubleVal.value)
          exgP.Size(row.colVals[6].i32Val.value)
          exgP.Listedexchange(row.colVals[2].i32Val.value)
          exgP.Reportingexchange(row.colVals[3].i32Val.value)
          exgP.Printtype(row.colVals[4].i32Val.value)
          DB.OnExgprints(exgP)
      fetchReq.orietntion=TFetchOrientation.FETCH_NEXT
    print 'done'
    #print str(swEnd-swBegin)

def doFetchNews(client,sessHandle,symbols,date,starttime,endtime):
  execReq=TExecuteStatementReq()
  #swBegin=Time.time()
  execReq.sessionHandle=sessHandle
  q="select * from marketdata.news where day = "+date.strftime('%Y%m%d')
  if starttime !='':
    q=q + " and msofday >= " + str(timeconvert2(starttime))
  if endtime !='':
    q=q + " and msofday <= " + str(timeconvert2(endtime))
  p=''
  if len(symbols)!=0 and len(symbols) < 400:
    for s in symbols:
      p=p+"','"+s
    p="("+p[2:]+"')"
    q=q+ ' and symbol in '+ p
  print "run the query, date: "+ date.strftime('%Y-%m-%d') + ". Please wait!"
  #print q
  execReq.statement=q
  execResp = client.ExecuteStatement(execReq)
  stmtHandle = execResp.operationHandle
  fetchReq=TFetchResultsReq()
  fetchReq.operationHandle=stmtHandle
  fetchReq.orietntion=TFetchOrientation.FETCH_FIRST
  fetchReq.maxRows=Maxrow
  count=0
  bmore=True
  #swEnd=Time.time()
  while bmore:
    resultsResp = client.FetchResults(fetchReq)
    resultSet = resultsResp.results
    count=len(resultSet.rows)
    bmore=resultsResp.hasMoreRows
    for row in resultSet.rows:
      symExi = False
      strSym = row.colVals[1].stringVal.value
      for sym in symbols:
        if sym == strSym or len(symbols)<400:
          symExi = True
          break
      if (symExi):
        news=DB.News()
        news.Date(row.colVals[9].i32Val.value)
        news.Time(timeFormat(row.colVals[2].i32Val.value))
        news.Symbol(row.colVals[1].stringVal.value)
        news.Seqno(row.colVals[0].i32Val.value)
        news.Category(row.colVals[3].stringVal.value)
        news.Source(row.colVals[4].stringVal.value)
        news.Headline(row.colVals[5].stringVal.value.replace("\"",""))
        news.Resourceid(row.colVals[6].stringVal.value)
        news.Story(row.colVals[7].stringVal.value.replace("\"",""))
        news.Tags(row.colVals[8].stringVal.value.replace("\"",""))
        DB.OnNews(news)
    fetchReq.orietntion=TFetchOrientation.FETCH_NEXT
  print 'done'
    #print str(swEnd-swBegin)


def doFetchTrade1(client,sessHandle,symbols,date,starttime,endtime):
    execReq=TExecuteStatementReq()
    #swBegin=Time.time()
    execReq.sessionHandle=sessHandle
    q="select * from marketdata.trades where day = "+date.strftime('%Y%m%d')
    p=''
    if len(symbols)!=0:
        for s in symbols:
          p=p+"','"+s
        p="("+p[2:]+"')"
        q=q+ ' and symbol in '+p
    if starttime !='':
       q=q + " and time >= " + str(timeconvert2(starttime))
    if endtime !='':
       q=q + " and time <= " + str(timeconvert2(endtime))
    q = q + " order by seqno limit " + str(Maxrow);
    print "run the query, date: "+ date.strftime('%Y-%m-%d') + ". Please wait!"
    #print q
    execReq.statement=q
    execResp = client.ExecuteStatement(execReq)
    stmtHandle = execResp.operationHandle
    fetchReq=TFetchResultsReq()
    fetchReq.operationHandle=stmtHandle
    fetchReq.orietntion=TFetchOrientation.FETCH_FIRST
    fetchReq.maxRows=Maxrow
    count=0
    bmore=True
    #swEnd=Time.time()
    while bmore:
      resultsResp = client.FetchResults(fetchReq)
      resultSet = resultsResp.results
      count=len(resultSet.rows)
      bmore=resultsResp.hasMoreRows;
      for row in resultSet.rows:
      	  trd=DB.Trade()
      	  trd.Date(row.colVals[19].i32Val.value)
      	  trd.Time(timeFormat(row.colVals[16].i32Val.value))
          trd.Open(row.colVals[9].doubleVal.value)
          trd.Symbol(row.colVals[2].stringVal.value)
          trd.Exg(row.colVals[18].i32Val.value)
          trd.High(row.colVals[10].doubleVal.value)
          trd.Last(row.colVals[12].doubleVal.value)
          trd.Low(row.colVals[11].doubleVal.value)
          trd.Price(row.colVals[3].doubleVal.value)
          trd.Size(row.colVals[7].i32Val.value)
          trd.Volume(row.colVals[14].i64Val.value)
          DB.OnTrade(trd)
      fetchReq.orietntion=TFetchOrientation.FETCH_NEXT
      #print '100%'
      #if(count!=1000):
        #break
    print 'done'
    #print str(swEnd-swBegin)

def doFetchTradeNo1(client,sessHandle,symbols,startdate,enddate,starttime,endtime):
    execReq=TExecuteStatementReq()
    #swBegin=Time.time()
    execReq.sessionHandle=sessHandle
    q="select * from marketdata.trades where day >= "+startdate.strftime('%Y%m%d') + " and day <= "+ enddate.strftime('%Y%m%d') 
    p=''
    if len(symbols)!=0:
        for s in symbols:
          p=p+"','"+s
        p="("+p[2:]+"')"
        q=q+ ' and symbol in '+p
    if starttime !='':
       q=q + " and time >= " + str(timeconvert2(starttime))
    if endtime !='':
       q=q + " and time <= " + str(timeconvert2(endtime))
    #q = q + " order by seqno limit " + str(Maxrow);
    print "run the query, begindate: "+ startdate.strftime('%Y-%m-%d') + ", enddate: "+enddate.strftime('%Y-%m-%d')+". Please wait!"
    #print q
    execReq.statement=q
    execResp = client.ExecuteStatement(execReq)
    stmtHandle = execResp.operationHandle
    fetchReq=TFetchResultsReq()
    fetchReq.operationHandle=stmtHandle
    fetchReq.orietntion=TFetchOrientation.FETCH_FIRST
    fetchReq.maxRows=Maxrow
    count=0
    bmore=True
    #swEnd=Time.time()
    while bmore:
      resultsResp = client.FetchResults(fetchReq)
      resultSet = resultsResp.results
      count=len(resultSet.rows)
      bmore=resultsResp.hasMoreRows;
      for row in resultSet.rows:
      	  trd=DB.Trade()
      	  trd.Date(row.colVals[19].i32Val.value)
      	  trd.Time(timeFormat(row.colVals[16].i32Val.value))
          trd.Open(row.colVals[9].doubleVal.value)
          trd.Symbol(row.colVals[2].stringVal.value)
          trd.Exg(row.colVals[18].i32Val.value)
          trd.High(row.colVals[10].doubleVal.value)
          trd.Last(row.colVals[12].doubleVal.value)
          trd.Low(row.colVals[11].doubleVal.value)
          trd.Price(row.colVals[3].doubleVal.value)
          trd.Size(row.colVals[7].i32Val.value)
          trd.Volume(row.colVals[14].i64Val.value)
          DB.OnTrade(trd)
      fetchReq.orietntion=TFetchOrientation.FETCH_NEXT
      #print '100%'
      #if(count!=1000):
        #break
    print 'done'
    #print str(swEnd-swBegin)

def doFetchTradeNo(client,sessHandle,symbols,startdate,enddate,starttime,endtime):
    execReq=TExecuteStatementReq()
    #swBegin=Time.time()
    execReq.sessionHandle=sessHandle
    q="select * from marketdata2.trades where day >= "+startdate.strftime('%Y%m%d') + " and day <= "+ enddate.strftime('%Y%m%d') 
    p=''
    if len(symbols)!=0:
        for s in symbols:
          p=p+"','"+s
        p="("+p[2:]+"')"
        q=q+ ' and symbol in '+p
    if starttime !='':
       q=q + " and time >= " + str(timeconvert2(starttime))
    if endtime !='':
       q=q + " and time <= " + str(timeconvert2(endtime))
    #q = q + " order by seqno limit " + str(Maxrow);
    print "run the query, begindate: "+ startdate.strftime('%Y-%m-%d') + ", enddate: "+enddate.strftime('%Y-%m-%d')+". Please wait!"
    #print q
    execReq.statement=q
    execResp = client.ExecuteStatement(execReq)
    stmtHandle = execResp.operationHandle
    fetchReq=TFetchResultsReq()
    fetchReq.operationHandle=stmtHandle
    fetchReq.orietntion=TFetchOrientation.FETCH_FIRST
    fetchReq.maxRows=Maxrow
    count=0
    bmore=True
    #swEnd=Time.time()
    while bmore:
      resultsResp = client.FetchResults(fetchReq)
      resultSet = resultsResp.results
      count=len(resultSet.rows)
      bmore=resultsResp.hasMoreRows;
      for row in resultSet.rows:
      	  trd=DB.Trade()
      	  trd.Date(row.colVals[18].i32Val.value)
      	  trd.Time(timeFormat(row.colVals[15].i32Val.value))
          trd.Open(row.colVals[8].doubleVal.value)
          trd.Symbol(row.colVals[1].stringVal.value)
          trd.Exg(row.colVals[17].i32Val.value)
          trd.High(row.colVals[9].doubleVal.value)
          trd.Last(row.colVals[11].doubleVal.value)
          trd.Low(row.colVals[10].doubleVal.value)
          trd.Price(row.colVals[2].doubleVal.value)
          trd.Size(row.colVals[6].i32Val.value)
          trd.Volume(row.colVals[13].i64Val.value)
          DB.OnTrade(trd)
      fetchReq.orietntion=TFetchOrientation.FETCH_NEXT
      #print '100%'
      #if(count!=1000):
        #break
    print 'done'
    #print str(swEnd-swBegin)

def doFetchTrade(client,sessHandle,symbols,date,starttime,endtime):
    execReq=TExecuteStatementReq()
    #swBegin=Time.time()
    execReq.sessionHandle=sessHandle
    q="select * from marketdata2.trades where day = "+date.strftime('%Y%m%d')
    p=''
    if len(symbols)!=0 and len(symbols) < 400:
        for s in symbols:
          p=p+"','"+s
        p="("+p[2:]+"')"
        q=q+ ' and symbol in '+p
    if starttime !='':
       q=q + " and time >= " + str(timeconvert2(starttime))
    if endtime !='':
       q=q + " and time <= " + str(timeconvert2(endtime))
    q = q + " order by seqno limit " + str(Maxrow);
    print "run the query, date: "+ date.strftime('%Y-%m-%d') + ". Please wait!"
    #print q
    execReq.statement=q
    execResp = client.ExecuteStatement(execReq)
    stmtHandle = execResp.operationHandle
    fetchReq=TFetchResultsReq()
    fetchReq.operationHandle=stmtHandle
    fetchReq.orietntion=TFetchOrientation.FETCH_FIRST
    fetchReq.maxRows=Maxrow
    count=0
    bmore=True
    #swEnd=Time.time()
    while bmore:
      resultsResp = client.FetchResults(fetchReq)
      resultSet = resultsResp.results
      count=len(resultSet.rows)
      bmore=resultsResp.hasMoreRows;
      for row in resultSet.rows:
        symExi = False
        strSym = row.colVals[1].stringVal.value
        for sym in symbols:
          if sym == strSym or len(symbols) < 400:
            symExi = True
            break
        if (symExi):
          trd=DB.Trade()
          trd.Date(row.colVals[18].i32Val.value)
          trd.Time(timeFormat(row.colVals[15].i32Val.value))
          trd.Open(row.colVals[8].doubleVal.value)
          trd.Symbol(row.colVals[1].stringVal.value)
          trd.Exg(row.colVals[17].i32Val.value)
          trd.High(row.colVals[9].doubleVal.value)
          trd.Last(row.colVals[11].doubleVal.value)
          trd.Low(row.colVals[10].doubleVal.value)
          trd.Price(row.colVals[2].doubleVal.value)
          trd.Size(row.colVals[6].i32Val.value)
          trd.Volume(row.colVals[13].i32Val.value)
          DB.OnTrade(trd)
      fetchReq.orietntion=TFetchOrientation.FETCH_NEXT
      #print '100%'
      #if(count!=1000):
        #break
    print 'done'
    #print str(swEnd-swBegin)

def doFetchQuote(client,sessHandle,symbols,date,starttime,endtime):
    execReq=TExecuteStatementReq()
    execReq.sessionHandle=sessHandle
    q="select * from marketdata.quotes where day = "+date.strftime('%Y%m%d')
    p=''
    if len(symbols)!=0 and len(symbols) < 400:
        for s in symbols:
          p=p+"','"+s
        p="("+p[2:]+"')"
        q=q+ ' and symbol in '+p
    if starttime !='':
       q=q + " and time >= " + str(timeconvert2(starttime))
    if endtime !='':
       q=q + " and time <= " + str(timeconvert2(endtime))
    q = q + " order by seqno limit " + str(Maxrow);   
    print "run the query, date: "+ date.strftime('%Y-%m-%d') + ". Please wait!"
    execReq.statement=q
    execResp = client.ExecuteStatement(execReq)
    stmtHandle = execResp.operationHandle
    fetchReq=TFetchResultsReq()
    fetchReq.operationHandle=stmtHandle
    fetchReq.orietntion=TFetchOrientation.FETCH_FIRST
    fetchReq.maxRows=Maxrow
    count=0
    bmore=True
    while bmore:
      resultsResp = client.FetchResults(fetchReq)
      resultSet = resultsResp.results
      count=len(resultSet.rows)
      bmore=resultsResp.hasMoreRows;
      for row in resultSet.rows:
        symExi = False
        strSym = row.colVals[2].stringVal.value
        for sym in symbols:
          if sym == strSym or len(symbols) < 400:
            symExi = True
            break
        if (symExi):
          quo=DB.Quote()
          quo.Date(row.colVals[19].i32Val.value)
          quo.Time(timeFormat(row.colVals[16].i32Val.value))
          quo.Symbol(row.colVals[2].stringVal.value) 
          quo.Ask(row.colVals[5].doubleVal.value)
          quo.Ask_size(row.colVals[3].i32Val.value)
          quo.Best_ask(row.colVals[8].doubleVal.value)
          quo.Best_ask_exg(row.colVals[12].i32Val.value)
          quo.Best_ask_size(row.colVals[11].i32Val.value)
          quo.Best_bid(row.colVals[9].doubleVal.value)
          quo.Best_bid_exg(row.colVals[13].i32Val.value)
          quo.Best_bid_size(row.colVals[12].i32Val.value)
          quo.Bid(row.colVals[6].doubleVal.value)
          quo.Bid_size(row.colVals[4].i32Val.value)
          DB.OnQuote(quo)
      fetchReq.orietntion=TFetchOrientation.FETCH_NEXT
    print 'done'

def doFetchDaily(client,sessHandle,symbols,date):
    execReq=TExecuteStatementReq()
    execReq.sessionHandle=sessHandle
    q="select * from marketdata.daily where day = " + date.strftime('%Y%m%d')
    p=''
    if len(symbols)!=0 and len(symbols) < 400:
        for s in symbols:
          p=p+"','"+s
        p="("+p[2:]+"')"
        q=q+ ' and symbol in '+p
    #print "run the query, date: "+ date.strftime('%Y-%m-%d') + ". Please wait!"
    execReq.statement=q
    execResp = client.ExecuteStatement(execReq)
    stmtHandle = execResp.operationHandle
    fetchReq=TFetchResultsReq()
    fetchReq.operationHandle=stmtHandle
    fetchReq.orietntion=TFetchOrientation.FETCH_FIRST
    fetchReq.maxRows=Maxrow
    count=0
    bmore=True
    while bmore:
      resultsResp = client.FetchResults(fetchReq)
      resultSet = resultsResp.results
      count=len(resultSet.rows)
      bmore=resultsResp.hasMoreRows;
      for row in resultSet.rows:
        symExi = False
        strSym = row.colVals[1].stringVal.value
        for sym in symbols:
          if sym == strSym or len(symbols) < 400:
            symExi = True
            break
        if (symExi):
          dai=DB.Daily()
          dai.Date(row.colVals[0].i32Val.value)
          dai.Symbol(row.colVals[1].stringVal.value) 
          dai.High(row.colVals[3].doubleVal.value)
          dai.Low(row.colVals[4].doubleVal.value)
          dai.Close(row.colVals[5].doubleVal.value)
          dai.Open(row.colVals[2].doubleVal.value)
          dai.Volume(row.colVals[6].i32Val.value)
          DB.OnDaily(dai)
      fetchReq.orietntion=TFetchOrientation.FETCH_NEXT
    print 'done'

def doFetchDailyNo(client,sessHandle,symbols,startdate,enddate):
    execReq=TExecuteStatementReq()
    execReq.sessionHandle=sessHandle
    q="select * from marketdata.daily where day >= " + startdate.strftime('%Y%m%d') + " and day <= "+ enddate.strftime('%Y%m%d') 
    p=''
    if len(symbols)!=0 and len(symbols) < 400:
        for s in symbols:
          p=p+"','"+s
        p="("+p[2:]+"')"
        q=q+ ' and symbol in '+p
    q = q + " order by day limit " + str(Maxrow);
    print "run the query, begindate: "+ startdate.strftime('%Y-%m-%d') + ", enddate: "+enddate.strftime('%Y-%m-%d')+". Please wait!"
    execReq.statement=q
    execResp = client.ExecuteStatement(execReq)
    stmtHandle = execResp.operationHandle
    fetchReq=TFetchResultsReq()
    fetchReq.operationHandle=stmtHandle
    fetchReq.orietntion=TFetchOrientation.FETCH_FIRST
    fetchReq.maxRows=Maxrow
    count=0
    bmore=True
    while bmore:
      resultsResp = client.FetchResults(fetchReq)
      resultSet = resultsResp.results
      count=len(resultSet.rows)
      bmore=resultsResp.hasMoreRows;
      for row in resultSet.rows:
        symExi = False
        strSym = row.colVals[1].stringVal.value
        for sym in symbols:
          if sym == strSym or len(symbols) < 400:
            symExi = True
            break
        if (symExi):
          dai=DB.Daily()
          dai.Date(row.colVals[0].i32Val.value)
          dai.Symbol(row.colVals[1].stringVal.value) 
          dai.High(row.colVals[3].doubleVal.value)
          dai.Low(row.colVals[4].doubleVal.value)
          dai.Close(row.colVals[5].doubleVal.value)
          dai.Open(row.colVals[2].doubleVal.value)
          dai.Volume(row.colVals[6].i32Val.value)
          DB.OnDaily(dai)
      fetchReq.orietntion=TFetchOrientation.FETCH_NEXT      
      #print '100%'
      #if(count!=1000):
        #break
    print 'done'

def doFetchNYSEImbalance(client,sessHandle,symbols,date,starttime,endtime):
    execReq=TExecuteStatementReq()
    execReq.sessionHandle=sessHandle
    q="select * from marketdata.imb_nyse where day = "+date.strftime('%Y%m%d')
    p=''
    if len(symbols)!=0 and len(symbols) < 400:
        for s in symbols:
          p=p+"','"+s
        p="("+p[2:]+"')"
        q=q+ ' and symbol in '+p
    if starttime !='':
       q=q + " and msofday >= " + str(timeconvert2(starttime))
    if endtime !='':
       q=q + " and msofday <= " + str(timeconvert2(endtime))
    print "run the query, date: "+ date.strftime('%Y-%m-%d') + ". Please wait!"
    #print q
    execReq.statement=q
    execResp = client.ExecuteStatement(execReq)
    stmtHandle = execResp.operationHandle
    fetchReq=TFetchResultsReq()
    fetchReq.operationHandle=stmtHandle
    fetchReq.orietntion=TFetchOrientation.FETCH_FIRST
    fetchReq.maxRows=Maxrow
    count=0
    bmore=True
    while bmore:
      resultsResp = client.FetchResults(fetchReq)
      resultSet = resultsResp.results
      count=len(resultSet.rows)
      bmore=resultsResp.hasMoreRows;
      for row in resultSet.rows:
        symExi = False
        strSym = row.colVals[0].stringVal.value
        for sym in symbols:
          if sym == strSym or len(symbols) < 400:
            symExi = True
            break
        if (symExi):
          Imb=DB.Imbalance()
          sidej = DB.Side.NONE
          typej = DB.ImbType.UNKNOWN
          if row.colVals[2].byteVal.value == 0:
            sidej = DB.Side.NONE
          elif row.colVals[2].byteVal.value == 1:
            sidej = DB.Side.BUY
          elif row.colVals[2].byteVal.value == 2:
            sidej = DB.Side.SELL
          if row.colVals[3].byteVal.value == 0:
            typej = DB.ImbType.UNKNOWN
          elif row.colVals[3].byteVal.value == 1:
            typej = DB.ImbType.OPEN
          elif row.colVals[3].byteVal.value == 4:
            typej = DB.ImbType.CLOSE
          elif row.colVals[3].byteVal.value == 5:
            typej = DB.ImbType.NO_IMBALANCE
          Imb.Date(row.colVals[13].i32Val.value)
          Imb.Time(timeFormat(row.colVals[1].i32Val.value))
          Imb.Symbol(row.colVals[0].stringVal.value)
          Imb.Clearing_price(row.colVals[11].doubleVal.value)
          Imb.Far_price(row.colVals[9].doubleVal.value)
          Imb.Imbalance_type(typej)
          Imb.Market_imbalance(row.colVals[6].i32Val.value)
          Imb.Near_price(row.colVals[10].doubleVal.value)
          Imb.Paired_shares(row.colVals[5].i32Val.value)
          Imb.Ref_price(row.colVals[8].doubleVal.value)
          Imb.Side(sidej)
          Imb.Total_imbalance(row.colVals[7].i32Val.value)
          Imb.Tag(row.colVals[12].byteVal.value)
          DB.OnImbalance(Imb)
      fetchReq.orietntion=TFetchOrientation.FETCH_NEXT
    print 'done'

def Subscribe(Flags,symbols,startdate,enddate,starttime,endtime,client):
    transport = TSocket.TSocket(client.server, '5049')
    user=client.username
    ps=client.password
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = TCLIService.Client(protocol)
    transport.open()
    req=TOpenSessionReq()
    req.username=user
    req.password=ps
    openResp=client.OpenSession(req)
    sessHandle=openResp.sessionHandle
    execReq=TExecuteStatementReq()
    execReq.sessionHandle=sessHandle
    date=startdate
    #query
    for flag in DB.SubscriptionFlags.ENUM:
    	option = flag & Flags
        if option == DB.SubscriptionFlags.TRADE:
        	#doFetchTradeNo1(client,sessHandle,symbols,startdate,enddate,starttime,endtime)
        	while (startdate<=enddate):
                 doFetchTrade(client,sessHandle,symbols,startdate,starttime,endtime)
                 try:
                   startdate=startdate.replace(day=startdate.day+1)
                 except Exception, e:
                  try:
                    startdate=startdate.replace(day=1)
                    startdate=startdate.replace(month=startdate.month+1)
                  except Exception, e:
                    startdate=startdate.replace(day=1)
                    startdate=startdate.replace(month=1)
                    startdate=startdate.replace(year=startdate.year+1)
        if option == DB.SubscriptionFlags.QUOTE:
        	while (startdate<=enddate):
                 doFetchQuote(client,sessHandle,symbols,startdate,starttime,endtime)
                 try:
                   startdate=startdate.replace(day=startdate.day+1)
                 except Exception, e:
                  try:
                    startdate=startdate.replace(day=1)
                    startdate=startdate.replace(month=startdate.month+1)
                  except Exception, e:
                    startdate=startdate.replace(day=1)
                    startdate=startdate.replace(month=1)
                    startdate=startdate.replace(year=startdate.year+1)
        if option == DB.SubscriptionFlags.DAILY:
          doFetchDailyNo(client,sessHandle,symbols,startdate,enddate)
        if option == DB.SubscriptionFlags.NYSE_IMBALANCE:
          while (startdate<=enddate):
                 doFetchNYSEImbalance(client,sessHandle,symbols,startdate,starttime,endtime)
                 try:
                   startdate=startdate.replace(day=startdate.day+1)
                 except Exception, e:
                  try:
                    startdate=startdate.replace(day=1)
                    startdate=startdate.replace(month=startdate.month+1)
                  except Exception, e:
                    startdate=startdate.replace(day=1)
                    startdate=startdate.replace(month=1)
                    startdate=startdate.replace(year=startdate.year+1)
        if option == DB.SubscriptionFlags.EXGPRINTS:
          while (startdate<=enddate):
            doFetchExgprints(client,sessHandle,symbols,startdate)
            try:
              startdate=startdate.replace(day=startdate.day+1)
            except Exception, e:
              try:
                startdate=startdate.replace(day=1)
                startdate=startdate.replace(month=startdate.month+1)
              except Exception, e:
                startdate=startdate.replace(day=1)
                startdate=startdate.replace(month=1)
                startdate=startdate.replace(year=startdate.year+1)
        if option == DB.SubscriptionFlags.NEWS:
          while (startdate<=enddate):
            doFetchNews(client,sessHandle,symbols,startdate,starttime,endtime)
            try:
              startdate=startdate.replace(day=startdate.day+1)
            except Exception, e:
              try:
                startdate=startdate.replace(day=1)
                startdate=startdate.replace(month=startdate.month+1)
              except Exception,e:
                startdate=startdate.replace(day=1)
                startdate=startdate.replace(month=1)
                startdate=startdate.replace(year=startdate.year+1)
    closeReq=TCloseSessionReq()
    closeReq.sessionHandle=sessHandle
    client.CloseSession(closeReq)
    transport.close()



# End of mymodule.py