require("quantmod");require("data.table");require("lubridate");require("pbapply");require("DBI")
require("RSQLite")

# assign DB Password
PASS <- new.env()
assign("pwd","DB_PASSWORD",envir=PASS)

# add 30 days to get next expiration
DATES = seq.Date(from=as.Date("2021-04-16"), to=Sys.Date()+30, by = "1 day")
# create NA xts to extract option expiration dates
NAxts = xts(rep(NA,length(DATES)), order.by = DATES)
DATES = DATES[options.expiry(NAxts)]
# **********************************************************************************************
#                               function to get Options from SQLite DB
# **********************************************************************************************
getOpsbySymbol = function(ticker,open_date,next_expiry){
  # connect to database
  library(DBI)
  con <- dbConnect(odbc(), Driver = "/usr/local/mysql-connector-odbc-8.0.27-macos11-x86-64bit/lib/libmyodbc8a.so", 
                   Server = "localhost", Database = "DATA", UID = "root", PWD = PASS$pwd, 
                   Port = 3306)
  ops = as.data.frame(dbGetQuery(con,paste0("SELECT * FROM CBOE_OPTIONS WHERE Symbol='",ticker,
                                            "' AND Date='",open_date,
                                            "' AND expiry='",next_expiry,"'")))
  
  dbDisconnect(con)
  ops
}
# **********************************************************************************************
#                                       covered call strategy
# **********************************************************************************************
# expiry_dates   : Pass in vector of expiration dates & read in the files/options on those days
# symbol         : Optionable stock/etf symbol
# strikesAboveATM: How many strikes above ATM should be used to write
coveredCallStrat = function(expiry_dates, symbol, strikesAboveATM){
  # pass in the expirations and extract options for ticker 
  ops = lapply(as.list(2:length(expiry_dates)), function(ii){
    # assign open_date & next_expiry (1-month expirations)
    open_date   = expiry_dates[ii-1]
    next_expiry = expiry_dates[ii]
    # read in options
    op = getOpsbySymbol(ticker=symbol,open_date = open_date, next_expiry = next_expiry)
    # get the ATM call i.e. the closest strike to the last traded stock price
    op$stkPrc2strike = as.numeric(op$stkClose) - as.numeric(op$strike)
    if(strikesAboveATM == 0){
      # eliminate OTM calls
      op = subset(op,op$stkPrc2strike > 0)
      # which has the least difference to the stk Close
      ATM = op[which.min(op$stkPrc2strike),]
    }else{
      # if strikesAboveATM is above MAX strikes available, last strike will be selected:
      if(strikesAboveATM > length(op$stkPrc2strike)){strikesAboveATM = length(op$stkPrc2strike)}
      # find the OTM calls and select 
      ATM = op[which(op$stkPrc2strike < 0)[strikesAboveATM],]
    }
    # extract desired columns
    ATM = ATM[,c("Date","expiry","days2Exp","stkClose","strike","Mid")]
    colnames(ATM) = c("openDate","expiry","days2exp","open_stkPrc","strike","call_premium")
    ATM
  })
  # combine options
  ops = rbindlist(ops,use.names = TRUE, fill = TRUE)
  # get latest quote for the last expiration
  lastPrc = getQuote(symbol)$Last
  # add expiration prices to ops
  ops$prcAtexp = c(ops$open_stkPrc[2:nrow(ops)],lastPrc)
  # calculate the net premium received
  ops$stk2strike = (as.numeric(ops$prcAtexp) - as.numeric(ops$strike))
  # net Premium
  # ops$netPremium = ifelse(ops$stk2strike>0, 
  #                         (as.numeric(ops$strike)-as.numeric(ops$open_stkPrc))+as.numeric(ops$call_premium), 
  #                         as.numeric(ops$call_premium)+ops$stk2strike)
  ops$netPremium = ifelse(ops$stk2strike>0, 
                          (as.numeric(ops$strike)-as.numeric(ops$open_stkPrc))+as.numeric(ops$call_premium), 
                          (as.numeric(ops$strike)-as.numeric(ops$open_stkPrc))+as.numeric(ops$call_premium)+ops$stk2strike)
  # caluculate returns
  ops$ccRet = round(ops$netPremium/ops$open_stkPrc,4)
  ops$stkRet = round(ops$prcAtexp/ops$open_stkPrc-1,4)
  
  # return results
  ops
}

# test function
tmp0 = coveredCallStrat(expiry_dates = DATES, symbol = "F", strikesAboveATM = 0)
tmp1 = coveredCallStrat(expiry_dates = DATES, symbol = "F", strikesAboveATM = 1)
# **********************************************************************************************
#                           get covered call strategy returns for symbols
# **********************************************************************************************
#tickers = c("AAPL","SPY","F","EBAY","QQQ","TSLA")

getTickers = function(minPRC,maxPRC, asOf)
{
  
  library(DBI)
  con <- dbConnect(odbc(), Driver = "/usr/local/mysql-connector-odbc-8.0.27-macos11-x86-64bit/lib/libmyodbc8a.so", 
                   Server = "localhost", Database = "DATA", UID = "root", PWD = PASS$pwd, 
                   Port = 3306)
  ops = as.data.frame(dbGetQuery(con,paste0("SELECT DISTINCT Symbol,stkClose FROM CBOE_OPTIONS WHERE (stkClose>=",minPRC,
                                            " AND stkClose<=",maxPRC,") AND Date='",asOf,"'")))
  
  dbDisconnect(con)
  # delete index options
  ops = ops[!str_detect(ops$Symbol,"\\_"),]
  ops
}
tickers = getTickers(minPRC = 10, maxPRC = 30, asOf="2021-11-18")
tickers = as.character(tickers$Symbol)
# apply function to selected symbols
getRets = lapply(as.list(tickers), function(x){
  # print current stock
  cat("\n",x," | #",which(x == tickers))
  # get results
  tmp = try(coveredCallStrat(expiry_dates = DATES,symbol = x, strikesAboveATM = 0),silent = TRUE)
  # if error - OUT (OUTPUT will be null)
  if(inherits(tmp,'try-error') | length(tmp) == 0 | is.null(tmp)){
    OUT <- NULL
  }else{
    if(nrow(tmp) == 0){OUT=NULL}else{
      # get Covered Call Summary + Sharpe
      OUT = as.data.frame(cbind(x,sum(tmp$stk2strike),sum(tmp$call_premium), 
                                round(sum(tmp$netPremium)-sum(tmp$call_premium),2),round(sum(tmp$netPremium),2),
                                round(mean(tmp$open_stkPrc),2),round(mean(tmp$ccRet),4),round(mean(tmp$stkRet),4),
                                round(sum(tmp$ccRet),4),
                                round(tmp$prcAtexp[length(tmp$prcAtexp)]/tmp$prcAtexp[1]-1,4),
                                length(tmp$stk2strike),
                                length(tmp$stk2strike[tmp$stk2strike < 0]),
                                length(tmp$stk2strike[tmp$stk2strike > 0]),
                                length(tmp$netPremium[tmp$netPremium > 0])))
      # assign column names
      colnames(OUT) = c("Symbol","mktPoints","grossPremium","mktLoss","netPremium","avgStkPRC",
                        "avgCCret","avgSTKret","totalCCret","buyNholdRet","N","nBelowStrk",
                        "nAboveStrk","netGains")
      # add Sharpe Ratio
      OUT$ccSharpe = round(mean(tmp$ccRet)/sd(tmp$ccRet),2)
      OUT$stkSharpe = round(mean(tmp$stkRet)/sd(tmp$stkRet),2)
    }}
  # return OUT (summary)
  OUT
})

# rbind results
res = rbindlist(getRets,use.names=TRUE,fill=TRUE)
# save.image("20211119CBOEIB.RData")
# load("20211119CBOEIB.RData")
N = max(as.numeric(res$N))
res = subset(res, res$N == paste(N))
res = subset(res, as.numeric(res$ccSharpe) >= 1)

gd = subset(res, as.numeric(res$netPremium) > as.numeric(res$mktPoints))
gd = subset(gd, as.numeric(gd$netPremium) >= 7.00)
gd = subset(gd, gd$netGains == 7)
gd = subset(gd, as.numeric(gd$nBelowStrk) <= 2)

# function to get premiums for next expiration
getCurrentCC = function(symbol, expiration, asOf){
  
  library(DBI)
  con <- dbConnect(odbc(), Driver = "/usr/local/mysql-connector-odbc-8.0.27-macos11-x86-64bit/lib/libmyodbc8a.so", 
                   Server = "localhost", Database = "DATA", UID = "root", PWD = PASS$pwd, 
                   Port = 3306)
  ops = as.data.frame(dbGetQuery(con,paste0("SELECT * FROM CBOE_OPTIONS WHERE expiry='",expiration,"'",
                                            " AND Symbol='",symbol,"' AND Date='",asOf,"' AND flag='C'")))
  
  dbDisconnect(con)
  # calculate the difference between the stock and strike
  ops$stk2strk = as.numeric(ops$stkClose) - as.numeric(ops$strike)
  # eliminate the negative's (OTM)
  ops = subset(ops, as.numeric(ops$stk2strk)>0)
  # ATM
  ATM = ops[which.min(ops$stk2strk),]
  ATM$NetPrem = as.numeric(ATM$Mid) - as.numeric(ATM$stk2strk)
  ATM$PctRet = round(as.numeric(ATM$NetPrem)/as.numeric(ATM$stkClose),4)
  # subset columns
  ATM = ATM[,c("Symbol","expiry","stkClose","strike","Mid","stk2strk","NetPrem","PctRet")]
  ATM
}

# returns a table for next expiration
nextExp = pblapply(as.list(gd$Symbol), function(x){
  getCurrentCC(symbol = x, expiration="2021-12-17",asOf = "2021-11-18")
})
# row bind 
nextExp = rbindlist(nextExp, use.names=TRUE, fill=TRUE)
nextExp = nextExp[1:2,]
# add limit price
# limit Price == underlying - credit premium
nextExp$lmtPRC = as.numeric(nextExp$stkClose) - as.numeric(nextExp$Mid)


coveredCallOrders =  function(nextExp, limit){
  
  if(limit==1){
    require("IBrokers")
    tws = twsConnect(port=7497)
    ac <- reqAccountUpdates(tws)
    
    # assign parameters
    stk    = nextExp$Symbol[1] 
    expiry = nextExp$expiry[1]
    strike = nextExp$strike[1]
    lmtPRC = nextExp$lmtPRC[1]   
      
    # create Equity order
    stk_det = reqContractDetails(tws,twsSTK(stk,exch="SMART",primary="NASDAQ",currency="USD"))
    
    contract = twsContract()
    contract$conId = stk_det[[1]]$conId
    contract$symbol = stk
    contract$sectype = "BAG"
    contract$currency = "USD"
    contract$primary = "NASDAQ"
    
    # create tws Contract
    contract2 <- twsContract()
    contract2$symbol <- stk
    contract2$currency <- "USD"
    contract2$sectype <- "OPT"
    contract2$expiry <- format(expiry,format="%Y%m%d") # "20211217"
    contract2$right  <- "C"
    contract2$strike <- strike
    contract2$primary<- "NASDAQ"
    CONTRACT <- reqContractDetails(tws,contract2)
    # my option order
    opt = CONTRACT[[1]]$contract
    
    leg1 = twsComboLeg(conId = stk_det[[1]]$conId,ratio = "100",action = "BUY",exchange = "SMART")
    leg2 = twsComboLeg(conId = opt$conId,ratio = "1",action = "SELL",exchange = "SMART")
    
    # we now have to wrap it in a twsBAG()
    cont = twsBAG()
    cont$conId = contract$conId
    cont$symbol = stk
    cont$sectype = "BAG"
    cont$exch = "SMART"
    cont$primary = "NASDAQ"
    cont$comboleg = list(leg1,leg2)
    
    
    orderID = as.numeric(reqIds(tws))
    myorder      = twsOrder(orderId = orderID,orderType = "LMT",lmtPrice = paste(lmtPRC),
                            outsideRTH = "0",action="BUY", totalQuantity = "1", transmit=FALSE)
    
    # place option order
    placeOrder(tws,cont, myorder)
    
    twsDisconnect(tws)
    
  }
  if(limit==2){
    
    require("IBrokers")
    tws = twsConnect(port=7497)
    ac <- reqAccountUpdates(tws)
    # assign parameters
    stk    = nextExp$Symbol[1] 
    expiry = nextExp$expiry[1]
    strike = nextExp$strike[1]
    lmtPRC = nextExp$lmtPRC[1]   
    
    # create Equity order
    stk_det = reqContractDetails(tws,twsSTK(stk,exch="SMART",primary="NASDAQ",currency="USD"))
    
    contract = twsContract()
    contract$conId = stk_det[[1]]$conId
    contract$symbol = stk
    contract$sectype = "BAG"
    contract$currency = "USD"
    contract$primary = "NASDAQ"
    
    # create tws Contract
    contract2 <- twsContract()
    contract2$symbol <- stk
    contract2$currency <- "USD"
    contract2$sectype <- "OPT"
    contract2$expiry <- format(expiry,format="%Y%m%d") # "20211217"
    contract2$right  <- "C"
    contract2$strike <- strike
    contract2$primary<- "NASDAQ"
    CONTRACT <- reqContractDetails(tws,contract2)
    # my option order
    opt = CONTRACT[[1]]$contract
    
    leg1 = twsComboLeg(conId = stk_det[[1]]$conId,ratio = "100",action = "BUY",exchange = "SMART")
    leg2 = twsComboLeg(conId = opt$conId,ratio = "1",action = "SELL",exchange = "SMART")
    
    # we now have to wrap it in a twsBAG()
    cont = twsBAG()
    cont$conId = contract$conId
    cont$symbol = stk
    cont$sectype = "BAG"
    cont$exch = "SMART"
    cont$primary = "NASDAQ"
    cont$comboleg = list(leg1,leg2)
    
    
    orderID = as.numeric(reqIds(tws))
    myorder      = twsOrder(orderId = orderID,orderType = "LMT",lmtPrice = paste(lmtPRC),
                            outsideRTH = "0",action="BUY", totalQuantity = "1", transmit=FALSE)
    
    # place option order
    placeOrder(tws,cont, myorder)
    
    Sys.sleep(3)
    tws = twsConnect(port=7497)
    ac <- reqAccountUpdates(tws)
    
    # assign parameters
    stk    = nextExp$Symbol[2] 
    expiry = nextExp$expiry[2]
    strike = nextExp$strike[2]
    lmtPRC = nextExp$lmtPRC[2]   
    # create Equity order
    stk_det = reqContractDetails(tws,twsSTK(stk,exch="SMART",primary="NASDAQ",currency="USD"))
    
    contract = twsContract()
    contract$conId = stk_det[[1]]$conId
    contract$symbol = stk
    contract$sectype = "BAG"
    contract$currency = "USD"
    contract$primary = "NASDAQ"
    
    # create tws Contract
    contract2 <- twsContract()
    contract2$symbol <- stk
    contract2$currency <- "USD"
    contract2$sectype <- "OPT"
    contract2$expiry <- format(expiry,format="%Y%m%d") # "20211217"
    contract2$right  <- "C"
    contract2$strike <- strike
    contract2$primary<- "NASDAQ"
    CONTRACT <- reqContractDetails(tws,contract2)
    # my option order
    opt = CONTRACT[[1]]$contract
    
    leg1 = twsComboLeg(conId = stk_det[[1]]$conId,ratio = "100",action = "BUY",exchange = "SMART")
    leg2 = twsComboLeg(conId = opt$conId,ratio = "1",action = "SELL",exchange = "SMART")
    
    # we now have to wrap it in a twsBAG()
    cont = twsBAG()
    cont$conId = contract$conId
    cont$symbol = stk
    cont$sectype = "BAG"
    cont$exch = "SMART"
    cont$primary = "NASDAQ"
    cont$comboleg = list(leg1,leg2)
    
    
    orderID = as.numeric(reqIds(tws))
    myorder      = twsOrder(orderId = orderID,orderType = "LMT",lmtPrice = paste(lmtPRC),
                            outsideRTH = "0",action="BUY", totalQuantity = "1", transmit=FALSE)
    
    # place option order
    placeOrder(tws,cont, myorder)
    
    twsDisconnect(tws)   
  }
  
  
}

coveredCallOrders(nextExp = nextExp, limit=2)



