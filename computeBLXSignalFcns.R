library(dplyr)
library(stringr)
library(tidyr)
library(lubridate)
library(RMySQL)
library(jsonlite)
library(RCurl)
library(ggplot2)
library(RMongo)
library(scales)
# library(parallel)
source("/home/cal/Documents/methodBLX/locf.R")



########################################################################################################################################################################################################
### 
###   Global Variables
### 
########################################################################################################################################################################################################

SQL_usr <<- "root"
SQL_pass <<- "Siger321"
SQL_host <<- "172.16.33.223"
SQL_port <<- 3306

Mongo_host1 <<- "localhost"
Mongo_port1 <<- 27017

local_path <<- "/home/cal/Documents/methodBLX/cache"
pic_path <<- "/home/gz2"


########################################################################################################################################################################################################
### 
###   Side Functions
### 
########################################################################################################################################################################################################
dataClean <- function(dt, machineType = "jc", missingValTol = 0.1){

  ### (-) MISSING VALUES
  
    # FIND ALL MISSING VALUES
    missingIndex <- which(complete.cases(dt) != 1)

    
    # NO MISSING ROWS
    if (is.na(missingIndex[1])){
      dt <- dt
      
      
    # MISSING UNDER TOLERANT LEVEL
    }else if((length(missingIndex)/nrow(dt) < missingValTol) && (nrow(dt)>1000)){
      dt <- dt[!missingIndex,]
      
      
    # FILL USING ADJACENT POINTS  
    }else{
      for (i in 1:ncol(dt)){
        dt[,i] <- forbak(dt[,i])
      }
    }
  
  
  ### (-) CYCLE_COUNTER ALWAYS INCREASING CHECK
    
    # DROP CYCLE_COUNTER == 0
    dt = dt[which(dt$cycle_counter!=0),]
    
    
    # DETECT DROPS IN CYCLE_COUNTER
    ind <- which((dt[['cycle_counter']] - lag(dt[['cycle_counter']])) < 0)
    
    
    # GET DROP CYCLE_COUNTER VALUES
    increment <- dt[['cycle_counter']][ind - 1] - dt[['cycle_counter']][ind]
  
    
    # INCREASE FOLLOWING CYCLE_COUNTER
    if (!is.na(increment[1])){
      
      for (i in 1:length(increment)){
        dt[['cycle_counter']][ind[i]:length(dt[['cycle_counter']])] = dt[['cycle_counter']][ind[i]:length(dt[['cycle_counter']])] + increment[i] + 1
      }
      
    }
  

  
  ### (-) FILTERING
  if (machineType == "jc"){
    
    dt2 <- dt %>% filter(alarm_state == 0,
                         program_state == 3,
                         operating_mode == 1)
    
  }else if (machineType == 36){
    
    dt2 <- dt %>% 
      filter(program_state == 3,
             operating_mode == 1,
             abs(spindle_speed - lag(spindle_speed)) < 1) %>%
      filter(abs(spindle_speed - lag(spindle_speed)) < 1,
             spindle_load > 1,
             spindle_speed > 0)
    
  }else if(machineType == 39){
    
    dt2 <- dt %>%
      filter(program_state == 3,
             operating_mode == 1,
             abs(spindle_speed - lag(spindle_speed)) < 1) %>%
      filter(abs(spindle_speed - lag(spindle_speed)) < 1,
             spindle_load > 1,
             spindle_speed > 0) %>%
      filter(abs(spindle_speed - lag(spindle_speed)))
    
  }
  
  return(dt2)
}



getDNSIP <- function(touchURL, typeString, paramDF){
  
  # PREPARE LIST TO TRANSFER
  list2json = list(ver = "1",
                   companyid = paramDF[['companyID']], 
                   projectid = paramDF[['projectID']])
  
  # LIST TO JSON
  json2post <- jsonlite::toJSON(list2json,method = "C", auto_unbox = TRUE)
  
  # JSON TO TRANSFER
  recData <- RCurl::postForm(uri = touchURL,
                             .opts = list(postfields = json2post, httpheader = c('Content-Type' = 'application/json', Accept = 'application/json')))
  
  # GET URL
  tmp <- fromJSON(recData)
  ipAddr <- tmp[["data"]][["statusCalcUrl"]]
  
  # GET ALARM URL
  newestURL <- paste0(ipAddr, typeString)
  
  return(newestURL)
}



killDbConnections <- function(){
  
  all_cons <- dbListConnections(MySQL())
  for (con in all_cons){DBI::dbDisconnect(con)}
  
}



########################################################################################################################################################################################################
### 
###   Main Functions
### 
########################################################################################################################################################################################################

computeBLXSignal <- function(){
  
  # READ TARGETS
  ruleVal <- readRDS(file = paste0(local_path, "/targets1", ".RData"))
  ruleVal <- unique(ruleVal[,c("PN", "spindleID", "machineID", "projectID","companyID")])

  
  # API
  urlAll = read.table(paste0(local_path, "/ipAddress"))
  URL_touchAlarm <- as.character(urlAll$V2[urlAll$V1 == "urlAlarm"])
  urlruleVal <- unique(ruleVal[,c("projectID", "companyID")])
  

  for (i in 1:nrow(urlruleVal)){

    current_ruleVal <- urlruleVal[i,]
    
    URL_Alarm <- getDNSIP(touchURL = URL_touchAlarm, typeString = "/Api/WarningMsg/SendWarningMsg", paramDF = data.frame(companyID = current_ruleVal$companyID, projectID = current_ruleVal$projectID))
    
    ruleVal$urlAddr[which((ruleVal[['companyID']] == current_ruleVal[['companyID']]) && (ruleVal[['projectID']] == current_ruleVal[['projectID']]))] = URL_Alarm 

  }
  

  # COMPUTE
  apply(ruleVal, 1, computeProcess)
  
}





computeProcess <- function(current_ruleVal){
a=Sys.time()
###********************************************** INITIALIZATION **********************************************###  
  # LOCATE TARGET
  URL_Alarm <- as.character(current_ruleVal[['urlAddr']])
  companyID <- as.numeric(current_ruleVal[['companyID']])
  projectID <- as.numeric(current_ruleVal[['projectID']])
  machineID <- as.numeric(current_ruleVal[['machineID']])
  spindleID <- as.numeric(current_ruleVal[['spindleID']])
  programNum <- as.numeric(current_ruleVal[['PN']]); 
  toolNum <- NULL
  machineType <- companyID
  if (!machineType %in% c(36,39)){machineType = "jc"}

  
  
  # PARSE MONGODB CONNECTION INFO
  # db_name_low = paste0("C_", companyID, "_", projectID)
  db_name_high = paste0("Cache_C_", companyID, "_", projectID)
  
  coll_name = paste0("machine_", machineID)
  tool_name = paste0("tn", spindleID)
  sl_name = paste0("sl", spindleID)
  ss_name = paste0("ss", spindleID)
  
  
  
  # SET SEARCH TIME
  endTime <- Sys.time()-8*60*60
  startTime <- endTime - 5*60
  
  eT <- paste0(as.numeric(lubridate::ymd_hms(endTime)),"000")
  sT <- paste0(as.numeric(lubridate::ymd_hms(startTime)),"000")
  
  
  
  # SET FEATURE TYPE & TRIM LEVEL
  if (machineType == "jc"){
    featureType <- "max"
    trimLevel <- 0.1
  }else{
    featureType <- "max"
    trimLevel <- 0.1
  }
  
  
  
  
  

  
###********************************************** MONGO **********************************************###  
  # MONGO QUERY
  tryCatch({
    mongoQuery <- paste0("{'t':{'$gte':{'$date':", sT, "}, '$lte':{'$date':", eT, "}}}")
    # connMongo_low <- RMongo::mongoDbConnect(dbName = db_name_low, host = Mongo_host1, port = Mongo_port1)
    # output_low <- RMongo::dbGetQuery(connMongo_low, coll_name, mongoQuery, skip=0, limit=10000)
    connMongo_high <- mongoDbConnect(dbName = db_name_high, host = Mongo_host1, port = Mongo_port1)
    output_high <- RMongo::dbGetQuery(connMongo_high, coll_name, mongoQuery, skip=0, limit=100000)
  }, error=function(e){
    # RMongo::dbDisconnect(connMongo_low)
    RMongo::dbDisconnect(connMongo_high)
    return(33)
  })
  # RMongo::dbDisconnect(connMongo_low)
  RMongo::dbDisconnect(connMongo_high)
  
  # if((nrow(output_low) == 0) && (nrow(output_high) == 0)){return(44)}
  if(nrow(output_high) == 0){return(44)}
  
  
  
  

  
###********************************************** DATA AGGREGATION **********************************************###  
  # MERGE & ORDER
  output <- output_high#rbind(output_low, output_high)
  output$t <- parse_date_time(output$t,orders="%a %m %d %H:%M:%S %Y")
  output <- output[order(output$t,decreasing=FALSE),]
  
  
  
  # RENAME
  output2 <- plyr::rename(output, c('mt'='machine_type', 'opm'='operating_mode', 'pst'='program_state', 'al'='alarm_state',
                                    'pn'='program_number', 'spn' = 'subprogram_number',
                                    'stn'='step_number','cc'='cycle_counter', 't'='time'))
  if ((!(sl_name %in% colnames(output2))) || (!(ss_name %in% colnames(output2))) || (!(tool_name %in% colnames(output2)))){return(45)}
  names(output2)[names(output2)==sl_name]="spindle_load"
  names(output2)[names(output2)==ss_name]="spindle_speed"
  names(output2)[names(output2)==tool_name]="tool_number"
  
  
  
  # DATA CLEAN
  dtTmp <- dataClean(output2, machineType = machineType)
  
  
  
  # STATUS CHECK
    # 1. No Data Check
    if (nrow(dtTmp) <= 1){return(47)}
    
    
    # 2. Constant Cycle Counter Check
    uniqueCycles <- sort(unique(dtTmp$cycle_counter[order(dtTmp$time, decreasing=TRUE)]), decreasing = TRUE)
    if ((length(uniqueCycles) == 1) && (machineID != 92)){return(48)} # if 5 minutes working on the same item, return
  
  
  
  # PROGRAM NUMBER
  dtCurr <- dtTmp %>% filter(cycle_counter == uniqueCycles[1])
  
  if(!is.na(which(unique(dtCurr$program_number) %in% programNum == 1)[1])){
    programNum <- unique(dtCurr$program_number)[which(unique(dtCurr$program_number) %in% programNum == 1)]
    dtTmp <- dtTmp %>% filter(program_number == programNum)
  }else if(!is.na(which(unique(dtCurr$subprogram_number) %in% programNum == 1)[1])){
    programNum <- unique(dtCurr$subprogram_number)[which(unique(dtCurr$subprogram_number) %in% programNum == 1)]
    dtTmp <- dtTmp %>% filter(subprogram_number == programNum)
  }else{
    return(46)
  }
  
  if (length(programNum) > 1){return(46)}

  
  # REMOVE DUPLICATES
  dtTmp[['X_id']] = NULL
  dtTmp <- dtTmp[!duplicated(dtTmp),]

  




    
###********************************************** COMPUTE CURRENT SIGNAL **********************************************###  
  # ESTABLISH BLX MYSQL CONNECTION
    connB <- dbConnect(RMySQL::MySQL(), dbname = "KM", username = SQL_usr, password = SQL_pass, host = SQL_host, port = SQL_port)
    on.exit(DBI::dbDisconnect(connB))
    
    
    
  # READ CURRENT BLX
    hist <- DBI::dbGetQuery(connB, sqlInterpolate(connB,
                                                 "select * from KM.KM_BLX where companyID = ?companyID and projectID = ?projectID and machineID = ?machineID and programNum = ?programNum and spindleID = ?spindleID",
                                                 companyID = companyID, projectID = projectID, machineID = machineID, programNum = programNum, spindleID = spindleID))
   
    
    
  # DATA COMPELETENESS CHECK 
    if(nrow(hist) <= 1){return(51)}
  



    
    
    
###********************************************** AUTO EMPTY & ALARM **********************************************###  
  if ((uniqueCycles[1] != unique(hist$currCycle)) || (is.na(unique(hist$currCycle)[1]))){
  
    ## OUTLIER POINT ALARM
    m <- data.frame(lastStep = hist$step_number, lastSignal = hist$act, lastUL = hist$UL, lastLL = hist$LL)
    m <- m[complete.cases(m),]
    alarmIndUL <- which(m$lastSignal > m$lastUL)
    alarmIndLL <- which(m$lastSignal < m$lastLL)
    alarmInd <- c(alarmIndUL, alarmIndLL)
    
    if (!is.na(alarmInd[1])){
      
      for (j in 1:length(alarmInd)){
        
        # PREPARE ALARM INFO TO TRANSFER
        curr_ind <- alarmInd[j]#which(hist$step_number == curr_step)
        curr_step <- m$lastStep[curr_ind]
        alarm_time <- hist$time[curr_ind]
        alarm_signal <- hist$act[curr_ind]
        alarm_tool <- hist$tool_number[curr_ind]
        
        
        
        # PREPARE TRANSFER DATA
        avg <- sqrt((hist$LL[curr_ind] * hist$UL[curr_ind])/0.99)
        sigma <- hist$STD_act[curr_ind]; 
        sigma <- as.numeric(sigma)
        if (sigma == 1){next}
        step_number_manual <- hist$step_number_manual[curr_ind]
        pVal <- hist$pVal[curr_ind]
        
        
        
        # PREPARE LIST TO TRANSFER
        list2json = list(ver = "1",
                         companyid = as.numeric(companyID), 
                         projectid = as.numeric(projectID), 
                         data = data.frame(category = 1,
                                           machine = as.numeric(machineID), 
                                           spindle_id = as.numeric(spindleID), 
                                           tool_number = alarm_tool, 
                                           pval = ifelse(is.na(pVal),100,pVal),
                                           step_number_manual = step_number_manual, 
                                           K_value = (10^(alarm_signal) - 10^avg)/sigma, 
                                           signal = 10^(alarm_signal), 
                                           time = ifelse(is.na(alarm_time), Sys.time(), alarm_time),
                                           cycle_counter = unique(hist$currCycle)))
        
        
        
        # LIST TO JSON
        json2post <- jsonlite::toJSON(list2json, method = "C", auto_unbox = TRUE)
        
        
        
        # TRANSFER
        rst = fromJSON(postForm(uri = URL_Alarm, 
                                .opts = list(postfields = json2post, 
                                             httpheader = c('Content-Type' = 'application/json', Accept = 'application/json'))))
        
        
        
        # TRANSFER ERROR HANDLING
        if (rst[['ret']] != 1){
          write.table(list2json, file = paste0(local_path, "/alarm_error"), append = TRUE, col.names = TRUE)
        }

        
      } # END ALARM FOR
    } # END ALARM IF

    
    
    
  ## AFTER ALARM, EMPTY KM_BLX INSTANT INFO
    # conn <- dbConnect(RMySQL::MySQL(), dbname = "KM", username = SQL_usr, password = SQL_pass, host = SQL_host, port = SQL_port)
    # on.exit(DBI::dbDisconnect(conn))
    
    # COMPUTE LAST CYCLE DURATION
    if (length(unique(dtTmp$cycle_counter)) >= 3){
      
      lastTimeVec1 <- dtTmp$time[dtTmp$cycle_counter == uniqueCycles[2]]; cycleTime1 <- difftime(last(lastTimeVec1), first(lastTimeVec1), units = "secs")
      lastTimeVec2 <- dtTmp$time[dtTmp$cycle_counter == uniqueCycles[3]]; cycleTime2 <- difftime(last(lastTimeVec2), first(lastTimeVec2), units = "secs")
      
      cycleTime <- max(cycleTime1[[1]], cycleTime2[[1]])

      DBI::dbSendQuery(connB, sqlInterpolate(connB,
                                            "update KM.KM_BLX set time = NULL, currCycle = ?currCycle, cycleTime = ?cycleTime, act = NULL, tool_number = NULL, stopStep = NULL where companyID = ?companyID AND projectID = ?projectID AND machineID = ?machineID AND programNum = ?programNum AND spindleID = ?spindleID",
                                            currCycle = uniqueCycles[1], cycleTime = cycleTime, companyID = companyID, projectID = projectID, machineID = machineID, programNum = programNum, spindleID = spindleID))

      
    }else{

      DBI::dbSendQuery(connB, sqlInterpolate(connB,
                                            "update KM.KM_BLX set time = NULL, currCycle = ?currCycle, act = NULL, tool_number = NULL, stopStep = NULL where companyID = ?companyID AND projectID = ?projectID AND machineID = ?machineID AND programNum = ?programNum AND spindleID = ?spindleID",
                                            currCycle = uniqueCycles[1], companyID = companyID, projectID = projectID, machineID = machineID, programNum = programNum, spindleID = spindleID))

    }
    

    DBI::dbSendQuery(connB, sqlInterpolate(connB,
                                          "update KM.KM_BLX set act = 0, tool_number = 9999 where companyID = ?companyID AND projectID = ?projectID AND machineID = ?machineID AND programNum = ?programNum AND spindleID = ?spindleID AND step_number_manual = 1",
                                          companyID = companyID, projectID = projectID, machineID = machineID, programNum = programNum, spindleID = spindleID))
    
    
    
  }
  
  
  
  

  
  

    
###********************************************** CURRENT SIGNAL FILLING **********************************************###  

### (-) ALGORITHM FOR DETECTING ABNORMAL STEP NUMBER
    
  ## CURRENT dtCurr
    # current cycle dtCurr
    dtCurr <- dtTmp %>% filter(cycle_counter == uniqueCycles[1])
      
    # order dtCurr
    dtCurr <- dtCurr[order(dtCurr$time, decreasing = FALSE),]
      
    
  ## TAIL TOLERANCE LEVEL
    tolerance <- hist$step_number[order(hist$step_number, decreasing = TRUE)][ceiling(length(hist$step_number)*0.02)]
    
    
  ## CUT INITIAL ABNORMAL STEPS
    # ALL ABNORMAL
      # return if all initial step numbers abnormal
      if ((mean(dtCurr$step_number) >= tolerance) && (machineID != 92)){return(61)}
    
    # BOTH NORMAL & ABNORMAL
      # lag of step number
      label = dtCurr$step_number - lag(dtCurr$step_number)
      
      # automatically has a value if sudden fallen happens
      endInd = max(which(label < -mean(dtCurr$step_number)))
      
      # cut abnormal step numbers if part of dtCurr
      if (endInd > -10000){dtCurr <- dtCurr[-c(1:(endInd - 1)),]}
    
    
  ## HEDGE VOLATITLITY FOR NEWEST STEPS SPINDLE_LOAD VALUES
    dtCurr <- dtCurr %>%
      filter(step_number != max(dtCurr$step_number)) %>%
      filter(step_number != ifelse(is.na(unique(sort(dtCurr$step_number, decreasing = TRUE))[2]), 0, unique(sort(dtCurr$step_number, decreasing = TRUE))[2]))
    if (nrow(dtCurr) == 0){return(60)}
    
    
  ## COMPUTE CURRENT SIGNAL
    currSignal <- dtCurr %>%
      dplyr::group_by(step_number) %>%
      dplyr::summarise(cycle_counter = unique(cycle_counter),
                       time = first(time),
                       tool_number = first(tool_number),
                       feature = switch(featureType, "avg" = mean(spindle_load, trim = trimLevel, na.rm = TRUE),
                                        "max" = max(spindle_load, na.rm = TRUE))) %>%
      dplyr::mutate(feature = log10(feature)) %>%
      dplyr::mutate(feature = ifelse(feature < 0, 0, feature))
    
    
    
    
### (-) ALGORITHM FOR FIXATING LL == 0 STEPS
    
  ## HIST 0 STEPS INDEX (HEAD & TAIL)
    
    # thresholds for initial & ending phase
    start_threshold = hist$step_number[order(hist$step_number, decreasing = FALSE)][ceiling(length(hist$step_number)*0.2)]
    end_threshold = hist$step_number[order(hist$step_number, decreasing = TRUE)][ceiling(length(hist$step_number)*0.2)]
    
    # get which LL == 0
    zeroM1 = hist %>% filter(LL == 0, step_number <= start_threshold)
    zeroM2 = hist %>% filter(LL == 0, step_number >= end_threshold)
    
    # LL == 0 steps index
    zeroStep = c(zeroM1$step_number, zeroM2$step_number)
    ht0Ind = which(hist$step_number %in% zeroStep == 1)
    
    
  ## IF POTENTIAL HEAD & TAIL ZEROS EXIST
    if (length(ht0Ind) >= 2){
      
      ## THREE IFS
      if ((!is.na(zeroM1[['step_number']][1])) && (!is.na(zeroM2[['step_number']][1]))){
      # BOTH HEAD & TAIL ZEROS
        # get difference in 0 steps index
        d = abs(diff(ht0Ind))
        
        # get ending index of initial zeros
        tag1 = which(d > 5)[1]
        
        # get starting index of ending zeros
        tag2 = max(which(d > 5)) + 1
        
        
      # FIX HEAD ZEROS
        # compute without zero INITIAL steps
        tmp <- currSignal %>% filter(step_number > hist$step_number[ht0Ind[tag1]])
        
        # concatenate
        currSignal <- rbind(tmp, data.frame(step_number = c(hist$step_number[ht0Ind[1:tag1]]),
                                            cycle_counter = unique(currSignal$cycle_counter),
                                            time = first(currSignal$time),
                                            tool_number = first(currSignal$tool_number),
                                            feature = 0))
        # order
        currSignal = currSignal[order(currSignal$step_number),]
        
        
      # FIX TAIL ZEROS
        if (max(currSignal$step_number) >= tolerance){
          # compute without zero ENDING steps
          currSignal <- currSignal %>% filter(step_number < hist$step_number[ht0Ind[tag2]])
          
          # concatenate
          currSignal <- rbind(currSignal, data.frame(step_number = c(hist$step_number[ht0Ind[tag2:length(ht0Ind)]]),
                                                     cycle_counter = unique(currSignal$cycle_counter),
                                                     time = last(currSignal$time),
                                                     tool_number = last(currSignal$tool_number),
                                                     feature = 0))
          # order
          currSignal <- currSignal[order(currSignal$step_number),]
        }
        
        
        
        
      }else if(!is.na(zeroM1[['step_number']][1])){
      # ONLY HEAD
        # compute without zero INITIAL steps
        tmp <- currSignal %>% filter(step_number > hist$step_number[max(ht0Ind)])
        
        # concatenate
        currSignal <- rbind(tmp, data.frame(step_number = c(hist$step_number[1:length(ht0Ind)]),
                                            cycle_counter = unique(currSignal$cycle_counter),
                                            time = first(currSignal$time),
                                            tool_number = first(currSignal$tool_number),
                                            feature = 0))
        # order
        currSignal = currSignal[order(currSignal$step_number),]
        
        
      }else if(!is.na(zeroM2[['step_number']][1])){
      # ONLY TAIL
        # compute without zero ENDING steps
        tmp <- currSignal %>% filter(step_number > hist$step_number[min(ht0Ind)])
        
        # concatenate
        currSignal <- rbind(tmp, data.frame(step_number = c(hist$step_number[1:length(ht0Ind)]),
                                            cycle_counter = unique(currSignal$cycle_counter),
                                            time = first(currSignal$time),
                                            tool_number = first(currSignal$tool_number),
                                            feature = 0))
        # order
        currSignal = currSignal[order(currSignal$step_number),]
        
        
      }
      
    }
    
    currSignal$step_number
    
    
    
    
    
    
c=Sys.time()
###********************************************** UPDATE CURRENT SIGNAL **********************************************###  
  # FILTER NEW INCOMING SIGNAL
  tmp <- dplyr::left_join(hist, currSignal, by=c("step_number" = "step_number"))
  tmp$tool_number.y <- forbak(tmp$tool_number.y)
    
  # NOTHING TO UPDATE, RETURN
  if (length(which(is.na(tmp$feature) == 1)) == nrow(tmp)){return(62)}
    
  # DIVIDE INTO NA & !NA SIGNALS
  tmp1 <- tmp[!is.na(tmp$feature),]
  tmp2 <- tmp[is.na(tmp$feature),] %>% filter(step_number <= max(currSignal$step_number))
  
  # SELECT STEPS FOR !NA SIGNALS
  indDiff <- which((abs(ifelse(is.na(tmp1$act),-1,tmp1$act) - tmp1$feature) > 0.00001) == 1)
  targetSteps <- tmp1$step_number[indDiff]
    
  # COMBINE NA & !NA STEPS
  targetSteps <- c(targetSteps, unique(tmp2$step_number))
  targetSteps <- targetSteps[order(targetSteps)]
    
  # FILTER TO NEW INCOMING STEPS
  curr_ind <- ifelse(is.na(which(!is.na(tmp$act) == 1)[1]), 1, max(which(!is.na(tmp$act) == 1)))
  targetSteps <- targetSteps[which(targetSteps >= tmp$step_number[curr_ind])]
    
  # SET CURRENT STEP
  stopStep <- max(currSignal$step_number)
  

  # UPDATE THOSE SIGNALS
  if (!is.na(targetSteps[1])){
    
    # UPDATE stopStep
    DBI::dbSendQuery(connB, sqlInterpolate(connB,
                                          "update KM.KM_BLX set stopStep = ?stopStep where companyID = ?companyID AND projectID = ?projectID AND machineID = ?machineID AND programNum = ?programNum AND spindleID = ?spindleID AND step_number <= ?stopStep2",
                                          stopStep = stopStep, companyID = companyID, projectID = projectID, machineID = machineID, programNum = programNum, spindleID = spindleID, stopStep2 = stopStep))

    # UPDATE EACH NEW STEP SIGNAL
    for (j in 1:length(targetSteps)){
      
      # CURRENT UPDATE-STEP
      curr_step <- targetSteps[j]
      
      
      # ONLY UPDATE UN-UPDATED ONES
      if (is.na(hist$act[which(hist$step_number == curr_step)])){
        
        # PAIR UPDATING
        if (!is.na(currSignal$feature[currSignal$step_number == targetSteps[j]][1])){
          # ACTUAL
          curr_tool <- tmp$tool_number.y[which(tmp$step_number == curr_step)]
          curr_Sind <- which(currSignal$step_number == curr_step)
          new_time <- currSignal$time[curr_Sind]
          new_signal <- currSignal$feature[curr_Sind]
          

          DBI::dbSendQuery(connB, sqlInterpolate(connB,
                                                "update KM.KM_BLX set time = ?time, act = ?signal, tool_number = ?tool where companyID = ?companyID AND projectID = ?projectID AND machineID = ?machineID AND programNum = ?programNum AND spindleID = ?spindleID AND step_number = ?stepNum",
                                                time = as.character(new_time), signal = new_signal, tool = curr_tool, companyID = companyID, projectID = projectID, machineID = machineID, programNum = programNum, spindleID = spindleID, stepNum = curr_step))

        }else{
          
          # APPROXIMATION
          curr_tool <- tmp$tool_number.y[which(tmp$step_number == curr_step)]
          curr_Hind <- which(hist$step_number == curr_step)
          new_signal <- sum(hist$LL[curr_Hind], hist$UL[curr_Hind])/2
          
          DBI::dbSendQuery(connB, sqlInterpolate(connB,
                                                "update KM.KM_BLX set act = ?signal, tool_number = ?tool where companyID = ?companyID AND projectID = ?projectID AND machineID = ?machineID AND programNum = ?programNum AND spindleID = ?spindleID AND step_number = ?stepNum",
                                                signal = new_signal, tool = curr_tool, companyID = companyID, projectID = projectID, machineID = machineID, programNum = programNum, spindleID = spindleID, stepNum = curr_step))

        }
      }
      
    }
    
    
    
  } # END OUTER if LOOP
 
  #killDbConnections() 
b=Sys.time()
return(c(b-c,b-a))
}

