library(DBI)
library(plyr)
library(Rserve)
library(dplyr)
library(tidyr)
library(lubridate)
library(RMySQL)
library(RCurl)
library(jsonlite)
library(RMongo)
library(ggplot2)
# library(plotly)
source("/home/cal/Documents/methodBLX/locf.R")



### GLOBAL VARIABLE
SQL_usr <<- "root"
SQL_pass <<- "Siger321"
SQL_host <<- "172.16.33.223"
SQL_port <<- 3306

SQL_huandao_usr <<- "root"
SQL_huandao_pass <<- "Siger321"
SQL_huandao_host <<- "172.16.33.223"
SQL_huandao_port <<- 3306

local_path <<- "/home/cal/Documents/methodBLX/cache"
pic_path <<- "/home/gz2"



########################################################################################################################################################################################################
### 
###   Main Functions
### 
########################################################################################################################################################################################################
computeBLX <- function(rulesAddr){

  # READ-IN RULES & DATA.FRAME IT
  tmp <- fromJSON(rulesAddr)
  ruleVal <- data.frame(startTime = tmp$StartTime, 
                        endTime = tmp$EndTime, 
                        companyID = tmp$companyID, 
                        projectID = tmp$projectID, 
                        machineID = tmp$machineID, 
                        spindleID = tmp$spindleID, 
                        sl_name = paste0("sl", tmp$spindleID), 
                        ss_name = paste0("ss", tmp$spindleID), 
                        tn_name = paste0("tn", tmp$spindleID),
                        programNum = tmp$PN)
  

  
  # API
  urlAll = read.table(paste0(local_path, "/ipAddress"))
  URL_touchBLX <- as.character(urlAll$V2[urlAll$V1 == "urlBLX"])
  URL_trainBLX <- getDNSIP(touchURL = URL_touchBLX, typeString = "/Api/convertDB/getDBDetails", paramDF = data.frame(companyID = ruleVal$companyID, projectID = ruleVal$projectID))
  

      
  # PREPARE LIST TO TRANSFER
  list2json = list(ver = "1",
                   companyid = ruleVal$companyID, 
                   projectid = ruleVal$projectID, 
                   data = data.frame(MachineID = ruleVal$machineID, Starttime = ruleVal$startTime, Endtime = ruleVal$endTime))
    
  
  
  # LIST TO JSON
  json2post <- jsonlite::toJSON(list2json,method = "C", auto_unbox = TRUE)

  
  
  # JSON TO TRANSFER
  recData <- RCurl::postForm(uri = URL_trainBLX,
                             .opts = list(postfields = json2post, httpheader = c('Content-Type' = 'application/json', Accept = 'application/json')))
 
  
  
  # GET DATA
  rst <- fromJSON(recData)
  output <- rst$data
  
 
  
  # RETURN IF NO DATA | CONNECTION FAILED
  if (rst[[1]] != 1){return(toJSON(list(ret = rst[[1]]), auto_unbox=TRUE))}
  if (is.null(nrow(output))){return(toJSON(list(ret = 33), auto_unbox=TRUE))}
  if (nrow(output) == 0){return(toJSON(list(ret = 44), auto_unbox=TRUE))}
  
  
  
  # RENAME
  output2 <- plyr::rename(output, c('mt'='machine_type', 'opm'='operating_mode', 'pst'='program_state', 'al'='alarm_state',
                                    'pn'='program_number', 'spn' = 'subprogram_number',
                                    'stn'='step_number','cc'='cycle_counter', 't'='time'))
  if ((!(ruleVal$sl_name %in% colnames(output2))) || (!(ruleVal$ss_name %in% colnames(output2))) || (!(ruleVal$tn_name %in% colnames(output2)))){return(44)}
  names(output2)[names(output2) == ruleVal$sl_name]="spindle_load"
  names(output2)[names(output2) == ruleVal$ss_name]="spindle_speed"
  names(output2)[names(output2) == ruleVal$tn_name]="tool_number"
  

  
  # CLEAN DATA
  machineType <- ruleVal[['companyID']]
  if (!machineType %in% c(36,39)){machineType = "jc"}
  dt <- dataClean(output2, machineType = machineType)
  
  
  
  # PROGRAM NUMBER
  if(!is.na(which(unique(dt$program_number) %in% ruleVal[['programNum']] == 1)[1])){
    programNum <- unique(dt$program_number)[which(unique(dt$program_number) %in% ruleVal[['programNum']] == 1)]
    dt <- dt %>% filter(program_number == programNum)
  }else if(!is.na(which(unique(dt$subprogram_number) %in% ruleVal[['programNum']] == 1)[1])){
    programNum <- unique(dt$subprogram_number)[which(unique(dt$subprogram_number) %in% ruleVal[['programNum']] == 1)]
    dt <- dt %>% filter(subprogram_number == programNum)
  }else{
    return(toJSON(list(ret = 46), auto_unbox=TRUE))
  }
  if (nrow(dt) == 0){return(toJSON(list(ret = 44), auto_unbox=TRUE))}
 
  
  
  # REMOVE DUPLICATES
  dt[['_Id']] = NULL
  dt = dt[!duplicated(dt),]
  
  
  
  # TRAIN BLX 
  coordBLX <- trainBLX(dt = dt, ruleVal = ruleVal)
  
  
  
  # SPECIFIC MODIFICATION FOR 92
  # if (ruleVal$machineID == 92){
  #   coordBLX[['step_number']] = seq(nrow(coordBLX))
  # }
  
  
  # ggplot(coordBLX, aes(x=step_number)) +
  #   geom_step(aes(y=LL), color="blue") +
  #   geom_step(aes(y=UL), color="red")
  
  
  
  
  
  
  # LIST TO JSON
  x = list(ret=jsonlite::unbox(1), step = coordBLX$step_number, lower = coordBLX$LL, upper = coordBLX$UL)
  tmp = jsonlite::toJSON(x)
  
  
  return(tmp)
  
}
  
  
  

plotHuandao <- function(rulesAddr){
  
  # READ-IN RULES & DATA.FRAME IT ------------
  tmp <- fromJSON(rulesAddr)
  ruleVal <- data.frame(startTime = as.POSIXct(as.numeric(tmp$StartTime), origin="1970-01-01"), 
                        endTime = as.POSIXct(as.numeric(tmp$EndTime), origin="1970-01-01"), 
                        companyID = tmp$companyID, projectID = tmp$projectID, machineID = tmp$machineID, spindleID = tmp$spindleID, sl_name = paste0("sl", tmp$spindleID), toolNum = tmp$toolLocationNum, programNum = tmp$PN)
  

  
  # ESTABLISH TREND MYSQL CONNECTION
  connT <- dbConnect(RMySQL::MySQL(), dbname = "KM", username = SQL_usr, password = SQL_pass, host = SQL_host, port = SQL_port)
  on.exit(DBI::dbDisconnect(connT))
  
  
  
  # READ KM_Trend RECORDS
  hist <- DBI::dbGetQuery(connT, sqlInterpolate(connT,
                                               "select * from KM.KM_Trend where companyID = ?companyID AND projectID = ?projectID AND machineID = ?machineID AND programNum = ?programNum AND spindleID = ?spindleID AND toolNum = ?toolNum AND time >= ?startTime AND time <= ?endTime",
                                               companyID = ruleVal$companyID, projectID = ruleVal$projectID, machineID = ruleVal$machineID, programNum = ruleVal$programNum, spindleID = ruleVal$spindleID, toolNum = ruleVal$toolNum, startTime = as.character(ruleVal$startTime), endTime = as.character(ruleVal$endTime)))
  hist <- hist[order(hist$time, decreasing = FALSE),]

  
  
  
  # ERROR
  if (nrow(hist) == 0){return(toJSON(list(ret=44), auto_unbox=TRUE))}
  
  
  
  # ESTABLISH HUANDAO MYSQL CONNECTION
  connH <- dbConnect(RMySQL::MySQL(), dbname = "siger", username = SQL_huandao_usr, password = SQL_huandao_pass, host = SQL_huandao_host, port = SQL_huandao_port)
  on.exit(DBI::dbDisconnect(connH))
  
  
  
  # READ HUANDAO RECORDS
  tmp <- DBI::dbGetQuery(connH, sqlInterpolate(connH,
                                              "select * from siger_tool_change_record where projectid = ?projectID AND machine_id = ?machineID AND mainaxis = ?spindleID AND tool_no = ?toolNum AND change_time >= ?startTime AND change_time <= ?endTime",
                                              projectID = ruleVal$projectID, machineID = ruleVal$machineID, spindleID = ruleVal$spindleID, toolNum = ruleVal$toolNum, startTime = as.numeric(tmp$StartTime), endTime = as.numeric(tmp$EndTime)))
  huandao <- as.POSIXct(as.numeric(tmp$change_time), origin = "1970-01-01")
  
  
  
  # PLOT
  path <- plotOneTime(hist = hist, huandao = huandao, ruleVal = ruleVal)
  fullpath <- paste0(pic_path, "/", path)

  
  
  # RST
  rst <- list(ret = 1, path = path)
  

  return(toJSON(rst, auto_unbox = TRUE))
}




computeBOXPLOT <- function(rulesAddr){
  
  # READ-IN RULES & DATA.FRAME IT
  tmp <- fromJSON(rulesAddr)
  ruleVal <- list(startTime = as.POSIXct(as.numeric(tmp$StartTime), origin="1970-01-01"), 
                  endTime = as.POSIXct(as.numeric(tmp$EndTime), origin="1970-01-01"), 
                  companyID = tmp$companyID, projectID = tmp$projectID, machineID = tmp$machineID, spindleID = tmp$spindleID, sl_name = paste0("sl", tmp$spindleID), toolNum = tmp$toolLocationNum, programNum = tmp$PN)
  
  
  
  # COMPUTE SUMMARY STATISTICS
  rst <- sapply(ruleVal$toolNum, computeStatistics, ruleVal = ruleVal)
  if (44 %in% rst){return(toJSON(list(ret=44), auto_unbox=TRUE))}
  
  
  # LIST TO JSON
  tmp = jsonlite::toJSON(list(ret=1, data=t(rst)), auto_unbox = TRUE)
  
  
  
  return(tmp)
}




computeSpeed <- function(rulesAddr){

  ### () READ-IN RULES & DATA.FRAME IT
  
    # READ-IN RULES
    tmp <- fromJSON(rulesAddr)
  
    # WRITE RULES TO HARD-DRIVE
    ruleWrite <- write.csv(tmp, file=paste0(local_path, "/tmpSSRule.csv"))
  
    # READ HARD-DRIVE RULES
    ruleRead <- read.csv(file = paste0(local_path, "/tmpSSRule.csv"))
  
    # DATA.FRAME RULES
    tmp <- as.data.frame(ruleRead)
  
  
  
  ### () UN-PUZZLE RULES
    
    # DELETE NOISE COLUMNS
    tmpRule <- as.vector(tmp[,3:ncol(tmp)])
  
    
    # IF INPUT RULE IS WRONG SIZE
    if((ncol(tmpRule) %% 8) != 0){return(toJSON(list(ret=22), auto_unbox=TRUE))}
  
    
    # DATA.FRAME RULES
    rules <- as.data.frame(t(matrix(tmpRule, 8, ncol(tmpRule)/8)))
  
    
    # RENAME COLUMN NAMES
    colnames(rules) <- c("companyID", "projectID", "spindleID", "machineID", "startTime", "endTime", "toolNum", "PN")
  
    
    # CONVERT TO WORKABLE RULEDATA
    ruleData <- list(companyID = as.numeric(unique(rules$companyID)),
                     projectID = as.numeric(unique(rules$projectID)),
                     machineID = as.numeric(unique(rules$machineID)),
                     programNum = as.numeric(unique(rules$PN)),
                     startTime = as.numeric(unique(rules$startTime)),
                     endTime = as.numeric(unique(rules$endTime)))
  
  
 
  ### () GET DATA
    
    # API
    urlAll = read.table(paste0(local_path, "/ipAddress"))
    URL_touchBLX <- as.character(urlAll$V2[urlAll$V1 == "urlBLX"])
    URL_trainBLX <- getDNSIP(touchURL = URL_touchBLX, typeString = "/Api/convertDB/getDBDetails", paramDF = data.frame(companyID = ruleData$companyID, projectID = ruleData$projectID))
    
    
    # PREPARE LIST TO TRANSFER
    list2json = list(ver = "1",
                     companyid = ruleData$companyID, 
                     projectid = ruleData$projectID, 
                     data = data.frame(MachineID = ruleData$machineID, Starttime = ruleData$startTime, Endtime = ruleData$endTime, Program = ruleData$programNum))
    
    
    # LIST TO JSON
    json2post <- jsonlite::toJSON(list2json, method = "C", auto_unbox = TRUE)
    

    # JSON TO TRANSFER
    recData <- RCurl::postForm(uri = URL_trainBLX,
                               .opts = list(postfields = json2post, httpheader = c('Content-Type' = 'application/json', Accept = 'application/json')))
    
    
    # GET DATA
    rst <- fromJSON(recData)
    output <- rst$data

 
    # RETURN IF NO DATA
    if (is.null(nrow(output))){return(toJSON(list(ret = 44), auto_unbox=TRUE))}
    if (nrow(output) < 5){return(toJSON(list(ret = 44), auto_unbox=TRUE))}
    
    
    # RENAME
    output2 <- plyr::rename(output, c('mt'='machine_type', 'opm'='operating_mode', 'pst'='program_state', 'al'='alarm_state',
                                      'pn'='program_number', 'spn' = 'subprogram_number',
                                      'stn'='step_number','cc'='cycle_counter', 't'='time'))
    
  
  ### () COMPUTATION
    
    # INITIAL EMPTY RESULT
    knivesStat = list(ret = 0, data = list())
  
    
    # COMPUTE BASED ON EACH SPINDLE
    for (i in 1:length(unique(rules$spindleID))){
      
      # CURRENT SPINDLE i RULEVAL
      tmp <- rules %>% filter(spindleID == unique(rules$spindleID)[[i]])
      curr_ruleVal <- data.frame(companyID = as.numeric(unique(tmp$companyID)),
                                 projectID = as.numeric(unique(tmp$projectID)),
                                 machineID = as.numeric(unique(tmp$machineID)),
                                 spindleID = as.numeric(unique(tmp$spindleID)),
                                 programNum = as.numeric(unique(tmp$PN)))
      
      
      # RENAME
      dtTmp <- output2
      if(paste0("sl", curr_ruleVal$spindleID) %in% colnames(dtTmp)){
        names(dtTmp)[names(dtTmp)==paste0("sl", curr_ruleVal$spindleID)]="spindle_load"
        names(dtTmp)[names(dtTmp)==paste0("ss", curr_ruleVal$spindleID)]="spindle_speed"
        names(dtTmp)[names(dtTmp)==paste0("tn", curr_ruleVal$spindleID)]="tool_number"
      }else{
        return(toJSON(list(ret=88), auto_unbox=TRUE))
      }
      

      # DATA CLEAN
        
        # GET CYCLE COUNTER
        currCycle <- plyr::count(dtTmp$cycle_counter)$x[which(plyr::count(dtTmp$cycle_counter)$freq == max(plyr::count(dtTmp[dtTmp$cycle_counter!=0,]$cycle_counter)$freq))]
        dtTmp$cycle_counter[which(dtTmp$cycle_counter == 0)] <- currCycle
        
        # DATA CLEAN
        dt <- dtTmp %>% 
          filter(cycle_counter == currCycle)
        # dt <- dataClean(dt) %>%
        #   filter(dt$cycle_counter == count(dt$cycle_counter)[which(count(dt$cycle_counter)$freq == max(count(dt$cycle_counter)$freq))])
      
      
      
      # RETURN IF NO DATA
      if (nrow(dt) < 1){return(toJSON(list(ret=47), auto_unbox=TRUE))}
      
      
      # CHOOSE KNIVES TO ANALYZE
      dtTool <- dt %>% 
        dplyr::group_by(tool_number) %>% 
        dplyr::filter(length(spindle_load) >= 3)
      knives <- sort(as.numeric(unique(dtTool$tool_number)))
      
    
      # # PROGRAM NUMBER
      # if(!is.na(which(unique(dt$program_number) %in% curr_ruleVal[['programNum']] == 1)[1])){
      #   programNum <- unique(dt$program_number)[which(unique(dt$program_number) %in% curr_ruleVal[['programNum']] == 1)]
      #   dt <- dt %>% filter(program_number == programNum)
      # }else if(!is.na(which(unique(dt$subprogram_number) %in% curr_ruleVal[['programNum']] == 1)[1])){
      #   programNum <- unique(dt$subprogram_number)[which(unique(dt$subprogram_number) %in% curr_ruleVal[['programNum']] == 1)]
      #   dt <- dt %>% filter(subprogram_number == programNum)
      # }else{
      #   return(toJSON(list(ret=46), auto_unbox=TRUE))
      # }
      
    
      # SET PATH
      path1 = paste0("/OneTime/", Sys.Date(), "_", as.numeric(Sys.time()), "_1.jpeg")
      path2 = paste0("/OneTime/", Sys.Date(), "_", as.numeric(Sys.time()), "_2.jpeg")
      path3 = paste0("/OneTime/", Sys.Date(), "_", as.numeric(Sys.time()), "_3.jpeg")
      
      fullpath1 = paste0(pic_path, as.character(path1))
      fullpath2 = paste0(pic_path, as.character(path2))
      fullpath3 = paste0(pic_path, as.character(path3))
      
      
      # COMPUTATION
      knivesStat[['data']][[i]] <- list(path = list(path1 = as.character(path1),
                                                    path2 = as.character(path2),
                                                    path3 = as.character(path3)),
                                        t(sapply(knives, computeSS, dt=dt, spindleID = curr_ruleVal$spindleID)))
      knivesStat[['ret']][[i]] <- 1        
      
      
      # PLOT
      dtPlot <- dt[order(dt$time, decreasing = FALSE),]
      
      
      # MODIFY SEVERAL COLUMNS
      dtPlot$time <- ymd_hms(dtPlot$time)
      dtPlot$spindle_load <- dtPlot$spindle_load/10000
      dtPlot$spindle_speed <- dtPlot$spindle_speed/10000
      dtPlot$fre <- dtPlot$fre/10000
      
      
      # PLOT SPINDLE_LOAD
      ggplot(dtPlot, aes(x=as.numeric(time-time[1]), y=spindle_load)) +
        geom_line(color = "gray", size=1, alpha=0.7) +
        geom_point(aes(color = as.factor(tool_number)), size=5.5, alpha=0.7) +
        scale_y_continuous(labels=scales::percent) +
        labs(colour = "刀具号") +
        xlab("时间(s)") +
        ylab("负载") +
        theme(legend.title = element_text(size=30),
              legend.text = element_text(size=30),
              axis.title.x = element_text(size=30),axis.text.x = element_text(size=30), 
              axis.title.y = element_text(size=30),axis.text.y = element_text(size=30),
              panel.background = element_rect(fill="#F4F4F4",color = "#F4F4F4"),
              plot.background = element_rect(fill = "#F4F4F4", color = "#F4F4F4"))
      
      ggsave(filename=fullpath1, device = "jpeg", width = 70, height = 20, dpi = 150, limitsize = FALSE, units = "cm")
      
      
      # PLOT SPINDLE_SPEED
      ggplot(dtPlot, aes(x=as.numeric(time-time[1]), y=spindle_speed)) +
        geom_line(color = "gray", size=1, alpha=0.7) +
        geom_point(aes(color = as.factor(tool_number)), size=5.5, alpha=0.7) +
        scale_y_continuous(labels=scales::percent) +
        labs(colour = "刀具号") +
        xlab("时间(s)") +
        ylab("转速") +
        theme(legend.title = element_text(size=30),
              legend.text = element_text(size=30),
              axis.title.x = element_text(size=30),axis.text.x = element_text(size=30), 
              axis.title.y = element_text(size=30),axis.text.y = element_text(size=30),
              panel.background = element_rect(fill="#F4F4F4",color = "#F4F4F4"),
              plot.background = element_rect(fill = "#F4F4F4", color = "#F4F4F4"))
      
      ggsave(filename=fullpath2, device = "jpeg", width = 70, height = 20, dpi = 150, limitsize = FALSE, units = "cm")
      
      
      # PLOT FEED_RATE
      ggplot(dtPlot, aes(x=as.numeric(time-time[1]), y=fre)) +
        geom_line(color = "gray", size=1, alpha=0.7) +
        geom_point(aes(color = as.factor(tool_number)), size=5.5, alpha=0.7) +
        # geom_point(aes(y=spindle_speed * coeffi, color=as.factor(tool_number)), size=5.5, alpha=1) +
        # geom_line(aes(y=spindle_speed * coeffi), color="grey", size=1, alpha = 0.7) +
        scale_y_continuous(labels=scales::percent) +
        labs(colour = "刀具号") +
        xlab("时间(s)") +
        ylab("进给量") +
        theme(legend.title = element_text(size=30),
              legend.text = element_text(size=30),
              axis.title.x = element_text(size=30),axis.text.x = element_text(size=30), 
              axis.title.y = element_text(size=30),axis.text.y = element_text(size=30),
              panel.background = element_rect(fill="#F4F4F4",color = "#F4F4F4"),
              plot.background = element_rect(fill = "#F4F4F4", color = "#F4F4F4"))
      
      ggsave(filename=fullpath3, device = "jpeg", width = 70, height = 20, dpi = 150, limitsize = FALSE, units = "cm")
    }
  
  
  
  ### () RETURN
  tmp <- toJSON(knivesStat, auto_unbox = TRUE)
  
  
  return(tmp)
}
  



getProgramNum <- function(rulesAddr){
  
  ### () READ-IN RULES & DATA.FRAME IT
  
    # READ-IN RULES
    tmp <- fromJSON(rulesAddr)
    
    # WRITE RULES TO HARD-DRIVE
    ruleWrite <- write.csv(tmp, file=paste0(local_path, "/tmpSSRule.csv"))
    
    # READ HARD-DRIVE RULES
    ruleRead <- read.csv(file = paste0(local_path, "/tmpSSRule.csv"))
    
    # DATA.FRAME RULES
    tmp <- as.data.frame(ruleRead)
  
  
  
  ### () UN-PUZZLE RULES
  
    # DELETE NOISE COLUMNS
    tmpRule <- as.vector(tmp[,3:ncol(tmp)])
    
    
    # IF INPUT RULE IS WRONG SIZE
    # if((ncol(tmpRule) %% 8) != 0){return(toJSON(list(ret=22), auto_unbox=TRUE))}
    
    
    # DATA.FRAME RULES
    # rules <- as.data.frame(t(matrix(tmpRule, 8, ncol(tmpRule)/8)))
    rules <- as.data.frame(tmpRule)
    
    
    # RENAME COLUMN NAMES
    colnames(rules) <- c("companyID", "projectID", "machineID", "spindleID", "startTime", "endTime")
    
    
    # CONVERT TO WORKABLE RULEDATA
    ruleData <- list(companyID = as.numeric(unique(rules$companyID)),
                     projectID = as.numeric(unique(rules$projectID)),
                     machineID = as.numeric(unique(rules$machineID)),
                     startTime = as.numeric(unique(rules$startTime)),
                     endTime = as.numeric(unique(rules$endTime)))
  
  
  ### ()  GET PROGRAM NUMBER
    
    # ESTABLISH KM MYSQL CONNECTION
    connT <- dbConnect(RMySQL::MySQL(), dbname = "KM", username = SQL_usr, password = SQL_pass, host = SQL_host, port = SQL_port)
    on.exit(DBI::dbDisconnect(connT))
    
    
    # GET CORRESEPONDING PROGRAM NUMBER
    allProgramNum <- DBI::dbGetQuery(connT, sqlInterpolate(connT,
                                                  "select programNum from KM.KM_Trend where companyID = ?companyID AND projectID = ?projectID AND machineID = ?machineID AND time >= ?startTime AND time <= ?endTime",
                                                  companyID = ruleData$companyID, projectID = ruleData$projectID, machineID = ruleData$machineID, startTime = as.character(as.POSIXct(ruleData$startTime, origin = "1970-01-01")), endTime = as.character(as.POSIXct(ruleData$endTime, origin = "1970-01-01"))))
    
    
    # ERROR
    if ((nrow(allProgramNum) == 0) | is.na(nrow(allProgramNum)[1])){return(toJSON(list(ret=44), auto_unbox=TRUE))}
    
    
    # RETURN PROGRAM NUMBER
    tmp <- list(ret = 1, data = list(programNum = as.array(unique(allProgramNum[['programNum']]))))
    rst <- toJSON(tmp, auto_unbox = TRUE)
    
    return(rst)
}




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
      
    }else if (machineType == 39){
      
      dt2 <- dt %>% 
        filter(program_state == 3,
               operating_mode == 1,
               abs(spindle_speed - lag(spindle_speed)) < 1) %>%
        filter(abs(spindle_speed - lag(spindle_speed)) < 1,
               spindle_load > 1,
               spindle_speed > 0) %>%
        filter(abs(spindle_speed - lag(spindle_speed)) < 1)
        
    }
  
  return(dt2)
}
  
  


getDNSIP <- function(touchURL, typeString, paramDF){
  
  # PREPARE LIST TO TRANSFER
  list2json = list(ver = "1",
                   companyid = paramDF$companyID, 
                   projectid = paramDF$projectID)
  
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




trainBLX <- function(dt, ruleVal, machineType = "jc"){

  
  # SET UP RAW DATASET
  dt <- dt %>% select(cycle_counter, step_number, spindle_load, time) %>% as.data.frame()
  
  
  
  # TRAIN BLX VALUES BASED ON MACHINE TYPE
  #if (machineType == "jc"){
        # SETTINGS
        trimLevel = 0.1
        max_sig <- 0.1; #max_sig <- ruleVal$max_sig_level
        min_sig <- 0.1; #min_sig <- ruleVal$min_sig_level
    
    
    
        # EXTRACT SUMMARISE TABLE
        tmp <- dt %>% 
          dplyr::group_by(cycle_counter, step_number) %>%
          dplyr::summarise(time = first(time), feature = max(spindle_load, trim=trimLevel, na.rm = TRUE)) %>%
          arrange(cycle_counter, step_number)

        
        
        # SET BLX LIMITS
        rst <- tmp %>% filter(feature > 0) %>%
               dplyr::group_by(step_number) %>%
               dplyr::summarise(ll = max(qnorm(0.0013, mean(feature), sd(feature), lower.tail = TRUE),
                                  (1-min_sig)*max(min(feature),2/(1-min_sig))),
                         ul = min(qnorm(0.0013, mean(feature), sd(feature), lower.tail = FALSE),
                                  (1+max_sig)*max(feature)),
                         LL_act = min(feature, na.rm=TRUE),
                         UL_act = max(feature, na.rm=TRUE),
                         LL = log10(ifelse(LL_act < 1, 1, LL_act)) * (1-min_sig),
                         UL = log10(ifelse(UL_act < 1, 3, UL_act)) * (1+max_sig),
                         #LL = log(ifelse(min(feature, na.rm=TRUE) < 1, 1, min(feature, na.rm=TRUE)) * (1 - min_sig)),
                         #UL = log(ifelse(max(feature, na.rm=TRUE) < 1, 3, max(feature, na.rm=TRUE)) * (1 + max_sig)),
                         std = ifelse(length(unique(feature)) == 1, 500000, sd(feature)),
                         pVal = ifelse(((length(unique(feature)) <= 1) | (length(feature) < 3)), NA, shapiro.test(feature)$p.value)) %>%
               #na.omit() %>%
               arrange(as.numeric(step_number))
        
        rst <- as.data.frame(rst)
        
        rst$LL[1] = log10(1) * (1-min_sig)
        rst$UL[1] = log10(3) * (1+max_sig)
      
    
  #}else if (machineType == "zc"){
    
    
    
    
    
    
  #}
  
  
  # INITIALIZATION
  companyID <- ruleVal$companyID
  projectID <- ruleVal$projectID
  machineID <- ruleVal$machineID
  spindleID <- ruleVal$spindleID
  programNum <- ruleVal$programNum
  
  
  
  # GET VALUES TO INSERT
  step_number <- rst$step_number
  step_number_manual <- seq_along(rst$step_number)
  UL <- rst$UL
  LL <- rst$LL 
  UL_act <- rst$UL_act
  LL_act <- rst$LL_act
  STD_act <- rst$std
  pVal <- rst$pVal
  
  
  
  # DATA.FRAME VALUES
  values <- data.frame(companyID=companyID, projectID=projectID, machineID=machineID, programNum = programNum, spindleID = spindleID,
                       step_number=step_number, step_number_manual=step_number_manual, LL=LL, UL=UL, LL_act = LL_act, UL_act = UL_act, STD_act=STD_act, pVal=pVal, time=NA, currCycle=NA, act=NA, tool_number=NA)
  
  
  
  # SET UP SQL CONNECTION
  connB <- dbConnect(RMySQL::MySQL(), dbname = "KM", username = SQL_usr, password = SQL_pass, host = SQL_host, port = SQL_port)
  on.exit(DBI::dbDisconnect(connB))
  
  
  
  # WRITE & SEND QUERY
  query <- sqlInterpolate(connB, 
                          "delete from KM.KM_BLX where companyID = ?companyID AND projectID = ?projectID AND machineID = ?machineID AND programNum = ?programNum AND spindleID = ?spindleID", 
                          companyID = companyID, projectID = projectID, machineID = machineID, programNum = programNum, spindleID = spindleID)
  dbSendQuery(connB, query)
    
  
  
  # UPDATE BLX 
  dbWriteTable(connB, "KM_BLX", values, overwrite = FALSE, append = TRUE, row.names = FALSE)
  
  
  
  return(rst)
}




plotOneTime <- function(hist, huandao, ruleVal, ruleDay = 7){

  # FORMAT & ORDER BY TIME
  hist$time = ymd_hms(hist$time, tz="Asia/Shanghai")
  hist = hist[order(hist$time, decreasing=FALSE),]
  
  
  
  # SET X AXIS
  hist$cycle_counter_manual = seq(nrow(hist))
  
  
  
  # SET AXIS STEP
  max_cc = max(hist$cycle_counter_manual)
  by_interval = ceiling(max_cc/10)
  
  
  
  # SCALING
  hist$feature = hist$feature/10000
  
  
  
  # CONVERT HUANDAO TIME POINTS TO CYCLE POINTS
  if (!is.na(huandao[1])){
    huandaoLoc = 0
    
    for (i in 1:length(huandao)){
      huandaoLoc <- c(huandaoLoc, which(hist$time >= huandao[i])[1])
    }
    
    huandaoLoc <- huandaoLoc[2:length(huandaoLoc)]
    huandaoLoc <- data.frame(huandao = huandaoLoc[which(!is.na(huandaoLoc))])
    
  }else{
    huandaoLoc = data.frame(huandao=1)
    
  }
  
  huandaoTime = hist$time[huandaoLoc$huandao]
  
  
  
  # DROP THE FIRST POINT AFTER HUANDAO
  hist = hist[-(unique(huandaoLoc$huandao)+1),]
  
  
  
  # SET PLOTTING DIRECTORY
  path = paste0("/OneTime/", Sys.Date(), "_", as.numeric(Sys.time()), "_", ruleVal$toolNum)
  fullpath = paste0(pic_path, as.character(path), ".jpeg")
  path2return = paste0(as.character(path), ".jpeg")
  
  
  
  # PLOT
  if (nrow(hist) > ruleDay * 3){
    
    # CREATE TWO CATEGORIES: 1. POINTS; 2. HUANDAO SEGMENT
    hist <- hist %>% mutate(point_subgroup = 0,
                            huandaoRec = 0)
    
    
    
    # SET HUANDAO SEGMENTS
    hist$huandaoRec[huandaoLoc$huandao] = 1
    hist$huandaoRec = cumsum(hist$huandaoRec)
    
    
    
    # SET UPPER & LOWER LIMITS FOR EACH SEGMENT
    hist <- hist %>% group_by(huandaoRec) %>%
      mutate(LL = max(0, mean(feature[1:min(length(feature), ruleDay)]) - 3*sd(feature[1:min(length(feature), ruleDay)])),
             UL = mean(feature[1:min(length(feature), ruleDay)]) + 3*sd(feature[1:min(length(feature), ruleDay)]),
             avg = mean(feature))
    hist$point_subgroup[which(hist$feature>hist$UL)] = 1
    hist$point_subgroup[which(hist$feature<hist$LL)] = 2
    
    
    
    # GGPLOT
    ggplot(hist, aes(x = time, y = feature, col = as.factor(point_subgroup))) +
      geom_line(color="#cccccc",size=0.5,alpha=0.6) +
      geom_point(size=4.5, alpha=0.8) +
      scale_color_manual(values = c("0" = "gray45", "1"= "red1", "2"="red1")) + 
      geom_step(aes(y=LL), color="red4", size=1.5, alpha=0.8) +
      geom_step(aes(y=UL), color="red4", size=1.5, alpha=0.8) +
      geom_step(aes(y=avg), color="green", size=1.5, alpha=1) +
      # geom_smooth(aes(group = as.factor(huandaoRec)), se = FALSE, color = "blue", method="lm", size=0.7, alpha=0.7) +
      geom_vline(data=as.data.frame(huandaoTime),aes(xintercept = huandaoTime), color="black", size=1.2) +
      xlab("时间") + ylab("负载") + ggtitle(paste0("刀具", ruleVal$toolNum, "-负载随时间变化趋势")) +
      #theme_bw() +
      # scale_x_continuous(breaks = seq(from = 0, to = max_cc, by = by_interval), label = seq(from = 0, to = max_cc, by = by_interval)) +
      scale_y_continuous(labels = scales::percent) +
      theme(legend.position = "none", 
            axis.title.x = element_text(size=30),axis.text.x = element_text(size=30), 
            axis.title.y = element_text(size=30),axis.text.y = element_text(size=30), 
            panel.grid.minor = element_blank(),
            panel.grid.major = element_blank(),
            panel.border = element_blank(),
            panel.background = element_rect(fill="#F4F4F4", colour = "#F4F4F4"),
            plot.title = element_text(size=30),
            plot.background = element_rect(fill = "#F4F4F4", colour = "#F4F4F4"))   
    
    
    
    # SAVE PLOT
    ggsave(filename = fullpath, device = "jpeg", width = 70, height = 20, dpi = 60, limitsize = FALSE, units = "cm")
    
  }else{
    
    # CREATE TWO CATEGORIES: 1. POINTS; 2. HUANDAO SEGMENT
    hist <- hist %>% mutate(point_subgroup = 0,
                            huandaoRec = 0)
    
    
    
    # SET HUANDAO SEGMENTS
    hist$huandaoRec[huandaoLoc$huandao] = 1
    hist$huandaoRec = cumsum(hist$huandaoRec)
    
    
    
    # GGPLOT
    ggplot(hist, aes(x = time,y = feature)) +
      geom_line(color="black",size=0.5,alpha=0.6) +
      #geom_step(aes(y=LL), linetype = "longdash", color="green4", size=0.8, alpha=0.8) +
      #geom_step(aes(y=UL), linetype = "longdash", color="red4", size=0.8, alpha=0.8) +
      #geom_step(aes(y=avg), color="yellow3", size=0.7, alpha=1) +
      geom_point(aes(color=as.factor(point_subgroup)), size=4, alpha=0.9) +
      # geom_smooth(aes(group=as.factor(huandaoRec)), se = FALSE, color = "blue", method="lm", size=0.7, alpha=0.7) +
      geom_vline(data=as.data.frame(huandaoTime),aes(xintercept = huandaoTime), color="black", size=1.2) +
      xlab("时间") + ylab("负载") + ggtitle(paste0("刀具", ruleVal$toolNum, "-负载随时间变化趋势")) +
      #theme_bw() +
      # scale_x_continuous(breaks = seq(from = 0, to = max_cc, by = by_interval), label = seq(from = 0, to = max_cc, by = by_interval)) +
      scale_y_continuous(labels=scales::percent) +
      theme(legend.position = "none", 
            axis.title.x = element_text(size=30),axis.text.x = element_text(size=30), 
            axis.title.y = element_text(size=30),axis.text.y = element_text(size=30), 
            plot.title = element_text(size=30), 
            panel.grid.minor = element_blank(),
            panel.grid.major = element_blank(),
            panel.border = element_blank(),
            panel.background = element_rect(fill="#F4F4F4",color = "#F4F4F4"),
            plot.background = element_rect(fill = "#F4F4F4", color = "#F4F4F4"))
    
    
    
    # SAVE PLOT
    ggsave(filename=fullpath, device = "jpeg", width = 60, height = 20, dpi = 70, limitsize = FALSE, units = "cm")
    
  }
  
  return(path2return)
}




computeStatistics <- function(knife, ruleVal){
  
  # ESTABLISH KM MYSQL CONNECTION
  connT <- dbConnect(RMySQL::MySQL(), dbname = "KM", username = SQL_usr, password = SQL_pass, host = SQL_host, port = SQL_port)
  on.exit(DBI::dbDisconnect(connT))
  
  
  
  # GET TREND DATA
  hist <- DBI::dbGetQuery(connT, sqlInterpolate(connT,
                                               "select * from KM.KM_Trend where companyID = ?companyID AND projectID = ?projectID AND machineID = ?machineID AND programNum = ?programNum AND spindleID = ?spindleID AND toolNum = ?toolNum AND time >= ?startTime AND time <= ?endTime",
                                               companyID = ruleVal$companyID, projectID = ruleVal$projectID, machineID = ruleVal$machineID, programNum = ruleVal$programNum, spindleID = ruleVal$spindleID, toolNum = knife, startTime = as.character(ruleVal$startTime), endTime = as.character(ruleVal$endTime)))
  hist <- hist[order(hist$time, decreasing = FALSE),]

  
  
  
  # IF NOT FOUND, RETURN ERROR
  if (nrow(hist) == 0){return(toJSON(list(ret = 44), auto_unbox=TRUE))}
  
  
  
  # COMPUTE STATISTICS
  tmp <- summary(hist$feature)
  
  
  
  # TO LIST
  rst <- c(toolNum = knife, min = tmp[['Min.']], q1 = tmp[['1st Qu.']], median = tmp[['Median']], q3 = tmp[['3rd Qu.']], max = tmp[['Max.']])
  
  
  return(rst)
}




computeSS <- function(knife, dt, spindleID){
  
  # TIME MODIFY
  dt$time <- as_datetime(dt$time)
  
  
  
  # FILTER TO THAT KNIFE
  dt2 <- dt %>% filter(tool_number == knife)
  
  
  
  # STEP NUMBER RANGE
  # tmp <- dt %>% 
  #   dplyr::group_by(cycle_counter) %>%
  #   dplyr::summarise(steps = length(spindle_load)) %>%
  #   dplyr::filter(steps > nrow(dt)/length(unique(dt$cycle_counter)))
  # dtTmp <- dt %>% dplyr::filter(cycle_counter %in% tmp$cycle_counter[1:10])
  # dtTime <- dt %>% dplyr::filter(cycle_counter %in% tmp$cycle_counter[1])
  # dt2Tmp <- dtTmp %>% dplyr::filter(tool_number == knife)
  # if (nrow(dt2Tmp) <= 3){return(49)}
  uniqueStep <- sort(unique(dt2$step_number))
  lagDiff <- abs(uniqueStep - lag(uniqueStep))
  tolerance <- mean(uniqueStep)/2.5
  
  
  
  # GET INDEX OF THE KNIFE WHICH GETS USED IN TWO SEPARATE STEP_NUMBER BLOCK
  cutInd <- which(lagDiff > tolerance)
  
  
  
  # LABEL EACH BLOCK
  m <- data.frame(uniqueStep = uniqueStep, label = 0)
  if(!is.na(cutInd[1])){
    m$label[cutInd] = 1
    m$label = cumsum(m$label)
  }
  
  
  
  # GET EACH BLOCK'S STEP_NUMBER SUMMARY
  rstStep <- m %>% 
    dplyr::group_by(label) %>%
    dplyr::summarise(firstStep = first(uniqueStep),
                     lastStep = last(uniqueStep))
  
  
  
  # PASTE TO STRING
    
    # SEPARATE STRING PART
    rstStepString <- "%%"
    
    
    # PASTE
    for (i in 1:nrow(rstStep)){
      tmp <- paste0(rstStep[i,][['firstStep']], "-", rstStep[i,][['lastStep']])
      rstStepString <- paste0(rstStepString, ", ", tmp)
    }
    
    
    # DELETE THE SEPARATE STRING PART
    rstStepString <- gsub("%%, ", "", rstStepString)
  
  
  
  # GET EACH KNIFE'S WORKING TIME
  # tmp <- dt2 %>% 
  #   dplyr::group_by(cycle_counter) %>%
  #   dplyr::summarise(steps = length(spindle_load)) %>%
  #   dplyr::filter(steps > nrow(dt2)/length(unique(dt2$cycle_counter))) 
  dtCycled <- dt2 #%>% dplyr::filter(cycle_counter %in% tmp$cycle_counter[1:10])
  
  
  
  # TIME
  prop_table <- prop.table(table(dt$tool_number))
  curr_prop <- prop_table[[as.character(knife)]]
  timeKnife <- abs((difftime(last(dt$time), first(dt$time), units = 'secs') * curr_prop)[[1]])
  
  
  
  # SPINDLE SPEED 
  featureSS <- max(dtCycled$spindle_speed, trim = 0.1)
  
  
  
  # SPINDLE LOAD
  featureLoad <- max(dtCycled$spindle_load, trim = 0.1)
  
  
  
  # LIST
  vec2return <- c(spindleID = spindleID, knife = knife, steps = rstStepString, spindleSpeed = featureSS, spindleLoad = featureLoad, timeKnife = timeKnife)
  
  return(vec2return)
}




testPlot <- function(k){
  p <- ggplot(data.frame(x=seq(k), y=rnorm(k)), aes(x=x)) + 
    geom_point(aes(y=y))
  q <- ggplotly(p)
  path <- paste0(as.numeric(Sys.time()), "_", runif(1), ".html")
  htmlwidgets::saveWidget(q, file = paste0("/home/gz2/testPlot/",path))
  
  return(path)
}