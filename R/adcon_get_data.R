get.adcon.data <- function(aws_dir, adt_dir){
    on.exit({
        DBI::dbDisconnect(conn)
        if(upload) ssh::ssh_disconnect(session)
    })

    tz <- "Africa/Addis_Ababa"
    Sys.setenv(TZ = tz)
    origin <- "1970-01-01"
    last_format <- "%Y%m%d%H%M%S"
    awsNET <- 1

    dirOUT <- file.path(aws_dir, "AWS_DATA", "DATA", "ADCON")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(aws_dir, "AWS_DATA", "LOG", "ADCON")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)

    mon <- format(Sys.time(), '%Y%m')
    logPROC <- file.path(dirLOG, paste0("processing_adcon_", mon, ".txt"))
    awsLOG <- file.path(dirLOG, paste0("AWS_LOG_", mon, ".txt"))
    uploadFailed <- file.path(dirLOG, 'upload_failed.txt')

    conn <- connect.adcon(aws_dir)
    if(is.null(conn)){
        msg <- "An error occurred when connecting to ADCON database"
        format.out.msg(msg, logPROC)
        return(1)
    }

    ###
    upload <- FALSE
    dirUPData <- file.path(adt_dir, "AWS_DATA", "DATA", "minutes", "ADCON")
    dirUPLog <- file.path(adt_dir, "AWS_DATA", "LOG", "ADCON")
    ###
    # session <- connect.ssh(aws_dir)
    # if(is.null(session)){
    #     msg <- "Unable to connect to ADT server"
    #     format.out.msg(msg, logPROC)
    #     upload <- FALSE
    # }else{
    #     dirUPData <- file.path(adt_dir, "AWS_DATA", "DATA", "minutes", "ADCON")
    #     dirUPLog <- file.path(adt_dir, "AWS_DATA", "LOG", "ADCON")
    #     ssh::ssh_exec_wait(session, command = c(
    #         paste0('if [ ! -d ', dirUPData, ' ] ; then mkdir -p ', dirUPData, ' ; fi'),
    #         paste0('if [ ! -d ', dirUPLog, ' ] ; then mkdir -p ', dirUPLog, ' ; fi')
    #     ))
    #     upload <- TRUE
    # }

    varFile <- file.path(aws_dir, "AWS_DATA", "CSV", "adcon_pars.csv")
    varTable <- utils::read.table(varFile, sep = ',', header = TRUE,
                                  stringsAsFactors = FALSE, quote = "\"",
                                  fileEncoding = 'UTF-8')
    crdFile <- file.path(aws_dir, "AWS_DATA", "CSV", "adcon_crds.csv")
    awsCrd <- utils::read.table(crdFile, sep = ',', header = TRUE,
                                stringsAsFactors = FALSE, quote = "\"",
                                fileEncoding = 'UTF-8')
    lastFile <- file.path(aws_dir, "AWS_DATA", "CSV", "adcon_lastDates.csv")
    awsLast <- utils::read.table(lastFile, sep = ',', header = TRUE, 
                                 colClasses = "character", na.strings = "",
                                 stringsAsFactors = FALSE, quote = "\"")

    for(j in seq_along(awsCrd$id)){
        awsID <- awsCrd$id[j]
        awsTZ <- awsCrd$time_zone[j]
        awsVAR <- varTable[varTable$id == awsID, , drop = FALSE]

        ilst <- which(awsLast$id == awsID)
        if(length(ilst) == 0){
            tmpLast <- data.frame(id = awsID, last = NA)
            awsLast <- rbind(awsLast, tmpLast)
            ilst <- length(awsLast$id)
        }

        if(is.na(awsLast$last[ilst])){
            init_time <- "20230101000000"
            ## last 5 days (comment at 1st run)
            # init_time <- format(Sys.time() - 5 * 24 * 3600, last_format)
            ##
            last <- as.POSIXct(init_time, format = last_format, tz = tz)
        }else{
            last <- as.POSIXct(awsLast$last[ilst], format = last_format, tz = tz)
            ## last 5 days (comment at 1st run)
            # last5d <- Sys.time() - 5 * 24 * 3600
            # if(last < last5d) last <- last5d
        }

        daty <- seq(last, Sys.time(), 'month')
        if(length(daty) == 1){
            daty <- c(daty, Sys.time())
        }

        for(mo in 1:(length(daty) - 1)){
            mm <- if(mo == 1) 0 else 1
            start_mo <- daty[mo] + mm
            end_mo <- daty[mo + 1]
            ##
            if(awsTZ == 'UTC'){
                start_mo <- time_local2utc_time(start_mo)
                end_mo <- time_local2utc_time(end_mo)
            }
            start_mo <- as.numeric(start_mo)
            end_mo <- as.numeric(end_mo)

            qres <- lapply(seq_along(awsVAR$node_var), function(i){
                query <- "SELECT * FROM historiandata WHERE"
                tag_id <- paste0("tag_id=", awsVAR$node_var[i])
                qtime1 <- paste0("startdate>=", start_mo)
                qtime2 <- paste0("enddate<=", end_mo)
                query <- paste(query, tag_id, "AND", qtime1, "AND", qtime2)

                qres <- try(DBI::dbGetQuery(conn, query), silent = TRUE)

                if(inherits(qres, "try-error")){
                    msg <- paste("Unable to get data, aws_id:", awsID,
                                 "/ parameter:", awsVAR$var_name[i])
                    format.out.msg(paste(msg, '\n', qres), awsLOG)
                    return(NULL)
                }

                if(nrow(qres) == 0) return(NULL)

                valid <- qres$tag_id == awsVAR$node_var[i] & qres$status == 0
                qres <- qres[valid, , drop = FALSE]

                if(nrow(qres) == 0) return(NULL)

                end <- as.POSIXct(qres$enddate, origin = origin, tz = awsTZ)
                start <- as.POSIXct(qres$startdate, origin = origin, tz = awsTZ)
                dft <- difftime(end, start, units = "mins")
                ## 10 and 15 mins interval,
                ## take obs with greater than 3 mins sampling and less than 20
                qres <- qres[dft >= 3 & dft < 20, , drop = FALSE]

                if(nrow(qres) == 0) return(NULL)

                qres <- qres[, c('enddate', 'measuringvalue'), drop = FALSE]

                return(qres)
            })

            inull <- sapply(qres, is.null)
            if(all(inull)) next

            qres <- qres[!inull]
            awsVAR1 <- awsVAR[!inull, , drop = FALSE]

            out <- try(parse.adcon_db.data(qres, awsVAR1, awsID, awsNET, awsTZ), silent = TRUE)
            if(inherits(out, "try-error")){
                mserr <- gsub('[\r\n]', '', out[1])
                msg <- paste("Unable to parse data for", awsID)
                format.out.msg(paste(mserr, '\n', msg), awsLOG)
                next
            }

            if(is.null(out)) next

            if(!is.na(awsLast$last[ilst])){
                last <- as.POSIXct(awsLast$last[ilst], format = last_format, tz = tz)
                it <- out$obs_time > as.numeric(last)
                out <- out[it, , drop = FALSE]
                if(nrow(out) == 0) next
            }

            temps <- as.POSIXct(out$obs_time, origin = origin, tz = tz)
            split_day <- format(temps, '%Y%m%d')
            index_day <- split(seq(nrow(out)), split_day)

            for(s in seq_along(index_day)){
                don <- out[index_day[[s]], , drop = FALSE]
                lastV <- as.POSIXct(max(don$obs_time), origin = origin, tz = tz)
                awsLast$last[ilst] <- format(lastV, last_format)

                locFile <- paste(range(don$obs_time), collapse = "_")
                locFile <- paste0(awsID, "_", locFile, '.rds')
                locFile <- file.path(dirOUT, locFile)
                saveRDS(don, locFile)

                utils::write.table(awsLast, lastFile, sep = ",", na = "", col.names = TRUE,
                                   row.names = FALSE, quote = FALSE)

                if(upload){
                    upFile <- file.path(dirUPData, basename(locFile))
                    ret <- try(ssh::scp_upload(session, locFile, to = upFile, verbose = FALSE), silent = TRUE)

                    if(inherits(ret, "try-error")){
                        if(grepl('disconnected', ret[1])){
                            Sys.sleep(1)
                            session <- connect.ssh(aws_dir)
                            upload <- if(is.null(session)) FALSE else TRUE
                        }
                        cat(basename(locFile), file = uploadFailed, append = TRUE, sep = '\n')
                    }
                }else{
                    ## uncomment this
                    # cat(basename(locFile), file = uploadFailed, append = TRUE, sep = '\n')
                }
            }
        }
    }

    if(upload){
        if(file.exists(logPROC)){
            logPROC1 <- file.path(dirUPLog, basename(logPROC))
            ssh::scp_upload(session, logPROC, to = logPROC1, verbose = FALSE)
        }

        if(file.exists(awsLOG)){
            awsLOG1 <- file.path(dirUPLog, basename(awsLOG))
            ssh::scp_upload(session, awsLOG, to = awsLOG1, verbose = FALSE)
        }
    }

    return(0)
}

parse.adcon_db.data <- function(tmp, awsVAR, awsID, awsNET, awsTZ){
    tz <- "Africa/Accra"
    origin <- "1970-01-01"

    nvar <- c("var_height", "var_code", "stat_code")
    tmp <- lapply(seq_along(awsVAR$node_var), function(i){
        x <- tmp[[i]]
        v <- awsVAR[i, , drop = FALSE]
        vm <- as.numeric(x$measuringvalue)
        if(v$multiplier > 0){
            vm <- vm * v$multiplier
        }else{
            if(v$multiplier == -10){
                ## degF to degC
                vm <- 5/9 * (vm - 32)
            }
        }
        x$measuringvalue <- vm
        v <- v[rep(1, nrow(x)), nvar, drop = FALSE]
        cbind(v, x)
    })

    tmp <- do.call(rbind, tmp)
    tmp <- tmp[!is.na(tmp$measuringvalue), , drop = FALSE]
    if(nrow(tmp) == 0) return(NULL)

    tmp$network <- awsNET
    tmp$id <- awsID
    tmp$limit_check <- NA

    ## time zone conversion
    # temps <- as.POSIXct(tmp$enddate, origin = origin, tz = awsTZ)
    # temps <- time_zone2time_local(temps, tz)
    ##
    if(awsTZ == 'UTC'){
        temps <- as.POSIXct(tmp$enddate, origin = origin, tz = "UTC")
        temps <- time_utc2time_local(temps, tz)
    }

    temps <- as.POSIXct(tmp$enddate, origin = origin, tz = tz)
    tmp$enddate <- as.numeric(temps)
    tmp <- tmp[!is.na(temps), , drop = FALSE]
    if(nrow(tmp) == 0) return(NULL)

    nvar <- c("network", "id", "var_height", 
              "var_code", "stat_code", "enddate",
              "measuringvalue", "limit_check")
    tmp <- tmp[, nvar, drop = FALSE]

    fun_format <- list(as.integer, as.character, as.numeric, as.integer,
                       as.integer, as.integer, as.numeric, as.integer)

    tmp <- lapply(seq_along(fun_format), function(j) fun_format[[j]](tmp[[j]]))
    tmp <- as.data.frame(tmp)
    names(tmp) <- c("network", "id", "height", "var_code", "stat_code",
                    "obs_time", "value", "limit_check")

    return(tmp)
}
