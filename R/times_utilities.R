
char_utc2local_time <- function(dates, format, tz){
    x <- strptime(dates, format, tz = "UTC")
    x <- as.POSIXct(x)
    x <- format(x, format, tz = tz)
    x <- strptime(x, format, tz = tz)
    as.POSIXct(x)
}

time_utc2local_char <- function(dates, format, tz){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = tz)
    x
}

time_zone2local_char <- function(dates, format, tz_local){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = tz_local)
    x
}

char_local2utc_time <- function(dates, format, tz){
    x <- strptime(dates, format, tz = tz)
    x <- as.POSIXct(x)
    x <- format(x, format, tz = "UTC")
    x <- strptime(x, format, tz = "UTC")
    as.POSIXct(x)
}

time_local2utc_char <- function(dates, format){
    x <- as.POSIXct(dates)
    x <- format(x, format, tz = "UTC")
    x
}

time_local2utc_time <- function(dates){
    format <- "%Y-%m-%d %H:%M:%S"
    x <- time_local2utc_char(dates, format)
    x <- strptime(x, format, tz = "UTC")
    as.POSIXct(x)
}

time_utc2time_local <- function(dates, tz){
    format <- "%Y-%m-%d %H:%M:%S"
    x <- time_utc2local_char(dates, format, tz)
    x <- strptime(x, format, tz = tz)
    as.POSIXct(x)
}

time_zone2time_local <- function(dates, tz_local){
    format <- "%Y-%m-%d %H:%M:%S"
    x <- time_zone2local_char(dates, format, tz_local)
    x <- strptime(x, format, tz = tz_local)
    as.POSIXct(x)
}

round.time.minutes10 <- function(times){
    mn <- format(times, "%M")
    mn <- as.integer(paste0(substr(mn, 1, 1), 0))
    # trunc.POSIXt  
    # times <- round.POSIXt(times, units = "mins")
    times$min[] <- mn
    times$sec[] <- 0
    times
}

addMonths <- function(daty, n = 1){
    foo <- function(daty, n){
        date0 <- seq(daty, by = paste(n, "months"), length = 2)[2]
        date1 <- seq(as.Date(paste(format(daty, '%Y-%m'), '01', sep = '-')),
                    by = paste(n + 1, "months"), length = 2)[2] - 1
        if(date0 > date1) date1 else date0
    }
    daty <- if(length(daty) == 1) foo(daty, n) else do.call(c, lapply(daty, foo, n))
    return(daty)
}
