#
# Run with:
#    source("plot_mimic.R")
# then    
#    plot_mimic()
#
plot_mimic <- function() {

library(gridExtra)
library(grid)
library(ggplot2)
library(lattice)
options(digits=3)
library(scales)
#theme_set(theme_bw())       

    s <- read.csv('../input-signal.csv')
    colnames(s)<-c('ts','key','val')

    sma <- read.csv('../movingAvg-signal.csv')
    colnames(sma)<-c('ts','key','val')

   
    p1 <- ggplot(data=s, aes(as.POSIXct(ts), val,colour=key)) +
          geom_line() + geom_point() + scale_x_datetime(breaks=date_breaks("1 day"))

    p2 <- ggplot(data=sma, aes(as.POSIXct(ts), val,colour=key)) +
        geom_line() + geom_point() + scale_x_datetime(breaks=date_breaks("1 day"))

    

    grid.arrange(p1,p2)

}
