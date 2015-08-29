#! /bin/sh
#  /etc/init.d/horae

### BEGIN INIT INFO
# Provides:          horae
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Short-Description: Starts the horae service
# Description:       This file is used to start the daemon
#                    and should be placed in /etc/init.d
### END INIT INFO

# Author:   http://vinoyang.com
# Url:      https://github.com/yanghua/horae
# Date:     28/08/2015

NAME="horae"
VERSION="0.1.0"
DESC="horae service"

# The path to Jsvc
EXEC="/usr/local/bin/jsvc"

# The path to the folder containing horae.jar
FILE_PATH="/usr/local/$NAME"

# The path to the folder containing the java runtime
JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.7.0_45.jdk/Contents/Home"

# Our classpath including our jar file and the Apache Commons Daemon library
CLASS_PATH="$FILE_PATH/horae-$VERSION.jar:$FILE_PATH/conf/log4j.properties"

for file in $FILE_PATH/libs/*.jar;
do
  CLASS_PATH=CLASS_PATH:$file
done

# The fully qualified name of the class to execute
CLASS="com.github.horae.Service"

# Any command line arguments to be passed to the our Java Daemon implementations init() method
ARGS="$FILE_PATH/conf/service.properties $FILE_PATH/conf/redisConf.properties $FILE_PATH/conf/partition.properties"

#The user to run the daemon as
USER="root"

# The file that will contain our process identification number (pid) for other scripts/programs that need to access it.
PID="/var/run/$NAME.pid"

# System.out writes to this file...
LOG_OUT="$FILE_PATH/log/$NAME.out"

# System.err writes to this file...
LOG_ERR="$FILE_PATH/err/$NAME.err"

jsvc_exec()
{
    cd $FILE_PATH
    $EXEC -home $JAVA_HOME -cp $CLASS_PATH -user $USER -outfile $LOG_OUT -errfile $LOG_ERR -pidfile $PID $1 $CLASS $ARGS
    echo "$EXEC -home $JAVA_HOME -cp $CLASS_PATH -user $USER -outfile $LOG_OUT -errfile $LOG_ERR -pidfile $PID $1 $CLASS $ARGS"
}

case "$1" in
    start)
        echo "Starting the $DESC..."

        # Start the service
        jsvc_exec

        echo "The $DESC has started."
    ;;
    stop)
        echo "Stopping the $DESC..."

        # Stop the service
        jsvc_exec "-stop"

        echo "The $DESC has stopped."
    ;;
    restart)
        if [ -f "$PID" ]; then

            echo "Restarting the $DESC..."

            # Stop the service
            jsvc_exec "-stop"

            # Start the service
            jsvc_exec

            echo "The $DESC has restarted."
        else
            echo "Daemon not running, no action taken"
            exit 1
        fi
            ;;
    *)
    echo "Usage: /etc/init.d/$NAME {start|stop|restart}" >&2
    exit 3
    ;;
esac