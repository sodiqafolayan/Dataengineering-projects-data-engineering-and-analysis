class TpcdsLog4J:
    def __init__(self, spark):
        # this is how you get a jvm object
        log4j = spark._jvm.org.apache.log4j
        # creating logger attribute and initializing it
        root_class = "asorock.spark.examples"
        # We are simply getting the app name here to append to our logger name
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

    # We can use the above initialized logger to log messages. To make
    # things simple, we can create four below methods
    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def debug(self, message):
        self.logger.debug(message)

    def error(self, message):
        self.logger.error(message)