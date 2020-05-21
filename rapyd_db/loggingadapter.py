import logging


class LogIdAdapter(logging.LoggerAdapter):
    """
    This adapter will look to see if there is a unique log record identifier.
    If present, it will log that value along with the message.
    This is used to tie log messages performing a unit of work for easy audits.
    """

    def process(self, msg, kwargs):
        log_id = self.extra.get("log_id")
        if log_id:
            return "{} - {}".format(log_id, msg), kwargs
        else:
            return msg, kwargs
