from __future__ import annotations

import logging
from typing import Any, MutableMapping


class LogIdAdapter(logging.LoggerAdapter):
    """
    This adapter will look to see if there is a unique log record identifier.
    If present, it will log that value along with the message.
    This is used to tie log messages performing a unit of work for easy audits.
    """

    def process(
        self,
        msg: str,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[str, MutableMapping[str, Any]]:
        log_id = self.extra.get("log_id") if self.extra else None
        if log_id:
            return f"{log_id} - {msg}", kwargs
        return msg, kwargs
