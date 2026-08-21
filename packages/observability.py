import logging

# Attributes every LogRecord carries. Derived from a throwaway record rather
# than a hand-written list so it cannot drift as the standard library changes;
# "message" and "asctime" are added later, during formatting.
_RESERVED = frozenset(
    logging.LogRecord(
        name="",
        level=0,
        pathname="",
        lineno=0,
        msg="",
        args=(),
        exc_info=None,
    ).__dict__
) | {"message", "asctime"}


class ContextFormatter(logging.Formatter):
    """Render the fields passed through ``extra`` alongside the message.

    Structured context attaches to the record, but a plain format string only
    interpolates the names it mentions, so everything passed through ``extra``
    is silently discarded. That is how a warning ends up reporting that a
    publish failed while dropping the broker's reason for refusing it.
    """

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        context = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _RESERVED and not key.startswith("_")
        }
        if not context:
            return rendered

        fields = " ".join(f"{key}={value!r}" for key, value in sorted(context.items()))
        return f"{rendered} {fields}"


def configure_logging(level: int = logging.INFO) -> None:
    """Install the root handler used by every long-running process.

    ``force`` replaces any handler a library installed first, so configuration
    does not depend on which import happened to run earliest.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(
        ContextFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    logging.basicConfig(level=level, handlers=[handler], force=True)
