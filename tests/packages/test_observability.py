import logging

from packages.observability import ContextFormatter, configure_logging


def format_one(message: str, **context: object) -> str:
    formatter = ContextFormatter("%(levelname)s %(name)s %(message)s")
    record = logging.LogRecord(
        name="apps.orders.publisher_worker",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )
    record.__dict__.update(context)
    return formatter.format(record)


def test_context_passed_through_extra_reaches_the_output() -> None:
    """A warning that drops its own reason is not worth logging.

    The publisher records the broker's refusal in ``extra``. A plain format
    string interpolates only the names it mentions, so the one detail that
    explains the failure never reaches an operator.
    """
    rendered = format_one(
        "publish failed; event rescheduled",
        event_id="8f2c",
        attempts=2,
        error="ServiceBusError: the topic is disabled for send",
    )

    assert "publish failed; event rescheduled" in rendered
    assert "the topic is disabled for send" in rendered
    assert "attempts=2" in rendered


def test_a_record_without_context_is_left_alone() -> None:
    assert format_one("consumer stopped") == (
        "WARNING apps.orders.publisher_worker consumer stopped"
    )


def test_standard_record_attributes_are_not_treated_as_context() -> None:
    """Without a reserved set every line would carry the whole LogRecord."""
    rendered = format_one("published event")

    for noise in ("pathname=", "levelno=", "msecs=", "args=", "lineno="):
        assert noise not in rendered


def test_configure_logging_replaces_a_handler_installed_earlier() -> None:
    """Import order must not decide whether context survives."""
    logging.basicConfig(level=logging.INFO, format="%(message)s", force=True)

    configure_logging()

    handlers = logging.getLogger().handlers
    assert len(handlers) == 1
    assert isinstance(handlers[0].formatter, ContextFormatter)
