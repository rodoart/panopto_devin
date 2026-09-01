"""Tests for BanamexCalendar with mocked Postgres."""

import datetime as dt

from mecv.calendar import BanamexCalendar


def test_is_business_day(postgres_connection):
    """is_business_day returns the value from the calendar sync table."""
    postgres_connection.set_results([(True,)])
    calendar = BanamexCalendar()
    assert calendar.is_business_day("2025-01-02") is True

    postgres_connection.set_results([(False,)])
    assert calendar.is_business_day("2025-01-01") is False


def test_is_business_day_defaults_to_true(postgres_connection):
    """is_business_day returns True when the date is absent from the table."""
    postgres_connection.set_results([])
    calendar = BanamexCalendar()
    assert calendar.is_business_day("2025-01-02") is True


def test_previous_business_days(postgres_connection):
    """previous_business_days returns the last n business days before a date."""
    postgres_connection.set_results([
        (dt.date(2025, 1, 2),),
        (dt.date(2025, 1, 1),),
    ])
    calendar = BanamexCalendar()
    result = calendar.previous_business_days("2025-01-03", n=2)
    assert result == [dt.date(2025, 1, 2), dt.date(2025, 1, 1)]


def test_next_business_day(postgres_connection):
    """next_business_day returns the first business day after a date."""
    postgres_connection.set_results([(dt.date(2025, 1, 3),)])
    calendar = BanamexCalendar()
    assert calendar.next_business_day("2025-01-02") == dt.date(2025, 1, 3)


def test_first_business_day_of_period(postgres_connection):
    """first_business_day_of_period returns the first business day as an ISO string."""
    postgres_connection.set_results([(dt.date(2025, 1, 2),)])
    calendar = BanamexCalendar()
    assert calendar.first_business_day_of_period("2025-01-15", "month") == "2025-01-02"


def test_last_business_day_of_period(postgres_connection):
    """last_business_day_of_period returns the last business day as an ISO string."""
    postgres_connection.set_results([(dt.date(2025, 1, 30),)])
    calendar = BanamexCalendar()
    assert calendar.last_business_day_of_period("2025-01-15", "month") == "2025-01-30"


def test_expected_information_date_daily():
    """expected_information_date for daily frequency returns the reference date."""
    calendar = BanamexCalendar()
    d = dt.date(2025, 1, 15)
    assert calendar.expected_information_date("daily", d) == "2025-01-15"
    assert calendar.expected_information_date("business_daily", d) == "2025-01-15"


def test_expected_information_date_weekly(postgres_connection):
    """expected_information_date for weekly/monthly queries the calendar."""
    postgres_connection.set_results([(dt.date(2025, 1, 10),)])
    calendar = BanamexCalendar()
    assert calendar.expected_information_date("weekly", dt.date(2025, 1, 12)) == "2025-01-10"
