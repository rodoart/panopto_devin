"""Módulo calendar con la(s) clase(s) BanamexCalendar."""

import calendar as cal
from datetime import date, datetime, timedelta
from typing import Any, List, Optional, Tuple

from mecv.config.tables import PROCESS_CONFIG
from mecv.logging import get_logger
from mecv.sessions import PostgresSession

logger = get_logger(__name__)


class BanamexCalendar:
    """Clase que representa BanamexCalendar."""
    def __init__(self) -> None:
        """Inicializa una nueva instancia de BanamexCalendar."""
        self.psql = PostgresSession()

    def is_business_day(self, calendar_date: Any) -> bool:
        """Método que realiza la operación "is_business_day"."""
        table = PROCESS_CONFIG.banamex_calendar_sync_table
        with self.psql.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT is_business_day FROM {table} WHERE calendar_date = %s",
                    (calendar_date,),
                )
                row = cur.fetchone()
        return row[0] if row else True

    def previous_business_days(self, calendar_date: Any, n: int = 1) -> List[date]:
        """Método que realiza la operación "previous_business_days"."""
        table = PROCESS_CONFIG.banamex_calendar_sync_table
        with self.psql.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT calendar_date
                    FROM {table}
                    WHERE calendar_date <= %s AND is_business_day = true
                    ORDER BY calendar_date DESC
                    LIMIT %s
                """,
                    (calendar_date, n),
                )
                rows = cur.fetchall()
        return [r[0] for r in rows]

    def expected_information_date(self, frequency: str, reference_date: Optional[Any] = None) -> str:
        """Método que realiza la operación "expected_information_date"."""
        if reference_date is None:
            reference_date = datetime.now().date()
        if frequency in ("daily", "business_daily"):
            return reference_date.isoformat()
        if frequency == "weekly":
            return self.last_business_day_of_period(reference_date, "week")
        if frequency == "monthly":
            return self.last_business_day_of_period(reference_date, "month")
        return reference_date.isoformat()

    def next_business_day(self, calendar_date: Any) -> Optional[date]:
        """Método que realiza la operación "next_business_day"."""
        table = PROCESS_CONFIG.banamex_calendar_sync_table
        with self.psql.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT calendar_date
                    FROM {table}
                    WHERE calendar_date > %s AND is_business_day = true
                    ORDER BY calendar_date ASC
                    LIMIT 1
                """,
                    (calendar_date,),
                )
                row = cur.fetchone()
        return row[0] if row else None

    @staticmethod
    def _to_date(calendar_date: Any) -> Any:
        """Helper interno que realiza la operación "to_date"."""
        if isinstance(calendar_date, str):
            return datetime.fromisoformat(calendar_date).date()
        return calendar_date

    def _period_bounds(self, calendar_date: Any, period: str) -> Tuple[Any, ...]:
        """Helper interno que realiza la operación "period_bounds"."""
        d = self._to_date(calendar_date)
        if period == "month":
            start = d.replace(day=1)
            _, last_day = cal.monthrange(d.year, d.month)
            end = d.replace(day=last_day)
        elif period == "week":
            start = d - timedelta(days=d.weekday())
            end = start + timedelta(days=6)
        else:
            start = end = d
        return start, end

    def first_business_day_of_period(self, calendar_date: Any, period: str) -> str:
        """Método que realiza la operación "first_business_day_of_period"."""
        start, end = self._period_bounds(calendar_date, period)
        table = PROCESS_CONFIG.banamex_calendar_sync_table
        with self.psql.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT calendar_date
                    FROM {table}
                    WHERE calendar_date >= %s AND calendar_date <= %s
                      AND is_business_day = true
                    ORDER BY calendar_date ASC
                    LIMIT 1
                """,
                    (start, end),
                )
                row = cur.fetchone()
        if row:
            return row[0].isoformat()
        d = self._to_date(calendar_date)
        return d.isoformat()

    def last_business_day_of_period(self, calendar_date: Any, period: str) -> str:
        """Método que realiza la operación "last_business_day_of_period"."""
        start, end = self._period_bounds(calendar_date, period)
        table = PROCESS_CONFIG.banamex_calendar_sync_table
        with self.psql.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT calendar_date
                    FROM {table}
                    WHERE calendar_date >= %s AND calendar_date <= %s
                      AND is_business_day = true
                    ORDER BY calendar_date DESC
                    LIMIT 1
                """,
                    (start, end),
                )
                row = cur.fetchone()
        if row:
            return row[0].isoformat()
        d = self._to_date(calendar_date)
        return d.isoformat()
