from datetime import date, datetime, timedelta
from typing import List, Optional

from mecv.sessions import PostgresSession


class BanamexCalendar:
    def __init__(self):
        self.psql = PostgresSession()

    def is_business_day(self, calendar_date) -> bool:
        with self.psql.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT is_business_day FROM banamex_calendar_sync_d WHERE calendar_date = %s",
                    (calendar_date,),
                )
                row = cur.fetchone()
        return row[0] if row else True

    def previous_business_days(self, calendar_date, n: int = 1) -> List[date]:
        with self.psql.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT calendar_date
                    FROM banamex_calendar_sync_d
                    WHERE calendar_date <= %s AND is_business_day = true
                    ORDER BY calendar_date DESC
                    LIMIT %s
                """,
                    (calendar_date, n),
                )
                rows = cur.fetchall()
        return [r[0] for r in rows]

    def expected_information_date(self, frequency: str, reference_date=None) -> str:
        if reference_date is None:
            reference_date = datetime.now().date()
        if frequency in ("daily", "business_daily"):
            return reference_date.isoformat()
        if frequency == "weekly":
            days = self.previous_business_days(reference_date, 5)
            return days[-1].isoformat() if days else reference_date.isoformat()
        if frequency == "monthly":
            days = self.previous_business_days(reference_date, 20)
            return days[-1].isoformat() if days else reference_date.isoformat()
        return reference_date.isoformat()

    def next_business_day(self, calendar_date) -> Optional[date]:
        with self.psql.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT calendar_date
                    FROM banamex_calendar_sync_d
                    WHERE calendar_date > %s AND is_business_day = true
                    ORDER BY calendar_date ASC
                    LIMIT 1
                """,
                    (calendar_date,),
                )
                row = cur.fetchone()
        return row[0] if row else None
