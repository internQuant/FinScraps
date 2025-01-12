import pandas as pd
import requests
from io import BytesIO
from pandas.tseries.offsets import CustomBusinessDay

class BRCal:
    """A high-performance calendar utility for ANBIMA national holidays.

    This class provides methods for:
        - Loading holidays from ANBIMA's .xls file upon initialization
        - Computing custom business day offsets
        - Generating business day ranges
        - Finding nth, previous, next business days
        - Counting business days in a range
        - Checking if a given date is a business day
    """

    def __init__(self):
        self.url = "https://www.anbima.com.br/feriados/arqs/feriados_nacionais.xls"
        self._holidays = self._fetch_holidays()
        self._custom_bday = CustomBusinessDay(
            weekmask="Mon Tue Wed Thu Fri",
            holidays=self._holidays
        )
    
    @property
    def now(self) -> pd.Timestamp:
        """Current timestamp (including time)."""
        return pd.Timestamp("now")

    @property
    def today(self) -> pd.Timestamp:
        """Today's date, normalized to midnight."""
        return self.now.normalize()

    def _fetch_holidays(self) -> set[pd.Timestamp]:
        """Fetch the ANBIMA holiday .xls from the official URL.
        """
        try:
            response = requests.get(self.url, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to fetch holidays from {self.url} due to: {e}")
            return set()
        
        holidays_df = pd.read_excel(BytesIO(response.content), parse_dates=[0]).dropna()['Data']
        return set(holidays_df)

    def day_range(self, start_date: str, end_date: str) -> pd.DatetimeIndex:
        """Generates a range of business days between two dates (inclusive)
        """
        start_date = pd.to_datetime(start_date).normalize()
        end_date   = pd.to_datetime(end_date).normalize()
        return pd.date_range(start=start_date, end=end_date, freq=self._custom_bday)

    def is_business_day(self, date) -> bool:
        """Check if a date is a business day.
        """
        date = pd.to_datetime(date).normalize()
        return (date.weekday() < 5) and (date not in self._holidays)

    def day_count(self, start_date, end_date) -> int:
        """Returns the number of business days between two dates (exclusive).
        """
        bdays = self.bday_range(start_date, end_date)
        return max(0, len(bdays) - 1)
    
    def previous_business_day(self, date) -> pd.Timestamp:
        """Returns the previous business day before a given date.
        """
        date = pd.to_datetime(date).normalize()
        return date - self._custom_bday