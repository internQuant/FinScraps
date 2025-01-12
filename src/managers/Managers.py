import os
import logging
from pathlib import Path
import pandas as pd

from src.scrapers.Scrapers import AnbimaIRTSScraper
from src.utils import BRCal

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s - %(name)s - %(message)s")

class AnbimaIRTSManager:
    """
    A manager for the AnbimaIRTSScraper. Handles the workflow of:
    - Validating the requested date
    - Downloading new data for a given date
    - Comparing new data with existing data
    - Appending and storing the latest data into a Feather file
    """

    def __init__(
        self,
        data_directory: str = "data/scraped/anbima",
        feather_filename: str = "irts_params.feather",
    ):
        """
        Parameters
        ----------
        data_directory : str
            Path to the directory where Feather files are stored.
        feather_filename : str
            Name of the Feather file to be updated with new data.
        """
        self.scraper = AnbimaIRTSScraper()
        self.data_directory = Path(data_directory)
        self.feather_filename = feather_filename
        self.data_directory.mkdir(parents=True, exist_ok=True)
        self.calendar = BRCal()

        self.logger = logging.getLogger(self.__class__.__name__)

    def scrape_and_update(self, date):
        """Download and parse the IRTS data for a given date, then merge
        it into existing dataset (if any), and store it in Feather format.

        Parameters
        ----------
        date : datetime or datetime-like
        Returns
        -------
        pd.DataFrame or None
            DataFrame containing the merged dataset (old + new) if scraping is done;
            None if skipped due to invalid date or data already present.
        """
        if not self._validate_date(date):
            return False

        feather_path = self.data_directory / self.feather_filename
        if feather_path.exists():
            existing_df = pd.read_feather(feather_path)
            existing_nrows = existing_df.shape[0]
            self.logger.info(
                f"Existing dataset loaded with {existing_nrows} rows."
            )
        else:
            self.logger.info("No existing Feather file found. Creating a new one.")
            existing_df = pd.DataFrame()

        if not existing_df.empty and "date" in existing_df.columns:
            if date in existing_df["date"].unique():
                self.logger.info(
                    f"Data for date {date.date()} is already present. Skipping scrape."
                )
                return False
            
        elif not existing_df.empty:

            self.logger.warning(
                "Existing DataFrame does not have 'date' column. "
                "Cannot check for duplicates by date."
            )

        self.logger.info(f"Starting data scrape for date: {date}...")
        try:
            new_data = self.scraper.scrape(date)
            new_nrows = new_data.shape[0]
        except Exception as e:
            self.logger.error(f"Error during scraping: {e}")
            raise

        self.logger.info(
            f"New data fetched for date {date.date()}: {new_nrows} rows"
        )

        combined_df = pd.concat([existing_df, new_data], ignore_index=True)
        combined_df.drop_duplicates(inplace=True) 
        combined_df.sort_values("date", inplace=True, ascending=True)
        combined_df.reset_index(drop=True, inplace=True)
        combined_df.to_feather(feather_path)
        self.logger.info(f"Combined dataset saved to {feather_path} with {combined_df.shape[0]} rows.")

        return True

    def _validate_date(self, date):
        """
        Validate the requested date to ensure:
        - It's not in the future
        - It's not older than 5 business days from today

        Parameters
        ----------
        date : datetime or datetime-like

        Returns
        -------
        bool
            True if the date is valid for scraping; otherwise False.
        """
        
        if not self.calendar.is_business_day(date):
            self.logger.warning(
                f"Provided date {date.date()} is not a business day. Skipping."
            )
            return False

        if date > self.calendar.today:
            self.logger.warning(
                f"Provided date {date.date()} is in the future. Skipping."
            )
            return False

        day_count = len(self.calendar.day_range(date, self.calendar.today))
        if day_count > 5:
            self.logger.warning(
                f"Provided date {date.date()} is older than 5 business days. Skipping."
            )
            return False

        return True