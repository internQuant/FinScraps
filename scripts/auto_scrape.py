from src.managers.Managers import AnbimaIRTSManager
from src.utils import BRCal
import logging

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(asctime)s - %(name)s - %(message)s")

brcal = BRCal()
br_business_day = brcal.previous_business_day(brcal.today)

if br_business_day:
    AIRTSM = AnbimaIRTSManager()
    AIRTSM.scrape_and_update(br_business_day)
    logging.info("Anbima IRTS data successfully scraped and updated.")
else:
    logging.info("Not a BR business day. Skipping BR scraping routines.")