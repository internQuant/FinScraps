import time
import pandas as pd
import requests
import xml.etree.ElementTree as ET

class AnbimaIRTSScraper:
    """A scraper for retrieving and parsing IRTS (Term Structure) data from ANBIMA in XML format.
    """

    def __init__(self):
        self.url = "https://www.anbima.com.br/informacoes/est-termo/CZ-down.asp"

    def download_xml(self, date) -> bytes:
        """Download the XML content from ANBIMA for a given date, with simple retry logic.

        Parameters
        ----------
        date : datetime-like

        Returns
        -------
        bytes: The raw XML content in bytes.

        Raises
        ------
        requests.exceptions.RequestException: If the request ultimately fails after all retries.
        """
        date_str = pd.Timestamp(date).strftime("%d/%m/%Y")
        form_data = {
            "Idioma": "PT",
            "Dt_Ref": date_str,
            "saida": "xml"
        }

        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = requests.post(self.url, data=form_data)
                response.raise_for_status()
                return response.content
            except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
                if attempt < max_retries - 1:
                    time.sleep(1) 
                else:
                    raise e

    def parse_params(self, xml_content, date):
        """Parse the XML content to extract IRTS parameters.

        Parameters
        ----------
        xml_content : The raw XML content in bytes.
        date : The date to associate with the extracted parameters (same date used in download_xml).

        Returns
        -------
        list of dict A list of dictionaries, each containing parameters 
        """
        
        root = ET.fromstring(xml_content)
        parametros = []

        type_mapping = {
            "PREFIXADOS": "pre",
            "IPCA": "ipca"
        }

        def convert(value):
            """Convert a string with decimal comma to float. Returns None if value is empty or None.
            """
            return float(value.replace(',', '.')) if value else None

        for parametro in root.findall(".//PARAMETRO"):
            original_type = parametro.get("Grupo")
            renamed_type = type_mapping.get(original_type, original_type)

            parametros.append({
                "date": pd.Timestamp(date),
                "type": renamed_type,
                "b1": convert(parametro.get("B1")),
                "b2": convert(parametro.get("B2")),
                "b3": convert(parametro.get("B3")),
                "b4": convert(parametro.get("B4")),
                "l1": convert(parametro.get("L1")),
                "l2": convert(parametro.get("L2")),
            })

        return parametros

    def scrape(self, date):
        """Download and parse parameters for a given date, returning a DataFrame.

        Parameters
        ----------
        date : The date for which data is to be downloaded and parsed.

        Returns
        -------
        DataFrame containing the scraped parameters on the provided date.
        """
        xml_content = self.download_xml(date)
        parametros = self.parse_params(xml_content, date)
        return pd.DataFrame(parametros)