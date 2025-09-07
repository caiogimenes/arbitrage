import datetime
import logging

from concurrent.futures import ThreadPoolExecutor


class PipelineIngestion:
    """ Orchestrates the ingestion pipeline by coordinating its components. """
    
    def __init__(self,
                 downloader, fetcher, publisher, 
                 country: str, limit: int,
                 start_date: datetime.datetime,
                 end_date:datetime.datetime):
        
        """Initializes the ingestion pipeline.

        Args:
            fetcher (object): An object responsible for fetching stock tickers for a country.
            downloader (object): An object responsible for downloading historical stock price.
            publisher (object): An object responsible for publishing data to a destination (e.g., Kafka).
            country (str): The country from which to fetch stock tickers (e.g., 'brazil').
            limit (int): The maximum number of tickers to process.
            start_date (datetime.datetime): The start date for downloading historical prices.
            end_date (datetime.datetime): The end date for downloading historical prices.

        """
        self.fetcher = fetcher
        self.downloader = downloader
        self.publisher = publisher
        self.country = country
        self.limit = limit
        self.start_date = start_date
        self.end_date = end_date
        
    def _publish_event(self, args):
        """Helper method to publish data for a single ticker."""

        tick, all_historical_data = args
        
        if tick not in all_historical_data.columns:
            logging.warning(f"None data columns for {tick} in the downloaded DataFrame. Skipping.")
            return
        
              
        self.publisher.publisher(
            data={
                'ticker':tick,
                'timeline':all_historical_data[tick].to_dict('split'),
            },
            asset_count=1,
            source='yfinance-historical-ingestion'        
        )
        
    def run(self):        
        tickers = self.fetcher.fetch(country=self.country, limit=self.limit)
        
        if not tickers:
            logging.warning("No tickers fetched. Pipeline execution finished.")
            return "No tickers found to process."
        
        historical_data = self.downloader(tickers=tickers, 
                                        start_date=self.start_date, 
                                        end_date=self.end_date)
        
        if historical_data.empty:
            logging.error("Failed to download historical data. Pipeline execution finished.")
            return "Historical data download failed."
               
        with ThreadPoolExecutor(max_workers=5) as executor:

            tasks = [(tick, historical_data) for tick in tickers]
            list(executor.map(self._publish_event, tasks))
        
        return f"Events for {len(tasks)} stocks have been processed."


            
            
            
            
        