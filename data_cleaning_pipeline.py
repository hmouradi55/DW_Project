# data_cleaning_pipeline.py
"""
Data cleaning and enrichment pipeline for bank reviews data
"""
import pandas as pd
import json
import re
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
import sys
import os
import time
from dateutil.relativedelta import relativedelta

# Add the directory containing scraper_utils to Python path
sys.path.append('.')  # Adjust based on your project structure

from scraper_utils import GoogleMapsUtils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_cleaning.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class DataCleaningPipeline:
    """Main pipeline for cleaning and enriching bank data"""
    
    def __init__(self, input_dir: str = "data/raw", output_dir: str = "data/cleaned"):
        project_root = Path(__file__).resolve().parent       # folder where the .py file lives
        self.input_dir  = project_root / "data" / "raw"
        self.output_dir = project_root / "data" / "cleaned"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # File paths
        for f in os.listdir(self.input_dir):
            if f.startswith("final_bank_branches") and f.endswith(".csv"):
                self.branches_file = f
                break
        else:
            raise FileNotFoundError("No file starting with 'final_bank_branches' was found.")
        for f in os.listdir(self.input_dir):
            if f.startswith("final_bank_reviews") and f.endswith(".csv"):
                self.reviews_file = f
                break
        else:
            raise FileNotFoundError("No file starting with 'final_bank_reviews' was found.")
        # self.branches_file = "final_bank_branches_20250604_104850.csv"
        # self.reviews_file = "final_bank_reviews_20250604_104850.csv"
        
        # URL to address mapping
        self.url_address_map = {}
        
    def enrich_branch_addresses(self, limit: int = None) -> Dict[str, str]:
        """
        Step 1: Visit each branch URL and extract proper addresses
        
        Args:
            limit: Limit number of branches to process (for testing)
            
        Returns:
            Dictionary mapping URL to address
        """
        logger.info("Starting branch address enrichment...")
        
        # Load branches CSV
        branches_df = pd.read_csv(self.input_dir / self.branches_file)
        logger.info(f"Loaded {len(branches_df)} branches")
        
        # Initialize scraper
        scraper = GoogleMapsUtils(headless=True)
        scraper.setup_driver()
        
        # Process branches
        enriched_data = []
        branches_to_process = branches_df.head(limit) if limit else branches_df
        
        for idx, row in branches_to_process.iterrows():
            logger.info(f"Processing branch {idx + 1}/{len(branches_to_process)}: {row['branch_name']}")
            
            # Get address from URL
            result = scraper.get_address_from_url(row['branch_url'])
            
            # Update mapping
            self.url_address_map[row['branch_url']] = result['address'] or row['address']
            
            # Store enriched data
            enriched_data.append({
                'bank_name': row['bank_name'],
                'branch_name': row['branch_name'],
                'branch_url': row['branch_url'],
                'original_address': row['address'],
                'enriched_address': result['address'],
                'phone': result.get('phone'),
                'rating': row.get('rating'),
                'review_count': row.get('review_count')
            })
            
            # Be respectful
            time.sleep(2)
            
        scraper.close()
        
        # Save enriched branches
        enriched_df = pd.DataFrame(enriched_data)
        output_file = self.output_dir / "branches_enriched.csv"
        enriched_df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Saved enriched branches to {output_file}")
        
        # Save URL-address mapping
        mapping_file = self.output_dir / "url_address_mapping.json"
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(self.url_address_map, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved URL-address mapping to {mapping_file}")
        
        return self.url_address_map
        
    def clean_review_text(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
        """
        Step 2: Remove reviews with weird symbols or empty text
        """
        logger.info("Cleaning review text...")
        initial_count = len(reviews_df)
        
        # Define patterns for weird symbols
        weird_patterns = [
            r'^[\s\n]*$',  # Empty or only whitespace/newlines
            r'^[\W_]+$',   # Only non-word characters
            r'^\s*[\u0000-\u001F\u007F-\u009F]+\s*$',  # Control characters
        ]
        
        # Function to check if text is valid
        def is_valid_text(text):
            if pd.isna(text) or text == '':
                return False
            
            # Check for weird patterns
            for pattern in weird_patterns:
                if re.match(pattern, str(text)):
                    return False
                    
            # Must have at least 3 alphabetic characters
            if len(re.findall(r'[a-zA-Z]', str(text))) < 3:
                return False
                
            return True
        
        # Filter reviews
        reviews_df['is_valid_text'] = reviews_df['review_text'].apply(is_valid_text)
        cleaned_df = reviews_df[reviews_df['is_valid_text']].drop('is_valid_text', axis=1)
        
        removed_count = initial_count - len(cleaned_df)
        logger.info(f"Removed {removed_count} reviews with invalid text ({removed_count/initial_count*100:.2f}%)")
        
        return cleaned_df
        
    def update_branch_addresses(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
        """
        Step 3: Update branch addresses in reviews using the mapping
        """
        logger.info("Updating branch addresses in reviews...")
        
        # Load mapping if not already loaded
        if not self.url_address_map:
            mapping_file = self.output_dir / "url_address_mapping.json"
            if mapping_file.exists():
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    self.url_address_map = json.load(f)
            else:
                logger.warning("No URL-address mapping found. Skipping address update.")
                return reviews_df
                
        # Update addresses
        def get_updated_address(row):
            url = row['branch_url']
            if url in self.url_address_map and self.url_address_map[url]:
                return self.url_address_map[url]
            return row['branch_address']
            
        reviews_df['branch_address_updated'] = reviews_df.apply(get_updated_address, axis=1)
        
        # Count updates
        updates = (reviews_df['branch_address'] != reviews_df['branch_address_updated']).sum()
        logger.info(f"Updated {updates} addresses")
        
        # Replace old address column
        reviews_df['branch_address'] = reviews_df['branch_address_updated']
        reviews_df = reviews_df.drop('branch_address_updated', axis=1)
        
        return reviews_df

    def normalize_review_dates(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert Google-Maps style relative dates (“3 months ago”, “a year ago”, “yesterday”…)
        to absolute YYYY-MM-DD strings, using the `scraped_at` column as the reference.
        Adds:
            • review_date_normalized  (datetime64[ns])
            • review_year
            • review_month
        """

        logger.info("Normalizing review dates…")

        def parse_relative_date(relative: str, scraped_at: str) -> str:
            """Return YYYY-MM-DD."""
            base = pd.to_datetime(scraped_at)

            # empty or NaN → assume “today”
            if pd.isna(relative) or relative == "":
                return base.strftime("%Y-%m-%d")

            text = str(relative).lower().strip()

            # explicit words first
            if text in {"today", "now"}:
                return base.strftime("%Y-%m-%d")
            if text == "yesterday":
                return (base - timedelta(days=1)).strftime("%Y-%m-%d")

            # capture “a month ago”, “3 years ago”, “an hour ago” …
            m = re.match(r"(a|an|\d+)\s+([a-z]+)", text)
            if not m:
                # fallback: give up and return the scrape date
                logger.warning(f"Unrecognised relative date «{relative}» – using scraped_at")
                return base.strftime("%Y-%m-%d")

            n_raw, unit = m.groups()
            n = 1 if n_raw in {"a", "an"} else int(n_raw)

            # pick the right offset object
            if "year" in unit:
                delta = relativedelta(years=n)
            elif "month" in unit:
                delta = relativedelta(months=n)
            elif "week" in unit:
                delta = relativedelta(weeks=n)
            elif "day" in unit:
                delta = timedelta(days=n)
            elif "hour" in unit:
                delta = timedelta(hours=n)
            elif "minute" in unit:
                delta = timedelta(minutes=n)
            else:
                logger.warning(f"Unhandled time unit in «{relative}» – using scraped_at")
                return base.strftime("%Y-%m-%d")

            return (base - delta).strftime("%Y-%m-%d")

        # vectorised apply
        reviews_df["review_date_normalized"] = reviews_df.apply(
            lambda row: parse_relative_date(row["review_date"], row["scraped_at"]),
            axis=1,
        )

        # convenience columns for grouping
        reviews_df["review_date_normalized"] = pd.to_datetime(
            reviews_df["review_date_normalized"]
        )
        reviews_df["review_year"] = reviews_df["review_date_normalized"].dt.year
        reviews_df["review_month"] = reviews_df["review_date_normalized"].dt.month

        return reviews_df

    # def normalize_review_dates(self, reviews_df: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     Step 4: Normalize review dates to standard format
    #     """
    #     logger.info("Normalizing review dates...")
        
    #     def parse_relative_date(review_date: str, scraped_at: str) -> str:
    #         """Convert relative dates to absolute dates"""
    #         try:
    #             # Parse scraped_at date
    #             base_date = pd.to_datetime(scraped_at)
                
    #             if pd.isna(review_date) or review_date == '':
    #                 return base_date.strftime('%Y-%m-%d')
                    
    #             review_date = str(review_date).lower()
                
    #             # Parse relative dates
    #             if 'year' in review_date:
    #                 years = int(re.findall(r'(\d+)', review_date)[0])
    #                 return (base_date - timedelta(days=365 * years)).strftime('%Y-%m-%d')
    #             elif 'month' in review_date:
    #                 months = int(re.findall(r'(\d+)', review_date)[0])
    #                 return (base_date - timedelta(days=30 * months)).strftime('%Y-%m-%d')
    #             elif 'week' in review_date:
    #                 weeks = int(re.findall(r'(\d+)', review_date)[0])
    #                 return (base_date - timedelta(weeks=weeks)).strftime('%Y-%m-%d')
    #             elif 'day' in review_date:
    #                 days = int(re.findall(r'(\d+)', review_date)[0])
    #                 return (base_date - timedelta(days=days)).strftime('%Y-%m-%d')
    #             else:
    #                 return base_date.strftime('%Y-%m-%d')
                    
    #         except Exception as e:
    #             logger.warning(f"Error parsing date '{review_date}': {str(e)}")
    #             return pd.to_datetime(scraped_at).strftime('%Y-%m-%d')
                
    #     # Apply normalization
    #     reviews_df['review_date_normalized'] = reviews_df.apply(
    #         lambda row: parse_relative_date(row['review_date'], row['scraped_at']), 
    #         axis=1
    #     )
        
    #     # Add review year and month for analysis
    #     reviews_df['review_date_normalized'] = pd.to_datetime(reviews_df['review_date_normalized'])
    #     reviews_df['review_year'] = reviews_df['review_date_normalized'].dt.year
    #     reviews_df['review_month'] = reviews_df['review_date_normalized'].dt.month
        
    #     return reviews_df
        
    def run_full_pipeline(self, enrich_addresses: bool = True, limit_branches: int = None):
        """
        Run the complete cleaning pipeline
        """
        logger.info("Starting data cleaning pipeline...")
        
        # Step 1: Enrich branch addresses (optional)
        if enrich_addresses:
            self.enrich_branch_addresses(limit=limit_branches)
        
        # Load reviews
        reviews_df = pd.read_csv(self.input_dir / self.reviews_file)
        logger.info(f"Loaded {len(reviews_df)} reviews")
        
        # Step 2: Clean review text
        reviews_df = self.clean_review_text(reviews_df)
        
        # Step 3: Update branch addresses
        reviews_df = self.update_branch_addresses(reviews_df)
        
        # Step 4: Normalize dates
        reviews_df = self.normalize_review_dates(reviews_df)
        
        # Save cleaned reviews
        output_file = self.output_dir / "reviews_cleaned.csv"
        reviews_df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Saved {len(reviews_df)} cleaned reviews to {output_file}")
        
        # Generate summary statistics
        self.generate_summary_stats(reviews_df)
        
        return reviews_df
        
    def generate_summary_stats(self, reviews_df: pd.DataFrame):
        """Generate summary statistics of the cleaned data"""
        stats = {
            'total_reviews': len(reviews_df),
            'unique_banks': reviews_df['bank_name'].nunique(),
            'unique_branches': reviews_df['branch_name'].nunique(),
            'avg_rating': reviews_df['rating'].mean(),
            'reviews_by_year': reviews_df['review_year'].value_counts().to_dict(),
            'reviews_by_rating': reviews_df['rating'].value_counts().to_dict()
        }
        
        stats_file = self.output_dir / "cleaning_summary.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
            
        logger.info(f"Summary statistics saved to {stats_file}")


def main():
    """Main execution function"""
    pipeline = DataCleaningPipeline()
    
    # Run with options
    # Option 1: Full pipeline with address enrichment (slow but complete)
    pipeline.run_full_pipeline(enrich_addresses=True, limit_branches=None)  # Test with 10 branches
    
    # Option 2: Just cleaning without address enrichment (fast)
    # pipeline.run_full_pipeline(enrich_addresses=False)
    
    logger.info("Data cleaning pipeline completed!")


if __name__ == "__main__":
    main()