"""
Google Maps Bank Reviews Scraper for Moroccan Banks - Improved Version
Date: 2025-01-06
Description: Enhanced scraper that first collects all branch URLs, then scrapes reviews
"""

import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, asdict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
from pathlib import Path
import re


# Configure logging
def setup_logging(level=logging.INFO):
    """Configure logging with the specified level"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('scraper.log', encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()


@dataclass
class BankBranch:
    """Data class for bank branch information"""
    bank_name: str
    branch_name: str
    branch_url: str
    address: str
    rating: Optional[float] = None
    review_count: Optional[int] = None


@dataclass
class Review:
    """Data class for storing review information"""
    bank_name: str
    branch_name: str
    branch_address: str
    branch_url: str
    reviewer_name: str
    rating: float
    review_text: str
    review_date: str
    helpful_count: int = 0
    response_from_owner: Optional[str] = None
    scraped_at: str = datetime.now().isoformat()


class GoogleMapsScraper:
    """Enhanced scraper for Google Maps reviews"""
    
    def __init__(self, headless: bool = False, wait_time: int = 15, max_branches_per_bank: int = None):
        self.headless = headless
        self.wait_time = wait_time
        self.max_branches_per_bank = max_branches_per_bank  # Limit branches for testing
        self.driver = None
        self.branches_collected = []
        self.reviews_collected = []
        
    def setup_driver(self):
        """Initialize Chrome driver with optimal settings"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--window-size=1920,1080")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        logger.info("Chrome driver initialized successfully")
        
    def search_bank(self, bank_name: str, city: str = "") -> None:
        """Search for a bank and wait for results"""
        query = f"{bank_name} {city} Morocco" if city else f"{bank_name} Morocco"
        search_url = f"https://www.google.com/maps/search/{query}"
        
        logger.info(f"Searching for: {query}")
        self.driver.get(search_url)
        time.sleep(5)  # Wait for initial load
        
        # Wait for results container
        try:
            WebDriverWait(self.driver, self.wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[role='main']"))
            )
            time.sleep(2)
        except TimeoutException:
            logger.error(f"Timeout waiting for search results for: {query}")
            
    def scroll_search_results(self) -> None:
        """Scroll through search results to load all branches"""
        try:
            # Find the scrollable container - it's usually a div with role="main" or similar
            scrollable_divs = self.driver.find_elements(By.CSS_SELECTOR, "[role='main'] [tabindex='-1'], [role='main'] > div > div")
            
            if not scrollable_divs:
                logger.warning("No scrollable container found")
                return
                
            scrollable_div = None
            for div in scrollable_divs:
                # Check if this div is scrollable
                height = self.driver.execute_script("return arguments[0].scrollHeight", div)
                if height > 0:
                    scrollable_div = div
                    break
                    
            if not scrollable_div:
                logger.warning("No valid scrollable container found")
                return
                
            logger.info("Found scrollable container, starting to scroll")
            
            # Hover over the container to ensure it's active
            actions = ActionChains(self.driver)
            actions.move_to_element(scrollable_div).perform()
            time.sleep(1)
            
            last_count = 0
            no_change_count = 0
            max_scrolls = 20
            
            for i in range(max_scrolls):
                # Count current results
                results = self.driver.find_elements(By.CSS_SELECTOR, "a.hfpxzc")
                current_count = len(results)
                
                logger.info(f"Scroll {i+1}: Found {current_count} results")
                
                # Scroll down
                self.driver.execute_script(
                    "arguments[0].scrollTop = arguments[0].scrollHeight", 
                    scrollable_div
                )
                time.sleep(2)
                
                # Check if we've reached the end
                if current_count == last_count:
                    no_change_count += 1
                    if no_change_count >= 3:
                        logger.info("No new results after 3 scrolls, stopping")
                        break
                else:
                    no_change_count = 0
                    
                last_count = current_count
                
        except Exception as e:
            logger.error(f"Error while scrolling search results: {str(e)}")
            
    def extract_branch_links(self, bank_name: str) -> List[BankBranch]:
        """Extract all branch information from search results"""
        branches = []
        
        try:
            # Find all result links
            result_elements = self.driver.find_elements(By.CSS_SELECTOR, "a.hfpxzc")
            logger.info(f"Found {len(result_elements)} potential branches")
            
            for element in result_elements:
                try:
                    # Get the parent container for this result
                    parent = element.find_element(By.XPATH, "./parent::div/parent::div")
                    
                    # Extract branch name
                    try:
                        branch_name_elem = parent.find_element(By.CSS_SELECTOR, "div.qBF1Pd.fontHeadlineSmall")
                        branch_name = branch_name_elem.text
                    except:
                        branch_name = bank_name
                    
                    # Extract URL
                    branch_url = element.get_attribute("href")
                    
                    # Extract address
                    try:
                        address_divs = parent.find_elements(By.CSS_SELECTOR, "div.W4Efsd")
                        address = ""
                        for div in address_divs:
                            text = div.text
                            if any(marker in text.lower() for marker in ['bd', 'avenue', 'rue', 'street', '·']):
                                # Clean up address
                                address_parts = text.split('·')
                                if len(address_parts) > 1:
                                    address = address_parts[1].strip()
                                else:
                                    address = text
                                break
                    except:
                        address = "Address not found"
                    
                    # Extract rating and review count
                    rating = None
                    review_count = None
                    try:
                        rating_elem = parent.find_element(By.CSS_SELECTOR, "span.MW4etd")
                        rating = float(rating_elem.text)
                        
                        review_elem = parent.find_element(By.CSS_SELECTOR, "span.UY7F9")
                        review_text = review_elem.text.strip("()")
                        review_count = int(review_text)
                    except:
                        pass
                    
                    branch = BankBranch(
                        bank_name=bank_name,
                        branch_name=branch_name,
                        branch_url=branch_url,
                        address=address,
                        rating=rating,
                        review_count=review_count
                    )
                    
                    branches.append(branch)
                    logger.info(f"Found branch: {branch_name} - {address} ({review_count} reviews)")
                    
                except Exception as e:
                    logger.warning(f"Error extracting branch info: {str(e)}")
                    continue
                    
            # Remove duplicates based on URL
            unique_branches = []
            seen_urls = set()
            for branch in branches:
                if branch.branch_url not in seen_urls:
                    seen_urls.add(branch.branch_url)
                    unique_branches.append(branch)
                    
            logger.info(f"Extracted {len(unique_branches)} unique branches for {bank_name}")
            return unique_branches
            
        except Exception as e:
            logger.error(f"Error extracting branch links: {str(e)}")
            return []
            
    def visit_branch_and_get_reviews(self, branch: BankBranch) -> List[Review]:
        """Visit a specific branch page and extract reviews"""
        reviews = []
        
        try:
            logger.info(f"Visiting branch: {branch.branch_name}")
            self.driver.get(branch.branch_url)
            time.sleep(3)
            
            # Click on reviews tab
            try:
                # Try multiple selectors for the reviews button
                reviews_button = None
                selectors = [
                    "//button[contains(@aria-label, 'Reviews')]",
                    "//button[contains(., 'Reviews')]",
                    "//div[@role='tab'][contains(., 'Reviews')]",
                    "//button[@data-tab-index='1']"
                ]
                
                for selector in selectors:
                    try:
                        reviews_button = self.driver.find_element(By.XPATH, selector)
                        break
                    except:
                        continue
                        
                if reviews_button:
                    reviews_button.click()
                    time.sleep(3)
                    logger.info("Clicked on reviews tab")
                else:
                    logger.warning("Could not find reviews tab")
                    return reviews
                    
            except Exception as e:
                logger.error(f"Error clicking reviews tab: {str(e)}")
                return reviews
                
            # Scroll to load all reviews
            self.scroll_reviews_panel()
            
            # Extract reviews
            reviews = self.extract_reviews_from_page(branch)
            
        except Exception as e:
            logger.error(f"Error visiting branch {branch.branch_name}: {str(e)}")
            
        return reviews
        
    def scroll_reviews_panel(self):
        """Scroll through the reviews panel to load more reviews"""
        try:
            # Find the reviews container
            scrollable_div = None
            selectors = [
                "[role='main'] [tabindex='-1']",
                "[data-review-id]",
                ".m6QErb.DxyBCb.kA9KIf.dS8AEf"
            ]
            
            for selector in selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elements:
                    height = self.driver.execute_script("return arguments[0].scrollHeight", elem)
                    if height > 500:  # Likely the reviews container
                        scrollable_div = elem
                        break
                if scrollable_div:
                    break
                    
            if not scrollable_div:
                logger.warning("Could not find reviews scrollable container")
                return
                
            # Hover over container
            actions = ActionChains(self.driver)
            actions.move_to_element(scrollable_div).perform()
            time.sleep(1)
            
            last_height = 0
            for i in range(10):
                # Scroll down
                self.driver.execute_script(
                    "arguments[0].scrollTop = arguments[0].scrollHeight", 
                    scrollable_div
                )
                time.sleep(2)
                
                new_height = self.driver.execute_script(
                    "return arguments[0].scrollHeight", 
                    scrollable_div
                )
                
                if new_height == last_height:
                    logger.info("Reached end of reviews")
                    break
                    
                last_height = new_height
                logger.info(f"Review scroll {i+1} completed")
                
        except Exception as e:
            logger.error(f"Error scrolling reviews: {str(e)}")
            
    def extract_reviews_from_page(self, branch: BankBranch) -> List[Review]:
        """Extract all reviews from the current page"""
        reviews = []
        seen_reviews = set()  # Track unique reviews to avoid duplicates
        
        try:
            # Wait a bit for reviews to load
            time.sleep(2)
            
            # Find review containers - try multiple selectors
            review_selectors = [
                "[data-review-id]",
                "[jscontroller='fIQYlf']",  # Common review controller
                "div[aria-label*='stars']",  # Review containers with ratings
                ".jftiEf"  # Review section class
            ]
            
            all_review_elements = []
            for selector in review_selectors:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    logger.info(f"Found {len(elements)} potential reviews with selector: {selector}")
                    all_review_elements.extend(elements)
                    
            if not all_review_elements:
                logger.warning("No review elements found with any selector")
                return reviews
            
            # Remove duplicate elements by checking their location
            unique_elements = []
            seen_locations = set()
            
            for element in all_review_elements:
                try:
                    location = element.location
                    loc_key = f"{location['x']},{location['y']}"
                    if loc_key not in seen_locations:
                        seen_locations.add(loc_key)
                        unique_elements.append(element)
                except:
                    continue
                    
            logger.info(f"Found {len(unique_elements)} unique review elements after deduplication")
            
            for idx, element in enumerate(unique_elements):
                try:
                    logger.debug(f"Processing review {idx + 1}")
                    
                    # Extract reviewer name - updated selectors
                    reviewer_name = "Anonymous"
                    name_selectors = [
                        ".d4r55",
                        "button.WEBjve",
                        "div.WEBjve",
                        "[data-review-id] button[aria-label]",
                        ".kvMYJc",
                        "a[href*='/contrib/']"
                    ]
                    
                    for selector in name_selectors:
                        try:
                            name_elem = element.find_element(By.CSS_SELECTOR, selector)
                            reviewer_name = name_elem.text.strip()
                            if reviewer_name and reviewer_name != "More":
                                logger.debug(f"Found reviewer name: {reviewer_name}")
                                break
                        except:
                            continue
                    
                    # Extract rating - updated approach
                    rating = 0
                    try:
                        # Try to find rating from aria-label
                        rating_elem = element.find_element(By.CSS_SELECTOR, "[role='img'][aria-label*='star']")
                        rating_text = rating_elem.get_attribute("aria-label")
                        rating_match = re.search(r'(\d+)\s*star', rating_text)
                        if rating_match:
                            rating = float(rating_match.group(1))
                            logger.debug(f"Found rating: {rating}")
                    except:
                        # Alternative: count filled stars
                        try:
                            filled_stars = element.find_elements(By.CSS_SELECTOR, ".hCCjke.vzX5Ic")  # Filled star class
                            if filled_stars:
                                rating = len(filled_stars)
                                logger.debug(f"Found rating from star count: {rating}")
                        except:
                            pass
                    
                    # Extract review text - updated selectors
                    review_text = ""
                    text_selectors = [
                        ".wiI7pd",
                        ".MyEned",
                        "span.wiI7pd",
                        "[data-review-id] > div > div > div > span",
                        ".Jtu6Td > span"
                    ]
                    
                    for selector in text_selectors:
                        try:
                            # Try finding within the review element first
                            text_elem = element.find_element(By.CSS_SELECTOR, selector)
                            review_text = text_elem.text.strip()
                            if review_text and review_text != "More" and len(review_text) > 5:
                                logger.debug(f"Found review text: {review_text[:50]}...")
                                break
                        except:
                            continue
                    
                    # Extract date - updated selectors
                    review_date = ""
                    date_selectors = [
                        ".rsqaWe",
                        "span.rsqaWe",
                        ".DU9Pgb > span",
                        "[class*='fontBodyMedium'] span"
                    ]
                    
                    for selector in date_selectors:
                        try:
                            date_elem = element.find_element(By.CSS_SELECTOR, selector)
                            review_date = date_elem.text.strip()
                            if review_date and any(time_word in review_date.lower() for time_word in ['ago', 'year', 'month', 'day', 'week']):
                                logger.debug(f"Found review date: {review_date}")
                                break
                        except:
                            continue
                    
                    # Create unique key to avoid duplicates
                    review_key = f"{reviewer_name}|{rating}|{review_text[:50] if review_text else ''}|{review_date}"
                    
                    # Only add review if we have meaningful data and it's not a duplicate
                    if review_key not in seen_reviews and (rating > 0 or (review_text and len(review_text) > 5)):
                        seen_reviews.add(review_key)
                        
                        # Create review object
                        review = Review(
                            bank_name=branch.bank_name,
                            branch_name=branch.branch_name,
                            branch_address=branch.address,
                            branch_url=branch.branch_url,
                            reviewer_name=reviewer_name,
                            rating=rating,
                            review_text=review_text,
                            review_date=review_date
                        )
                        
                        reviews.append(review)
                        logger.info(f"Successfully extracted review {len(reviews)}: {reviewer_name} - {rating} stars")
                    else:
                        logger.debug(f"Skipping duplicate or empty review: {review_key}")
                    
                except Exception as e:
                    logger.warning(f"Error extracting individual review {idx + 1}: {str(e)}")
                    continue
                    
            logger.info(f"Successfully extracted {len(reviews)} unique reviews with data")
            return reviews
            
        except Exception as e:
            logger.error(f"Error extracting reviews: {str(e)}")
            return reviews
            
    def scrape_bank_branches(self, bank_name: str, cities: List[str] = None):
        """Main method to scrape all branches and reviews for a bank"""
        all_branches = []
        
        # Search in different cities if provided
        if cities:
            for city in cities:
                logger.info(f"\n--- Searching {bank_name} in {city} ---")
                self.search_bank(bank_name, city)
                self.scroll_search_results()
                branches = self.extract_branch_links(bank_name)
                logger.info(f"Found {len(branches)} branches in {city}")
                all_branches.extend(branches)
                time.sleep(3)
        else:
            # General search
            logger.info(f"\n--- Searching {bank_name} (general) ---")
            self.search_bank(bank_name)
            self.scroll_search_results()
            all_branches = self.extract_branch_links(bank_name)
            
        # Remove duplicates
        unique_branches = []
        seen_urls = set()
        for branch in all_branches:
            if branch.branch_url not in seen_urls:
                seen_urls.add(branch.branch_url)
                unique_branches.append(branch)
                
        logger.info(f"\nFound {len(unique_branches)} unique branches for {bank_name}")
        logger.info(f"Branch names: {[b.branch_name for b in unique_branches[:5]]}...")  # Show first 5
        
        self.branches_collected.extend(unique_branches)
        
        # Visit each branch and collect reviews
        logger.info(f"\n--- Starting to collect reviews from {len(unique_branches)} branches ---")
        
        for i, branch in enumerate(unique_branches):
            logger.info(f"\n[{i+1}/{len(unique_branches)}] Processing: {branch.branch_name}")
            logger.info(f"  Address: {branch.address}")
            logger.info(f"  URL: {branch.branch_url[:80]}...")
            
            try:
                reviews = self.visit_branch_and_get_reviews(branch)
                logger.info(f"  ✓ Collected {len(reviews)} reviews")
                self.reviews_collected.extend(reviews)
                
                # Show sample of reviews collected
                if reviews:
                    logger.info(f"  Sample review: {reviews[0].reviewer_name} - {reviews[0].rating}★")
                    
            except Exception as e:
                logger.error(f"  ✗ Error collecting reviews: {str(e)}")
                
            # Respect rate limits
            time.sleep(5)
            
        logger.info(f"\n--- Completed {bank_name} ---")
        logger.info(f"Total reviews for {bank_name}: {len([r for r in self.reviews_collected if r.bank_name == bank_name])}")
            
    def save_data(self, final=False):
        """Save both branches and reviews data"""
        output_dir = Path("data/raw")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Use consistent timestamp for the entire session
        if not hasattr(self, 'session_timestamp'):
            self.session_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
        timestamp = self.session_timestamp
        
        # Add prefix to differentiate progress saves from final saves
        prefix = "final_" if final else "progress_"
        
        # Save branches
        if self.branches_collected:
            branches_df = pd.DataFrame([asdict(b) for b in self.branches_collected])
            branches_file = output_dir / f"{prefix}bank_branches_{timestamp}.csv"
            branches_df.to_csv(branches_file, index=False, encoding='utf-8')
            logger.info(f"Saved {len(self.branches_collected)} branches to {branches_file}")
            
        # Save reviews
        if self.reviews_collected:
            reviews_df = pd.DataFrame([asdict(r) for r in self.reviews_collected])
            reviews_file = output_dir / f"{prefix}bank_reviews_{timestamp}.csv"
            reviews_df.to_csv(reviews_file, index=False, encoding='utf-8')
            
            # Also save as JSON
            reviews_json = output_dir / f"{prefix}bank_reviews_{timestamp}.json"
            with open(reviews_json, 'w', encoding='utf-8') as f:
                json.dump([asdict(r) for r in self.reviews_collected], f, ensure_ascii=False, indent=2)
                
            logger.info(f"Saved {len(self.reviews_collected)} reviews to {reviews_file}")
            
            # Save a summary file
            summary_file = output_dir / f"{prefix}scraping_summary_{timestamp}.txt"
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(f"Scraping Summary - {datetime.now()}\n")
                f.write(f"{'='*50}\n")
                f.write(f"Total branches found: {len(self.branches_collected)}\n")
                f.write(f"Total reviews collected: {len(self.reviews_collected)}\n")
                f.write(f"\nBranches by bank:\n")
                
                bank_counts = {}
                for branch in self.branches_collected:
                    bank_counts[branch.bank_name] = bank_counts.get(branch.bank_name, 0) + 1
                    
                for bank, count in bank_counts.items():
                    f.write(f"  {bank}: {count} branches\n")
            
    def close(self):
        """Close the driver"""
        if self.driver:
            self.driver.quit()
            logger.info("Driver closed")


# Configuration for Moroccan banks and cities
MOROCCAN_BANKS = [
    "CIH Bank"
]

# [
#     "BMCE Bank of Africa",
#     "Société Générale Maroc",
#     "CIH Bank",
#     "Crédit du Maroc",
#     "Crédit Agricole du Maroc",
#     "Bank Al-Maghrib",
#     "Attijariwafa Bank",
#     "Banque Populaire",
# ]

MAJOR_CITIES = [
    "Rabat"
]

# [
#     "Casablanca",
#     "Rabat",
#     "Marrakech",
#     "Fès",
#     "Tanger",
#     "Agadir",
#     "Meknès",
#     "Oujda"
# ]


def main():
    """Main execution function"""
    scraper = GoogleMapsScraper(headless=True)
    
    try:
        scraper.setup_driver()
        
        # Scrape each bank
        for i, bank in enumerate(MOROCCAN_BANKS):  # Start with first 2 banks for testing
            logger.info(f"\n{'='*50}")
            logger.info(f"Starting to scrape bank {i+1}/{len(MOROCCAN_BANKS)}: {bank}")
            logger.info(f"{'='*50}")
            
            # Search in major cities
            scraper.scrape_bank_branches(bank, MAJOR_CITIES)  # Start with first 3 cities
            
            # Optional: Save progress after each bank (creates intermediate files)
            # Uncomment the next line if you want progress saves
            # scraper.save_data(final=False)
            
            # Longer pause between banks
            time.sleep(10)
            
        logger.info(f"\nScraping completed!")
        logger.info(f"Total branches found: {len(scraper.branches_collected)}")
        logger.info(f"Total reviews collected: {len(scraper.reviews_collected)}")
        
        # Save final data only once at the end
        scraper.save_data(final=True)
    
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        # Save whatever data we have
        scraper.save_data(final=True)
        scraper.close()
        
    finally:
        scraper.close()


def test_single_branch():
    """Test function to debug a single branch"""
    scraper = GoogleMapsScraper(headless=False)
    
    try:
        scraper.setup_driver()
        
        # Test with a specific branch URL
        test_branch = BankBranch(
            bank_name="Attijariwafa Bank",
            branch_name="Test Branch",
            branch_url="https://www.google.com/maps/place/Attijariwafa+Bank/data=!4m7!3m6!1s0xda7d2f41adc6d3d:0x84e2d59c9af9fefa!8m2!3d33.6005462!4d-7.6380016!16s%2Fg%2F11f1zld805!19sChIJPW3cGvTSpw0R-v75mpzV4oQ",
            address="Test Address"
        )
        
        logger.info("Testing single branch review extraction...")
        reviews = scraper.visit_branch_and_get_reviews(test_branch)
        
        logger.info(f"Extracted {len(reviews)} reviews")
        for i, review in enumerate(reviews[:5]):  # Show first 5 reviews
            logger.info(f"Review {i+1}:")
            logger.info(f"  Reviewer: {review.reviewer_name}")
            logger.info(f"  Rating: {review.rating}")
            logger.info(f"  Text: {review.review_text[:100]}..." if review.review_text else "  Text: [Empty]")
            logger.info(f"  Date: {review.review_date}")
            
    except Exception as e:
        logger.error(f"Test error: {str(e)}")
        raise
        
    finally:
        scraper.close()


if __name__ == "__main__":
    # Uncomment the line below to run the test function
    # test_single_branch()
    
    # Normal execution
    main()
