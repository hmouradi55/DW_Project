# scraper_utils.py
"""
Reusable scraper utilities extracted from the main scraper
"""
import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

logger = logging.getLogger(__name__)


class GoogleMapsUtils:
    """Utility class for Google Maps scraping operations"""
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        
    def setup_driver(self):
        """Initialize Chrome driver"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--window-size=1920,1080")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
    def get_address_from_url(self, url: str) -> dict:
        """Extract address and other details from a Google Maps URL"""
        try:
            self.driver.get(url)
            time.sleep(3)  # Wait for page to load
            
            # Try multiple selectors for address
            address_selectors = [
                "button[data-item-id='address']",
                "button[aria-label*='Address:']",
                "[data-tooltip='Copy address']",
                "div.rogA2c .Io6YTe"  # Direct address text
            ]
            
            address = None
            for selector in address_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    # Try to get from aria-label first
                    aria_label = element.get_attribute('aria-label')
                    if aria_label and 'Address:' in aria_label:
                        address = aria_label.replace('Address:', '').strip()
                        break
                    
                    # Otherwise get text content
                    address_elem = element.find_element(By.CSS_SELECTOR, ".Io6YTe")
                    if address_elem:
                        address = address_elem.text.strip()
                        break
                except:
                    continue
                    
            # Also try to get phone number and hours
            phone = None
            try:
                phone_elem = self.driver.find_element(By.CSS_SELECTOR, "button[data-item-id='phone']")
                phone = phone_elem.get_attribute('aria-label').replace('Phone:', '').strip()
            except:
                pass
                
            return {
                'url': url,
                'address': address,
                'phone': phone,
                'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            logger.error(f"Error extracting address from {url}: {str(e)}")
            return {
                'url': url,
                'address': None,
                'phone': None,
                'error': str(e)
            }
            
    def close(self):
        """Close the driver"""
        if self.driver:
            self.driver.quit()