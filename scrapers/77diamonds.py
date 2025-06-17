from scrapers.base_scraper import BaseScraper
from selenium.webdriver.common.by import By
import time

class SeventySevenScraper(BaseScraper):
    def scrape(self, url):
        self.logger.info(f"Scraping: {url}")
        self.driver.get(url)
        time.sleep(2)  # Better to use WebDriverWait

        data = {
            "url": url,
            "title": self.driver.title,
            "price": None,
            "description": None,
            "variants": {},
            "dom_html": self.driver.page_source
        }

        try:
            price_elem = self.driver.find_element(By.CLASS_NAME, "js-price-value")
            data["price"] = price_elem.text.strip()
        except Exception as e:
            self.logger.warning(f"Price not found: {e}")

        try:
            desc = self.driver.find_element(By.CSS_SELECTOR, ".product-details__text")
            data["description"] = desc.text.strip()
        except Exception:
            pass

        # Example for selecting a variant (e.g., Shape)
        try:
            shape_elements = self.driver.find_elements(By.CSS_SELECTOR, ".shape-selector__shape")
            variants = []
            for elem in shape_elements:
                variants.append(elem.text.strip())
            data["variants"]["shapes"] = variants
        except Exception:
            pass

        return data
