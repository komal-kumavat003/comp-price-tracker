from helpers.webdriver_manager import get_firefox_driver
# from sites.site_77diamonds import scrape_77diamonds
# from sites.site_brilliantearth import scrape_brilliantearth
# from sites.site_diamondsfactory import scrape_diamondsfactory


def main():
    driver = get_firefox_driver(headless=False)

    data = []
    try:
        url = "https://www.diamondsfactory.co.uk/design/white-gold-round-diamond-engagement-ring-clrn34901?search=clrn349_01"
        driver.get(url)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
