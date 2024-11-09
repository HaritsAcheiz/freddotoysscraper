from scraper import FTScraper

if __name__ == '__main__':
	scraper = FTScraper()
	# scraper.fetch_search_result_html('https://freddotoys.com/collections/all')
	# product_urls = scraper.get_product_urls()
	# scraper.fetch_product_html(product_urls)
	product_datas = scraper.get_data()
	scraper.create_csv(product_datas, 'raw_freddotoys_products.csv')
