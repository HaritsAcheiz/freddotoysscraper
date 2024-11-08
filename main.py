from scraper import FTScraper

if __name__ == '__main__':
	urls = [
		'https://freddotoys.com/products/fr6446-freddo-e-chopper-36v-1-seater-leather-seat',
		'https://freddotoys.com/products/12v-freddo-firetruck-1-seater-ride-on',
		'https://freddotoys.com/products/24v-lamborghini-huracan-2-seater-kids-electric-ride-on'
	]

	search_results_url = 'https://www.redcatracing.com/pages/search-results?findify_offset=3600'

	scraper = FTScraper()
	scraper.run(urls)
