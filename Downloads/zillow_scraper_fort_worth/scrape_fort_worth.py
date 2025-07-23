
import requests
from bs4 import BeautifulSoup
import csv

def scrape_fort_worth():
    url = "https://www.zillow.com/fort-worth-tx/"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Failed to retrieve data")
        return

    soup = BeautifulSoup(response.text, "html.parser")

    listings = soup.find_all("article")
    results = []

    for listing in listings:
        address_tag = listing.find("address")
        price_tag = listing.find("span", {"data-test": "property-card-price"})

        if address_tag and price_tag:
            address = address_tag.text.strip()
            price = price_tag.text.strip()
            results.append([address, price])

    with open("leads.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["address", "price"])
        writer.writerows(results)

    print("Scraping complete. Results saved to leads.csv")

if __name__ == "__main__":
    scrape_fort_worth()
