import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

url25 = 'https://www.tsa.gov/travel/passenger-volumes/'
html = requests.get(url25).text

soup = BeautifulSoup(html, 'html5lib')

table = soup.find_all('tbody')[0]
rows = table.find_all('tr')

dates = []
passengers = []
for row in rows:
    vals = row.find_all('td')
    dates.append(vals[0].get_text())
    passengers.append(float(vals[1].get_text().replace(',', '')))

df = pd.DataFrame({'date':dates[::-1], 'passengers':passengers[::-1]})
df.to_csv('passengers25.csv')

