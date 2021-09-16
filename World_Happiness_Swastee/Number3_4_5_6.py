import csv
from decimal import Decimal
import geopandas as gpd
import holoviews as hv
import hvplot.pandas
import json
import numpy as np
import os
import pandas as pd
import pypyodbc as odbc
import world_bank_data as wb

class DecimalEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Decimal):
      return str(obj)
    return json.JSONEncoder.default(self, obj)

def getHappinessStatus(happinessScore):
    if (happinessScore < 2.6): status = "Red"
    elif (happinessScore < 5.6): status = "Amber"
    else: status = "Green"
    return status

def getRegionInUpperCase(regionJson):

    if not regionJson: region = np.nan
    else: region = regionJson.upper()
    return region

DRIVER = 'SQL Server'
SERVER_NAME = 'L-R910128K'
DATABASE_NAME = 'WorldHappinessReport'

#establish the database connection
try:
    conn = odbc.connect('DRIVER={}; SERVER={}; DATABASE={}; Trusted_Connection=yes;'.format(DRIVER, SERVER_NAME, DATABASE_NAME))
except odbc.DataError as e:
    print(f'Database Error: {str(e.value[1])}')
except odbc.Error as e:
    print(f'Connection Error: {str(e.value[1])}')

cursor = conn.cursor() 
reportDbDetails = cursor.execute('''
        SELECT TrackingYear, CountryName, RANK() OVER(PARTITION BY TrackingYear ORDER BY HappinessScore DESC) AS 'OverallRank', HappinessScore, Economy, 
        Family, SocialSupport, Health, Freedom, Generosity, Trust
        FROM HappinessReportDetails LEFT JOIN Country
        ON HappinessReportDetails.CountryId = Country.CountryId
    '''
).fetchall()
countryDetails = cursor.execute('''
        SELECT CountryName FROM Country
    '''
).fetchall()
conn.commit()

#load the 'countries_continents_codes_flags_url' json file
with open(os.path.join(os.getcwd(), 'Data Files\countries_continents_codes_flags_url.json')) as jsonFile:
  jsonDetails = json.load(jsonFile)

#create a new csv file - Number3
with open(os.path.join(os.getcwd(), 'Number3.csv'), 'w', newline = '') as file:
    writer = csv.writer(file)
    #write the header
    writer.writerow(['Year', 'Country', 'Country Url', 'Region Code', 'Region', 'Rank Per Region', 'Overall Rank', 'Happiness Score', 'Happiness Status', 
    'GDP per capita', 'Family', 'Social Support', 'Healthy life expectancy', 'Freedom to make life choices', 'Generosity', 'Perceptions of corruption', 'alpha-2', 'alpha-3'])

    #iterate over each row for the World Happiness Report details and store the corresponding values
    for dbDetail in reportDbDetails:
        year, country, overallRank, happinessScore, economy, family, socialSupport, health, freedom, generosity, trust = dbDetail['trackingyear'], dbDetail['countryname'], dbDetail['overallrank'], dbDetail['happinessscore'], dbDetail['economy'], dbDetail['family'], dbDetail['socialsupport'], dbDetail['health'], dbDetail['freedom'], dbDetail['generosity'], dbDetail['trust']

        #iterate over each record for the json details
        for jsonDetail in jsonDetails:
            #if the country from the particular json record matches that from the row of the World Happiness Report details - write to the 'Number3' csv file
            if jsonDetail['country'] == country:

                writer.writerow([year, country, jsonDetail['image_url'], jsonDetail['region-code'], getRegionInUpperCase(jsonDetail['region']), jsonDetail['region'], overallRank, happinessScore, 
                getHappinessStatus(happinessScore), economy, family, socialSupport, health, freedom, generosity, trust, jsonDetail['alpha-2'], jsonDetail['alpha-3']])
                continue

df = pd.read_csv('Number3.csv')
#convert to parquet format
df.to_parquet('Number3.parquet')

###########################################################################################################################################

jsonList =[]
#iterate over each country
for country in countryDetails:

    #fetch the highest rank, lowest rank, highest happiness score and lowest happiness score
    individualCountry = [detail for detail in reportDbDetails if country['countryname'] in detail['countryname']]
    highestRank, lowestRank = max(individualCountry, key= lambda item: item['overallrank'])['overallrank'], min(individualCountry, key= lambda item: item['overallrank'])['overallrank']
    highestHappinessScore, lowestHappinessScore = max(individualCountry, key= lambda item: item['happinessscore'])['happinessscore'], min(individualCountry, key= lambda item: item['happinessscore'])['happinessscore']

    jsonList.append({ 
        "Country" : country[0], 
        "Highest-Rank" : highestRank, 
        "Lowest-Rank" : lowestRank, 
        "Highest-Happiness-Score" : highestHappinessScore, 
        "Lowest-Happiness-Score" : lowestHappinessScore 
    })

#create a new json file - Number4.json and dump the list to the json file
with open(os.path.join(os.getcwd(), 'Number4.json'), 'w') as file:
    json.dump(jsonList, file, indent=2, cls=DecimalEncoder)

################################################################################################################################################

#create a dataframe having only the necessary columns - 'Year', 'Country', 'Happiness Score', 'Happiness Status'
#'alpha-2' and 'alpha-3' will be used for merging purposes
filteredWorldHappiness = pd.read_csv('Number3.csv', usecols=['Year', 'Country', 'Happiness Score', 'Happiness Status', 'alpha-2', 'alpha-3'])

world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
worldHappinessFinal = world.merge(filteredWorldHappiness, how="left", left_on=['iso_a3'], right_on=['alpha-3'])

figure = worldHappinessFinal.hvplot(c="Happiness Score",
                             cmap="BrBG",
                             hover_fill_color="blue",
                             width=1000,height=800,
                             title="World Happiness Report",
                             groupby = 'Year',
                             geo = True
                            )
hv.save(figure, 'Number5.html')

###################################################################################################################################################

#fetch countries from the World Bank Data API - only the necessary columns are selected
getCountries = wb.get_countries()[['iso2Code', 'capitalCity', 'longitude', 'latitude']]
dfNum3 = pd.read_csv('Number3.csv')
mergeDfNum3WorldBank = dfNum3.merge(getCountries, how="left", left_on=['alpha-2'], right_on=['iso2Code'])
print(mergeDfNum3WorldBank)