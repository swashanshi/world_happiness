import glob 
import os
import pypyodbc as odbc
import pandas as pd
import re

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

#Iterate over each file in the 'Data Files' folder
for file in glob.glob(os.path.join(os.getcwd(), 'Data Files\*.csv')):

    #use regex to extract the year part from the filename - assumption: the corresponding year has been appended to each data file
    trackingYear = re.compile(r'\d+').search(file).group(0)
    #create the dataframe
    df = pd.read_csv(file) 

    #replace the column names to be consistent for each dataframe
    df.columns = df.columns.str.replace(r'^.*Country.*$', 'CountryName', regex=True)
    df.columns = df.columns.str.replace(r'^.*Score.*$', 'HappinessScore', regex=True)
    df.columns = df.columns.str.replace(r'^.*[C|c]apita.*$', 'Economy', regex=True)
    df.columns = df.columns.str.replace(r'^.*Social.*$', 'SocialSupport', regex=True)
    df.columns = df.columns.str.replace(r'^.*Health.*$', 'Health', regex=True)
    df.columns = df.columns.str.replace(r'^.*Freedom.*$', 'Freedom', regex=True)
    df.columns = df.columns.str.replace(r'^.*[C|c]orruption.*$', 'Trust', regex=True)
    df.columns = df.columns.str.replace(r'^.*Dystopia.*$', 'DystopiaResidual', regex=True)

    #drop rows having null or na values
    df = df.dropna()

    #iterate over each row of the dataframe
    for row in df.itertuples():

        cursor = conn.cursor() 
        countryId = cursor.execute('''
            SELECT CountryId FROM Country WHERE CountryName = ?
        ''',
        (row.CountryName,)
        ).fetchone()
        #if the countryId is null - skip the insert sql statement and get the value for the 'countryId' accordingly
        if (countryId == None):
            cursor.execute('''
                SET NOCOUNT ON;
                DECLARE @NEWID TABLE(ID INT);
                INSERT INTO Country
                OUTPUT inserted.CountryId INTO @NEWID    
                VALUES (?)

                SELECT ID FROM @NEWID
            ''',
            (row.CountryName,)
            )
            countryId = cursor.fetchone()[0]
        else:
            countryId = str(countryId[0])
            
        #if the 'Family' column exists in the dataframe - assign the 'family' variable the corresponding value otherwise assign it zero
        if 'Family' in df.columns: family = row.Family
        else: family = 0
        #if the 'SocialSupport' column exists in the dataframe - assign the 'socialSupport' variable the corresponding value otherwise assign it zero
        if 'SocialSupport' in df.columns: socialSupport = row.SocialSupport
        else: socialSupport = 0
        #if the 'DystopiaResidual' column exists in the dataframe - assign the 'dystopiaResidual' variable the corresponding value otherwise assign it zero
        if 'DystopiaResidual' in df.columns: dystopiaResidual = row.DystopiaResidual
        else: dystopiaResidual = 0

        cursor.execute('''
            INSERT INTO HappinessReportDetails(CountryId, TrackingYear, HappinessScore, Economy, Family, SocialSupport, Health, Freedom, Trust, Generosity, DystopiaResidual)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (countryId, trackingYear, row.HappinessScore, row.Economy, family, socialSupport, row.Health, row.Freedom, row.Trust, row.Generosity, dystopiaResidual)
        )
conn.commit()
