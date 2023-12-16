#
import numpy as np
import pandas as pd
import pycountry
import pycountry_convert as pc
import pypopulation
import warnings
import glob

#

warnings.filterwarnings("ignore")  #
#

pathGlob_CovidInfection = 'G:/wsp/dataset/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*2023.csv'
#
path_CovidVaccination = 'G:/wsp/dataset/covid-19-data/public/data/vaccinations/vaccinations.csv'
date_filterinDataAfter = pd.to_datetime('2023-01-01')
#

#
#

#
#

#

#
#
#
#
#
#
#
#
#
#
#
#
#
#


#
#
#
#
#
#

#

df_fileCombined = pd.DataFrame()
arr_filePath = glob.glob(pathGlob_CovidInfection)

arr_fileData = []
for filename in arr_filePath:
    data = pd.read_csv(filename, index_col=None, header=0)
    arr_fileData.append(data)

df_fileCombined = pd.concat(arr_fileData, axis=0, ignore_index=True)
df = df_fileCombined
#
#
#
#

#

#

#
#
#
#
#
df['Last_Update'] = pd.to_datetime(df['Last_Update'])
df['Confirmed'] = pd.to_numeric(df['Confirmed'])
df['Deaths'] = pd.to_numeric(df['Deaths'])

df.loc[df['Country_Region'] == 'US', 'Country_Region'] = 'United States'
#

df['Date'] = df['Last_Update'].dt.normalize()  #

df_fileCombined['Year'] = df_fileCombined['Last_Update'].dt.year
df_fileCombined['Month'] = df_fileCombined['Last_Update'].dt.month  #

pd.set_option('display.multi_sparse', False)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)

#

#

#
#
#
#
#
#
#

ser_BrokenData = df['Date'].isin([pd.to_datetime('2023-02-10'), pd.to_datetime('2023-02-11')])
#
df = df[~ser_BrokenData]

#

#


#

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
df = (df.groupby(['Date', 'Country_Region', 'Province_State'], dropna=False, sort=False)
      .agg({'Confirmed': np.sum, 'Deaths': np.sum, 'Lat': np.average, 'Long_': np.average}).reset_index())
#

#
#

#
duplicates = df.duplicated(subset=['Date', 'Country_Region', 'Province_State'], keep=False)
df_duplicates = df[duplicates]
if not df_duplicates.empty:
    print(df_duplicates)
    raise Exception('Duplicate matches found -- there are more than one upload,, at the same day,, for the same country + state')

#
#
#

#
#
df['Confirmed_inc'] = df.groupby(['Country_Region', 'Province_State'], dropna=False, sort=False)['Confirmed'].diff()
df['Deaths_inc'] = df.groupby(['Country_Region', 'Province_State'], dropna=False, sort=False)['Deaths'].diff()
#

#
#
#

#
df_DecreasingCumulative = df.loc[(df['Confirmed_inc'] < 0) | (df['Deaths_inc'] < 0)]
if not df_DecreasingCumulative.empty:
    #
    #
    #
    msg = 'Cumulative data is decreasing... :: \n' + str(df_DecreasingCumulative)
    #
    warnings.warn(msg)
    #

#

df['Year'] = df['Date'].dt.year
df['Month'] = df['Date'].dt.month
#
df['Dayofweek'] = df['Date'].dt.dayofweek


#
#

#

#

#

def get_CountryInfo(country_name):
    country_continent_name = None
    population = None
    country_alpha3 = None
    countryCode_IsoOfficialName = None
    #
    #
    try:
        country_alpha2 = pc.country_name_to_country_alpha2(country_name)
        country_alpha3 = pc.map_country_alpha2_to_country_alpha3().get(country_alpha2)
        #
        countryCode_IsoOfficialName = pycountry.countries.get(alpha_2=country_alpha2).official_name
        country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
        country_continent_name = pc.convert_continent_code_to_continent_name(country_continent_code)
        population = pypopulation.get_population(country_alpha2)
        #
        return (country_continent_name, population, country_alpha3, countryCode_IsoOfficialName)
    except KeyError as err:
        warnings.warn(repr(err))
        return (country_continent_name, population, country_alpha3, countryCode_IsoOfficialName)


#
#
#
#
df['Continent'], df['Country_Population'], df['CountryCode_IsoAlpha3'], df['CountryCode_IsoOfficalName'] = zip(*df['Country_Region'].map(get_CountryInfo))

#

#
#
#

#


#
df = df.loc[(df['Date'].isnull()) | (df['Date'] >= date_filterinDataAfter)]

#
#
#
#
#


#

#
#
#
#
#
df_CovidVaccination = pd.read_csv(path_CovidVaccination, index_col=None, header=0)

df_CovidVaccination['date'] = pd.to_datetime(df_CovidVaccination['date'])
#

df_CovidVaccination = df_CovidVaccination.loc[(df_CovidVaccination['people_vaccinated'].notnull()) & (df_CovidVaccination['date'] >= date_filterinDataAfter)].reset_index(drop=True)

df_CovidVaccination = df_CovidVaccination[['date', 'location', 'iso_code', 'people_vaccinated']]
#

#


#
#
#
#
#
df = df.sort_values('Date')
df_CovidVaccination = df_CovidVaccination.sort_values('date')
df = pd.merge_asof(df, df_CovidVaccination, left_by='CountryCode_IsoAlpha3', right_by='iso_code', left_on='Date', right_on='date', direction='backward', suffixes=('_left', '_right'))

#
df['people_vaccinated_inc'] = df.groupby(['Country_Region', 'Province_State'], dropna=False, sort=False)['people_vaccinated'].diff()


df = df[['Date', 'Country_Region', 'Province_State'
    , 'Confirmed', 'Confirmed_inc', 'Deaths', 'Deaths_inc'
    , 'Lat', 'Long_', 'Continent', 'Country_Population', 'CountryCode_IsoOfficalName'  #
    , 'people_vaccinated'
    #
    , 'Year', 'Month', 'Dayofweek'  #
         ]]
print(df)
#
#

#

df_CovidInfection = df

#
#
