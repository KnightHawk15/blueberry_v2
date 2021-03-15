# Distance
from haversine import haversine, Unit
from googleplaces import GooglePlaces
import random
import googlemaps

# Data Manipulation
import requests
import pandas as pd

# Region Identification
import json
from shapely.geometry import shape, Point
from shapely.geometry import Polygon
from shapely.ops import cascaded_union

api_key = "API_KEY"

my_types = ['accounting', 'airport', 'amusement_park', 'aquarium', 'art_gallery',
         'atm', 'bakery', 'bank', 'bar', 'beauty_salon', 'bicycle_store',
         'book_store', 'bowling_alley', 'bus_station', 'cafe', 'campground',
         'car_dealer', 'car_rental', 'car_repair', 'car_wash', 'casino', 'cemetery',
         'church', 'city_hall', 'clothing_store', 'convenience_store', 'courthouse',
         'dentist', 'department_store', 'doctor', 'drugstore', 'electrician',
         'electronics_store', 'embassy', 'fire_station', 'florist', 'funeral_home',
         'furniture_store', 'gas_station', 'gym', 'hair_care', 'hardware_store',
         'hindu_temple', 'home_goods_store', 'hospital', 'insurance_agency',
         'jewelry_store', 'laundry', 'lawyer', 'library', 'light_rail_station',
         'liquor_store', 'local_government_office', 'locksmith', 'lodging',
         'meal_delivery', 'meal_takeaway', 'mosque', 'movie_rental',
         'movie_theater', 'moving_company', 'museum', 'night_club', 'painter',
         'park', 'parking', 'pet_store', 'pharmacy', 'physiotherapist',
         'plumber', 'police', 'post_office', 'primary_school', 'real_estate_agency',
         'restaurant', 'roofing_contractor', 'rv_park', 'school', 'secondary_school',
         'shoe_store', 'shopping_mall', 'spa', 'stadium', 'storage', 'store',
         'subway_station', 'supermarket', 'synagogue', 'taxi_stand', 'tourist_attraction',
         'train_station', 'transit_station', 'travel_agency', 'university',
         'veterinary_care', 'zoo']

# CKAN Download helper
url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show"


def get_ckan(package):
    for idx, resource in enumerate(package["result"]["resources"]):
        if resource["datastore_active"]:
            url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/datastore_search"
            p = {"id": resource["id"], "limit": 4000}
            data = requests.get(url, params=p).json()
            df = pd.DataFrame(data["result"]["records"])
            return df
            break

# First we download the regional based data including bounding polygons for niehgbourhood regions, income and
# population density, civics and equity, housing and safety. These attributes were selected based on their overall
# impact on homeless populations.

### ADDITIONAL ATTRIBUTES

# Regional Bounding Polygons for Toronto's Neighbourhoods

params = {"id": "4def3f65-2a65-4a4f-83c4-b2a4aed72d46"}
package = requests.get(url, params=params).json()
print(package["result"])
df_hoods = get_ckan(package)

# Gathering Income and Density values for each hood

params = {"id": "6e19a90f-971c-46b3-852c-0c48c436d1fc"}
package = requests.get(url, params=params).json()
print(package["result"])
df_region = get_ckan(package)

df_region = df_region.transpose()
df_region = (df_region.iloc[6:]).reset_index()
df_region = df_region[[0, 7, 944]]
df_region.columns = ['id', 'density', 'income']
df_region['id'] = df_region['id'].astype(int)
df_region = df_region.sort_values(by='id')

# CIVICS & EQUITY INDICATORS
df_temp = pd.read_excel(
    'https://ckan0.cf.opendata.inter.prod-toronto.ca/download_resource/f62b0d1d-dc2d-4e0e-a9d3-aee112b9c400',
    sheet_name=2)
df_temp.columns = df_temp.iloc[0]
df_temp = df_temp.rename(columns={'Neighbourhood Id': "id"})
df_temp = df_temp.iloc[1:]

df_region = df_temp.merge(df_region, left_on="id", right_on="id", how='left')

# HOUSING
df_temp = pd.read_excel(
    'https://ckan0.cf.opendata.inter.prod-toronto.ca/download_resource/30aa3bdd-7c64-416b-984d-3391c2c9599a')
df_temp = df_temp.rename(columns={'Neighbourhood': "id", "RGI": "subsidized"})

df_region = df_region.merge(df_temp, left_on="id", right_on="id", how='left')

# SAFETY
params = {"id": "fc4d95a6-591f-411f-af17-327e6c5d03c7"}
package = requests.get(url, params=params).json()
print(package["result"])
df_temp = get_ckan(package)
df_temp = df_temp[['Hood_ID', 'BreakandEnter_2019', 'Homicide_2019', 'TheftOver_2019']]
df_temp = df_temp.rename(columns={'Hood_ID': 'id'})
df_temp['id'] = df_temp['id'].astype(int)

df_region = df_region.merge(df_temp, left_on="id", right_on="id", how='left')
df_region['id'] = df_region['id'].astype(int)

# RECORD SERVICES
# To generate the bulk of the info for each record we needed to scrape several resource datasets,
# we stored each of these in the type column for future modelling. Most of the data was taken from a broad study on
# youth suffering from homelessness and their resources available.

df_main = pd.DataFrame()

# YOUTH SERVICES

# 24 Sheets
leg = pd.read_excel(
    "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/735c2177-513b-49dd-b4bc-6435d6a80efe/resource/5413c3d7-6c97-4437-987d-e47036f69324/download/wellbeing-toronto-youth-services-data-excel.xlsx",
    sheet_name=0)
leg = leg.dropna()
l = leg['LEGEND'].str.split("\xa0 ", n=1, expand=True)[1]

for i in range(1, 24):
    df_temp = pd.read_excel(
        "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/735c2177-513b-49dd-b4bc-6435d6a80efe/resource/5413c3d7-6c97-4437-987d-e47036f69324/download/wellbeing-toronto-youth-services-data-excel.xlsx",
        sheet_name=i)
    df_temp = df_temp[['AgencyName', 'Address', 'Neighbourhood']]
    df_temp['type'] = l.iloc[i]
    df_main = df_main.append(df_temp)

## BULK LOADING:
params = [
    # ADULT EDUCATION UPGRADING
    {"id": "c01b9ad1-0720-4f4c-ab35-743f55756b85"},
    # SUBSTANCE USE TREATMENT
    {"id": "4db2d9d9-6590-41e6-bf2b-b9188436d044"},
    # ALTERNATIVE ADULT EDUCATION
    {"id": "9308a7e1-3781-45fd-95c7-582b2030f2c1"},
    # TRANSITIONAL HOUSING
    {"id": "cefad70f-2deb-425f-81d1-7d56cf682e65"},
    # LEGAL JUSTICE SUPPORT
    {"id": "ca757aba-734e-4a4f-8c63-07396abcb1fd"},
    # ABORIGINAL YOUTH
    {"id": "ee43541f-220c-41f1-af52-cadf5de1dd9b"},
    # SEXUAL HEALTH
    {"id": "0edbbd59-37e4-4d43-9d79-ac7b5d24db3d"},
    # FINANCIAL SERVICES
    {"id": "8dbb3143-416c-4f2e-ab67-c4af7d2d5edf"},
    # EDUCATIONAL SERVICES
    {"id": "0edbbd59-37e4-4d43-9d79-ac7b5d24db3d"},
    # LGBTQ
    {"id": "bb40b7c9-a37d-46be-a89b-c23273d86c85"},
    # EMPLOYMENT
    {"id": "764c1564-0761-44b0-9b3a-5b2e914e66fb"},
    # MENTAL HEALTH
    {"id": "c9f4bc42-32b0-4198-a2a0-abd26a5f2a6b"},
    # REFUGEE HOUSING
    {"id": "c9f4bc42-32b0-4198-a2a0-abd26a5f2a6b"},
    # HOUSING EVICTION HELP
    {"id": "279f11b4-aaf8-4275-b6af-fdcf679ecc2f"}
]
types = [
    "ADULT EDUCATION UPGRADING",
    "SUBSTANCE USE TREATMENT",
    "ALTERNATIVE ADULT EDUCATION",
    "TRANSITIONAL HOUSING",
    "LEGAL JUSTICE SUPPORT",
    "ABORIGINAL YOUTH",
    "SEXUAL HEALTH",
    "FINANCIAL SERVICES",
    "EDUCATIONAL SERVICES",
    "LGBTQ",
    "EMPLOYMENT",
    "MENTAL HEALTH",
    "REFUGEE HOUSING",
    "HOUSING EVICTION HELP"
]

# Dataset stacker
for i in range(len(params)):
    package = requests.get(url, params=params[i]).json()
    print(package["result"])
    df_temp = get_ckan(package)
    df_temp = df_temp[['AGENCY_NAME', 'ADDRESS_FULL', 'NEIGHBOURHOOD']]
    df_temp = df_temp.rename(
        columns={'AGENCY_NAME': 'AgencyName', 'ADDRESS_FULL': 'Address', 'NEIGHBOURHOOD': 'Neighbourhood'})
    df_temp['type'] = types[i]
    print(types[i])

    df_main = df_main.append(df_temp)

# # Extracted neighbourhood id:
# df_main['id'] = df_main['Neighbourhood'].str.split(", ", n = 1, expand = True)[1].astype(int)

# SHELTER DATA
params = {"id": "8a6eceb2-821b-4961-a29d-758f3087732d"}
package = requests.get(url, params=params).json()
print(package["result"])
df_temp = get_ckan(package)
df_temp = df_temp[['SHELTER_NAME', 'SHELTER_ADDRESS', 'SHELTER_CITY']]
df_temp = df_temp.rename(
    columns={'SHELTER_NAME': 'AgencyName', 'SHELTER_ADDRESS': 'Address', 'SHELTER_CITY': 'Neighbourhood'})
df_temp = df_temp.drop_duplicates()
df_temp['type'] = "SHELTER"

df_main = df_main.append(df_temp)

# Remove NA values:
df_main = df_main.dropna(how='any')

df_main = df_main.reset_index(drop=True)

# ADDRESS ATTRIBUTES
# Get closest distance to nearest queries.

def get_lat_lng(string_addr):
    try:
        string_addr = str(string_addr) + " Toronto Ontario"
        gmaps = googlemaps.Client(key=api_key)
        result = gmaps.geocode(string_addr)
        if result:
            return result[0]['geometry']['location']['lat'], result[0]['geometry']['location']['lng']
        return None
    except:
        return None


def get_placeID(string_addr):
    gmaps = googlemaps.Client(key=api_key)
    result = gmaps.geocode(string_addr)
    if result:
        return result[0]['place_id']
    return None


def process(lat, lng, string_addr, radius=50):
    google_places = GooglePlaces(api_key)
    query_result = get_list_loc(google_places, lat, lng, string_addr, radius)
    if query_result.places:
        place = query_result.places[0]
        place.get_details()
        return place
    return None


def get_nearest(lat, lng, nearest):
    location = lat + ", " + lng
    url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={location}&rankby=" \
          f"distance&keyword={nearest}&key={api_key}"
    response = requests.get(url).json()

    try:
        for nearest in response["results"]:
            if nearest["business_status"] == "OPERATIONAL":
                return get_distance(float(lat), float(lng), nearest["geometry"]["location"]["lat"],
                                    nearest["geometry"]["location"]["lng"])
    except:
        return None


def get_list_loc(google_places, lat, lng, string_addr, radius):
    try:
        query_result = google_places.nearby_search(
            lat_lng={'lat': lat, 'lng': lng},
            keyword=string_addr,
            radius=radius,
            rankby="distance"
        )
        return query_result
    except:
        return None


def get_distance(lat, lng, dest_lat, dest_lng):
    dist_in_meters = haversine((lat, lng), (dest_lat, dest_lng), unit=Unit.METERS)
    return dist_in_meters


def get_region(lat, lng):
    pnt = Point(lng, lat)
    for _, f in df_hoods.iterrows():
        poly = shape(eval(f['geometry']))
        if poly.contains(pnt):
            return int(f.AREA_SHORT_CODE)


def merge_polys():
    polys = []
    for _, f in df_hoods.iterrows():
        polys.append(shape(eval(f['geometry'])))
    u = cascaded_union(polys)
    return u


def random_coord_in_toronto(poly, num_points):
    min_x, min_y, max_x, max_y = poly.bounds

    points = []

    while len(points) < num_points:
        random_point = Point([random.uniform(min_x, max_x), random.uniform(min_y, max_y)])
        if (random_point.within(poly)):
            points.append(random_point)

    return points


def get_total_ratings_count(lat, lng, radius, type_of):
    location = lat + ", " + lng
    url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={location}&radius=" \
          f"{radius}&type={type_of}&key={api_key}"
    response = requests.get(url).json()

    try:
        for business in response["results"]:
            if business["business_status"] == "OPERATIONAL":
                return business["user_ratings_total"]

    except:
        return None


def get_everything(lat, lng, radius, type_of):
    location = lat + ", " + lng
    url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={location}&radius=" \
          f"{radius}&type={type_of}&key={api_key}"
    response = requests.get(url).json()

    try:
        for business in response["results"]:
            if business["business_status"] == "OPERATIONAL" and business["user_ratings_total"] > 500:
                return [business["place_id"], type_of, business["user_ratings_total"], business["rating"]]
    except:
        return None


def business_above_threshold(lat, lng, radius, type_of):
    location = lat + ", " + lng
    url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={location}&radius=" \
          f"{radius}&type={type_of}&key={api_key}"
    response = requests.get(url).json()

    try:
        for business in response["results"]:
            if business["business_status"] == "OPERATIONAL" and business["user_ratings_total"] > 500:
                return business["place_id"], business["geometry"]["location"]["lat"], business["geometry"]["location"][
                    "lng"]
    except:
        return None


def get_rating(place_id):
    url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=name,rating,types&key={api_key}"
    response = requests.get(url).json()
    return response["result"]["name"], response["result"]["rating"], response["result"]["types"]

# GETTING RANDOM DATA

radius = 1500
toronto = merge_polys()
points = random_coord_in_toronto(toronto, 100000)
ratings = []
totals = []
places = []
counter = 0

for point in points:
    lng = point.x
    lat = point.y

    rand_choice = random.choice(my_types)
    curr_stats = get_everything(str(lat), str(lng), radius, rand_choice)

    if curr_stats:
        if curr_stats[0] in places:
            continue
        totals.append(curr_stats)
        places.append(curr_stats[0])
        counter += 1
    if counter == 1000:
        break

