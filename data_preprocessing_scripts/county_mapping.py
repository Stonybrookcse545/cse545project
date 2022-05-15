
from geopy.geocoders import Nominatim
import csv

#Script to get the county names from latitude longitude values
if __name__ == '__main__':
    geolocator = Nominatim(user_agent="http")

    reader = csv.reader(open("Data/stations_US1.txt"), delimiter="\t")
    output_str =[]
    with open('countries1_4.csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f)
        for row in reader:
            str_list = row[0].split()
            station = str_list[0]
            latitude = str_list[1]
            longitude = str_list[2]
            lat_long_str = latitude+', '+longitude

            location = geolocator.reverse(lat_long_str)
            if location==None:
               continue
            output_str.append(station)
            output_str.append(latitude)
            output_str.append(longitude)

            location_dict = location.raw
            print(location_dict)
            output_str.append(location_dict['place_id'])
            location_address = location_dict['address']
            if ('country' in location_address and location_address['country']=='United States') or \
                    ('country_code' in location_address and location_address['country_code']=='us'):
                if 'county' in location_address:
                    county=location_address['county']
                    output_str.append(county)
                print(output_str)
                writer.writerow(output_str)
                output_str.clear()
