# x = ('abspence', '127.0.0.1', 'Leader')
# y = [('something', '127.0.0.1', 'InDHT'), ('else', '127.0.0.1', 'InDHT')]

# z = []

# z.append(x)

# for user in y:
#     z.append(user)

# print(z)

# big_list = [1,2,2,3,3,3,4,5,5,6,6,7,7]

# print(big_list[4:])
import os
import sys
from csv import DictReader


with open(os.path.join(sys.path[0], "StatsCountry.csv"), "r") as data_file:
    csv_reader = DictReader(data_file)
    # # Iterate over each row in the csv using reader object

    for row in csv_reader:
        print(row)