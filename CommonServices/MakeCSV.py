import csv

class CSV_Maker():
    def write_to_csv(self, data, filename="Default.csv"):
        # name of csv file
        filename = filename
        # writing to csv file
        with open(filename, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(data)