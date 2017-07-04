class AddressLine:
    find_address = ""
    centroid = ""

    def __init__(self, region, city, address, index):
        self.region = region
        self.city = city
        self.address = address
        self.index = index

    def __str__(self):
        return "Line: | {0} | {1} | {2} | {3} | \n".format(self.region, self.city, self.address, self.index)