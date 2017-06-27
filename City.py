class City:
    map_id = 0
    name = ""
    centroid = ""
    adm_level1 = None
    adm_level2 = None

    def __init__(self, map_id, name, centroid, adm_level1, adm_level2):
        self.map_id = map_id
        self.name = name
        self.centroid = centroid
        self.adm_level1 = adm_level1
        self.adm_level2 = adm_level2
