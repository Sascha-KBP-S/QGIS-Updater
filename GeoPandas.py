import geopandas as gpd
import pandas as pd


pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

import fiona

probennummern = [0,1,2,3,4,5]
proben = []

PATH = r"C:\Users\SASBOITE\Downloads\pn_protokoll_alt.gpkg"
pn_group = gpd.read_file(PATH, layer="PN_Gruppe")
pn_ptcl = gpd.read_file(PATH, layer="PN_Protokoll")

class Probe:

    def __init__(self):
        self.pn = int
        self.group = int


    def get(self, pn, pn_ptcl, pn_group):
        if pn not in self.ptcl_by_nummer.index:
            raise ValueError(f"Keine Probe mit Nummer {pn} gefunden.")
        probe = self.ptcl_by_nummer.loc[pn]
        uuid = probe["Ref_pn_Gruppe"]

        gruppe = self.group_uuid.loc[uuid] if uuid in self.group_uuid.index else None
        probenliste = self.proben_by_uuid.get(uuid, [])

        return {
            "probe": probe,
            "gruppen_uuid": uuid,
            "gruppe": gruppe,
            "gruppe_nummer": None if gruppe is None else gruppe.get("Gruppe_Nummer", None),
            "proben_in_gruppe": probenliste
        }


for i in probennummern:
    proben.append(Probe())
    print(proben[i].get(i+1, pn_ptcl, pn_group))






