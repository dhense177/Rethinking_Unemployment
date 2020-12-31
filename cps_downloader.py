import pandas as pd
import numpy as np
import requests, zipfile, io, re




def load_zip(url, target_path):
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))

    z.extractall(path=target_path)





if __name__=='__main__':
    #Specify filepath on local/external drive to store data (~52 GB)
    filepath = '/home/dhense/PublicData/cps_files/'
    quote_page = 'https://www2.census.gov/programs-surveys/cps/datasets/'
    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    zip_list = [[quote_page+str(i)+'/basic/'+month+str(i)[-2:]+'pub.zip' for month in months] for i in range(1999,2021)]

    zips = [item for sublist in zip_list for item in sublist]
    for zfile in zips:
        load_zip(zfile,filepath+zfile[-23:-19])
        # print(zfile)
