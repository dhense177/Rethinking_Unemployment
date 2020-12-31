# Rethinking Unemployment
Deconstructing the unemployment rate calculated by Bureau of Labor Statistics (BLS) in order to understand which segments of the population are accounted for in the Labor Force and which aren't.

Calculating new unemployment rates based on additions/removals of different population segments from the Labor Force.

File           | Type          | Description
-------------- | ------------- | ---------------
livingwage<nolink>.py| Data Download | Scrape living wage data from livingwage.mit.edu and historical pages at archive.org
cps_downloader.py  | Data Download | Dowloand all Current Population Survey (CPS) data from www2.census.gov
pop_est_all.py | Data Processing | Clean and process various historical population estimates and population projections data downloaded from from www2.census.gov (look at UnemploymentAnalysis.pdf doc for specific files to download)
cps_export.py  | Data Processing | Initial processing of cps data and merging with livingwage data
cps_parser.py  | Data Processing | Second stage of cps data processing, adding weights to survey response rows and merging population data
cps_calcs.py   | Calculate Stats | Calculate unemployment rates and other stats
cps_analysis.py | Calculate Stats | Calculate unemployment rate stats for various subpopulation groups
subpop_analysis | Calculate Stats | Calculate growth rates for unemployment stats  
