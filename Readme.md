# AIS-Stop-Identification-Algorithm 
### Author: Sanjeev Bhurtyal

Script to identify stop areas for AIS vessels.
The script is based on following papers:
1. "Exploring AIS data for intelligent maritime routes extraction" - Yan et al. (2020)
2. "Extracting ship stopping information from AIS data" - Yan et al. (2022)
3. "Inland waterway network mapping of AIS data for freight transportation planning" - Asborno et al. (2022)


### Instruction
1.  AIS data raw files can be downloaded in batch from Marine Cadastre website using wget (DownloadAISData_BatchFile.bat)
2. Raw data does not have consistent file type. Use DataReadFor2009to2014.py to export data from 2009 to 2014 to server. Use DataReadFor2015to2021.py for 2015 and beyond. This step also resamples frequency from 1 minute to 5 minute.
3. AIS_Stop_Identification.py identifies stop pings. Although it has section that identifies stop area, it is commented out. DBSCAN is used instead.
4. AIS_Stop_Identification_DBSCAN.py aggregates stop pings to stop area.
