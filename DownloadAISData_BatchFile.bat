:: Directory where the files will be downloaded
cd AIS Data\From Marine Cadastre 

ECHO ***********Downloading 2022 *******************
wget -np -r -nH -L --cut-dirs=3 https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2022/