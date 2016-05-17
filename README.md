# Big-Data-Analysis-Rodent-Inspection-in-NYC 

<p>
    Rodents and other pests are often a big problem for businesses in New York City. Pest complaints often lead to poor Health Inspection grades, poor customer satisfaction, and poor food hygiene. The goal of this project is to track their locations in NYC neighboorhoods by using the New York City 311 complaint data and sorting it for rodent inspections. Once we find the neighborhoods that rodents often congregate, the project will focus on creating a heatmap of the most common locations for rodents in the city. Hopefully, when businesses move to other neighboorhoods, they will use this data to see whether or not the area is optimal due to high rodent populations. 

</p>
## Chart from our final result shows the positive correlation between number of restaurants and 311 call complaints for rats in neighborhood
![alt tag](/Restaurant_VS_Rat.png)

## Chart from our final result shows the positive correlation between building age and number of 311 call complaints for rats in New York city
![alt tag](/BuildingAge_VS_Rat.png)

## Commands used to run this big data project:
#### Commands used to connect to NYU Cusp cluster:
ssh [YourCuspID]@gw.cusp.nyu.edu<br>
ssh cluster.cusp.nyu.edu <br>
git clone https://github.com/KhadeejaDin/Big-Data-Analysis-Rodent-Inspection-in-NYC.git<br>
cd Big-Data-Analysis-Rodent-Inspection-in-NYC/<br>
chmod 777 submit_Subway.sh<br>
chmod 777 submit_Building.sh<br>
chmod 777 submit_Restaurant.sh<br>
./submit_Subway.sh subway.py /gws/classes/bdma/ccny/groups/8/311_Service_Requests_from_2010_to_Present.csv output_Subway_Rat Subway_Rat.txt 32<br>
./submit_Building.sh Mn_BuildingAge_Rat.py /gws/classes/bdma/ccny/groups/8/311_Service_Requests_from_2010_to_Present.csv output_BuildingAge_Rat BuildingAge_Rat.txt 22<br>

## Other commands used for handling files merge or read:
#### Command to mereg csv files:
cat *.csv >merge.csv

#### statement to read multiple csv files into one Rdd:
Rdd = sc.textFile("Restautant_311/*.csv")   // Restautant_311 folder contains all the csv files


##Contributors:
Khadeeja Din <br>
Xue Wei Fan  josephxwf@gmail.com<br>
Davide Libi-Bourne<br>
John Settineri
