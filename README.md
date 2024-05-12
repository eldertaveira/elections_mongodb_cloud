# MongoDB project using GCP

This project aims to create an archtecture using Google Cloud Platform - GCP and integrating frameworks as Docker and MongoDB in order to connect the data in Metabse. The figure below shows the archtecture implemmented. 

![image](https://github.com/eldertaveira/elections_mongodb_cloud/assets/142034363/f0d91075-b15e-40ab-8d01-9b9007c374e7)


First, using the GCP it was selected the Cloud Engine to create a Virtual Machine - VM to create an Debian 12 enviroment. With Debian, it is possible to create Docker containers. Hence, the MongoDB and the Metabase has been used to alocate the dataset and visualize 
the data through docker-compose file.

Also, a developer enviroment using python was created to make the download of the dataset locally. After this, the csv file is converted to json and finally the json file is compressed to parquet. This reason is that the parquet file is better to store in cloud.

Moreover, the MongoDB Compass was used to visualize the data and it allow us to make many consults in more than 9 millions of rows or manipulate the data, as showed in the figure below:

![image](https://github.com/eldertaveira/elections_mongodb_cloud/assets/142034363/d4ab3c67-6f0f-4677-9b4a-c9809c686ee5)



Finally, the Power BI is connected with MongoDB in other to load the dataset that is ready to  data visualization management. 

### Results: 

The dataset is about the 2022 elections in Brazil. Hence, the dashboard created in Metabase shows result as:
1. Number of vots per positions in the first turn.
2. Number of vots to president in the state of RN in the first turn.
3. Number of vots per president's cadidates, considering the whole Brazil, in the first turn.
   

![image](https://github.com/eldertaveira/elections_mongodb_cloud/assets/142034363/84c9c6b1-fdd4-445d-8641-a3fb801a371b)


This project results in a great data engineeting tool using NoSQL database and cloud, two subjects that is relevant when the subject is data analysys, data engineering, data archtecture and storage data.
