## **Overview**  
This project presents a high-performance computational framework using Apache Hadoop and custom Java-based MapReduce applications to analyse UK energy consumption data over a decade. This solution efficiently handles large datasets, revealing trends and consumption patterns in under 2 seconds.

## **Objectives**  
- **Total Average Consumption**: Determine average electricity consumption over ten years.
- **Seasonal Variation**: Analyse how energy usage varies between winter and summer months.

## **Solution Framework**  
The framework includes two MapReduce approaches:
1. **MapReduce Chaining** for comprehensive analysis of total average energy consumption.
2. **Single MapReduce Job** for seasonal variation assessment.

### Framework Visualizations  
Below are the framework diagrams for the MapReduce implementations:

| MapReduce Chaining | Single MapReduce Job |
|--------------------|----------------------|
| ![MapReduce Chaining Diagram]([images/mapreduce_chaining.png](https://github.com/hawra-nawi/HPC-Framework-for-Analyzing-UK-Energy-Consumption/blob/main/MapReduce%20Frameworks/1.%20MapReduce%20Chaining%20Workflow.png)) | ![Single MapReduce Job Diagram]([images/single_mapreduce.png](https://github.com/hawra-nawi/HPC-Framework-for-Analyzing-UK-Energy-Consumption/blob/main/MapReduce%20Frameworks/2.%20Single%20MapReduce%20Workflow.png)) |

## **Technologies and Tools**  
- **Apache Hadoop**: Distributed data processing framework.
- **Java-Based MapReduce**: For scalable and efficient data computation.
- **Tableau**: Used to create visualizations for decision-making insights.

## **Key Achievements**  
- **Efficiency**: Processed over a decade of data in under 2 seconds.
- **Insights**: Discovered 6 million kWh in average energy usage with observable seasonal trends.

## **Repository Structure**  
Here's a breakdown of the repository files and folders:

1. **Files Folder**
   - [**Data**](https://github.com/hawra-nawi/HPC-Framework-for-Analyzing-UK-Energy-Consumption/blob/main/Files/Data/EnergyConsumptionModified1.csv): Contains the modified energy consumption dataset (`EnergyConsumptionModified1.csv`).
   - [**Java Scripts**](https://github.com/hawra-nawi/HPC-Framework-for-Analyzing-UK-Energy-Consumption/tree/main/Files): Code for the MapReduce applications.
   
2. **Images**  
   - [Framework diagrams](https://github.com/hawra-nawi/HPC-Framework-for-Analyzing-UK-Energy-Consumption/tree/main/MapReduce%20Frameworks) for both the MapReduce Chaining and Single MapReduce applications.

3. **Comprehensive Analysis Report**  
   - Detailed [report](https://github.com/hawra-nawi/HPC-Framework-for-Analyzing-UK-Energy-Consumption/blob/main/Documentation%20Report.pdf) covering design, implementation, and findings of the analysis.

4. [**Results Folder**](https://github.com/hawra-nawi/HPC-Framework-for-Analyzing-UK-Energy-Consumption/tree/main/Results)
   - Includes Tableau visualisations and analysis results.
  
> **Note**: This computational analysis was conducted on a virtual machine to simulate real-world, large-scale data processing conditions.
