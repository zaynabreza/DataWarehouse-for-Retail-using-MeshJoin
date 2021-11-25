# DataWarehouse-for-Retail-using-MeshJoin
Data loading from database using Mesh Join and OLAP Queries for a retail store <br/>
<br/>
Steps to run MeshJoin on Eclipse using Java SE 8 </br>
Step 1: </br>
    Run createDW.sql in mySQL - this creates a schema 'dwh' if it doesnt already exist and creates the fact and dimension tables within it </br>
Step 2:</br>
    Select import projects in Eclipse</br>
Step 3:</br>
    Under Maven, select Existing Maven projects</br>
Step 4:</br>
    Choose the project folder as root directory and click finish</br>
Step 5:</br>
    Set the compiler version to Java SE 8 in case another version is selected</br>
Step 6:</br>
    Adjust mySQl database credentials if needed in lines 27-30 of MeshJoin.java which can be found in the src folder</br>
    Current credentials set as follows:</br>
    username: "root" (line 27)</br>
    password: "1234" (line 28)</br>
    source schema for fetching data: "projectsource" (line 29)</br>
    target schema for dwh: "dwh" (line 30)</br>
*Step 7 may be required only if the error that "main class MeshJoin not found" is displayed when pressing run*</br>
Step 7:</br>
    Select "Run Configurations" from the drop down menu next to the "Run" button</br>
	Click on the  "New Launch Configuration" button if the current project is not enlisted under "Java Application" on the right</br>
	If the new configuration does not populate the project automatically, click on Browse and select "DWHMeshJoin" as the project</br>
	Click on search and select "MeshJoin" as the main class</br>
	Press Apply and close</br>
Step 8:</br>
    Click on "Run" to execute</br>
