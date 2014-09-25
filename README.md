ESKoIE
======

Acronym for "Enriching Structured Knowledbe Bases with Open Information Extractions"

Project to enrich Structured knowledge Bases with the knowledge from Open Information Extraction Systems..


Installation
===================

Linux OS and Eclipse IDE

Requirements

1. Have the  jdk higher than the version 1.7 installed. Not jre but jdk !!
 
2. Have maven installed, atleast maven2 

3. have a github account
 


Project Setup

1. Clone the repository https://github.com/kraktos/OIE-Integration.git from the eclipse "Git Repository Exploring" view. 
Say for example, your local repository is /home/user/Workspaces/Projects/OIE-Integration, then a .git folder should be created here after cloning

2. Change perspective to java and import project

3. Must select an existing Maven Project from the options. 

4. Browse to the folder in your machine where the project is freshly cloned in Step 1. 
/home/user/Workspaces/Projects/OIE-Integration in this case, and import the project to Eclipse workspace

5. open command prompt and change to working directory, e.g. /home/user/Workspaces/Projects/OIE-Integration and run the command

mvn clean assembly:single install
