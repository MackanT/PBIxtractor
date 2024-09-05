NOTE
----------------------------------------

All features do not work perfectly, have not worked on this project for the last ~6 months, but the code runs fine.
Some known features that do not work correctly
-Buttons on pages do not always correctly connect together
-Hirearchies are sometimes missed
-Bookmarks sometimes work and sometimes don't
-Shapes and images occasionally pop-up and sometimes don't.
-Long story short, anything that is not a visual/slicer has not gotten sufficient love and may or may not be documented correctly :/


How to use
----------------------------------------

Simplest Solution
-From terminal in this folder run 'python .\PB-Ixtractor.py --ui', the ui flag will give you additional options for settings etc.
-Select .pbix file and choose the selected file
-If a .bim file is found in the same folder as the .pbix it will be selected, otherwise select it manually.
-The .bim file is generated from TabularEditor 2 (a program which is required to be installed for this program to work!). 
	Open the model in TabularEditor and select 'file/save as' which will generate the .bim file for the file.
-Select the output name, a new folder will be created (or overwrite an existing folder) and save all the program output
-You can skip "Description tag"r
-When running 'Generate tsv file', ensure the .pbix file is opened as it will allow more information to be extracted (data types for measures will become "unknown" if it is closed, a bug in TabularEditor I believe (?))
-Press 'Run PB-Ixtractor'

Longer Description
-----------------
-Description tag is optional. In my measures I often added a description in the measure itself instead of as an external description. For these to be catched I started and ended all those comments with '\\\\' which is what this line catches. If I remember the code will look at the descriptions defined in PBI as well, but cannot remember 100%...
-Press 'Run PB-Ixtractor', will read the .pbix file as a json string with information on where all the measures/columns are used and what visuals/pages/filters exist and attempt to parse that as good as possible. The generated .tsv file (requires TabularEditor 2) contains additional info on all measures/columns, descriptions, definitions etc.




EXTRA
------------------------------------------
-'Additional Settings' allows for modifying the colors used in the output, not recommended to change, default colors match pbi, but a fun extra feature.
-'Logs' prints out the log files after run completion with some results. Not fully readable results, but potentially simpler than opening the generated .txt file
-'User Input' allows for simplified input of parameters into the code.
	-Can't really remember at the top of my head what "Data Type" does.... Believe it might be for categoricals in 	visuals (?), longitude/latitude/size/legend/x/y etc. Some day it will be made clear....!
	-"Function Name" are the PBI DAX commands that should be color coded in the output. Have thus far only added the ones I have used the most, so if any are missing they can be entered here
	-"Visual Types" add "support" for new visual types. Right now if the visual type is not defined it will be completely skipped, by adding it in here the code will attempt to parse its data
	-"TE Location" is the full path to where TabularEditor 2 is stored on the PC. The default locations are included by default, but if another location is used it needs to be specified



NOTES
-----------------------------------------
-Feel free to contact me if something important does not seem to work. If I can get access to the .pbix/.bim I might be able to find and patch the issue
-I have the code in a personal git-repo, but have not always committed up the latest code, sometimes I've been lazy with my side-project :|
- https://github.com/MackanT/PBIxtractor