# NGD_A-Complete-Incomplete-Ranges

This project takes address point data that was  converted from csv data available at https://openaddresses.io/ and joins it to the NGD_A for the purpose of comparing those address ranges against the NGD_AL. This process is completed in several steps listed below. Note that since this project is unfinished these steps are at various levels of completness.

1.) The OpenAddressIO is cleaned and converted into a format that matches the address format of the NGD. - Completed but with some known bugs - Some parts of street names such as Saint (St) are mistaken for street types and are mistakenly removed by the filter.

2.) The data is then made into address ranges by creating a spatial join between the address points and the NGD_A data and then using the statistics tool to create the min max addresses for each street in each BB - Completed

3.) The incomplete ranges within the NGD_AL are checked against the OpenAddressIO range data by BB_UID and flag if an acceptable number is found within the OpenAddressIO data if a match is found complete range and flag record for a manual check against the data. This is output as a new csv file for the checks - Somewhat done checks for matches implimented but flagging not added. Also need to add flag for if original range is odd but new value is even. This needs an additional manual review flag. 
