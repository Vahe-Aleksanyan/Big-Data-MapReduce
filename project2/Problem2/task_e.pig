access_info = LOAD 'C:/Users/wpiguest/Desktop/project2/cs4433_project2/access_logs.csv'
    USING PigStorage(',')
    AS (AccessId:INT, ByWho:INT, WhatPage:INT, TypeOfAccess:CHARARRAY, AccessTime:INT);

pages = LOAD 'C:/Users/wpiguest/Desktop/project2/cs4433_project2/pages.csv'
    USING PigStorage(',')
    AS (ID:INT, Name:CHARARRAY, Nationality:CHARARRAY, CountryCode:INT, Hobby:CHARARRAY);

grouped_accesses = GROUP access_info BY ByWho;

total_accesses = FOREACH grouped_accesses GENERATE
    group AS PersonID,
    COUNT(access_info) AS TotalAccesses;

casted_access_info = FOREACH access_info GENERATE ByWho AS PersonID, (INT) WhatPage AS WhatPage;

grouped_casted_access = GROUP casted_access_info BY PersonID;

flattened_page = FOREACH grouped_casted_access {
    unique_pages = DISTINCT casted_access_info.WhatPage;
    GENERATE group AS PersonID, COUNT(unique_pages) AS DistinctPagesAccessed;
}

flattened_favorites = JOIN total_accesses BY PersonID, flattened_page BY PersonID;

flattened_favorites_with_name = JOIN flattened_favorites BY total_accesses::PersonID, pages BY ID;

computed_output = FOREACH flattened_favorites_with_name GENERATE
    pages::Name AS Name,
    total_accesses::TotalAccesses AS TotalAccesses,
    flattened_page::DistinctPagesAccessed AS DistinctPagesAccessed;

STORE computed_output INTO 'C:/Users/wpiguest/Desktop/project2/cs4433_project2/task_e_output' USING PigStorage(',');
