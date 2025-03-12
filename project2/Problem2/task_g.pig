pages = LOAD 'C:/Users/wpiguest/Desktop/project2/cs4433_project2/pages.csv'
        USING PigStorage(',')
        AS (ID:INT, Name:CHARARRAY, Nationality:CHARARRAY, CountryCode:INT, Hobby:CHARARRAY);

friends = LOAD 'C:/Users/wpiguest/Desktop/project2/cs4433_project2/friends.csv'
         USING PigStorage(',')
         AS (FriendRel:INT, PersonID:INT, MyFriend:INT, DateofFriendship:INT, Desc:CHARARRAY);

access_logs = LOAD 'C:/Users/wpiguest/Desktop/project2/cs4433_project2/access_logs.csv'
        USING PigStorage(',')
        AS (AccessId:INT, ByWho:INT, WhatPage:INT, TypeOfAccess:CHARARRAY, AccessTime:INT);

connected_people = FOREACH friends GENERATE PersonID;
connected_people = DISTINCT connected_people;

latest_access_time = GROUP access_logs ALL;
latest_access_time = FOREACH latest_access_time GENERATE MAX(access_logs.AccessTime) AS max_time;

time_value = FOREACH latest_access_time GENERATE (max_time - 14) AS time_threshold;

recent_accesses = FILTER access_logs BY AccessTime > time_value.time_threshold;
active_people = FOREACH recent_accesses GENERATE ByWho;
active_people = DISTINCT active_people;

all_people = FOREACH pages GENERATE ID AS PersonID, Name;

all_people_with_connections = JOIN all_people BY PersonID LEFT OUTER, connected_people BY PersonID;
inactive_people = FILTER all_people_with_connections BY connected_people::PersonID IS NULL;
inactive_people = FOREACH inactive_people GENERATE all_people::PersonID AS PersonID, all_people::Name AS Name;

inactive_activity_people = JOIN inactive_people BY PersonID LEFT OUTER, active_people BY ByWho;
result_people = FILTER inactive_activity_people BY active_people::ByWho IS NULL;

result_people = FILTER result_people BY inactive_people::PersonID IS NOT NULL;
result_people = FOREACH result_people GENERATE inactive_people::PersonID AS PersonID, inactive_people::Name AS Name;

STORE result_people INTO 'C:/Users/wpiguest/Desktop/project2/cs4433_project2/task_g_output' USING PigStorage(',');

DUMP result_people;
