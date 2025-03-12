friends = LOAD 'C:/Users/wpiguest/Desktop/project2/cs4433_project2/friends.csv'
         USING PigStorage(',')
         AS (FriendRel:INT, PersonID:INT, MyFriend:INT, DateofFriendship:INT, Desc:CHARARRAY);

pages = LOAD 'C:/Users/wpiguest/Desktop/project2/cs4433_project2/pages.csv'
        USING PigStorage(',')
        AS (ID:INT, Name:CHARARRAY, Nationality:CHARARRAY, CountryCode:INT, Hobby:CHARARRAY);

friend_counts = GROUP friends BY PersonID;
friend_for_each_person = FOREACH friend_counts GENERATE group AS PersonID, COUNT(friends) AS num_friends;

summary = GROUP friend_for_each_person ALL;
stats = FOREACH summary GENERATE
            SUM(friend_for_each_person.num_friends) AS total_friends_value,
            COUNT(friend_for_each_person) AS total_people_value;

avg_friends = FOREACH stats GENERATE (double) total_friends_value / total_people_value AS avg_friends_value;

above_average_people = FILTER friend_for_each_person BY num_friends > avg_friends.avg_friends_value;

popular_name_info = JOIN above_average_people BY PersonID, pages BY ID;

computed_result = FOREACH popular_name_info GENERATE TRIM(pages::Name) AS Name, above_average_people::num_friends AS FriendCount;

DUMP friend_for_each_person;

DUMP computed_result;

STORE computed_result INTO 'C:/Users/wpiguest/Desktop/project2/cs4433_project2/task_h_output' USING PigStorage(',');
