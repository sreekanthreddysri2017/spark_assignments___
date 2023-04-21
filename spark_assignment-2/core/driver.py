from Spark_assign_2.core.util import *

#creating a spark session
spark=Spark_Session()


#separating the fields(log_level,timeStamp,ghTorrent_details) in textFile with the given delimiter ","
ghtorrent_df=create_torrent_DataFrame(spark)

#extracting various fields as per the requiremnets
ghtorrent_fields_extract=ghtorrent_fields(ghtorrent_df)
ghtorrent_fields_extract.show()

#below function result will give the count of the total number of lines in the dataFrame
count_lines=total_line(ghtorrent_fields_extract)
count_lines.show(truncate=False)

#below function result will give the count of the total number of lines in textFile which has loglevel as "WARN"
count_warn=warn_count(ghtorrent_df)
count_warn.show(truncate=False)

#below function result will give the count on how many repositories where processed under "api_client"
repo_api_client=api_client_repo(ghtorrent_fields_extract)
repo_api_client.show()

#below function gives the client who gave most http request
most_http_request=most_http(ghtorrent_fields_extract)
most_http_request.show()

#below function gives the client who had most failed http request
failed_http_request=failed_request(ghtorrent_fields_extract)
failed_http_request.show()

#below function gives the most active hour of the day
most_active_hour_Ofday=active_hour(ghtorrent_df)
most_active_hour_Ofday.show()

#below function gives the most active repository
most_active_repository=active_repo(ghtorrent_fields_extract)
most_active_repository.show()