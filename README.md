Tool for AWS Redshift ETL phase. It has been developed and tested.
Main task: Automatically extract data from raw data for the ETL phase at 01:00:00 AM GMT+7 every day. In case of a system crash, 
it will automatically save error logs and re-attempt any unfinished tasks when the system restarts.
Other tasks: Check for data loss in the fact tables, update dimension tables, compare DC2 vs RA3, and send logs via a bot....
