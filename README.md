# MongoDB to PostgreSQL ETL project

This project has basic code functionality for ETL from MongoDB to PostgreSQL database.

Code structure:
1. `upload_data.py` - responsible for first copy of available data to MongoDB.
2. `postgres.py` - connection to PostrgeSQL.
3. `bulk_queries.py` - provides functionality for bulk upserts into PostrgeSQL.
4. `etl.py` - where all ETL is orchestrated.
5. `application.py` - Flask based application to run as a web-service. Configured to run on Azure as an App Service.
6. `constrants.py` - all queries that are specific to database structure are found here.
7. `logger.py` - basic logging functionality.

This project was written as an interview take home assignment, and by no means represents a production ready code. The code is publicly shared as the interviewers did not forbid it, however, the interviewer company name is not mentioned for ethical reasons.