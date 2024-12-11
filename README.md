# gostmarql
Golang test and benchmark SQL

This tool will read SQL file and execute every line in it and
output the result to file or benchmark it.

env file .env should have following parameters:
```
DBHOST=127.0.0.1
DBPORT=5432
DBUSER=databaseuser
DBPASSWORD=databasepassword
DBSCHEMA=databaseschema
```

or following parameters:
```
DBURL=postgres://username:password@localhost:5432/database_name
```
