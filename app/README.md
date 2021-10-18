## Cluster it up!

#### Dumping the database using ts-dump
- Drop all materialized views
- Use this command to dump the db:
`ts-dump --db-URI=postgres://postgres:pass@localhost:5432/TimeScaleDB --dump-dir=TimeScaleDB_Dump_2021_10_17/ --verbose=1;`

#### Restoring the database from the ts-dump OR using ts-restore

- First log back into the database from the command line using `psql -U postgres`
- And then delete the database "TimeScaleDB" using the psql command `DROP DATABASE IF EXISTS "TimeScaleDB";`
- Make another fresh database called "TimeScaleDB" again using the command `CREATE DATABASE "TimeScaleDB";`
- Create the extension in the new database using `CREATE EXTENSION timescaledb;`
- Log out of the console
- Now download the latest dump, and extract it. 
- Use this command to restore the db: (with the correct --dump-dir ofc) 
`ts-restore --db-URI=postgres://postgres:pass@localhost:5432/TimeScaleDB --dump-dir=TimeScaleDB_Dump_2021_10_17/ --verbose=1;`
- Dont forget to delete the zipped file and the extracted files after this is done.