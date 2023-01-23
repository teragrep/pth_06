#!/bin/bash
echo "Setting up databases";
mariadb <<< "CREATE DATABASE streamdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;";
mariadb <<< "CREATE DATABASE journaldb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;";
echo "Setting up privileges";
mariadb <<< "grant all privileges on streamdb.* to streamdb@'127.0.0.1' identified by 'streamdb_pass';";
mariadb <<< "grant all privileges on journaldb.* to streamdb@'127.0.0.1' identified by 'streamdb_pass';";
echo "Importing journaldb";
mariadb -D journaldb < init/journaldb.sql;
echo "Importing streamdb";
mariadb -D streamdb < init/streamdb.sql;
echo "Flushing privileges";
mariadb <<< "flush privileges;";
