#!/bin/bash
# Root has no password
(sudo -u mysql mariadbd) &
sleep 2;
sudo -u mysql mariadb <<< "CREATE DATABASE streamdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
sudo -u mysql mariadb <<< "CREATE DATABASE journaldb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
sudo -u mysql mariadb <<< "grant all privileges on streamdb.* to streamdb@'127.0.0.1' identified by 'streamdb_pass';"
sudo -u mysql mariadb <<< "grant all privileges on journaldb.* to streamdb@'127.0.0.1' identified by 'streamdb_pass';"
sudo -u mysql mariadb -D journaldb < database/journaldb.sql
sudo -u mysql mariadb -D streamdb < database/streamdb.sql
sudo -u mysql mariadb <<< "flush privileges;"
