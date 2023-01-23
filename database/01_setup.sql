CREATE DATABASE streamdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE journaldb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
grant all privileges on streamdb.* to streamdb@'127.0.0.1' identified by 'streamdb_pass';
grant all privileges on journaldb.* to streamdb@'127.0.0.1' identified by 'streamdb_pass';
