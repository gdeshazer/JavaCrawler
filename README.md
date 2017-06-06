# Basic Crawler

This is a basic web crawler based around a PostgreSQL database.  Rather
than storing information in memory the database saves urls and whether
they've been visited or not between crawling sessions.

# Use this code

Start your own PostgreSQL server in any directory and open a new database
with the command `psql crawler`.

Create the following tables in the crawler database:

```
    CREATE TABLE record (
        recordid SERIAL PRIMARY KEY,
        url text,
        visited boolean
    );

    CREATE TABLE blacklist (
        id SERIAL PRIMARY KEY,
        url text
    );

```

Download the repository and in the DB class change the constructor to match
the port number to match your Postgres server.

Run `mvn package`.  The jar file produced can then be run with default values.

To modify how the cralwer behaves, go to the Spider class file and modify
the variables, MAX_PAGES_TO_SEARCH, DELAY_TO_REQUEST, and
NUMBER_OF_LINKS_PER_DOMAIN.  In order, they control the amount of pages
to visit in a single crawl session, the number of milliseconds to delay
between making a request, and the number of links to save for a given
domain.

# Future Updates

* Implement parsing of robot.txt files
* Log file recording of crawler performance
* Implement a user interface