-- @block q1
-- Get all statuses from 'flights' table
SELECT status FROM flights;

-- @block q2
-- Get unique values of statuses:
SELECT DISTINCT status FROM flights;

-- @block q3
-- Get all records (rows) from 'aircrafts' table
SELECT * FROM aircrafts

-- @block q4
-- Get models and ranges in descending order:
SELECT model, range FROM aircrafts
ORDER BY range DESC;

-- @block q5
-- More complex query with filter by flight status:
SELECT * FROM flights
WHERE status='Arrived';

-- @block q6
-- Even more complex query with filter by flight status and arrival airport:
SELECT * FROM flights
WHERE status='Arrived' AND arrival_airport = 'LED';

-- @block q7
-- Another query, '%' below means any symbols at this place:
SELECT * FROM aircrafts
WHERE model LIKE 'Boeing%';

-- @block q8
-- Yet another example of matching as above:
SELECT * FROM aircrafts
WHERE model LIKE '%R%';

-- @block q9
-- When work with NULL, use 'IS' instead of '='
SELECT * FROM flights
WHERE actual_departure IS NULL;

-- @block q10
-- Two alternative queries (do the same):
EXPLAIN
SELECT * FROM flights WHERE status='Arrived' OR status='Scheduled';

-- @block q10-faster
EXPLAIN
SELECT * FROM flights WHERE status IN ('Arrived', 'Scheduled');

-- @block q11
-- Get number of records in table:
SELECT count(*) FROM bookings;

-- @block q12
-- Get minimal amount of money spent for booking by client:
SELECT min(total_amount) FROM bookings;

-- @block q13
-- Get total amount of money spent for booking by client:
SELECT sum(total_amount) FROM bookings;

-- @block q14
-- Operations applied on the all records in table:
SELECT min(total_amount)
     , max(total_amount)
     , sum(total_amount)
     , round(avg(total_amount), 2)
FROM bookings;

-- @block q15
-- Get number of bookings had been made on specific date:
SELECT count(*) FROM bookings
WHERE date(book_date) = '2017-07-14';

-- @block q16
-- When we use group operations like 'count' it is necessary to specify the
-- grouping criteria of data using 'GROUP BY' statement:
SELECT date(book_date), count(*) FROM bookings
GROUP BY date(book_date
ORDER BY 2 DESC;

-- @block q17
-- '2' after 'ORDER BY' statement below means that
-- ordering will be made based on the second column:
SELECT date(book_date), count(*)
                      , min(total_amount)
                      , max(total_amount)
                      , sum(total_amount)
                      , round(avg(total_amount), 2)
                      , round(avg(total_amount), 2) + 20 FROM bookings
GROUP BY date(book_date)
ORDER BY 2 DESC;

-- @block q18
-- Get most loaded routes (flights):
SELECT departure_airport, arrival_airport, count(DISTINCT flight_no) FROM flights
GROUP BY departure_airport, arrival_airport
ORDER BY 3 DESC;

-- @block q19
-- Join two tables:
SELECT a.model, f.aircraft_code, count(*)
FROM flights f
JOIN aircraft a ON f.aircraft_code = a.aircraft_code
WHERE status = 'Scheduled'
GROUP BY a.model;

-- @block q20
-- Another join:
SELECT *
FROM ticket_flights tf
LEFT JOIN flights f ON tf.flight_id = f.flight_id
LEFT JOIN aircrafts a ON f.aircraft_code = a.aircraft_code;

-- @block q21
-- More complex joins:
SELECT date(f.scheduled_Departure)
     , a.model
     , tf.fare_conditions
     , count(DISTINCT ticket_no)
FROM ticket_flights tf
LEFT JOIN flights f ON tf.flight_id = f.flight_id
LEFT JOIN aircrafts a ON f.aircraft_code = a.aircraft_code
WHERE f.status = 'Scheduled'
GROUP BY date(f.scheduled_Departure), a.model, tf.fare_conditions
ORDER BY 1, 2, 3;

-- @block q22
-- Query results may be united with 'UNION':
SELECT date(f.scheduled_Departure)
     , a.model
     , tf.fare_conditions
     , count(DISTINCT ticket_no)
     , 'Planned' as st
FROM ticket_flights tf
LEFT JOIN flights f ON tf.flight_id = f.flight_id
LEFT JOIN aircrafts a ON f.aircraft_code = a.aircraft_code
WHERE f.status = 'Scheduled'
GROUP BY date(f.scheduled_Departure), a.model, tf.fare_conditions

UNION

SELECT date(f.scheduled_Departure)
     , a.model
     , tf.fare_conditions
     , count(DISTINCT ticket_no)
     , 'Already completed' as st
FROM ticket_flights tf
LEFT JOIN flights f ON tf.flight_id = f.flight_id
LEFT JOIN aircrafts a ON f.aircraft_code = a.aircraft_code
WHERE f.status = 'Arrived'
GROUP BY date(f.scheduled_Departure), a.model, tf.fare_conditions
ORDER BY 1, 2, 3;

-- @block q23
-- Difference between 'UNION' and 'UNION ALL':
SELECT 1 UNION SELECT 1
SELECT 1 UNION ALL SELECT 1

-- @block q24
-- In following case no difference:
SELECT 1 UNION SELECT 2
SELECT 1 UNION ALL SELECT 2

-- @block q25
-- Non-optimal joins (more efficient version at the bottom of file):
SELECT date(f.scheduled_Departure)
     , a.model
     , tf.fare_conditions
     , count(DISTINCT ticket_no)
     , f.status
FROM ticket_flights tf
LEFT JOIN flights f ON tf.flight_id = f.flight_id
LEFT JOIN aircrafts a ON f.aircraft_code = a.aircraft_code
WHERE f.status IN ('Arrived', 'Scheduled')
GROUP BY date(f.scheduled_Departure), a.model, tf.fare_conditions, f.status
ORDER BY 1, 2, 3;

-- @block q26
-- Here we know very less about booking with max total amount:
SELECT max(total_amount) FROM bookings;

-- @block q27
-- Here we know more, but in wrong way:
SELECT max(total_amount) FROM bookings
WHERE total_amount = (SELECT max(total_amount));

-- @block q28
-- Better way to achieve the same result:
SELECT * FROM bookings
ORDER BY total_amount DESC
LIMIT 1;

-- @block q29
-- Better version of non-optimal join query above:
SELECT date(t1.scheduled_Departure)
     , a.model
     , tf.fare_conditions
     , t1.status
     , count(DISTINCT ticket_no)
FROM ticket_flights tf
LEFT JOIN (
            SELECT * FROM flights WHERE status = 'Scheduled'
) t1 ON tf.flight_id = t1.flight_id
LEFT JOIN aircrafts a ON t1.aircraft_code = a.aircraft_code
GROUP BY date(t1.scheduled_Departure)
       , a.model
       , tf.fare_conditions
       , t1.status
ORDER BY 1, 2, 3, 4;

-- EXPLAIN - gives us a cost of the query