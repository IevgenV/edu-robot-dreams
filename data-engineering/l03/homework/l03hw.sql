-- @block q1
-- вывести количество фильмов в каждой категории,
-- отсортировать по убыванию.
SELECT c.name AS category_name
     , count(*) AS film_count
FROM category AS c
JOIN film_category AS fc ON c.category_id = fc.category_id
JOIN film AS f ON f.film_id = fc.film_id
GROUP BY c.category_id
ORDER BY film_count DESC;

-- @block q2
-- вывести 10 актеров, чьи фильмы большего всего арендовали,
-- отсортировать по убыванию.
SELECT a.first_name
     , a.last_name
     , count(*) AS rental_count
FROM actor AS a
JOIN film_actor AS fa ON a.actor_id = fa.actor_id
JOIN inventory AS i ON fa.film_id = i.film_id
JOIN rental AS r ON i.inventory_id = r.inventory_id
GROUP BY a.actor_id
ORDER BY rental_count DESC
LIMIT 10;


-- @block q3
-- вывести категорию фильмов, на которую потратили больше всего денег.
SELECT c.name AS category_name
     , sum(p.amount) AS total_amount
FROM category AS c
JOIN film_category AS fc ON c.category_id = fc.category_id
JOIN inventory AS i ON fc.film_id = i.film_id
JOIN rental AS r ON i.inventory_id = r.inventory_id
JOIN payment AS p ON r.rental_id = p.rental_id
GROUP BY c.category_id
ORDER BY total_amount DESC
LIMIT 1;

-- @block q4
-- вывести названия фильмов, которых нет в inventory.
-- Написать запрос без использования оператора IN.
SELECT f.film_id, f.title FROM film AS f
WHERE NOT EXISTS (SELECT 1 FROM inventory AS i WHERE i.film_id = f.film_id)

-- @block q5
-- вывести топ 3 актеров, которые больше всего появлялись в фильмах
-- в категории “Children”.
-- Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
WITH children_ordered_actors (first_name, last_name, film_count) AS 
     (SELECT a.first_name
           , a.last_name
           , count(*)
      FROM actor AS a
      JOIN film_actor AS fa ON a.actor_id = fa.actor_id
      JOIN film_category AS fc ON fa.film_id = fc.film_id
      JOIN category AS c ON fc.category_id = c.category_id
                        AND c.name = 'Children'
      GROUP BY a.actor_id
      ORDER BY 3 DESC)
SELECT first_name
     , last_name
     , film_count
FROM children_ordered_actors
WHERE film_count >= (SELECT film_count 
                     FROM children_ordered_actors 
                     LIMIT 1 OFFSET 3)

-- @block q6
-- вывести города с количеством активных и неактивных клиентов
-- (активный — customer.active = 1).
-- Отсортировать по количеству неактивных клиентов по убыванию.
WITH active_passive (city_id, city, active, customer_count) AS
     (SELECT city.city_id
           , city.city
           , customer.active
           , count(*)
      FROM city
      LEFT JOIN address ON city.city_id = address.city_id
      LEFT JOIN customer ON address.address_id = customer.address_id
      GROUP BY city.city_id, customer.active)
SELECT active_passive.city
     , COALESCE(ap_active.customer_count, 0) AS active_customer_count
     , COALESCE(ap_passive.customer_count, 0) AS passive_customer_count
FROM active_passive 
LEFT JOIN active_passive AS ap_active
     ON ap_active.city_id = active_passive.city_id
     AND ap_active.active = 1
LEFT JOIN active_passive AS ap_passive 
     ON active_passive.city_id = ap_passive.city_id
     AND ap_passive.active <> 1
ORDER BY passive_customer_count DESC, active_customer_count DESC;

-- @block q7
-- вывести категорию фильмов, у которой самое большое кол-во часов
-- суммарной аренды в городах (customer.address_id в этом city),
-- и которые начинаются на букву “a”. То же самое сделать для городов,
-- в которых есть символ “-”. Написать все в одном запросе.
WITH rent_cities_ordered_by_rent_and_grouped_by_film_categories
     (cat_name, city, rental_duration)
AS  (SELECT cat.name
          , cty.city
          , sum(rnt.return_date - rnt.rental_date) AS rental_duration
     FROM category AS cat
     JOIN film_category AS flm_cat ON cat.category_id = flm_cat.category_id
     JOIN inventory AS inv ON flm_cat.film_id = inv.film_id
     JOIN rental AS rnt ON inv.inventory_id = rnt.rental_id
     JOIN customer AS cst ON rnt.customer_id = cst.customer_id
     JOIN address AS adr ON cst.address_id = adr.address_id
     JOIN city AS cty ON adr.city_id = cty.city_id
     GROUP BY cat.category_id, cty.city_id)
(SELECT cat_name
      , justify_interval(sum(rental_duration)) AS rental_duration
 FROM rent_cities_ordered_by_rent_and_grouped_by_film_categories
 WHERE city LIKE 'a%'
 GROUP BY cat_name
 ORDER BY rental_duration DESC
 LIMIT 1)
UNION
(SELECT cat_name
      , justify_interval(sum(rental_duration)) AS rental_duration
 FROM rent_cities_ordered_by_rent_and_grouped_by_film_categories
 WHERE city LIKE '%-%'
 GROUP BY cat_name
 ORDER BY rental_duration DESC
 LIMIT 1);
