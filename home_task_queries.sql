/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT
    c.name as category_name,
    COUNT(DISTINCT f.film_id) AS film_count
FROM film_category f
JOIN category c ON f.category_id = c.category_id
GROUP BY 1
ORDER BY film_count DESC;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
SELECT
    a.first_name,
    a.last_name,
    count(r.rental_id) AS rentals
FROM rental r
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film_actor fa ON fa.film_id = i.film_id
JOIN actor a ON a.actor_id = fa.actor_id
GROUP BY a.first_name, a.last_name
ORDER BY count(r.rental_id) DESC
LIMIT 10;

/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
/* Sum amount by category */
WITH films AS (
    SELECT c.name AS category_name,
        SUM(p.amount) AS total_payment
    FROM film_category fc
    JOIN category c ON fc.category_id = c.category_id
    JOIN film f ON f.film_id = fc.film_id
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    JOIN payment p ON p.rental_id = r.rental_id
    GROUP BY name
),
/* Select one or more categories (in case amounts are equal) */
cat_rank AS (
    SELECT category_name,
        rank() OVER(ORDER BY total_payment DESC) AS rnk
    FROM films
)
SELECT category_name
FROM cat_rank
WHERE rnk = 1;

/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
SELECT f.title
    FROM film f
    LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE i.film_id is null;

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
WITH top_child_actors AS (
    SELECT
        fa.actor_id,
        COUNT(distinct fa.film_id) AS film_count
    FROM film_actor fa
    JOIN film_category fc ON fc.film_id = fa.film_id
    JOIN category c ON fc.category_id = c.category_id
    WHERE c.name = 'Children'
    GROUP BY actor_id
    ORDER BY 2 DESC
    LIMIT 3)
SELECT
    a.first_name,
    a.last_name
FROM top_child_actors c
JOIN actor a ON c.actor_id = a.actor_id;
