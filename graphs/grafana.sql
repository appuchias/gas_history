WITH converted AS (
   SELECT precio_gasoleo_a, precio_gasolina_95,  date || 'T00:00:00Z' AS datetime FROM Prices
)
SELECT datetime, avg(precio_gasoleo_a), avg(precio_gasolina_95) FROM converted GROUP BY datetime
