WITH rating_stats AS (
  SELECT
    AVG(rating) AS mean_rating,
    STDDEV_POP(rating) AS stddev_rating
  FROM ratings
),
ratings_with_zscore AS (
  SELECT
    rating_id,
    user_id,
    wine_id,
    rating,
    (rating - (SELECT mean_rating FROM rating_stats)) / (SELECT stddev_rating FROM rating_stats) AS z_score
  FROM ratings
)
SELECT *
FROM ratings_with_zscore
WHERE ABS(z_score) > 3 -- Ratings more than 3 std dev away are considered outliers
ORDER BY ABS(z_score) DESC;
