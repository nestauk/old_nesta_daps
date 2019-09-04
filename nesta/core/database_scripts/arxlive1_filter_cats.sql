CREATE TEMPORARY TABLE tmp_article_ids
SELECT DISTINCT(article_id)
FROM arxiv_article_categories
JOIN arxiv_categories ON id = category_id
WHERE id = "stat.ML" OR id LIKE "cs.%%";
