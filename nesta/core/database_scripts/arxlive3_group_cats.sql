CREATE TEMPORARY TABLE tmp_article_cats
SELECT 
    a.article_id AS article_id,
    a.city AS institute_city,
    a.country AS institute_country,
    a.is_multinational as is_multinational,
    a.name as institute_name,
    GROUP_CONCAT(DISTINCT c.category_id SEPARATOR "|") AS arxiv_category_ids,
    GROUP_CONCAT(DISTINCT ac.description SEPARATOR "|") AS arxiv_category_descs    
FROM
    tmp_article_ids_2 a
JOIN arxiv_article_categories c ON a.article_id = c.article_id
JOIN arxiv_categories ac ON ac.id = c.category_id    
GROUP BY a.article_id;
