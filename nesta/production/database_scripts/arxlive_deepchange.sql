SELECT
    a.id as article_id,
    a.created AS article_created,
    a.updated AS article_updated,
    a.citation_count,
    GROUP_CONCAT(DISTINCT c.id SEPARATOR "|") AS arxiv_category_ids,
    GROUP_CONCAT(DISTINCT c.description SEPARATOR "|") AS arxiv_category_descs,
    gi.city AS institute_city,
    gi.country AS institute_country

FROM arxiv_articles a

INNER JOIN arxiv_article_categories ac ON ac.article_id = a.id
INNER JOIN arxiv_categories c ON c.id = ac.category_id
INNER JOIN arxiv_article_corex_topics ct on ct.article_id = a.id
LEFT JOIN arxiv_article_institutes ai ON ai.article_id = a.id AND ai.matching_score >= 0.9
LEFT JOIN grid_institutes gi ON gi.id = ai.institute_id

WHERE (c.id = "stat.ML" OR c.id LIKE "cs.%%") AND ai.is_multinational = false

GROUP BY
    a.id,
    a.created,
    a.updated,
    a.citation_count,
    gi.city,
    gi.country
;
