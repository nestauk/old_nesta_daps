SELECT
     a.id as article_id,
     a.created AS article_created,
     a.updated AS article_updated,
     a.title AS article_title,
     a.citation_count,
     GROUP_CONCAT(distinct c.id) AS arxiv_categories,
     ai.is_multinational,
     gi.city AS institute_city,
     gi.state AS institute_state,
     gi.country AS institute_country

 FROM arxiv_articles a
 JOIN arxiv_article_categories ac ON ac.article_id = a.id
 JOIN arxiv_categories c ON c.id = ac.category_id
 LEFT JOIN arxiv_article_institutes ai ON ai.article_id = a.id AND ai.matching_score >= 0.9
 LEFT JOIN grid_institutes gi ON gi.id = ai.institute_id

 WHERE
     c.id = "stat.ML" OR c.id like "cs.%%"

 GROUP BY
     a.id,
     a.created,
     a.updated,
     a.title,
     a.citation_count,
     ai.is_multinational,
     gi.city,
     gi.state,
     gi.country
